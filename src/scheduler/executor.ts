import { fork, spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { resolve as pathResolve, dirname } from 'node:path'
import { createInterface } from 'node:readline'
import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { getDag } from '../dag/registry.js'
import { acquire, release } from './pool.js'
import { acquirePool, releasePool } from '../pools/index.js'
import { appendLog } from '../logs/index.js'
import { enqueueTask } from '../queue/producer.js'
import { sensorOutcome } from './sensor.js'
import { recordTry } from './tries.js'

const __dirname = dirname(fileURLToPath(import.meta.url))

// In production (compiled JS), import.meta.url points to dist/scheduler/executor.js
// and worker.js exists alongside it. In development tsx runs .ts directly.
// Detect by checking whether the current file ends with .js (compiled) or .ts (dev).
const IS_COMPILED = import.meta.url.endsWith('.js')
const WORKER_SCRIPT = IS_COMPILED
  ? pathResolve(__dirname, 'worker.js')        // production: compiled JS
  : pathResolve(__dirname, 'worker.ts')        // development: tsx source

// In production use the system node binary; in dev use tsx.
const EXEC_PATH = IS_COMPILED
  ? process.execPath                           // production: same node process
  : pathResolve(__dirname, '../../node_modules/.bin/tsx')  // dev: tsx transpiler

// When REDIS_URL is set, use BullMQ (distributed). Otherwise use local fork.
const USE_BULLMQ = Boolean(process.env.REDIS_URL)

type WorkerDoneMsg = { type: 'done'; outcome: 'success' | 'reschedule' | 'fail'; error?: string }

export async function executeTask(db: Db, ti: TaskInstance): Promise<void> {
  const dag = getDag(ti.dag_id)
  if (!dag) { await markFailed(db, ti, `Dag '${ti.dag_id}' not found in registry`); return }

  const taskDef = dag.tasks[ti.task_id]
  if (!taskDef) { await markFailed(db, ti, `Task '${ti.task_id}' not found in dag '${ti.dag_id}'`); return }

  // Shell task — spawn interpreter directly, no worker needed
  if (taskDef.shell) {
    return executeShellTask(db, ti, taskDef.shell)
  }

  // Python task — spawn python3 (or configured interpreter) directly
  if (taskDef.python) {
    return executePythonTask(db, ti, taskDef.python)
  }

  // Java task — spawn java with -jar or -cp + mainClass
  if (taskDef.java) {
    return executeJavaTask(db, ti, taskDef.java)
  }

  // Container task — run in an isolated Docker container via Docker socket
  if (taskDef.container) {
    return executeContainerTask(db, ti, taskDef.container)
  }

  // HITL approval-only tasks (no run body) — succeed immediately after approval
  if (ti.is_hitl && !taskDef.run && !taskDef.poke) {
    void recordTry(db, ti, 'success', new Date())
    await markSuccess(db, ti)
    console.log(`[executor] ✓ ${ti.dag_id}.${ti.task_id} (HITL approved, no-op)`)
    return
  }

  // Sensors must run locally — BullMQ workers don't have reschedule semantics yet
  if (USE_BULLMQ && ti.is_sensor) {
    await markFailed(db, ti, 'Sensor tasks require local execution mode (REDIS_URL must not be set)')
    return
  }

  if (USE_BULLMQ) {
    // Distributed: enqueue to Redis — BullMQ worker picks it up
    await enqueueTask(ti, taskDef.run!.toString())
    console.log(`[executor] enqueued ${ti.dag_id}.${ti.task_id} → BullMQ`)
    return
  }

  // Local: fork directly — acquire global slot, then per-pool slot (if task declares a pool)
  await acquire()
  if (ti.pool) await acquirePool(db, ti.pool)

  const label = ti.is_sensor ? 'poking' : 'running'
  console.log(`[executor] ${label} ${ti.dag_id}.${ti.task_id} (run: ${ti.dag_run_id})`)

  return new Promise((done) => {
    const child = fork(WORKER_SCRIPT, [], {
      execPath: EXEC_PATH,
      env: { ...process.env },
      silent: true,
    })

    // ── Timeout (task-level, not sensor deadline) ─────────────────────
    let timedOut = false
    let killTimer: ReturnType<typeof setTimeout> | null = null

    if (ti.timeout_ms > 0) {
      killTimer = setTimeout(() => {
        timedOut = true
        child.kill('SIGTERM')
        const msg = `Task timed out after ${ti.timeout_ms}ms`
        console.error(`[executor] ⏱ ${ti.dag_id}.${ti.task_id}: ${msg}`)
        release()
        if (ti.pool) releasePool(ti.pool)
        void recordTry(db, ti, 'failed', new Date(), msg)
        void markFailed(db, ti, msg).then(() => done())
      }, ti.timeout_ms)
    }

    const clearKillTimer = () => {
      if (killTimer !== null) { clearTimeout(killTimer); killTimer = null }
    }

    // ── Stdio logging ─────────────────────────────────────────────────
    const rl_out = createInterface({ input: child.stdout! })
    rl_out.on('line', (line) => {
      process.stdout.write(`${line}\n`)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stdout', line)
    })

    const rl_err = createInterface({ input: child.stderr! })
    rl_err.on('line', (line) => {
      process.stderr.write(`${line}\n`)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stderr', line)
    })

    // Base ctx — includes mapIndex/mapValue for mapped task instances
    const workerCtx = {
      dagId: ti.dag_id,
      runId: ti.dag_run_id,
      taskId: ti.task_id,
      mapIndex: ti.map_index ?? null,
      mapValue: ti.map_value ?? null,
    }

    // Send appropriate message type to worker
    if (ti.is_sensor) {
      child.send({ type: 'poke', fn: taskDef.poke!.toString(), ctx: workerCtx })
    } else {
      child.send({ type: 'run', fn: taskDef.run!.toString(), ctx: workerCtx })
    }

    child.on('message', async (msg: WorkerDoneMsg) => {
      if (msg.type !== 'done') return
      if (timedOut) return
      clearKillTimer()
      release()
      if (ti.pool) releasePool(ti.pool)

      if (msg.outcome === 'reschedule') {
        // Sensor: poke returned false — compute next outcome based on deadline
        const now = new Date()
        // first_poked_at is stamped on first reschedule; never null after first poke
        const firstPokedAt = ti.first_poked_at ?? now
        const result = sensorOutcome(false, firstPokedAt, now, ti.sensor_timeout_ms)

        if (result === 'timeout') {
          await markFailed(db, ti, `Sensor timed out after ${ti.sensor_timeout_ms}ms`)
          console.error(`[executor] ⏱ sensor ${ti.dag_id}.${ti.task_id} timed out`)
        } else {
          // reschedule: requeue with next_poke_at; do NOT touch try_number
          await schedulePoke(db, ti, firstPokedAt, now)
          console.log(`[executor] ↻ sensor ${ti.dag_id}.${ti.task_id} requeued (poke #${ti.poke_count + 1})`)
        }
      } else if (msg.outcome === 'success') {
        const endedAt = new Date()
        void recordTry(db, ti, 'success', endedAt)
        await markSuccess(db, ti)
        console.log(`[executor] ✓ ${ti.dag_id}.${ti.task_id}`)
      } else {
        // outcome === 'fail'
        const error = msg.error ?? 'unknown error'
        const endedAt = new Date()
        if (!ti.is_sensor && ti.try_number < ti.max_retries) {
          // Record this try as failed before requeueing for the next try
          void recordTry(db, ti, 'failed', endedAt, error)
          await scheduleRetry(db, ti, error)
          console.warn(`[executor] ↩ ${ti.dag_id}.${ti.task_id} retrying (${ti.try_number + 1}/${ti.max_retries + 1})`)
        } else {
          void recordTry(db, ti, 'failed', endedAt, error)
          await markFailed(db, ti, error)
          console.error(`[executor] ✗ ${ti.dag_id}.${ti.task_id}: ${error}`)
        }
      }
      done()
    })

    child.on('error', async (err) => {
      if (timedOut) return
      clearKillTimer()
      release()
      if (ti.pool) releasePool(ti.pool)
      void recordTry(db, ti, 'failed', new Date(), err.message)
      await markFailed(db, ti, err.message)
      done()
    })

    child.on('exit', (code) => {
      if (timedOut) return
      if (code !== 0 && code !== null) {
        console.error(`[executor] worker exited with code ${code} for ${ti.task_id}`)
      }
    })
  })
}

// ── Shared subprocess runner ───────────────────────────────────────────────────

interface SpawnOpts {
  /** Display label for logs, e.g. 'shell(bash)' or 'python(3.13)' */
  label: string
  /** Binary to spawn */
  binary: string
  /** Arguments passed to the binary */
  args: string[]
  /** Working directory */
  cwd?: string
  /** Extra environment variables (merged with process.env + context vars) */
  env?: Record<string, string>
  /** Timeout in ms; 0 = no timeout */
  timeoutMs: number
  /** Human-readable name for timeout error message, e.g. 'Shell' or 'Python' */
  kind: string
  /**
   * Optional callback invoked when the timeout fires, before SIGTERM.
   * Used by container tasks to `docker kill` the running container by name,
   * since killing the `docker run` client process alone doesn't stop the container.
   */
  onTimeout?: () => void
}

/**
 * Shared subprocess executor used by shell and python tasks.
 * Spawns the binary, pipes stdout/stderr to task logs, handles timeout/retries.
 */
async function spawnTask(db: Db, ti: TaskInstance, opts: SpawnOpts): Promise<void> {
  await acquire()
  if (ti.pool) await acquirePool(db, ti.pool)

  console.log(`[executor] ${opts.label} ${ti.dag_id}.${ti.task_id}`)

  return new Promise((done) => {
    const child = spawn(opts.binary, opts.args, {
      cwd: opts.cwd ?? process.cwd(),
      env: {
        ...process.env,
        ...(opts.env ?? {}),
        DAG_ID:  ti.dag_id,
        RUN_ID:  ti.dag_run_id,
        TASK_ID: ti.task_id,
      },
    })

    let timedOut = false
    let errored = false   // set by error handler so close handler doesn't double-process ENOENT
    let killTimer: ReturnType<typeof setTimeout> | null = null

    if (opts.timeoutMs > 0) {
      killTimer = setTimeout(() => {
        timedOut = true
        opts.onTimeout?.()   // e.g. docker kill <container-name> for container tasks
        child.kill('SIGTERM')
        const msg = `${opts.kind} task timed out after ${opts.timeoutMs}ms`
        console.error(`[executor] ⏱ ${ti.dag_id}.${ti.task_id}: ${msg}`)
        release()
        if (ti.pool) releasePool(ti.pool)
        void recordTry(db, ti, 'failed', new Date(), msg)
        void markFailed(db, ti, msg).then(() => done())
      }, opts.timeoutMs)
    }

    const clearKillTimer = () => {
      if (killTimer !== null) { clearTimeout(killTimer); killTimer = null }
    }

    const stderrLines: string[] = []

    createInterface({ input: child.stdout }).on('line', (line) => {
      process.stdout.write(`${line}\n`)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stdout', line)
    })

    createInterface({ input: child.stderr }).on('line', (line) => {
      process.stderr.write(`${line}\n`)
      stderrLines.push(line)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stderr', line)
    })

    child.on('error', async (err) => {
      if (timedOut) return
      errored = true
      clearKillTimer()
      release()
      if (ti.pool) releasePool(ti.pool)
      const msg = (err as NodeJS.ErrnoException).code === 'ENOENT'
        ? `${opts.kind} binary '${opts.binary}' not found — is it installed in the runtime image?`
        : err.message
      void recordTry(db, ti, 'failed', new Date(), msg)
      await markFailed(db, ti, msg)
      done()
    })

    child.on('close', async (code) => {
      if (timedOut || errored) return   // already handled by error or timeout handler
      clearKillTimer()
      release()
      if (ti.pool) releasePool(ti.pool)
      const endedAt = new Date()

      if (code === 0) {
        void recordTry(db, ti, 'success', endedAt)
        await markSuccess(db, ti)
        console.log(`[executor] ✓ ${ti.dag_id}.${ti.task_id} (${opts.label}, exit 0)`)
      } else {
        const stderr = stderrLines.slice(-5).join('\n')
        const error = `${opts.kind} exited with code ${code}${stderr ? `: ${stderr}` : ''}`
        if (ti.try_number < ti.max_retries) {
          void recordTry(db, ti, 'failed', endedAt, error)
          await scheduleRetry(db, ti, error)
          console.warn(`[executor] ↩ ${ti.dag_id}.${ti.task_id} retrying (${ti.try_number + 1}/${ti.max_retries + 1})`)
        } else {
          void recordTry(db, ti, 'failed', endedAt, error)
          await markFailed(db, ti, error)
          console.error(`[executor] ✗ ${ti.dag_id}.${ti.task_id}: ${error}`)
        }
      }
      done()
    })
  })
}

// ── Container task ────────────────────────────────────────────────────────────

async function executeContainerTask(
  db: Db,
  ti: TaskInstance,
  container: NonNullable<import('../dag/types.js').TaskDefinition['container']>,
): Promise<void> {
  // Unique container name — allows `docker kill <name>` on timeout
  const containerName = `airflow-${ti.dag_run_id}-${ti.task_id}`.replace(/[^a-zA-Z0-9_.-]/g, '-').slice(0, 64)

  const args: string[] = [
    'run', '--rm',
    '--name', containerName,
    // Inject context env vars
    '-e', `DAG_ID=${ti.dag_id}`,
    '-e', `RUN_ID=${ti.dag_run_id}`,
    '-e', `TASK_ID=${ti.task_id}`,
  ]

  // Extra env vars
  for (const [k, v] of Object.entries(container.env ?? {})) {
    args.push('-e', `${k}=${v}`)
  }

  // Volume mounts
  for (const vol of container.volumes ?? []) {
    args.push('-v', vol)
  }

  // Port mappings (host:container)
  for (const port of container.ports ?? []) {
    args.push('-p', port)
  }

  // Working directory
  if (container.workdir) {
    args.push('-w', container.workdir)
  }

  // Network
  if (container.network) {
    args.push('--network', container.network)
  }

  // Resource limits
  if (container.memory)      args.push('--memory',      container.memory)
  if (container.memorySwap)  args.push('--memory-swap', container.memorySwap)
  if (container.cpus)        args.push('--cpus',        container.cpus)
  if (container.storageSize) args.push('--storage-opt', `size=${container.storageSize}`)

  // Image
  args.push(container.image)

  // Command override
  if (container.command?.length) {
    args.push(...container.command)
  }

  const timeoutMs = container.timeout ?? (ti.timeout_ms > 0 ? ti.timeout_ms : 0)

  // For container tasks, timeout kills the container by name (not just the `docker run` client process)
  const killContainer = () => {
    spawn('docker', ['kill', containerName], { stdio: 'ignore' })
  }

  return spawnTask(db, ti, {
    label:     `container(${container.image})`,
    binary:    'docker',
    args,
    timeoutMs,
    kind:      'Container',
    onTimeout: killContainer,
  })
}

// ── Java task ──────────────────────────────────────────────────────────────────

async function executeJavaTask(
  db: Db,
  ti: TaskInstance,
  java: NonNullable<import('../dag/types.js').TaskDefinition['java']>,
): Promise<void> {
  const binary = java.binary ?? 'java'
  const jvmArgs = java.jvmArgs ?? []
  const taskArgs = java.args ?? []

  let args: string[]
  if (java.jar) {
    // java [jvmArgs] -jar my.jar [args]
    args = [...jvmArgs, '-jar', java.jar, ...taskArgs]
  } else if (java.mainClass) {
    // java [jvmArgs] [-cp classpath] MainClass [args]
    const cp = java.classpath?.join(':') ?? ''
    args = [...jvmArgs, ...(cp ? ['-cp', cp] : []), java.mainClass, ...taskArgs]
  } else {
    await markFailed(db, ti, "Java task requires either 'jar' or 'mainClass'")
    void recordTry(db, ti, 'failed', new Date(), "Java task requires either 'jar' or 'mainClass'")
    return
  }

  return spawnTask(db, ti, {
    label:     `java(${binary})`,
    binary,
    args,
    cwd:       java.cwd,
    env:       java.env,
    timeoutMs: java.timeout ?? (ti.timeout_ms > 0 ? ti.timeout_ms : 0),
    kind:      'Java',
  })
}

// ── Shell task ─────────────────────────────────────────────────────────────────

async function executeShellTask(
  db: Db,
  ti: TaskInstance,
  shell: NonNullable<import('../dag/types.js').TaskDefinition['shell']>,
): Promise<void> {
  const interpreter = shell.interpreter ?? 'bash'
  return spawnTask(db, ti, {
    label:     `shell(${interpreter})`,
    binary:    interpreter,
    args:      ['-c', shell.command],
    cwd:       shell.cwd,
    env:       shell.env,
    timeoutMs: shell.timeout ?? (ti.timeout_ms > 0 ? ti.timeout_ms : 0),
    kind:      'Shell',
  })
}

// ── Python task ────────────────────────────────────────────────────────────────

async function executePythonTask(
  db: Db,
  ti: TaskInstance,
  python: NonNullable<import('../dag/types.js').TaskDefinition['python']>,
): Promise<void> {
  const binary = python.interpreter ?? 'python3'
  // Inline code runs via `python3 -c <code>`; script file runs via `python3 <path>`
  const args = python.script
    ? [python.script, ...(python.args ?? [])]
    : ['-c', python.code!]
  return spawnTask(db, ti, {
    label:     `python(${binary})`,
    binary,
    args,
    cwd:       python.cwd,
    env:       python.env,
    timeoutMs: python.timeout ?? (ti.timeout_ms > 0 ? ti.timeout_ms : 0),
    kind:      'Python',
  })
}

/**
 * Requeue a sensor task after a false poke.
 * Does NOT touch try_number — reschedule ≠ retry.
 */
/** Unique filter for a single task instance — includes map_index for mapped tasks. */
function tiFilter(ti: TaskInstance) {
  return { dag_run_id: ti.dag_run_id, task_id: ti.task_id, map_index: ti.map_index ?? null }
}

async function schedulePoke(db: Db, ti: TaskInstance, firstPokedAt: Date, now: Date): Promise<void> {
  const nextPokeAt = new Date(now.getTime() + ti.poke_interval_ms)
  await db.collection('task_instances').updateOne(
    tiFilter(ti),
    {
      $set: {
        state: 'queued',
        started_at: null,
        next_poke_at: nextPokeAt,
        first_poked_at: firstPokedAt,
      },
      $inc: { poke_count: 1 },
    },
  )
}

async function scheduleRetry(db: Db, ti: TaskInstance, error: string): Promise<void> {
  const requeue = async () => {
    await db.collection('task_instances').updateOne(
      tiFilter(ti),
      { $set: { state: 'queued', started_at: null, ended_at: null, error }, $inc: { try_number: 1 } }
    )
  }
  ti.retry_delay > 0 ? setTimeout(() => void requeue(), ti.retry_delay) : await requeue()
}

async function markSuccess(db: Db, ti: TaskInstance): Promise<void> {
  await db.collection('task_instances').updateOne(
    tiFilter(ti),
    { $set: { state: 'success', ended_at: new Date() } }
  )
}

async function markFailed(db: Db, ti: TaskInstance, error: string): Promise<void> {
  await db.collection('task_instances').updateOne(
    tiFilter(ti),
    { $set: { state: 'failed', ended_at: new Date(), error } }
  )
}
