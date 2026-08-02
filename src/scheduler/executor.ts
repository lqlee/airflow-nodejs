import { fork, spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { resolve as pathResolve, dirname } from 'node:path'
import { createInterface } from 'node:readline'
import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { getDag } from '../dag/registry.js'
import { acquire, release } from './pool.js'
import { acquirePool, releasePool } from '../pools/index.js'
import { appendLog, parseLevelFromLine } from '../logs/index.js'
import { enqueueTask } from '../queue/producer.js'
import { sensorOutcome } from './sensor.js'
import { recordTry } from './tries.js'
import { getRunMeta, getRunConf } from './run-conf.js'
import { USE_KUBERNETES_EXECUTOR, executeTaskOnKubernetes } from './kubernetes-executor.js'
import { xcomPush, xcomPull } from '../xcom/index.js'
import { renderTemplate, renderArgs, renderEnv, type TemplateContext } from './template.js'

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

type WorkerDoneMsg = {
  type: 'done'
  outcome: 'success' | 'reschedule' | 'fail' | 'deferred'
  error?: string
  triggerFn?: string      // set when outcome === 'deferred'
  deferInterval?: number
  deferTimeout?: number   // 0 = use task-level timeout or no timeout
}

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

  // Kubernetes task — run as an ephemeral Pod on a K8s cluster via kubectl
  if (taskDef.kubernetes) {
    return executeKubernetesTask(db, ti, taskDef.kubernetes)
  }

  // Branch task — runs a function that returns which downstream task_ids to activate.
  // The result is stored as XCom key '_branch_decision' then the scheduler applies skips.
  if (taskDef.branch) {
    return executeBranchTask(db, ti, taskDef.branch)
  }

  // HITL approval-only tasks (no run body) — succeed immediately after approval
  if (ti.is_hitl && !taskDef.run && !taskDef.poke) {
    void recordTry(db, ti, 'success', new Date())
    await markSuccess(db, ti)
    console.log(`[executor] ✓ ${ti.dag_id}.${ti.task_id} (HITL approved, no-op)`)
    return
  }

  // Kubernetes executor — dispatch task as a pod (run: tasks only; sensors/deferred not supported)
  if (USE_KUBERNETES_EXECUTOR) {
    return executeTaskOnKubernetes(db, ti)
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
    // parseLevelFromLine detects [INFO]/[WARN]/[ERROR]/[DEBUG] prefixes from ctx.log.*
    const rl_out = createInterface({ input: child.stdout! })
    rl_out.on('line', (line) => {
      process.stdout.write(`${line}\n`)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stdout', line, parseLevelFromLine(line) ?? 'info')
    })

    const rl_err = createInterface({ input: child.stderr! })
    rl_err.on('line', (line) => {
      process.stderr.write(`${line}\n`)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stderr', line, parseLevelFromLine(line) ?? 'error')
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
      } else if (msg.outcome === 'deferred') {
        // Task called ctx.defer() — free slot, store trigger fn, mark deferred
        release()
        if (ti.pool) releasePool(ti.pool)
        const interval = msg.deferInterval ?? 10_000
        // defer timeout: use defer()-supplied value > task-level > no timeout
        const deferTimeoutMs = (msg.deferTimeout && msg.deferTimeout > 0)
          ? msg.deferTimeout
          : (ti.timeout_ms > 0 ? ti.timeout_ms : 0)
        const nextCheckAt = new Date(Date.now() + interval)
        await db.collection('task_instances').updateOne(
          tiFilter(ti),
          {
            $set: {
              state: 'deferred',
              deferred_trigger_fn: msg.triggerFn ?? null,
              deferred_at: new Date(),
              defer_timeout_ms: deferTimeoutMs,
              next_poke_at: nextCheckAt,
              poke_interval_ms: interval,
            },
          },
        )
        console.log(`[executor] ⏸ ${ti.dag_id}.${ti.task_id} deferred — next check: ${nextCheckAt.toISOString()}`)
        done()
        return  // skip the release() below (already released)
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
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stdout', line, parseLevelFromLine(line) ?? 'info')
    })

    createInterface({ input: child.stderr }).on('line', (line) => {
      process.stderr.write(`${line}\n`)
      stderrLines.push(line)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stderr', line, parseLevelFromLine(line) ?? 'error')
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
  const meta = await getRunMeta(db, ti.dag_run_id)
  const tctx: TemplateContext = { dag_id: ti.dag_id, run_id: ti.dag_run_id, task_id: ti.task_id, ...meta }

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

  // Extra env vars (rendered)
  const renderedEnv = container.env ? renderEnv(container.env, tctx) : {}
  for (const [k, v] of Object.entries(renderedEnv)) {
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

  // Command override (rendered)
  if (container.command?.length) {
    args.push(...renderArgs(container.command, tctx))
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

// ── Kubernetes task ───────────────────────────────────────────────────────────

/**
 * Build a valid RFC-1123 pod name from dag/run/task IDs.
 * Rules: lowercase alphanumeric + '-', max 63 chars, start and end alphanumeric.
 *
 * docker-style container names allow uppercase, '_', '.' and slice to 64 —
 * none of those are valid for K8s pod names.
 */
export function buildPodName(
  prefix: string | undefined,
  dagId: string,
  runId: string,
  taskId: string,
): string {
  const base = prefix ?? `airflow-${dagId}-${taskId}`
  // Lowercase, replace anything not alphanumeric or hyphen with '-'
  const safe = base.toLowerCase().replace(/[^a-z0-9-]/g, '-')
  // Use last 8 chars of runId as a unique suffix
  const suffix = runId.replace(/[^a-z0-9]/g, '').slice(-8) || 'run'
  const name = `${safe}-${suffix}`
  // Strip leading/trailing hyphens and enforce ≤63 chars
  return name.replace(/^-+|-+$/g, '').slice(0, 63).replace(/-+$/, '')
}

async function executeKubernetesTask(
  db: Db,
  ti: TaskInstance,
  k8s: NonNullable<import('../dag/types.js').TaskDefinition['kubernetes']>,
): Promise<void> {
  const namespace = k8s.namespace ?? process.env.KUBECTL_NAMESPACE ?? 'default'
  const podName = buildPodName(k8s.podName, ti.dag_id, ti.dag_run_id, ti.task_id)

  const args: string[] = [
    'run', podName,
    '--image', k8s.image,
    '--namespace', namespace,
    '--restart', 'Never',
    '--rm',
    '--attach',
    '--quiet',
  ]

  // kubeconfig / context
  if (k8s.kubeconfig) args.push('--kubeconfig', k8s.kubeconfig)
  if (k8s.context)    args.push('--context',    k8s.context)

  // Service account
  if (k8s.serviceAccount) args.push('--serviceaccount', k8s.serviceAccount)

  // Resource limits — kubectl uses --limits=cpu=…,memory=… and --requests=…
  // We set request = limit (guaranteed QoS) which is the right default for batch tasks.
  const limits: string[] = []
  const requests: string[] = []
  if (k8s.cpu)    { limits.push(`cpu=${k8s.cpu}`);       requests.push(`cpu=${k8s.cpu}`) }
  if (k8s.memory) { limits.push(`memory=${k8s.memory}`); requests.push(`memory=${k8s.memory}`) }
  if (limits.length)   args.push(`--limits=${limits.join(',')}`)
  if (requests.length) args.push(`--requests=${requests.join(',')}`)

  // Env vars — DAG_ID/RUN_ID/TASK_ID + user-supplied
  const envVars: Record<string, string> = {
    DAG_ID:  ti.dag_id,
    RUN_ID:  ti.dag_run_id,
    TASK_ID: ti.task_id,
    ...(k8s.env ?? {}),
  }
  for (const [k, v] of Object.entries(envVars)) {
    args.push('--env', `${k}=${v}`)
  }

  // Command override — must come after `--` separator
  if (k8s.command?.length) {
    args.push('--', ...k8s.command)
  }

  const timeoutMs = k8s.timeout ?? (ti.timeout_ms > 0 ? ti.timeout_ms : 0)

  // On timeout, force-delete the pod so it doesn't linger in the cluster
  const killPod = () => {
    spawn('kubectl', ['delete', 'pod', podName, '--namespace', namespace, '--force', '--grace-period=0'], { stdio: 'ignore' })
  }

  return spawnTask(db, ti, {
    label:     `kubernetes(${k8s.image})`,
    binary:    'kubectl',
    args,
    timeoutMs,
    kind:      'Kubernetes',
    onTimeout: killPod,
  })
}

// ── Java task ──────────────────────────────────────────────────────────────────

async function executeJavaTask(
  db: Db,
  ti: TaskInstance,
  java: NonNullable<import('../dag/types.js').TaskDefinition['java']>,
): Promise<void> {
  const meta = await getRunMeta(db, ti.dag_run_id)
  const tctx: TemplateContext = { dag_id: ti.dag_id, run_id: ti.dag_run_id, task_id: ti.task_id, ...meta }

  const binary = java.binary ?? 'java'
  const jvmArgs = renderArgs(java.jvmArgs ?? [], tctx)
  const taskArgs = renderArgs(java.args ?? [], tctx)

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
    env:       java.env ? renderEnv(java.env, tctx) : java.env,
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
  const meta = await getRunMeta(db, ti.dag_run_id)
  const tctx: TemplateContext = { dag_id: ti.dag_id, run_id: ti.dag_run_id, task_id: ti.task_id, ...meta }

  const interpreter = shell.interpreter ?? 'bash'
  return spawnTask(db, ti, {
    label:     `shell(${interpreter})`,
    binary:    interpreter,
    args:      ['-c', renderTemplate(shell.command, tctx)],
    cwd:       shell.cwd,
    env:       shell.env ? renderEnv(shell.env, tctx) : shell.env,
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
  const meta = await getRunMeta(db, ti.dag_run_id)
  const tctx: TemplateContext = { dag_id: ti.dag_id, run_id: ti.dag_run_id, task_id: ti.task_id, ...meta }

  const binary = python.interpreter ?? 'python3'
  // Inline code runs via `python3 -c <code>`; script file runs via `python3 <path>`
  const args = python.script
    ? [python.script, ...renderArgs(python.args ?? [], tctx)]
    : ['-c', renderTemplate(python.code!, tctx)]
  return spawnTask(db, ti, {
    label:     `python(${binary})`,
    binary,
    args,
    cwd:       python.cwd,
    env:       python.env ? renderEnv(python.env, tctx) : python.env,
    timeoutMs: python.timeout ?? (ti.timeout_ms > 0 ? ti.timeout_ms : 0),
    kind:      'Python',
  })
}

// ── Branch task ────────────────────────────────────────────────────────────────

/**
 * Run a branch task.
 * Wraps the user-supplied branch fn in a run fn that:
 *   1. Calls branch(ctx)
 *   2. Stores the result as XCom key '_branch_decision'
 *   3. Returns normally (success) so the scheduler can read the decision
 *
 * The wrapper is serialized and sent to the worker via the existing fork path —
 * no protocol change needed.
 */
async function executeBranchTask(
  db: Db,
  ti: TaskInstance,
  branchFn: NonNullable<import('../dag/types.js').TaskDefinition['branch']>,
): Promise<void> {
  // Wrap branch fn: call it, push decision to XCom, return the decision
  const branchFnStr = branchFn.toString()
  const wrapperFn = new Function(`return async function(ctx) {
    const branchFn = (${branchFnStr});
    const decision = await branchFn(ctx);
    const selected = decision == null ? [] : (Array.isArray(decision) ? decision : [decision]);
    await ctx.xcom.push('_branch_decision', selected);
    return selected;
  }`)()

  // Re-use the fork machinery from executeTask by temporarily shimming as run: task
  const dag = getDag(ti.dag_id)
  if (!dag) { await markFailed(db, ti, `Dag '${ti.dag_id}' not found`); return }

  return executeRunFn(db, ti, wrapperFn)
}

/**
 * Execute a JS function (run: or branch wrapper) in a forked worker.
 * Extracted to share between executeTask (run:) and executeBranchTask.
 */
async function executeRunFn(
  db: Db,
  ti: TaskInstance,
  fn: (ctx: unknown) => Promise<unknown>,
): Promise<void> {
  await acquire()
  if (ti.pool) await acquirePool(db, ti.pool)

  const label = ti.is_sensor ? 'poking' : ti.is_branch ? 'branching' : 'running'
  console.log(`[executor] ${label} ${ti.dag_id}.${ti.task_id} (run: ${ti.dag_run_id})`)

  return new Promise((done) => {
    const child = fork(WORKER_SCRIPT, [], {
      execPath: EXEC_PATH,
      env: { ...process.env },
      silent: true,
    })

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

    const rl_out = createInterface({ input: child.stdout! })
    rl_out.on('line', (line) => {
      process.stdout.write(`${line}\n`)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stdout', line, parseLevelFromLine(line) ?? 'info')
    })

    const rl_err = createInterface({ input: child.stderr! })
    rl_err.on('line', (line) => {
      process.stderr.write(`${line}\n`)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stderr', line, parseLevelFromLine(line) ?? 'error')
    })

    const workerCtx = {
      dagId: ti.dag_id,
      runId: ti.dag_run_id,
      taskId: ti.task_id,
      mapIndex: ti.map_index ?? null,
      mapValue: ti.map_value ?? null,
    }

    child.send({ type: 'run', fn: fn.toString(), ctx: workerCtx })

    child.on('message', async (msg: WorkerDoneMsg) => {
      if (msg.type !== 'done') return
      if (timedOut) return
      clearKillTimer()
      release()
      if (ti.pool) releasePool(ti.pool)

      if (msg.outcome === 'success') {
        const endedAt = new Date()
        void recordTry(db, ti, 'success', endedAt)
        await markSuccess(db, ti)
        console.log(`[executor] ✓ ${ti.dag_id}.${ti.task_id}`)
      } else {
        const error = msg.error ?? 'unknown error'
        const endedAt = new Date()
        if (!ti.is_branch && ti.try_number < ti.max_retries) {
          void recordTry(db, ti, 'failed', endedAt, error)
          await scheduleRetry(db, ti, error)
          console.warn(`[executor] ↩ ${ti.dag_id}.${ti.task_id} retrying`)
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

export async function scheduleRetry(db: Db, ti: TaskInstance, error: string): Promise<void> {
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

// ── Deferred task poller ───────────────────────────────────────────────────────

/**
 * Poll all deferred tasks whose next_poke_at has arrived.
 * Runs the stored trigger function IN the scheduler process (no fork) —
 * so trigger functions must be self-contained (no module-scope closures).
 *
 * On trigger() === true  → mark task success
 * On trigger() === false → reschedule to next check
 * On deadline exceeded   → mark task failed
 * On trigger() throws    → mark task failed
 */
export async function pollDeferredTasks(db: Db): Promise<void> {
  const now = new Date()

  const deferredTasks = await db.collection<TaskInstance>('task_instances').find({
    state: 'deferred',
    next_poke_at: { $lte: now },
  }).toArray()

  if (deferredTasks.length === 0) return

  for (const ti of deferredTasks) {
    if (!ti.deferred_trigger_fn) {
      // No trigger fn stored — mark failed
      await markFailed(db, ti, 'Deferred task has no trigger function')
      continue
    }

    // Check deadline
    if (ti.defer_timeout_ms > 0 && ti.deferred_at) {
      const elapsed = now.getTime() - new Date(ti.deferred_at).getTime()
      if (elapsed > ti.defer_timeout_ms) {
        await markFailed(db, ti, `Deferred task timed out after ${ti.defer_timeout_ms}ms`)
        console.error(`[executor] ⏱ deferred ${ti.dag_id}.${ti.task_id} timed out`)
        continue
      }
    }

    try {
      // Re-hydrate the trigger function
      // eslint-disable-next-line no-new-func
      const triggerFn = new Function(`return (${ti.deferred_trigger_fn})`)() as
        (ctx: unknown) => Promise<boolean>

      // Build minimal trigger context (in-process, no DB_NAME env needed — uses same DB)
      const conf = await getRunConf(db, ti.dag_run_id)
      const tctx = {
        dagId:    ti.dag_id,
        runId:    ti.dag_run_id,
        taskId:   ti.task_id,
        mapIndex: ti.map_index,
        mapValue: ti.map_value,
        conf,
        xcom: {
          push: (key: string, value: unknown) =>
            xcomPush(db, ti.dag_run_id, ti.dag_id, ti.task_id, ti.map_index, key, value),
          pull: (fromTaskId: string, key: string) =>
            xcomPull(db, ti.dag_run_id, fromTaskId, key),
        },
      }

      const ready = await triggerFn(tctx)

      if (ready) {
        void recordTry(db, ti, 'success', now)
        await markSuccess(db, ti)
        console.log(`[executor] ✓ ${ti.dag_id}.${ti.task_id} (deferred → resumed)`)
      } else {
        // Reschedule
        const interval = ti.poke_interval_ms || 10_000
        const nextCheck = new Date(now.getTime() + interval)
        await db.collection('task_instances').updateOne(
          tiFilter(ti),
          { $set: { next_poke_at: nextCheck }, $inc: { poke_count: 1 } },
        )
        console.log(`[executor] ↻ deferred ${ti.dag_id}.${ti.task_id} — next check: ${nextCheck.toISOString()}`)
      }
    } catch (err: unknown) {
      const error = err instanceof Error ? err.message : String(err)
      await markFailed(db, ti, `Deferred trigger threw: ${error}`)
      console.error(`[executor] ✗ deferred ${ti.dag_id}.${ti.task_id}: trigger threw: ${error}`)
    }
  }
}
