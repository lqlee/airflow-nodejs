import { fork } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { resolve as pathResolve, dirname } from 'node:path'
import { createInterface } from 'node:readline'
import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { getDag } from '../dag/registry.js'
import { acquire, release } from './pool.js'
import { appendLog } from '../logs/index.js'
import { enqueueTask } from '../queue/producer.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const WORKER_SCRIPT = pathResolve(__dirname, 'worker.ts')
const TSX_BIN = pathResolve(__dirname, '../../node_modules/.bin/tsx')

// When REDIS_URL is set, use BullMQ (distributed). Otherwise use local fork.
const USE_BULLMQ = Boolean(process.env.REDIS_URL)

type WorkerMsg = { type: 'done'; success: boolean; error?: string }

export async function executeTask(db: Db, ti: TaskInstance): Promise<void> {
  const dag = getDag(ti.dag_id)
  if (!dag) { await markFailed(db, ti, `Dag '${ti.dag_id}' not found in registry`); return }

  const taskDef = dag.tasks[ti.task_id]
  if (!taskDef) { await markFailed(db, ti, `Task '${ti.task_id}' not found in dag '${ti.dag_id}'`); return }

  if (USE_BULLMQ) {
    // Distributed: enqueue to Redis — BullMQ worker picks it up
    await enqueueTask(ti, taskDef.run.toString())
    console.log(`[executor] enqueued ${ti.dag_id}.${ti.task_id} → BullMQ`)
    // Don't wait — advanceRun will check completion on next tick
    return
  }

  // Local: fork directly
  await acquire()
  console.log(`[executor] running ${ti.dag_id}.${ti.task_id} (run: ${ti.dag_run_id})`)

  return new Promise((done) => {
    const child = fork(WORKER_SCRIPT, [], {
      execPath: TSX_BIN,
      env: { ...process.env },
      silent: true,
    })

    // ── Timeout ───────────────────────────────────────────────────────
    let timedOut = false
    let killTimer: ReturnType<typeof setTimeout> | null = null

    if (ti.timeout_ms > 0) {
      killTimer = setTimeout(() => {
        timedOut = true
        child.kill('SIGTERM')
        const msg = `Task timed out after ${ti.timeout_ms}ms`
        console.error(`[executor] ⏱ ${ti.dag_id}.${ti.task_id}: ${msg}`)
        release()
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

    child.send({
      type: 'run',
      fn: taskDef.run.toString(),
      ctx: { dagId: ti.dag_id, runId: ti.dag_run_id, taskId: ti.task_id },
    })

    child.on('message', async (msg: WorkerMsg) => {
      if (msg.type !== 'done') return
      if (timedOut) return  // timeout handler already resolved
      clearKillTimer()
      release()
      if (msg.success) {
        await markSuccess(db, ti)
        console.log(`[executor] ✓ ${ti.dag_id}.${ti.task_id}`)
      } else {
        const error = msg.error ?? 'unknown error'
        if (ti.try_number < ti.max_retries) {
          await scheduleRetry(db, ti, error)
          console.warn(`[executor] ↩ ${ti.dag_id}.${ti.task_id} retrying (${ti.try_number + 1}/${ti.max_retries + 1})`)
        } else {
          await markFailed(db, ti, error)
          console.error(`[executor] ✗ ${ti.dag_id}.${ti.task_id} (try ${ti.try_number + 1}/${ti.max_retries + 1}): ${error}`)
        }
      }
      done()
    })

    child.on('error', async (err) => {
      if (timedOut) return
      clearKillTimer()
      release()
      await markFailed(db, ti, err.message)
      done()
    })

    child.on('exit', (code) => {
      if (timedOut) return  // expected kill — already handled
      if (code !== 0 && code !== null) {
        console.error(`[executor] worker exited with code ${code} for ${ti.task_id}`)
      }
    })
  })
}

async function scheduleRetry(db: Db, ti: TaskInstance, error: string): Promise<void> {
  const requeue = async () => {
    await db.collection('task_instances').updateOne(
      { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
      { $set: { state: 'queued', started_at: null, ended_at: null, error }, $inc: { try_number: 1 } }
    )
  }
  ti.retry_delay > 0 ? setTimeout(() => void requeue(), ti.retry_delay) : await requeue()
}

async function markSuccess(db: Db, ti: TaskInstance): Promise<void> {
  await db.collection('task_instances').updateOne(
    { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
    { $set: { state: 'success', ended_at: new Date() } }
  )
}

async function markFailed(db: Db, ti: TaskInstance, error: string): Promise<void> {
  await db.collection('task_instances').updateOne(
    { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
    { $set: { state: 'failed', ended_at: new Date(), error } }
  )
}
