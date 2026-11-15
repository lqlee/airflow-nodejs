/**
 * BullMQ Worker — can run on any machine with access to Redis + MongoDB.
 * Pulls task jobs from the queue and executes them.
 *
 * Start with: npx tsx src/queue/consumer.ts
 * Or alongside the scheduler: the scheduler calls startWorker() on boot.
 */
import { Worker, type Job } from 'bullmq'
import { createDbClient } from '../db/factory.js'
import { createInterface } from 'node:readline'
import { fork } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { resolve as pathResolve, dirname } from 'node:path'
import { createRedisConnection, QUEUE_NAME } from './connection.js'
import { appendLog } from '../logs/index.js'
import { xcomPush, xcomPull } from '../xcom/index.js'
import type { Db } from 'mongodb'
import type { TaskJobData } from './producer.js'
import { INSTANCE_ID, startLiveness, shutdownLiveness } from '../scheduler/instance.js'
import { retryUpdate, successUpdate, failedUpdate } from '../scheduler/ti-writes.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const WORKER_SCRIPT = pathResolve(__dirname, '../scheduler/worker.ts')
const TSX_BIN = pathResolve(__dirname, '../../node_modules/.bin/tsx')
const DB_NAME   = process.env.DB_NAME   ?? 'airflow'
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY ?? 4)

let bullWorker: Worker<TaskJobData> | null = null

/**
 * Adopt the claim of a queued job: the scheduler marked the instance running under its own id; this
 * process now owns it, so its heartbeat is what keeps the task from being reaped. Returns the filter
 * that matches the instance only while this process still owns that running try (use it for result
 * writes), or null when the claim changed since the job was queued (reaped, cancelled, re-claimed).
 */
export async function adoptClaim(db: Db, ti: TaskJobData['ti']): Promise<Record<string, unknown> | null> {
  const tiKey = { dag_run_id: ti.dag_run_id, task_id: ti.task_id, map_index: ti.map_index ?? null }
  const adopted = await db.collection('task_instances').updateOne(
    { ...tiKey, state: 'running', claimed_by: ti.claimed_by ?? null, try_number: ti.try_number },
    { $set: { claimed_by: INSTANCE_ID, started_at: new Date() } },
  )
  return adopted.matchedCount === 0 ? null : { ...tiKey, state: 'running', claimed_by: INSTANCE_ID }
}

async function processJob(job: Job<TaskJobData>): Promise<void> {
  const { ti, fn } = job.data
  const client = createDbClient()

  try {
    await client.connect({ light: true })
    const db = client.db(DB_NAME)

    const owned = await adoptClaim(db, ti)
    if (!owned) {
      console.warn(`[worker] skipping stale job ${job.id}: ${ti.dag_id}.${ti.task_id} is no longer running under the claim that queued it`)
      return
    }

    // Fork the worker script
    await new Promise<void>((resolve, reject) => {
      const child = fork(WORKER_SCRIPT, [], {
        execPath: TSX_BIN,
        env: { ...process.env },
        silent: true,
      })

      const rl_out = createInterface({ input: child.stdout! })
      rl_out.on('line', (line) => {
        process.stdout.write(`[${ti.task_id}] ${line}\n`)
        void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stdout', line)
      })

      const rl_err = createInterface({ input: child.stderr! })
      rl_err.on('line', (line) => {
        process.stderr.write(`[${ti.task_id}] ${line}\n`)
        void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stderr', line)
      })

      child.send({ type: 'run', fn, ctx: { dagId: ti.dag_id, runId: ti.dag_run_id, taskId: ti.task_id, mapIndex: ti.map_index ?? null, mapValue: ti.map_value ?? null } })

      child.on('message', async (msg: { type: string; success: boolean; error?: string }) => {
        if (msg.type !== 'done') return
        if (msg.success) {
          await db.collection('task_instances').updateOne(owned, successUpdate())
          console.log(`[worker] ✓ ${ti.dag_id}.${ti.task_id}`)
          resolve()
        } else {
          const error = msg.error ?? 'unknown error'
          if (ti.try_number < ti.max_retries) {
            // Requeue for retry
            // up_for_retry with next_retry_at — the scheduler re-claims and re-enqueues once the delay passes
            await db.collection('task_instances').updateOne(owned, retryUpdate(ti, error))
            console.warn(`[worker] ↩ ${ti.dag_id}.${ti.task_id} retrying (${ti.try_number + 1}/${ti.max_retries + 1})`)
          } else {
            await db.collection('task_instances').updateOne(owned, failedUpdate(error))
            console.error(`[worker] ✗ ${ti.dag_id}.${ti.task_id}: ${error}`)
          }
          resolve()
        }
      })

      child.on('error', async (err) => {
        await db.collection('task_instances').updateOne(owned, failedUpdate(err.message))
        reject(err)
      })
    })
  } finally {
    await client.close()
  }
}

export function startWorker(): Worker<TaskJobData> {
  if (bullWorker) return bullWorker

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  bullWorker = new Worker<TaskJobData>(QUEUE_NAME, processJob, {
    connection: createRedisConnection() as any,
    concurrency: CONCURRENCY,
  })

  bullWorker.on('completed', (job) => {
    console.log(`[worker] job ${job.id} completed`)
  })

  bullWorker.on('failed', (job, err) => {
    console.error(`[worker] job ${job?.id} failed:`, err.message)
  })

  console.log(`[worker] started — concurrency: ${CONCURRENCY}`)
  return bullWorker
}

export async function stopWorker(): Promise<void> {
  if (bullWorker) {
    await bullWorker.close()
    bullWorker = null
    console.log('[worker] stopped')
  }
}

// Allow running as standalone: npx tsx src/queue/consumer.ts
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  // A standalone worker owns the tasks it adopts, so it must heartbeat (never leads the scheduler)
  const livenessClient = createDbClient()
  await livenessClient.connect({ light: true })
  const livenessDb = livenessClient.db(DB_NAME)
  await startLiveness(livenessDb, { lease: false, role: 'worker' })
  startWorker()
  console.log(`[worker] standalone mode (${INSTANCE_ID}) — Ctrl+C to stop`)

  const stop = async () => {
    await stopWorker()   // waits for active jobs
    await shutdownLiveness(livenessDb)
    await livenessClient.close()
    process.exit(0)
  }
  process.once('SIGTERM', () => void stop())
  process.once('SIGINT', () => void stop())
}
