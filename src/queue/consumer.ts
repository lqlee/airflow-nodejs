/**
 * BullMQ Worker — can run on any machine with access to Redis + MongoDB.
 * Pulls task jobs from the queue and executes them.
 *
 * Start with: npx tsx src/queue/consumer.ts
 * Or alongside the scheduler: the scheduler calls startWorker() on boot.
 */
import { Worker, type Job } from 'bullmq'
import { MongoClient } from 'mongodb'
import { createInterface } from 'node:readline'
import { fork } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { resolve as pathResolve, dirname } from 'node:path'
import { createRedisConnection, QUEUE_NAME } from './connection.js'
import { appendLog } from '../logs/index.js'
import { xcomPush, xcomPull } from '../xcom/index.js'
import type { TaskJobData } from './producer.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const WORKER_SCRIPT = pathResolve(__dirname, '../scheduler/worker.ts')
const TSX_BIN = pathResolve(__dirname, '../../node_modules/.bin/tsx')
const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME   = process.env.DB_NAME   ?? 'airflow'
const CONCURRENCY = Number(process.env.WORKER_CONCURRENCY ?? 4)

let bullWorker: Worker<TaskJobData> | null = null

async function processJob(job: Job<TaskJobData>): Promise<void> {
  const { ti, fn } = job.data
  const client = new MongoClient(MONGO_URL)

  try {
    await client.connect()
    const db = client.db(DB_NAME)

    // Mark task running
    await db.collection('task_instances').updateOne(
      { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
      { $set: { state: 'running', started_at: new Date() } }
    )

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

      child.send({ type: 'run', fn, ctx: { dagId: ti.dag_id, runId: ti.dag_run_id, taskId: ti.task_id } })

      child.on('message', async (msg: { type: string; success: boolean; error?: string }) => {
        if (msg.type !== 'done') return
        if (msg.success) {
          await db.collection('task_instances').updateOne(
            { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
            { $set: { state: 'success', ended_at: new Date() } }
          )
          console.log(`[worker] ✓ ${ti.dag_id}.${ti.task_id}`)
          resolve()
        } else {
          const error = msg.error ?? 'unknown error'
          if (ti.try_number < ti.max_retries) {
            // Requeue for retry
            await db.collection('task_instances').updateOne(
              { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
              { $set: { state: 'queued', started_at: null, ended_at: null, error }, $inc: { try_number: 1 } }
            )
            console.warn(`[worker] ↩ ${ti.dag_id}.${ti.task_id} retrying (${ti.try_number + 1}/${ti.max_retries + 1})`)
          } else {
            await db.collection('task_instances').updateOne(
              { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
              { $set: { state: 'failed', ended_at: new Date(), error } }
            )
            console.error(`[worker] ✗ ${ti.dag_id}.${ti.task_id}: ${error}`)
          }
          resolve()
        }
      })

      child.on('error', async (err) => {
        await db.collection('task_instances').updateOne(
          { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
          { $set: { state: 'failed', ended_at: new Date(), error: err.message } }
        )
        reject(err)
      })
    })
  } finally {
    await client.close()
  }
}

export function startWorker(): Worker<TaskJobData> {
  if (bullWorker) return bullWorker

  bullWorker = new Worker<TaskJobData>(QUEUE_NAME, processJob, {
    connection: createRedisConnection(),
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
  startWorker()
  console.log('[worker] standalone mode — Ctrl+C to stop')
}
