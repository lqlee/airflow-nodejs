import { connectDb, closeDb } from './db/client.js'
import { ensureIndexes } from './db/indexes.js'
import { isProcessorEmbedded, maybeRunDagProcessor } from './dag/processor.js'
import { syncDagsFromDb } from './dag/sync.js'
import { startScheduler, stopScheduler } from './scheduler/index.js'
import { releaseOwnedTasks } from './scheduler/zombies.js'
import { shutdownLiveness, INSTANCE_ID } from './scheduler/instance.js'
import { releaseLease } from './scheduler/lease.js'
import { buildServer } from './api/server.js'
import { startWorker, stopWorker } from './queue/consumer.js'
import { closeQueue } from './queue/producer.js'
import { drainPool, activeWorkers, queueDepth, pendingWork } from './scheduler/pool.js'
import { installJsonConsole } from './observability/json-console.js'
import { initTracing, shutdownTracing } from './observability/tracing.js'

// LOG_FORMAT=json: one JSON object per console line, for Splunk — before anything else logs
installJsonConsole()

const PORT = Number(process.env.PORT ?? 3000)
const HOST = process.env.HOST ?? '0.0.0.0'
const USE_BULLMQ = Boolean(process.env.REDIS_URL)
const DRAIN_TIMEOUT_MS = Number(process.env.DRAIN_TIMEOUT_MS ?? 20_000)

async function main(): Promise<void> {
  if (await initTracing()) console.log('[tracing] OpenTelemetry traces on — a span per Dag run and per task try')
  const db = await connectDb()
  await ensureIndexes(db)
  // No startup recovery: tasks left running by a dead process are reaped as zombies once its
  // heartbeat goes stale (zombies.ts) — safe with several replicas sharing one database.
  console.log(`[main] instance ${INSTANCE_ID}`)
  // Parse Dag files in a child process into serialized_dags, then load those — this process never
  // imports user code (DAG_PROCESSOR=external: a separate dag-processor process does the parsing)
  if (isProcessorEmbedded()) await maybeRunDagProcessor(db, { force: true })
  await syncDagsFromDb(db)

  startScheduler(db)

  // Start embedded BullMQ worker when Redis is configured
  if (USE_BULLMQ) {
    startWorker()
    console.log('[main] BullMQ mode — tasks distributed via Redis')
  } else {
    console.log('[main] local fork mode — tasks run in child processes')
  }

  const app = buildServer(db)
  await app.listen({ port: PORT, host: HOST })
  console.log(`[api] listening on http://localhost:${PORT}`)
  console.log(`[api] UI: http://localhost:${PORT}/`)

  let shuttingDown = false

  const shutdown = async (signal: string) => {
    // Re-entrancy guard: second signal (e.g. Ctrl-C twice, k8s SIGKILL escalation)
    if (shuttingDown) {
      console.error(`[main] ${signal} received again — forcing exit`)
      process.exit(1)
    }
    shuttingDown = true
    console.log(`\n[main] ${signal} received — graceful shutdown started`)

    // 1. Stop scheduler — no new ticks, no new cron fires, no new advanceRun calls.
    //    advanceRun's claim loop checks isShuttingDown() and bails after current wave.
    stopScheduler()
    //    Give up leadership now so another replica takes over cron/catch-up without waiting for expiry.
    await releaseLease(db, INSTANCE_ID).catch(err => console.warn('[main] lease release failed:', (err as Error).message))

    // 2. Drain HTTP — wait for in-flight requests (including POST /trigger → advanceRun).
    //    Fastify closes the server and waits for outstanding keep-alive/in-flight requests.
    console.log('[main] closing HTTP server...')
    await app.close()
    console.log('[main] HTTP server closed')

    // 3. Drain worker pool (local-fork mode only).
    //    MUST drain before closeDb(): queued waiters will acquire() → fork() → write DB
    //    once a slot frees even after we stopped forking new tasks. Active, queued and pending
    //    (outcome writes, try records, log uploads) must all be zero.
    if (!USE_BULLMQ) {
      const a = activeWorkers()
      const q = queueDepth()
      const p = pendingWork()
      if (a > 0 || q > 0 || p > 0) {
        console.log(`[main] draining worker pool (active=${a}, queued=${q}, pending=${p}, timeout=${DRAIN_TIMEOUT_MS}ms)...`)
        const result = await drainPool({ timeoutMs: DRAIN_TIMEOUT_MS })
        if (result.drained) {
          console.log('[main] worker pool drained')
        } else {
          console.warn(`[main] drain timeout — ${result.remaining} worker(s) still running; DB writes may be lost`)
        }
      }
    }

    // 4. BullMQ worker drain (waits for active BullMQ jobs).
    await stopWorker()
    await closeQueue()
    //    The last jobs' try records and log uploads
    if (USE_BULLMQ && pendingWork() > 0) await drainPool({ timeoutMs: DRAIN_TIMEOUT_MS })

    // 5. Hand tasks this process still owns back to the queue (no retry spent), then deregister.
    await releaseOwnedTasks(db).catch(err => console.warn('[main] task hand-back failed:', (err as Error).message))
    await shutdownLiveness(db)
    await shutdownTracing()   // flush the last spans

    // 6. Close DB last — all in-flight writes are done.
    await closeDb()

    console.log('[main] shutdown complete')
    process.exit(0)
  }

  process.on('SIGTERM', () => void shutdown('SIGTERM'))
  process.on('SIGINT',  () => void shutdown('SIGINT'))
}

main().catch((err) => {
  console.error('[main] fatal error:', err)
  process.exit(1)
})
