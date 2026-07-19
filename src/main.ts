import { connectDb, closeDb } from './db/client.js'
import { ensureIndexes } from './db/indexes.js'
import { loadDags } from './dag/loader.js'
import { startScheduler, stopScheduler } from './scheduler/index.js'
import { recoverOrphanedRuns } from './scheduler/recovery.js'
import { buildServer } from './api/server.js'
import { startWorker, stopWorker } from './queue/consumer.js'
import { closeQueue } from './queue/producer.js'
import { drainPool, activeWorkers, queueDepth } from './scheduler/pool.js'

const PORT = Number(process.env.PORT ?? 3000)
const HOST = process.env.HOST ?? '0.0.0.0'
const USE_BULLMQ = Boolean(process.env.REDIS_URL)
const DRAIN_TIMEOUT_MS = Number(process.env.DRAIN_TIMEOUT_MS ?? 20_000)

async function main(): Promise<void> {
  const db = await connectDb()
  await ensureIndexes(db)
  await recoverOrphanedRuns(db)
  await loadDags()

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

    // 2. Drain HTTP — wait for in-flight requests (including POST /trigger → advanceRun).
    //    Fastify closes the server and waits for outstanding keep-alive/in-flight requests.
    console.log('[main] closing HTTP server...')
    await app.close()
    console.log('[main] HTTP server closed')

    // 3. Drain worker pool (local-fork mode only).
    //    MUST drain before closeDb(): queued waiters will acquire() → fork() → write DB
    //    once a slot frees even after we stopped forking new tasks. Both counters must
    //    be zero, not just active.
    if (!USE_BULLMQ) {
      const a = activeWorkers()
      const q = queueDepth()
      if (a > 0 || q > 0) {
        console.log(`[main] draining worker pool (active=${a}, queued=${q}, timeout=${DRAIN_TIMEOUT_MS}ms)...`)
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

    // 5. Close DB last — all in-flight writes are done.
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
