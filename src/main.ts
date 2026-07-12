import { connectDb, closeDb } from './db/client.js'
import { ensureIndexes } from './db/indexes.js'
import { loadDags } from './dag/loader.js'
import { startScheduler, stopScheduler } from './scheduler/index.js'
import { recoverOrphanedRuns } from './scheduler/recovery.js'
import { buildServer } from './api/server.js'
import { startWorker, stopWorker } from './queue/consumer.js'
import { closeQueue } from './queue/producer.js'

const PORT = Number(process.env.PORT ?? 3000)
const HOST = process.env.HOST ?? '0.0.0.0'
const USE_BULLMQ = Boolean(process.env.REDIS_URL)

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

  const shutdown = async () => {
    console.log('\n[main] shutting down...')
    stopScheduler()
    await stopWorker()
    await closeQueue()
    await app.close()
    await closeDb()
    process.exit(0)
  }

  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)
}

main().catch((err) => {
  console.error('[main] fatal error:', err)
  process.exit(1)
})
