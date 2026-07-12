import { connectDb, closeDb } from './db/client.js'
import { ensureIndexes } from './db/indexes.js'
import { loadDags } from './dag/loader.js'
import { startScheduler, stopScheduler } from './scheduler/index.js'
import { buildServer } from './api/server.js'

const PORT = Number(process.env.PORT ?? 3000)
const HOST = process.env.HOST ?? '0.0.0.0'

async function main(): Promise<void> {
  const db = await connectDb()
  await ensureIndexes(db)
  await loadDags()

  startScheduler(db)

  const app = buildServer(db)
  await app.listen({ port: PORT, host: HOST })
  console.log(`[api] listening on http://localhost:${PORT}`)

  // Graceful shutdown
  const shutdown = async () => {
    console.log('\n[main] shutting down...')
    stopScheduler()
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
