/**
 * Standalone Dag processor: `npm run dag-processor` (or `node dist/dag/processor-cli.js`).
 * Run it as its own deployment and set DAG_PROCESSOR=external on the schedulers/API, so only this
 * process (through its parse children) reads the Dag folder. It checks for file changes every
 * DAG_PROCESSOR_POLL_MS (default 5s) and re-parses when something changed.
 */
import { connectDb, closeDb } from '../db/client.js'
import { ensureIndexes } from '../db/indexes.js'
import { maybeRunDagProcessor, stopDagProcessor } from './processor.js'

const POLL_MS = Number(process.env.DAG_PROCESSOR_POLL_MS ?? 5_000)

async function main(): Promise<void> {
  const db = await connectDb()
  await ensureIndexes(db)
  console.log(`[dag-processor] started — checking the Dag folder every ${POLL_MS / 1000}s`)

  let stopping = false
  let first = true
  const loop = async () => {
    while (!stopping) {
      try {
        await maybeRunDagProcessor(db, { force: first })
        first = false
      } catch (err) {
        console.error('[dag-processor] run failed:', err)
      }
      await new Promise(r => setTimeout(r, POLL_MS))
    }
  }
  const done = loop()

  const shutdown = async (signal: string) => {
    if (stopping) process.exit(1)
    stopping = true
    console.log(`[dag-processor] ${signal} — stopping`)
    stopDagProcessor()
    await Promise.race([done, new Promise(r => setTimeout(r, POLL_MS + 1_000))])
    await closeDb()
    process.exit(0)
  }
  process.on('SIGTERM', () => void shutdown('SIGTERM'))
  process.on('SIGINT', () => void shutdown('SIGINT'))
}

main().catch((err) => {
  console.error('[dag-processor] fatal error:', err)
  process.exit(1)
})
