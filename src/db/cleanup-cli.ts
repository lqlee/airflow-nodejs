/**
 * Purge finished runs older than N days: `npm run db:cleanup -- --days 30 [--dry-run] [--batch-size 100]`
 * Uses the same DB settings as the server (DB_BACKEND, MONGO_URL / SPANNER_*, DB_NAME).
 */
import { parseArgs } from 'node:util'
import { fileURLToPath } from 'node:url'
import { connectDb, closeDb } from './client.js'
import { purgeOldRuns } from './cleanup.js'

export function parseCleanupArgs(argv: string[]): { days: number; dryRun: boolean; batchSize: number } {
  const { values } = parseArgs({
    args: argv,
    options: { days: { type: 'string' }, 'dry-run': { type: 'boolean', default: false }, 'batch-size': { type: 'string', default: '100' } },
  })
  const days = Number(values.days)
  if (!Number.isFinite(days) || days <= 0) throw new Error('--days must be a positive number')
  const batchSize = Number(values['batch-size'])
  if (!Number.isInteger(batchSize) || batchSize < 1) throw new Error('--batch-size must be a positive integer')
  return { days, dryRun: values['dry-run'] === true, batchSize }
}

async function main(): Promise<void> {
  const args = parseCleanupArgs(process.argv.slice(2))
  const db = await connectDb()
  try {
    const res = await purgeOldRuns(db, { olderThan: new Date(Date.now() - args.days * 86_400_000), dryRun: args.dryRun, batchSize: args.batchSize })
    console.log(JSON.stringify(res, null, 2))
  } finally {
    await closeDb()
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch(err => { console.error(`[cleanup] ${(err as Error).message}`); process.exit(1) })
}
