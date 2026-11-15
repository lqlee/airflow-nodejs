/**
 * Re-encrypt every stored secret with the primary key: `npm run rotate-keys [-- --dry-run]`
 * Uses the same DB and key settings as the server (DB_BACKEND, MONGO_URL / SPANNER_*, DB_NAME, ENCRYPTION_KEYS).
 * Exits 1 when a value could not be decrypted with any configured key — retire no key until it passes.
 */
import { parseArgs } from 'node:util'
import { fileURLToPath } from 'node:url'
import { connectDb, closeDb } from '../db/client.js'
import { getEncryptionKeys, keyId } from './index.js'
import { rotateEncryptedValues } from './rotate.js'

async function main(): Promise<void> {
  const { values } = parseArgs({ args: process.argv.slice(2), options: { 'dry-run': { type: 'boolean', default: false } } })
  const keys = getEncryptionKeys()
  console.log(`[rotate-keys] primary key ${keyId(keys[0])}; ${keys.length - 1} older key(s): ${keys.slice(1).map(keyId).join(', ') || 'none'}`)
  const db = await connectDb()
  try {
    const res = await rotateEncryptedValues(db, { dryRun: values['dry-run'] === true })
    console.log(JSON.stringify(res, null, 2))
    if (res.failed.length > 0) process.exitCode = 1
  } finally {
    await closeDb()
  }
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  main().catch(err => { console.error(`[rotate-keys] ${(err as Error).message}`); process.exit(1) })
}
