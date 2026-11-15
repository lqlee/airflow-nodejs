/**
 * Re-encrypt stored Connection and Variable secrets with the primary key (ENCRYPTION_KEYS[0]).
 *
 * Each document is rewritten with a compare-and-set on the old ciphertext, so a value edited while the
 * rotation runs is left alone (it was just written with the primary key anyway). Safe to re-run: values
 * already on the primary key are skipped.
 */
import type { Db } from 'mongodb'
import { decrypt, encrypt, needsReencrypt, type EncryptedValue } from './index.js'

/** Encrypted fields per collection, and the field that names a document in reports. */
const TARGETS = [
  { collection: 'connections', idField: 'conn_id', fields: ['password_enc', 'extra_enc'] },
  { collection: 'variables', idField: 'key', fields: ['value_enc'] },
] as const

export interface RotationResult {
  /** Documents re-encrypted (or that would be, with dryRun), per collection. */
  rotated: Record<string, number>
  /** Documents already on the primary key. */
  current: Record<string, number>
  /** Documents that could not be decrypted with any configured key. */
  failed: Array<{ collection: string; id: string; error: string }>
}

const isEncrypted = (v: unknown): v is EncryptedValue =>
  typeof v === 'object' && v !== null && typeof (v as EncryptedValue).ciphertext === 'string'

export async function rotateEncryptedValues(db: Db, opts: { dryRun?: boolean } = {}): Promise<RotationResult> {
  const result: RotationResult = { rotated: {}, current: {}, failed: [] }

  for (const target of TARGETS) {
    result.rotated[target.collection] = 0
    result.current[target.collection] = 0
    // Connections and variables are a small, admin-managed set — one read is fine
    const docs = await db.collection(target.collection).find({}).toArray()
    for (const doc of docs) {
      const id = String(doc[target.idField])
      const stale = target.fields.filter(f => isEncrypted(doc[f]) && needsReencrypt(doc[f] as EncryptedValue))
      if (stale.length === 0) { result.current[target.collection]++; continue }

      const set: Record<string, EncryptedValue> = {}
      const filter: Record<string, unknown> = { _id: doc._id }
      try {
        for (const f of stale) {
          const old = doc[f] as EncryptedValue
          set[f] = encrypt(decrypt(old))
          filter[`${f}.ciphertext`] = old.ciphertext
        }
      } catch (err) {
        result.failed.push({ collection: target.collection, id, error: (err as Error).message })
        continue
      }
      if (!opts.dryRun) {
        const res = await db.collection(target.collection).updateOne(filter, { $set: set })
        if (res.matchedCount === 0) continue   // edited meanwhile — the new value already uses the primary key
      }
      result.rotated[target.collection]++
    }
  }
  return result
}
