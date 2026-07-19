/**
 * Variable store — encrypted key-value config store.
 *
 * Variables with `is_secret: true` have their value AES-256-GCM encrypted.
 * Non-secret variables are stored plaintext (compatible with tasks that don't
 * need ENCRYPTION_KEY set). GET on a secret variable masks the value.
 * Tasks read via ctx.variables.get(key) which decrypts in the worker.
 */
import type { Db } from 'mongodb'
import { encrypt, decrypt } from '../crypto/index.js'

export interface VariableDoc {
  key: string           // unique identifier
  /** Plaintext value (for non-secret variables). Null when is_secret=true. */
  value: string | null
  /** AES-256-GCM encrypted value. Null when is_secret=false. */
  value_enc?: { iv: string; authTag: string; ciphertext: string } | null
  is_secret: boolean
  description?: string
  created_at: Date
  updated_at: Date
}

export interface VariableSummary {
  key: string
  value: string | null   // null when is_secret=true (masked)
  is_secret: boolean
  description: string | null
  created_at: Date
  updated_at: Date
}

export interface SetVariableInput {
  key: string
  value: string
  is_secret?: boolean
  description?: string
}

function toSummary(doc: VariableDoc): VariableSummary {
  return {
    key: doc.key,
    value: doc.is_secret ? null : doc.value,  // mask secrets in API responses
    is_secret: doc.is_secret,
    description: doc.description ?? null,
    created_at: doc.created_at,
    updated_at: doc.updated_at,
  }
}

export async function listVariables(db: Db): Promise<VariableSummary[]> {
  const docs = await db.collection<VariableDoc>('variables').find({}).sort({ key: 1 }).toArray()
  return docs.map(toSummary)
}

export async function getVariable(db: Db, key: string): Promise<VariableSummary | null> {
  const doc = await db.collection<VariableDoc>('variables').findOne({ key })
  return doc ? toSummary(doc) : null
}

/** Decrypt a variable for task runtime use. Never call from API routes. */
export async function getVariableRuntime(db: Db, key: string): Promise<string | null> {
  const doc = await db.collection<VariableDoc>('variables').findOne({ key })
  if (!doc) return null

  if (doc.is_secret && doc.value_enc) {
    return decrypt(doc.value_enc)
  }
  return doc.value
}

export async function setVariable(db: Db, input: SetVariableInput): Promise<VariableSummary> {
  const now = new Date()
  const isSecret = input.is_secret ?? false

  let value: string | null = null
  let value_enc: VariableDoc['value_enc'] = null

  if (isSecret) {
    value_enc = encrypt(input.value)
  } else {
    value = input.value
  }

  await db.collection<VariableDoc>('variables').updateOne(
    { key: input.key },
    {
      $set: {
        key: input.key,
        value,
        value_enc,
        is_secret: isSecret,
        description: input.description,
        updated_at: now,
      },
      $setOnInsert: { created_at: now } as Partial<VariableDoc>,
    },
    { upsert: true },
  )

  const doc = await db.collection<VariableDoc>('variables').findOne({ key: input.key })
  return toSummary(doc!)
}

export async function deleteVariable(db: Db, key: string): Promise<boolean> {
  const result = await db.collection<VariableDoc>('variables').deleteOne({ key })
  return result.deletedCount > 0
}
