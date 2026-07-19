/**
 * Connection store — encrypted credentials for external systems.
 *
 * Secrets (password, extra JSON) are AES-256-GCM encrypted at rest.
 * GET endpoints NEVER return the plaintext password/extra — write-only.
 * Tasks decrypt in the worker process via ctx.connections.get(id).
 */
import type { Db } from 'mongodb'
import { encrypt, decrypt, isEncryptionConfigured } from '../crypto/index.js'

export interface ConnectionDoc {
  conn_id: string          // unique identifier, e.g. 'my_postgres'
  conn_type: string        // e.g. 'postgres', 'http', 's3', 'ssh'
  host?: string
  port?: number | null
  schema?: string
  login?: string
  /** AES-256-GCM encrypted password. Null if no password. */
  password_enc?: { iv: string; authTag: string; ciphertext: string } | null
  /** AES-256-GCM encrypted extra JSON string. Null if not set. */
  extra_enc?: { iv: string; authTag: string; ciphertext: string } | null
  description?: string
  created_at: Date
  updated_at: Date
}

/** Shape returned to API callers — password/extra are redacted */
export interface ConnectionSummary {
  conn_id: string
  conn_type: string
  host: string | null
  port: number | null
  schema: string | null
  login: string | null
  has_password: boolean
  has_extra: boolean
  description: string | null
  created_at: Date
  updated_at: Date
}

/** Shape returned to tasks at runtime — plaintext decrypted */
export interface ConnectionRuntime {
  conn_id: string
  conn_type: string
  host: string | null
  port: number | null
  schema: string | null
  login: string | null
  password: string | null
  extra: Record<string, unknown> | null
}

export interface UpsertConnectionInput {
  conn_id: string
  conn_type: string
  host?: string
  port?: number | null
  schema?: string
  login?: string
  password?: string        // plaintext — encrypted before storage
  extra?: string           // JSON string — encrypted before storage
  description?: string
}

function toSummary(doc: ConnectionDoc): ConnectionSummary {
  return {
    conn_id: doc.conn_id,
    conn_type: doc.conn_type,
    host: doc.host ?? null,
    port: doc.port ?? null,
    schema: doc.schema ?? null,
    login: doc.login ?? null,
    has_password: doc.password_enc != null,
    has_extra: doc.extra_enc != null,
    description: doc.description ?? null,
    created_at: doc.created_at,
    updated_at: doc.updated_at,
  }
}

export async function listConnections(db: Db): Promise<ConnectionSummary[]> {
  const docs = await db.collection<ConnectionDoc>('connections').find({}).sort({ conn_id: 1 }).toArray()
  return docs.map(toSummary)
}

export async function getConnection(db: Db, connId: string): Promise<ConnectionSummary | null> {
  const doc = await db.collection<ConnectionDoc>('connections').findOne({ conn_id: connId })
  return doc ? toSummary(doc) : null
}

/** Decrypt a connection for task runtime use. Never call from API routes. */
export async function getConnectionRuntime(db: Db, connId: string): Promise<ConnectionRuntime | null> {
  const doc = await db.collection<ConnectionDoc>('connections').findOne({ conn_id: connId })
  if (!doc) return null

  let password: string | null = null
  let extra: Record<string, unknown> | null = null

  if (doc.password_enc) {
    password = decrypt(doc.password_enc)
  }
  if (doc.extra_enc) {
    try {
      extra = JSON.parse(decrypt(doc.extra_enc)) as Record<string, unknown>
    } catch {
      extra = null
    }
  }

  return {
    conn_id: doc.conn_id,
    conn_type: doc.conn_type,
    host: doc.host ?? null,
    port: doc.port ?? null,
    schema: doc.schema ?? null,
    login: doc.login ?? null,
    password,
    extra,
  }
}

export async function upsertConnection(db: Db, input: UpsertConnectionInput): Promise<ConnectionSummary> {
  const now = new Date()

  const update: Partial<ConnectionDoc> = {
    conn_type: input.conn_type,
    host: input.host,
    port: input.port ?? null,
    schema: input.schema,
    login: input.login,
    description: input.description,
    updated_at: now,
  }

  // Only encrypt if provided — omitting password on update preserves existing
  if (input.password !== undefined) {
    update.password_enc = input.password ? encrypt(input.password) : null
  }
  if (input.extra !== undefined) {
    update.extra_enc = input.extra ? encrypt(input.extra) : null
  }

  await db.collection<ConnectionDoc>('connections').updateOne(
    { conn_id: input.conn_id },
    {
      $set: update,
      $setOnInsert: { conn_id: input.conn_id, created_at: now } as Partial<ConnectionDoc>,
    },
    { upsert: true },
  )

  const doc = await db.collection<ConnectionDoc>('connections').findOne({ conn_id: input.conn_id })
  return toSummary(doc!)
}

export async function deleteConnection(db: Db, connId: string): Promise<boolean> {
  const result = await db.collection<ConnectionDoc>('connections').deleteOne({ conn_id: connId })
  return result.deletedCount > 0
}
