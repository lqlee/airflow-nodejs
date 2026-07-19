/**
 * API key management backed by MongoDB.
 *
 * Keys are stored hashed (scrypt). The raw key is returned once at creation
 * and never stored. Validation is a constant-time hash comparison.
 *
 * Each key carries a role: 'viewer' | 'editor' | 'admin'.
 * Bootstrap: set ADMIN_KEY env var to seed the first key on startup,
 * or leave API_KEYS set for the legacy env-only mode (still supported).
 */

import { randomBytes, scrypt, timingSafeEqual } from 'node:crypto'
import { promisify } from 'node:util'
import type { Db } from 'mongodb'

const scryptAsync = promisify(scrypt)

const SCRYPT_KEYLEN = 32
const PREFIX = 'an_'  // "airflow-node" prefix — easy to grep in logs

export type Role = 'viewer' | 'editor' | 'admin'

export const VALID_ROLES: readonly Role[] = ['viewer', 'editor', 'admin']

export function isRole(v: unknown): v is Role {
  return typeof v === 'string' && (VALID_ROLES as readonly string[]).includes(v)
}

export interface ApiKey {
  name: string
  role: Role
  key_hash: string   // hex: salt:hash
  created_at: Date
  last_used_at: Date | null
  revoked: boolean
}

export interface ApiKeySummary {
  id: string
  name: string
  role: Role
  created_at: Date
  last_used_at: Date | null
  revoked: boolean
}

/** Generate a new random key, hash it, store it. Returns the raw key (shown once). */
export async function createApiKey(
  db: Db,
  name: string,
  role: Role = 'viewer',
): Promise<{ raw: string; id: string }> {
  const raw = PREFIX + randomBytes(24).toString('base64url')
  const hash = await hashKey(raw)

  const res = await db.collection<ApiKey>('api_keys').insertOne({
    name,
    role,
    key_hash: hash,
    created_at: new Date(),
    last_used_at: null,
    revoked: false,
  })

  return { raw, id: res.insertedId.toString() }
}

/**
 * Validate a raw key against stored hashes.
 * Returns { name, role } on match (updates last_used_at fire-and-forget).
 * Returns null if invalid or revoked.
 * Keys without a stored role (pre-migration) default to 'viewer' (fail-closed).
 */
export async function validateApiKey(
  db: Db,
  raw: string,
): Promise<{ name: string; role: Role } | null> {
  if (!raw) return null

  const keys = await db
    .collection<ApiKey>('api_keys')
    .find({ revoked: false })
    .toArray()

  for (const k of keys) {
    if (await verifyKey(raw, k.key_hash)) {
      // Fire-and-forget last_used_at update
      void db.collection('api_keys').updateOne(
        { _id: (k as { _id: unknown })._id },
        { $set: { last_used_at: new Date() } },
      )
      // Fail-closed: keys created before the role field default to 'viewer'
      return { name: k.name, role: k.role ?? 'viewer' }
    }
  }
  return null
}

/** Revoke (soft-delete) a key by id. */
export async function revokeApiKey(db: Db, keyId: string): Promise<boolean> {
  const { ObjectId } = await import('mongodb')
  if (!ObjectId.isValid(keyId)) return false
  const res = await db.collection('api_keys').updateOne(
    { _id: new ObjectId(keyId) },
    { $set: { revoked: true } },
  )
  return res.matchedCount > 0
}

/** List all keys (never returns hashes). */
export async function listApiKeys(db: Db): Promise<ApiKeySummary[]> {
  const keys = await db
    .collection<ApiKey>('api_keys')
    .find({})
    .sort({ created_at: -1 })
    .toArray()

  return keys.map(k => ({
    id: (k as { _id: { toString(): string } })._id.toString(),
    name: k.name,
    role: k.role ?? 'viewer',
    created_at: k.created_at,
    last_used_at: k.last_used_at,
    revoked: k.revoked,
  }))
}

/** Hash a raw key → "salt:hash" hex string. */
async function hashKey(raw: string): Promise<string> {
  const salt = randomBytes(16)
  const hash = (await scryptAsync(raw, salt, SCRYPT_KEYLEN)) as Buffer
  return `${salt.toString('hex')}:${hash.toString('hex')}`
}

/** Constant-time verify raw key against stored "salt:hash". */
async function verifyKey(raw: string, stored: string): Promise<boolean> {
  const [saltHex, hashHex] = stored.split(':')
  if (!saltHex || !hashHex) return false
  try {
    const salt = Buffer.from(saltHex, 'hex')
    const expected = Buffer.from(hashHex, 'hex')
    const actual = (await scryptAsync(raw, salt, SCRYPT_KEYLEN)) as Buffer
    return actual.length === expected.length && timingSafeEqual(actual, expected)
  } catch {
    return false
  }
}
