/**
 * Task tokens for the execution API — short-lived HS256 JWTs scoped to ONE try of ONE task instance.
 *
 *   claims: { sub: 'ti', dag_id, run_id, task_id, map_index, try_number, iat, exp, aud: 'execution-api' }
 *
 * A token only works while that try is running (the API checks the task instance on every call),
 * so a leaked token dies with the attempt. Workers refresh it before it expires (POST /token/refresh).
 *
 * Signing key, first that is set:
 *   EXECUTION_API_SECRET   any string (≥ 32 chars recommended) — the same on every replica
 *   ENCRYPTION_KEYS        an HKDF derivation of the primary key (replicas already share it)
 *   otherwise              a random per-process key: fine for one process, but tokens issued by one
 *                          replica are then rejected by another
 *
 *   EXECUTION_TOKEN_TTL_MS  lifetime (default 30 min)
 */
import { createHmac, hkdfSync, randomBytes, timingSafeEqual } from 'node:crypto'
import { getEncryptionKey, isEncryptionConfigured } from '../crypto/index.js'

export interface TaskScope {
  dagId: string
  runId: string
  taskId: string
  mapIndex: number | null
  tryNumber: number
}

const AUDIENCE = 'execution-api'
export const tokenTtlMs = () => Number(process.env.EXECUTION_TOKEN_TTL_MS ?? 30 * 60_000)

let ephemeral: Buffer | null = null

function signingKey(): Buffer {
  const explicit = process.env.EXECUTION_API_SECRET
  if (explicit) return Buffer.from(explicit, 'utf8')
  if (isEncryptionConfigured()) {
    return Buffer.from(hkdfSync('sha256', getEncryptionKey(), Buffer.alloc(0), 'airflow-nodejs execution-api token', 32))
  }
  if (!ephemeral) {
    ephemeral = randomBytes(32)
    console.warn('[execution-api] neither EXECUTION_API_SECRET nor ENCRYPTION_KEYS is set — task tokens are only valid in this process')
  }
  return ephemeral
}

const b64url = (b: Buffer | string) => Buffer.from(b).toString('base64url')
const HEADER = b64url(JSON.stringify({ alg: 'HS256', typ: 'JWT' }))

export function signTaskToken(scope: TaskScope, ttlMs = tokenTtlMs(), now = Date.now()): string {
  const iat = Math.floor(now / 1000)
  const payload = b64url(JSON.stringify({
    sub: 'ti', aud: AUDIENCE, iat, exp: iat + Math.max(1, Math.round(ttlMs / 1000)),
    dag_id: scope.dagId, run_id: scope.runId, task_id: scope.taskId, map_index: scope.mapIndex, try_number: scope.tryNumber,
  }))
  const sig = createHmac('sha256', signingKey()).update(`${HEADER}.${payload}`).digest()
  return `${HEADER}.${payload}.${b64url(sig)}`
}

/** The token's scope when its signature, audience and expiry check out; null otherwise. */
export function verifyTaskToken(token: string, now = Date.now()): TaskScope | null {
  const parts = token.split('.')
  if (parts.length !== 3 || parts[0] !== HEADER) return null
  const expected = createHmac('sha256', signingKey()).update(`${parts[0]}.${parts[1]}`).digest()
  const given = Buffer.from(parts[2], 'base64url')
  if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null
  let c: Record<string, unknown>
  try { c = JSON.parse(Buffer.from(parts[1], 'base64url').toString('utf8')) } catch { return null }
  if (c.sub !== 'ti' || c.aud !== AUDIENCE || typeof c.exp !== 'number' || c.exp * 1000 <= now) return null
  if (typeof c.dag_id !== 'string' || typeof c.run_id !== 'string' || typeof c.task_id !== 'string' || typeof c.try_number !== 'number') return null
  if (c.map_index !== null && typeof c.map_index !== 'number') return null
  return { dagId: c.dag_id, runId: c.run_id, taskId: c.task_id, mapIndex: c.map_index as number | null, tryNumber: c.try_number }
}

/** Expiry (ms epoch) read from a token without verifying it — for the worker's refresh timing. */
export function tokenExpiry(token: string): number | null {
  try {
    const exp = JSON.parse(Buffer.from(token.split('.')[1], 'base64url').toString('utf8')).exp
    return typeof exp === 'number' ? exp * 1000 : null
  } catch {
    return null
  }
}
