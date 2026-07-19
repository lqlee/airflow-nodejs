import type { FastifyRequest, FastifyReply } from 'fastify'
import type { Db } from 'mongodb'
import { validateApiKey } from './keys.js'

// ── Legacy env-based keys (backward compat) ───────────────────────────
const rawKeys = process.env.API_KEYS ?? ''
const ENV_KEYS = new Set(
  rawKeys.split(',').map(k => k.trim()).filter(Boolean)
)

// Auth is considered enabled if either env keys or DB-backed keys are in use.
// We determine DB mode at runtime (when db is wired in via setDb).
export const AUTH_ENABLED = ENV_KEYS.size > 0 || Boolean(process.env.ADMIN_KEY)

let _db: Db | null = null

/** Wire the database so authHook can validate DB-backed keys. */
export function setDb(db: Db): void {
  _db = db
}

if (AUTH_ENABLED) {
  if (ENV_KEYS.size > 0) {
    console.log(`[auth] env API key auth enabled (${ENV_KEYS.size} key(s))`)
  }
  if (process.env.ADMIN_KEY) {
    console.log('[auth] ADMIN_KEY set — DB-backed key management enabled')
  }
} else {
  console.log('[auth] no API_KEYS or ADMIN_KEY configured — auth disabled (open access)')
}

// Routes that skip auth
const PUBLIC_PATHS = new Set(['/', '/health'])
const isPublicPath = (path: string) =>
  PUBLIC_PATHS.has(path) || path.startsWith('/assets') || path.endsWith('.js') || path.endsWith('.css')

/**
 * Fastify preHandler hook — validates Bearer token on protected routes.
 *
 * Validation order:
 *   1. Check env API_KEYS set (fast, in-memory)
 *   2. Check MongoDB api_keys collection (if DB wired in)
 */
export async function authHook(req: FastifyRequest, reply: FastifyReply): Promise<void> {
  if (!AUTH_ENABLED) return
  if (isPublicPath(req.url)) return

  const header = req.headers.authorization ?? ''
  const token = header.startsWith('Bearer ') ? header.slice(7).trim() : ''

  if (!token) {
    return reply.status(401).send({ error: 'Unauthorized — provide a valid API key in Authorization: Bearer <key>' })
  }

  // 1. Env keys (fast path)
  if (ENV_KEYS.has(token)) return

  // 2. ADMIN_KEY (for bootstrapping key management)
  if (process.env.ADMIN_KEY && token === process.env.ADMIN_KEY) return

  // 3. DB-backed keys
  if (_db && await validateApiKey(_db, token)) return

  return reply.status(401).send({ error: 'Unauthorized — provide a valid API key in Authorization: Bearer <key>' })
}

/**
 * Validate a single key (for scripts/tests).
 * When AUTH_ENABLED is false, always returns true.
 */
export function isValidKey(key: string): boolean {
  if (!AUTH_ENABLED) return true
  return ENV_KEYS.has(key) || (Boolean(process.env.ADMIN_KEY) && key === process.env.ADMIN_KEY)
}
