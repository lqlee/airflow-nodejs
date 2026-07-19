import type { FastifyRequest, FastifyReply } from 'fastify'
import type { Db } from 'mongodb'
import { validateApiKey, type Role } from './keys.js'

// ── Role hierarchy ─────────────────────────────────────────────────────────
export { type Role }

/** Numeric rank so we can compare roles with >=. */
const ROLE_RANK: Record<Role, number> = { viewer: 1, editor: 2, admin: 3 }

/** True if `actual` meets the `required` minimum. */
function hasRole(actual: Role, required: Role): boolean {
  return ROLE_RANK[actual] >= ROLE_RANK[required]
}

// ── Required role per request ──────────────────────────────────────────────
/**
 * Derive the minimum role needed for a request.
 *
 * Rules (fail-closed — unknown routes require editor):
 *   /api-keys (any method)    → admin   (key management is admin-only)
 *   GET / HEAD                → viewer
 *   everything else           → editor  (triggers, pauses, backfills, etc.)
 *
 * Routes can override by setting `config.requiredRole` in their route options.
 */
function requiredRoleFor(req: FastifyRequest): Role {
  const override = (req.routeOptions?.config as { requiredRole?: Role } | undefined)?.requiredRole
  if (override) return override

  const path = req.url.split('?')[0]
  if (path.startsWith('/api-keys')) return 'admin'

  const method = req.method.toUpperCase()
  if (method === 'GET' || method === 'HEAD') return 'viewer'

  return 'editor'
}

// ── Legacy env-based keys (backward compat) ───────────────────────────────
const rawKeys = process.env.API_KEYS ?? ''
const ENV_KEYS = new Set(
  rawKeys.split(',').map(k => k.trim()).filter(Boolean),
)

// Auth is considered enabled if either env keys or DB-backed keys are in use.
export const AUTH_ENABLED = ENV_KEYS.size > 0 || Boolean(process.env.ADMIN_KEY)

let _db: Db | null = null

/** Wire the database so authHook can validate DB-backed keys. */
export function setDb(db: Db): void {
  _db = db
}

if (AUTH_ENABLED) {
  if (ENV_KEYS.size > 0) {
    console.log(`[auth] env API key auth enabled (${ENV_KEYS.size} key(s)) — env keys have admin role`)
  }
  if (process.env.ADMIN_KEY) {
    console.log('[auth] ADMIN_KEY set — DB-backed key management enabled')
  }
} else {
  console.log('[auth] no API_KEYS or ADMIN_KEY configured — auth disabled (open access)')
}

// Routes that skip auth entirely (no token required, no role check)
const PUBLIC_PATHS = new Set(['/', '/health'])
const isPublicPath = (path: string) =>
  PUBLIC_PATHS.has(path) ||
  path.startsWith('/assets') ||
  path.endsWith('.js') ||
  path.endsWith('.css')

// ── FastifyRequest augmentation ────────────────────────────────────────────
declare module 'fastify' {
  interface FastifyRequest {
    /** Role of the authenticated caller. Undefined when auth is disabled. */
    authRole: Role | undefined
  }
}

/**
 * Fastify preHandler hook — validates Bearer token and enforces RBAC.
 *
 * Flow:
 *   1. Auth disabled → open access (authRole = undefined)
 *   2. Public path → skip
 *   3. No/invalid token → 401
 *   4. Valid token but insufficient role → 403
 *   5. Sufficient role → pass through
 *
 * Env keys (API_KEYS) and ADMIN_KEY always resolve to the 'admin' role.
 * DB-backed keys carry the role they were created with (default 'viewer').
 */
export async function authHook(req: FastifyRequest, reply: FastifyReply): Promise<void> {
  req.authRole = undefined

  if (!AUTH_ENABLED) return
  if (isPublicPath(req.url)) return

  const header = req.headers.authorization ?? ''
  const token = header.startsWith('Bearer ') ? header.slice(7).trim() : ''

  if (!token) {
    return reply.status(401).send({
      error: 'Unauthorized — provide a valid API key in Authorization: Bearer <key>',
    })
  }

  let callerRole: Role | null = null

  // 1. Env keys → admin (legacy compat; env keys are operator-level)
  if (ENV_KEYS.has(token)) {
    callerRole = 'admin'
  }
  // 2. ADMIN_KEY → admin (bootstrap)
  else if (process.env.ADMIN_KEY && token === process.env.ADMIN_KEY) {
    callerRole = 'admin'
  }
  // 3. DB-backed keys
  else if (_db) {
    const result = await validateApiKey(_db, token)
    if (result) callerRole = result.role
  }

  if (callerRole === null) {
    return reply.status(401).send({
      error: 'Unauthorized — provide a valid API key in Authorization: Bearer <key>',
    })
  }

  req.authRole = callerRole

  // Role check — 403 when authenticated but insufficient
  const required = requiredRoleFor(req)
  if (!hasRole(callerRole, required)) {
    return reply.status(403).send({
      error: `Forbidden — this action requires the '${required}' role (you have '${callerRole}')`,
    })
  }
}

/**
 * Validate a single key (for scripts/tests).
 * When AUTH_ENABLED is false, always returns true.
 */
export function isValidKey(key: string): boolean {
  if (!AUTH_ENABLED) return true
  return ENV_KEYS.has(key) || (Boolean(process.env.ADMIN_KEY) && key === process.env.ADMIN_KEY)
}
