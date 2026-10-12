import type { FastifyRequest, FastifyReply } from 'fastify'
import type { Db } from 'mongodb'
import { validateApiKey, type Role } from './keys.js'
import { loadOidcConfig, createOidcVerifier, type OidcVerifier } from './oidc.js'

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
export function requiredRoleForRoute(method: string, path: string, override?: Role): Role {
  if (override) return override
  if (path.startsWith('/api-keys')) return 'admin'
  const m = method.toUpperCase()
  return m === 'GET' || m === 'HEAD' ? 'viewer' : 'editor'
}

function requiredRoleFor(req: FastifyRequest): Role {
  const override = (req.routeOptions?.config as { requiredRole?: Role } | undefined)?.requiredRole
  return requiredRoleForRoute(req.method, req.url.split('?')[0], override)
}

// ── Legacy env-based keys (backward compat) ───────────────────────────────
const rawKeys = process.env.API_KEYS ?? ''
const ENV_KEYS = new Set(
  rawKeys.split(',').map(k => k.trim()).filter(Boolean),
)

// OIDC bearer-token validation (see ./oidc.ts). A misconfigured OIDC_ISSUER keeps auth ON
// with no valid OIDC token, rather than silently opening the API.
const OIDC = loadOidcConfig(process.env)
const oidcVerifier: OidcVerifier | null = OIDC.enabled && 'config' in OIDC ? createOidcVerifier(OIDC.config) : null

// Auth is considered enabled if env keys, DB-backed keys or OIDC are in use.
export const AUTH_ENABLED = ENV_KEYS.size > 0 || Boolean(process.env.ADMIN_KEY) || OIDC.enabled

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
  if (OIDC.enabled) {
    if ('error' in OIDC) console.error(`[auth] OIDC misconfigured: ${OIDC.error}`)
    else console.log(`[auth] OIDC bearer-token auth enabled (issuer ${OIDC.config.issuer}, audience ${OIDC.config.audience})`)
  }
} else {
  console.log('[auth] no API_KEYS, ADMIN_KEY or OIDC_ISSUER configured — auth disabled (open access)')
}

// Routes that skip auth entirely (no token required, no role check)
const PUBLIC_PATHS = new Set(['/', '/health'])
/** `url` may carry a query string — only the pathname decides, and only root-level static files qualify. */
export const isPublicPath = (url: string) => {
  const path = url.split('?')[0]
  return PUBLIC_PATHS.has(path) || path.startsWith('/assets/') || /^\/[^/]+\.(js|css)$/.test(path)
}

// ── FastifyRequest / FastifyContextConfig augmentation ────────────────────
declare module 'fastify' {
  interface FastifyRequest {
    /** Role of the authenticated caller. Undefined when auth is disabled. */
    authRole: Role | undefined
  }
  interface FastifyContextConfig {
    /** Override the minimum role required for this route (default: method-based). */
    requiredRole?: Role
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
      error: 'Unauthorized — provide a valid API key or OIDC token in Authorization: Bearer <token>',
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
  // 4. OIDC JWT (only tokens shaped like a JWT are sent to the verifier)
  if (callerRole === null && oidcVerifier && token.split('.').length === 3) {
    const claims = await oidcVerifier.verify(token)
    if (claims) {
      const role = oidcVerifier.roleFor(claims)
      if (!role) {
        return reply.status(403).send({ error: 'Forbidden — token is valid but carries no role mapped to airflow-nodejs' })
      }
      callerRole = role
    }
  }

  if (callerRole === null) {
    return reply.status(401).send({
      error: 'Unauthorized — provide a valid API key or OIDC token in Authorization: Bearer <token>',
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
