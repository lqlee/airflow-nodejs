import type { FastifyRequest, FastifyReply } from 'fastify'
import type { Db } from 'mongodb'
import { validateApiKey, type Role } from './keys.js'
import { loadOidcConfig, createOidcVerifier, type OidcConfig, type OidcVerifier } from './oidc.js'
import { CSRF_HEADER, SESSION_COOKIE, getSession, parseCookies, safeEqual, type Session } from './sessions.js'

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

/** The UI can sign in with an API key (exchanged for a session cookie) when keys are configured. */
export const API_KEY_LOGIN_ENABLED = ENV_KEYS.size > 0 || Boolean(process.env.ADMIN_KEY)

/** OIDC config + verifier for the browser login, or null when OIDC_CLIENT_ID is not set. */
export function getBrowserOidc(): { config: OidcConfig; verifier: OidcVerifier } | null {
  if (!oidcVerifier || !('config' in OIDC) || !OIDC.config.clientId) return null
  return { config: OIDC.config, verifier: oidcVerifier }
}

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
    else console.log(`[auth] OIDC bearer-token auth enabled (issuer ${OIDC.config.issuer}, audience ${OIDC.config.audience})${OIDC.config.clientId ? `; browser login as client ${OIDC.config.clientId}` : ''}`)
  }
} else {
  console.log('[auth] no API_KEYS, ADMIN_KEY or OIDC_ISSUER configured — auth disabled (open access)')
}

// Routes that skip auth entirely (no token required, no role check). /auth/* are the login endpoints —
// they establish identity themselves (and /auth/logout checks the CSRF token).
const PUBLIC_PATHS = new Set(['/', '/health'])
/** `url` may carry a query string — only the pathname decides, and only root-level static files qualify. */
export const isPublicPath = (url: string) => {
  const path = url.split('?')[0]
  return PUBLIC_PATHS.has(path) || path.startsWith('/assets/') || path.startsWith('/vendor/') || path.startsWith('/auth/') ||
    path.startsWith('/execution/') ||   // task-token auth, checked by the execution routes
    /^\/[^/]+\.(js|css)$/.test(path)
}

const SAFE_METHODS = new Set(['GET', 'HEAD', 'OPTIONS'])

/**
 * Role for an API key: env keys and ADMIN_KEY are admin; DB-backed keys carry their own role.
 * Returns the key's display name too (sessions created from a key show it).
 */
export async function roleForApiKey(token: string): Promise<{ role: Role; name: string } | null> {
  if (!token) return null
  if (ENV_KEYS.has(token)) return { role: 'admin', name: 'env key' }
  if (process.env.ADMIN_KEY && safeEqual(token, process.env.ADMIN_KEY)) return { role: 'admin', name: 'admin key' }
  if (_db) {
    const result = await validateApiKey(_db, token)
    if (result) return { role: result.role, name: result.name }
  }
  return null
}

/** The browser session carried by the request's cookie, if any and still valid. */
export async function sessionFromRequest(req: FastifyRequest): Promise<{ sid: string; session: Session } | null> {
  if (!_db) return null
  const sid = parseCookies(req.headers.cookie)[SESSION_COOKIE]
  if (!sid) return null
  const session = await getSession(_db, sid)
  return session ? { sid, session } : null
}

/** True when a state-changing request carries the session's CSRF token. */
export function hasValidCsrf(req: FastifyRequest, session: Session): boolean {
  const sent = req.headers[CSRF_HEADER]
  return typeof sent === 'string' && session.csrfToken !== '' && safeEqual(sent, session.csrfToken)
}

// ── FastifyRequest / FastifyContextConfig augmentation ────────────────────
declare module 'fastify' {
  interface FastifyRequest {
    /** Role of the authenticated caller. Undefined when auth is disabled. */
    authRole: Role | undefined
    /** Who the caller is (session user, key name); undefined when auth is disabled. */
    authUser: string | undefined
  }
  interface FastifyContextConfig {
    /** Override the minimum role required for this route (default: method-based). */
    requiredRole?: Role
  }
}

/**
 * Fastify preHandler hook — authenticates the caller and enforces RBAC.
 *
 * Flow:
 *   1. Auth disabled → open access (authRole = undefined)
 *   2. Public path → skip
 *   3. Bearer token (API key or OIDC JWT) → role; no CSRF check (browsers never attach it by themselves)
 *   4. Otherwise the UI session cookie → role; state-changing methods need the X-CSRF-Token header
 *   5. Neither → 401; insufficient role → 403
 *
 * Env keys (API_KEYS) and ADMIN_KEY always resolve to the 'admin' role.
 * DB-backed keys carry the role they were created with (default 'viewer').
 */
export async function authHook(req: FastifyRequest, reply: FastifyReply): Promise<void> {
  req.authRole = undefined
  req.authUser = undefined

  if (!AUTH_ENABLED) return
  if (isPublicPath(req.url)) return

  const header = req.headers.authorization ?? ''
  const token = header.startsWith('Bearer ') ? header.slice(7).trim() : ''

  let callerRole: Role | null = null
  let caller: string | undefined

  if (token) {
    // 1–3. API keys (env, ADMIN_KEY, DB-backed)
    const key = await roleForApiKey(token)
    if (key) { callerRole = key.role; caller = `api-key:${key.name}` }
    // 4. OIDC JWT (only tokens shaped like a JWT are sent to the verifier)
    if (callerRole === null && oidcVerifier && token.split('.').length === 3) {
      const claims = await oidcVerifier.verify(token)
      if (claims) {
        const role = oidcVerifier.roleFor(claims)
        if (!role) {
          return reply.status(403).send({ error: 'Forbidden — token is valid but carries no role mapped to airflow-nodejs' })
        }
        callerRole = role
        caller = typeof claims.sub === 'string' ? `oidc:${claims.sub}` : 'oidc'
      }
    }
  } else {
    // 5. Browser session
    const found = await sessionFromRequest(req)
    if (found) {
      if (!SAFE_METHODS.has(req.method.toUpperCase()) && !hasValidCsrf(req, found.session)) {
        return reply.status(403).send({ error: 'Forbidden — missing or invalid CSRF token (X-CSRF-Token header)' })
      }
      callerRole = found.session.role
      caller = `${found.session.method}:${found.session.name ?? found.session.subject}`
    }
  }

  if (callerRole === null) {
    return reply.status(401).send({
      error: 'Unauthorized — sign in, or provide a valid API key or OIDC token in Authorization: Bearer <token>',
    })
  }

  req.authRole = callerRole
  req.authUser = caller

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
