/**
 * Browser sessions for the UI: a random session id in an HttpOnly cookie, the session (role, user,
 * CSRF token) in the `ui_sessions` collection — shared by every replica, revocable by deleting the row.
 *
 * Only a SHA-256 of each id is stored, so a database read does not hand out live sessions.
 * The same collection holds short-lived OIDC login attempts (state → nonce + PKCE verifier).
 *
 * Cookies
 *   anode_session  HttpOnly, SameSite=Strict, Path=/       the session id
 *   anode_login    HttpOnly, SameSite=Lax,    Path=/auth   binds an OIDC login attempt to this browser
 *                  (Lax: it must survive the top-level redirect back from the identity provider)
 * Both are Secure when the request came over HTTPS (directly or via X-Forwarded-Proto) or when
 * SESSION_COOKIE_SECURE=true.
 *
 * CSRF: a session-bound synchronizer token. The UI reads it from GET /auth/session and sends it as
 * `X-CSRF-Token` on every state-changing request; requests authenticated by a Bearer token need none
 * (a header the browser never attaches on its own). SameSite=Strict is a second layer.
 */
import { createHash, randomBytes, timingSafeEqual } from 'node:crypto'
import type { FastifyReply, FastifyRequest } from 'fastify'
import type { Db } from 'mongodb'
import type { Role } from './keys.js'

export const SESSION_COOKIE = 'anode_session'
export const LOGIN_COOKIE = 'anode_login'
export const CSRF_HEADER = 'x-csrf-token'
const COLLECTION = 'ui_sessions'
const LOGIN_TTL_MS = 10 * 60_000

export type SessionMethod = 'oidc' | 'api_key'

export interface SessionDoc {
  kind: 'session' | 'login'
  key_hash: string
  expires_at: Date
  created_at: Date
  // kind: 'session'
  role?: Role
  subject?: string
  name?: string | null
  method?: SessionMethod
  csrf_token?: string
  /** Kept for RP-initiated logout (id_token_hint). */
  id_token?: string | null
  // kind: 'login'
  nonce?: string
  code_verifier?: string
  return_to?: string
  browser_hash?: string
}

export interface Session {
  role: Role
  subject: string
  name: string | null
  method: SessionMethod
  csrfToken: string
  idToken: string | null
  expiresAt: Date
}

export const sha256 = (s: string) => createHash('sha256').update(s).digest('hex')
export const randomToken = (bytes = 32) => randomBytes(bytes).toString('base64url')

export function sessionTtlMs(): number {
  const n = Number(process.env.SESSION_TTL_MS ?? 8 * 3_600_000)
  return Number.isFinite(n) && n > 0 ? n : 8 * 3_600_000
}

/** Constant-time string comparison (false on length mismatch). */
export function safeEqual(a: string, b: string): boolean {
  const ab = Buffer.from(a)
  const bb = Buffer.from(b)
  return ab.length === bb.length && timingSafeEqual(ab, bb)
}

// ── Sessions ──────────────────────────────────────────────────────────────────

export async function createSession(
  db: Db,
  info: { role: Role; subject: string; name?: string | null; method: SessionMethod; idToken?: string | null },
  now = new Date(),
): Promise<{ sid: string; session: Session }> {
  const sid = randomToken()
  const doc: SessionDoc = {
    kind: 'session', key_hash: sha256(sid), created_at: now, expires_at: new Date(now.getTime() + sessionTtlMs()),
    role: info.role, subject: info.subject, name: info.name ?? null, method: info.method,
    csrf_token: randomToken(), id_token: info.idToken ?? null,
  }
  await db.collection<SessionDoc>(COLLECTION).insertOne(doc)
  // Opportunistic cleanup of expired rows (Mongo also has a TTL index; Spanner does not)
  void db.collection(COLLECTION).deleteMany({ expires_at: { $lt: now } }).catch(() => {})
  return { sid, session: toSession(doc) }
}

function toSession(doc: SessionDoc): Session {
  return {
    role: doc.role!, subject: doc.subject ?? '', name: doc.name ?? null, method: doc.method ?? 'api_key',
    csrfToken: doc.csrf_token ?? '', idToken: doc.id_token ?? null, expiresAt: doc.expires_at,
  }
}

export async function getSession(db: Db, sid: string, now = new Date()): Promise<Session | null> {
  if (!sid) return null
  const doc = await db.collection<SessionDoc>(COLLECTION).findOne({ key_hash: sha256(sid), kind: 'session' })
  if (!doc || new Date(doc.expires_at).getTime() <= now.getTime()) return null
  return toSession(doc)
}

export async function deleteSession(db: Db, sid: string): Promise<void> {
  if (sid) await db.collection(COLLECTION).deleteOne({ key_hash: sha256(sid), kind: 'session' })
}

// ── OIDC login attempts ───────────────────────────────────────────────────────

export async function createLoginState(
  db: Db, info: { nonce: string; codeVerifier: string; returnTo: string; browserId: string }, now = new Date(),
): Promise<string> {
  const state = randomToken()
  const doc: SessionDoc = {
    kind: 'login', key_hash: sha256(state), created_at: now, expires_at: new Date(now.getTime() + LOGIN_TTL_MS),
    nonce: info.nonce, code_verifier: info.codeVerifier, return_to: info.returnTo, browser_hash: sha256(info.browserId),
  }
  await db.collection<SessionDoc>(COLLECTION).insertOne(doc)
  return state
}

/**
 * Take (single use) the login attempt for `state`, only if it was started by this browser and has not
 * expired. A replayed or forged callback finds nothing.
 */
export async function consumeLoginState(
  db: Db, state: string, browserId: string, now = new Date(),
): Promise<{ nonce: string; codeVerifier: string; returnTo: string } | null> {
  if (!state || !browserId) return null
  const filter = { key_hash: sha256(state), kind: 'login' as const }
  const doc = await db.collection<SessionDoc>(COLLECTION).findOne(filter)
  if (!doc) return null
  const { deletedCount } = await db.collection(COLLECTION).deleteOne(filter)
  if (deletedCount !== 1) return null   // another request consumed it first
  if (new Date(doc.expires_at).getTime() <= now.getTime()) return null
  if (!doc.browser_hash || !safeEqual(doc.browser_hash, sha256(browserId))) return null
  return { nonce: doc.nonce!, codeVerifier: doc.code_verifier!, returnTo: doc.return_to ?? '/' }
}

// ── Cookies ───────────────────────────────────────────────────────────────────

export function parseCookies(header: string | undefined): Record<string, string> {
  const out: Record<string, string> = {}
  for (const part of (header ?? '').split(';')) {
    const i = part.indexOf('=')
    if (i < 1) continue
    const name = part.slice(0, i).trim()
    if (!(name in out)) {
      try { out[name] = decodeURIComponent(part.slice(i + 1).trim()) } catch { /* malformed — skip */ }
    }
  }
  return out
}

export function isSecureRequest(req: FastifyRequest): boolean {
  if (process.env.SESSION_COOKIE_SECURE !== undefined) return process.env.SESSION_COOKIE_SECURE === 'true'
  const forwarded = String(req.headers['x-forwarded-proto'] ?? '').split(',')[0].trim()
  return forwarded === 'https' || req.protocol === 'https'
}

export interface CookieOptions { maxAgeMs: number; sameSite: 'Strict' | 'Lax'; path: string }

export function serializeCookie(name: string, value: string, opts: CookieOptions & { secure: boolean }): string {
  return [
    `${name}=${encodeURIComponent(value)}`,
    `Path=${opts.path}`,
    `Max-Age=${Math.max(0, Math.floor(opts.maxAgeMs / 1000))}`,
    'HttpOnly',
    `SameSite=${opts.sameSite}`,
    ...(opts.secure ? ['Secure'] : []),
  ].join('; ')
}

/** Append a Set-Cookie header (keeps any already set on the reply). */
export function setCookie(req: FastifyRequest, reply: FastifyReply, name: string, value: string, opts: CookieOptions): void {
  const prev = reply.getHeader('set-cookie')
  const list = prev === undefined ? [] : Array.isArray(prev) ? prev.map(String) : [String(prev)]
  reply.header('set-cookie', [...list, serializeCookie(name, value, { ...opts, secure: isSecureRequest(req) })])
}

export function clearCookie(req: FastifyRequest, reply: FastifyReply, name: string, path: string): void {
  setCookie(req, reply, name, '', { maxAgeMs: 0, sameSite: 'Lax', path })
}

export function sessionCookieOptions(): CookieOptions {
  return { maxAgeMs: sessionTtlMs(), sameSite: 'Strict', path: '/' }
}

export function loginCookieOptions(): CookieOptions {
  return { maxAgeMs: LOGIN_TTL_MS, sameSite: 'Lax', path: '/auth' }
}
