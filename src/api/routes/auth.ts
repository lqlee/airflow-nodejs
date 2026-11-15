/**
 * UI sign-in: OIDC browser login (authorization code + PKCE), API-key sign-in, session info, logout.
 * All of them hand the browser a session cookie — tokens and API keys never live in browser storage.
 *
 *   GET  /auth/session    who am I: auth mode, role, user, CSRF token, available sign-in methods
 *   GET  /auth/login      start OIDC login → 302 to the identity provider
 *   GET  /auth/callback   finish OIDC login → session cookie, 302 back into the UI
 *   POST /auth/key-login  { key } → session cookie (API key checked once, not on every request)
 *   POST /auth/logout     drop the session (needs X-CSRF-Token); returns the IdP logout URL, if any
 *
 * These paths skip authHook (they establish identity themselves).
 */
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import {
  AUTH_ENABLED, API_KEY_LOGIN_ENABLED, getBrowserOidc, roleForApiKey, sessionFromRequest, hasValidCsrf,
} from '../../auth/index.js'
import {
  LOGIN_COOKIE, SESSION_COOKIE, clearCookie, consumeLoginState, createLoginState, createSession, deleteSession,
  isSecureRequest, loginCookieOptions, parseCookies, randomToken, sessionCookieOptions, setCookie,
} from '../../auth/sessions.js'
import { buildAuthorizationUrl, buildLogoutUrl, exchangeCode, resolveLoginIdentity, safeReturnTo } from '../../auth/browser-login.js'

function redirectUri(req: FastifyRequest, configured: string | null): string {
  if (configured) return configured
  const host = String(req.headers['x-forwarded-host'] ?? req.headers.host ?? 'localhost').split(',')[0].trim()
  return `${isSecureRequest(req) ? 'https' : 'http'}://${host}/auth/callback`
}

/** Back into the UI with a short error code it can show; details go to the server log only. */
function loginFailed(reply: FastifyReply, code: string, detail?: string): FastifyReply {
  if (detail) console.warn(`[auth] OIDC login failed (${code}): ${detail}`)
  return reply.redirect(`/?login_error=${encodeURIComponent(code)}`, 302)
}

export async function authRoutes(app: FastifyInstance): Promise<void> {
  const db = app.mongo
  const authRateLimit = { max: Number(process.env.RATE_LIMIT_AUTH_MAX ?? 10), timeWindow: '1 minute' }

  app.get('/auth/session', async (req) => {
    const oidc = getBrowserOidc()
    const methods = { sso: Boolean(oidc), api_key: API_KEY_LOGIN_ENABLED }
    if (!AUTH_ENABLED) return { auth_enabled: false, authenticated: true, role: null, user: null, csrf_token: null, methods }
    const found = await sessionFromRequest(req)
    if (!found) return { auth_enabled: true, authenticated: false, role: null, user: null, csrf_token: null, methods }
    const { session } = found
    return {
      auth_enabled: true, authenticated: true, role: session.role,
      user: { name: session.name ?? session.subject, method: session.method },
      csrf_token: session.csrfToken, expires_at: session.expiresAt, methods,
    }
  })

  app.get('/auth/login', { config: { rateLimit: authRateLimit } }, async (req, reply) => {
    const oidc = getBrowserOidc()
    if (!oidc) return reply.status(404).send({ error: 'SSO login is not configured (set OIDC_ISSUER and OIDC_CLIENT_ID)' })
    const browserId = randomToken()
    const nonce = randomToken()
    const codeVerifier = randomToken(48)   // 64 chars — within RFC 7636's 43–128
    const returnTo = safeReturnTo((req.query as { return_to?: string }).return_to)
    try {
      const state = await createLoginState(db, { nonce, codeVerifier, returnTo, browserId })
      const url = await buildAuthorizationUrl(oidc.verifier, oidc.config, {
        state, nonce, codeVerifier, redirectUri: redirectUri(req, oidc.config.redirectUri),
      })
      setCookie(req, reply, LOGIN_COOKIE, browserId, loginCookieOptions())
      return reply.redirect(url, 302)
    } catch (err) {
      return loginFailed(reply, 'idp_unavailable', (err as Error).message)
    }
  })

  app.get('/auth/callback', { config: { rateLimit: authRateLimit } }, async (req, reply) => {
    const oidc = getBrowserOidc()
    if (!oidc) return reply.status(404).send({ error: 'SSO login is not configured' })
    const q = req.query as { code?: string; state?: string; error?: string; error_description?: string }
    clearCookie(req, reply, LOGIN_COOKIE, '/auth')
    if (q.error) return loginFailed(reply, 'idp_error', `${q.error}${q.error_description ? `: ${q.error_description}` : ''}`)
    if (!q.code || !q.state) return loginFailed(reply, 'invalid_callback', 'missing code or state')

    const attempt = await consumeLoginState(db, q.state, parseCookies(req.headers.cookie)[LOGIN_COOKIE] ?? '')
    if (!attempt) return loginFailed(reply, 'invalid_state', 'unknown, expired, replayed or foreign state')

    try {
      const tokens = await exchangeCode(oidc.verifier, oidc.config, {
        code: q.code, codeVerifier: attempt.codeVerifier, redirectUri: redirectUri(req, oidc.config.redirectUri),
      })
      const identity = await resolveLoginIdentity(oidc.verifier, oidc.config, tokens, attempt.nonce)
      if (!identity.ok) return loginFailed(reply, identity.error, identity.error === 'no_role' ? 'valid login, but no claim maps to a role' : 'ID token rejected')
      const { sid } = await createSession(db, {
        role: identity.role, subject: identity.subject, name: identity.name, method: 'oidc', idToken: tokens.idToken,
      })
      setCookie(req, reply, SESSION_COOKIE, sid, sessionCookieOptions())
      console.log(`[auth] ${identity.name ?? identity.subject} signed in via OIDC as ${identity.role}`)
      return reply.redirect(attempt.returnTo, 302)
    } catch (err) {
      return loginFailed(reply, 'token_exchange_failed', (err as Error).message)
    }
  })

  app.post('/auth/key-login', { config: { rateLimit: authRateLimit } }, async (req, reply) => {
    if (!API_KEY_LOGIN_ENABLED) return reply.status(404).send({ error: 'API-key sign-in is not configured' })
    const key = (req.body as { key?: unknown } | null)?.key
    if (typeof key !== 'string' || !key.trim()) return reply.status(400).send({ error: 'key is required' })
    const found = await roleForApiKey(key.trim())
    if (!found) return reply.status(401).send({ error: 'Invalid API key' })
    const { sid, session } = await createSession(db, { role: found.role, subject: found.name, name: found.name, method: 'api_key' })
    setCookie(req, reply, SESSION_COOKIE, sid, sessionCookieOptions())
    return { role: session.role, csrf_token: session.csrfToken, expires_at: session.expiresAt }
  })

  app.post('/auth/logout', async (req, reply) => {
    const found = await sessionFromRequest(req)
    if (!found) { clearCookie(req, reply, SESSION_COOKIE, '/'); return { logged_out: true, logout_url: null } }
    if (!hasValidCsrf(req, found.session)) return reply.status(403).send({ error: 'Forbidden — missing or invalid CSRF token (X-CSRF-Token header)' })
    await deleteSession(db, found.sid)
    clearCookie(req, reply, SESSION_COOKIE, '/')
    const oidc = getBrowserOidc()
    const logoutUrl = found.session.method === 'oidc' && oidc ? await buildLogoutUrl(oidc.verifier, oidc.config, found.session.idToken) : null
    return { logged_out: true, logout_url: logoutUrl }
  })
}
