/**
 * UI sign-in end to end: OIDC authorization code + PKCE against a fake identity provider, API-key
 * sign-in, the session cookie, CSRF enforcement, logout, and the browser security headers.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach, vi } from 'vitest'
import { createServer, type Server } from 'node:http'
import { createHash, createSign, generateKeyPairSync, randomBytes } from 'node:crypto'
import { MongoClient, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'

const rsa = generateKeyPairSync('rsa', { modulusLength: 2048 })
const CLIENT_ID = 'airflow-ui'
const CLIENT_SECRET = 's3cret'
const ADMIN_KEY = 'admin-key-for-tests'
const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'

// ── Fake identity provider ────────────────────────────────────────────────────
let idp: Server
let ISS = ''
/** Codes issued by the fake /authorize, with what the token request must prove. */
const codes = new Map<string, { challenge: string; nonce: string; redirectUri: string; claims: Record<string, unknown> }>()
let nextUser: Record<string, unknown> = {}
let idTokenOverrides: Record<string, unknown> = {}
const b64 = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url')
function sign(claims: Record<string, unknown>): string {
  const input = `${b64({ alg: 'RS256', kid: 'k1' })}.${b64(claims)}`
  return `${input}.${createSign('RSA-SHA256').update(input).sign(rsa.privateKey).toString('base64url')}`
}

/** Plays the user's browser at the IdP: approve the authorization request, return the callback URL. */
function approve(authorizeUrl: string): string {
  const u = new URL(authorizeUrl)
  expect(u.searchParams.get('code_challenge_method')).toBe('S256')
  expect(u.searchParams.get('client_id')).toBe(CLIENT_ID)
  const code = randomBytes(8).toString('hex')
  codes.set(code, {
    challenge: u.searchParams.get('code_challenge')!, nonce: u.searchParams.get('nonce')!,
    redirectUri: u.searchParams.get('redirect_uri')!, claims: nextUser,
  })
  const cb = new URL(u.searchParams.get('redirect_uri')!)
  cb.searchParams.set('code', code)
  cb.searchParams.set('state', u.searchParams.get('state')!)
  return `${cb.pathname}${cb.search}`
}

beforeAll(async () => {
  idp = createServer((req, res) => {
    let raw = ''
    req.on('data', c => { raw += c })
    req.on('end', () => {
      const json = (status: number, body: unknown) => { res.writeHead(status, { 'content-type': 'application/json' }); res.end(JSON.stringify(body)) }
      if (req.url === '/.well-known/openid-configuration') {
        return json(200, { issuer: ISS, authorization_endpoint: `${ISS}/authorize`, token_endpoint: `${ISS}/token`, end_session_endpoint: `${ISS}/logout`, jwks_uri: `${ISS}/jwks` })
      }
      if (req.url === '/jwks') return json(200, { keys: [{ ...rsa.publicKey.export({ format: 'jwk' }), kid: 'k1', use: 'sig' }] })
      if (req.url === '/token' && req.method === 'POST') {
        const p = new URLSearchParams(raw)
        if (req.headers.authorization !== `Basic ${Buffer.from(`${CLIENT_ID}:${CLIENT_SECRET}`).toString('base64')}`) return json(401, { error: 'invalid_client' })
        const grant = codes.get(p.get('code') ?? '')
        codes.delete(p.get('code') ?? '')
        if (!grant) return json(400, { error: 'invalid_grant' })
        // PKCE: the verifier must hash to the challenge sent at /authorize
        if (createHash('sha256').update(p.get('code_verifier') ?? '').digest('base64url') !== grant.challenge) return json(400, { error: 'invalid_grant', error_description: 'PKCE' })
        if (p.get('redirect_uri') !== grant.redirectUri) return json(400, { error: 'invalid_grant' })
        const now = Math.floor(Date.now() / 1000)
        return json(200, {
          access_token: 'opaque-access-token', token_type: 'Bearer',
          id_token: sign({ iss: ISS, aud: CLIENT_ID, exp: now + 300, iat: now, nonce: grant.nonce, ...grant.claims, ...idTokenOverrides }),
        })
      }
      json(404, {})
    })
  })
  await new Promise<void>(r => idp.listen(0, '127.0.0.1', r))
  ISS = `http://127.0.0.1:${(idp.address() as { port: number }).port}`
})
afterAll(async () => { await new Promise(r => idp.close(r)) })

// ── Server under test (auth reads its env at import → fresh modules per test) ─────
const ENV = ['API_KEYS', 'ADMIN_KEY', 'OIDC_ISSUER', 'OIDC_AUDIENCE', 'OIDC_CLIENT_ID', 'OIDC_CLIENT_SECRET', 'OIDC_REDIRECT_URI', 'OIDC_ROLE_CLAIM', 'OIDC_ADMIN_VALUES', 'OIDC_EDITOR_VALUES', 'OIDC_VIEWER_VALUES', 'OIDC_POST_LOGOUT_REDIRECT_URI', 'SESSION_TTL_MS', 'RATE_LIMIT_MAX']
let saved: Record<string, string | undefined>
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_browser_login')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })

beforeEach(() => {
  saved = Object.fromEntries(ENV.map(k => [k, process.env[k]]))
  for (const k of ENV) delete process.env[k]
  vi.resetModules()
  vi.spyOn(console, 'log').mockImplementation(() => {})
  vi.spyOn(console, 'warn').mockImplementation(() => {})
  nextUser = { sub: 'u-1', name: 'Ada Lovelace', groups: ['airflow-editors'] }
  idTokenOverrides = {}
})
afterEach(async () => {
  await app?.close()
  for (const k of ENV) { if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k] }
  vi.restoreAllMocks()
  await db.collection('ui_sessions').deleteMany({})
  await db.collection('pools').deleteMany({})
})

async function start(env: Record<string, string> = {}): Promise<FastifyInstance> {
  Object.assign(process.env, {
    RATE_LIMIT_MAX: '0', ADMIN_KEY,
    OIDC_ISSUER: ISS, OIDC_CLIENT_ID: CLIENT_ID, OIDC_CLIENT_SECRET: CLIENT_SECRET,
    OIDC_ROLE_CLAIM: 'groups', OIDC_EDITOR_VALUES: 'airflow-editors', OIDC_ADMIN_VALUES: 'airflow-admins',
    OIDC_POST_LOGOUT_REDIRECT_URI: 'http://localhost/bye',
    ...env,
  })
  const { buildServer } = await import('../../api/server.js')
  app = buildServer(db)
  await app.ready()
  return app
}

/** Cookie name → value from a response's Set-Cookie headers, plus the raw header lines. */
function cookies(res: { headers: Record<string, unknown> }): { jar: Record<string, string>; raw: string[] } {
  const h = res.headers['set-cookie']
  const raw = h === undefined ? [] : Array.isArray(h) ? h.map(String) : [String(h)]
  const jar: Record<string, string> = {}
  for (const line of raw) { const [pair] = line.split(';'); const i = pair.indexOf('='); jar[pair.slice(0, i)] = decodeURIComponent(pair.slice(i + 1)) }
  return { jar, raw }
}

/** Full SSO round trip; returns the session cookie value. */
async function ssoLogin(returnTo?: string): Promise<{ sid: string; location: string; raw: string[] }> {
  const login = await app.inject({ method: 'GET', url: `/auth/login${returnTo ? `?return_to=${encodeURIComponent(returnTo)}` : ''}` })
  expect(login.statusCode).toBe(302)
  const loginCookie = cookies(login).jar.anode_login
  const cb = await app.inject({ method: 'GET', url: approve(login.headers.location as string), headers: { cookie: `anode_login=${loginCookie}` } })
  expect(cb.statusCode).toBe(302)
  const { jar, raw } = cookies(cb)
  return { sid: jar.anode_session ?? '', location: cb.headers.location as string, raw }
}

const session = (sid: string) => app.inject({ method: 'GET', url: '/auth/session', headers: { cookie: `anode_session=${sid}` } })

describe('OIDC browser login (authorization code + PKCE)', () => {
  it('signs the user in with an HttpOnly, SameSite=Strict session cookie and maps the role from the ID token', async () => {
    await start()
    const { sid, location, raw } = await ssoLogin()
    expect(location).toBe('/')
    expect(raw.find(l => l.startsWith('anode_session='))).toMatch(/HttpOnly; SameSite=Strict/)

    const info = (await session(sid)).json()
    expect(info).toMatchObject({ auth_enabled: true, authenticated: true, role: 'editor', user: { name: 'Ada Lovelace', method: 'oidc' } })
    expect(info.csrf_token).toEqual(expect.any(String))
    expect((await app.inject({ method: 'GET', url: '/dags', headers: { cookie: `anode_session=${sid}` } })).statusCode).toBe(200)
    // Only a hash of the session id is stored
    expect(await db.collection('ui_sessions').countDocuments({ key_hash: sid })).toBe(0)
  })

  it('state is single-use and bound to the browser that started the login', async () => {
    await start()
    const login = await app.inject({ method: 'GET', url: '/auth/login' })
    const callback = approve(login.headers.location as string)

    const foreign = await app.inject({ method: 'GET', url: callback, headers: { cookie: 'anode_login=someone-else' } })
    expect(foreign.headers.location).toBe('/?login_error=invalid_state')
    expect(cookies(foreign).jar.anode_session).toBeUndefined()
    // The foreign attempt consumed the state — the real browser cannot reuse it either
    const replay = await app.inject({ method: 'GET', url: callback, headers: { cookie: `anode_login=${cookies(login).jar.anode_login}` } })
    expect(replay.headers.location).toBe('/?login_error=invalid_state')
  })

  it('rejects an ID token whose nonce does not match the login attempt', async () => {
    await start()
    idTokenOverrides = { nonce: 'attacker-nonce' }
    const { sid, location } = await ssoLogin()
    expect(location).toBe('/?login_error=invalid_id_token')
    expect(sid).toBe('')
  })

  it('a valid login without a mapped role gets no session', async () => {
    await start()
    nextUser = { sub: 'u-2', groups: ['marketing'] }
    expect((await ssoLogin()).location).toBe('/?login_error=no_role')
  })

  it('return_to only accepts same-origin paths', async () => {
    await start()
    expect((await ssoLogin('/?dag=etl')).location).toBe('/?dag=etl')
    expect((await ssoLogin('//evil.example.com')).location).toBe('/')
    expect((await ssoLogin('https://evil.example.com')).location).toBe('/')
  })

  it('an IdP error comes back as a login_error, not a session', async () => {
    await start()
    const res = await app.inject({ method: 'GET', url: '/auth/callback?error=access_denied&state=x' })
    expect(res.headers.location).toBe('/?login_error=idp_error')
  })

  it('/auth/login is 404 when no OIDC client is configured', async () => {
    await start({ OIDC_CLIENT_ID: '', OIDC_AUDIENCE: 'api' })
    expect((await app.inject({ method: 'GET', url: '/auth/login' })).statusCode).toBe(404)
    expect((await app.inject({ method: 'GET', url: '/auth/session' })).json().methods).toEqual({ sso: false, api_key: true })
  })
})

describe('session cookie: CSRF, expiry, logout', () => {
  it('state-changing requests need the session CSRF token; Bearer callers need none', async () => {
    await start()
    const { sid } = await ssoLogin()
    const csrf = (await session(sid)).json().csrf_token as string
    const cookie = `anode_session=${sid}`

    expect((await app.inject({ method: 'POST', url: '/pools', headers: { cookie }, payload: { name: 'p', slots: 1 } })).statusCode).toBe(403)
    expect((await app.inject({ method: 'POST', url: '/pools', headers: { cookie, 'x-csrf-token': 'wrong' }, payload: { name: 'p', slots: 1 } })).statusCode).toBe(403)
    expect((await app.inject({ method: 'POST', url: '/pools', headers: { cookie, 'x-csrf-token': csrf }, payload: { name: 'p', slots: 1 } })).statusCode).toBeLessThan(300)
    expect((await app.inject({ method: 'POST', url: '/pools', headers: { authorization: `Bearer ${ADMIN_KEY}` }, payload: { name: 'q', slots: 1 } })).statusCode).toBeLessThan(300)
  })

  it('an expired session is no longer accepted', async () => {
    await start({ SESSION_TTL_MS: '50' })
    const { sid } = await ssoLogin()
    await new Promise(r => setTimeout(r, 80))
    expect((await session(sid)).json().authenticated).toBe(false)
    expect((await app.inject({ method: 'GET', url: '/dags', headers: { cookie: `anode_session=${sid}` } })).statusCode).toBe(401)
  })

  it('logout revokes the session server-side and returns the IdP logout URL', async () => {
    await start()
    const { sid } = await ssoLogin()
    const csrf = (await session(sid)).json().csrf_token as string

    expect((await app.inject({ method: 'POST', url: '/auth/logout', headers: { cookie: `anode_session=${sid}` } })).statusCode).toBe(403)
    const out = await app.inject({ method: 'POST', url: '/auth/logout', headers: { cookie: `anode_session=${sid}`, 'x-csrf-token': csrf } })
    const logoutUrl = new URL(out.json().logout_url)
    expect(`${logoutUrl.origin}${logoutUrl.pathname}`).toBe(`${ISS}/logout`)
    expect(logoutUrl.searchParams.get('post_logout_redirect_uri')).toBe('http://localhost/bye')
    expect(logoutUrl.searchParams.get('id_token_hint')).toEqual(expect.any(String))
    expect((await app.inject({ method: 'GET', url: '/dags', headers: { cookie: `anode_session=${sid}` } })).statusCode).toBe(401)
  })
})

describe('API-key sign-in', () => {
  it('exchanges a valid key for a session; an invalid key gets 401 and no cookie', async () => {
    await start()
    const bad = await app.inject({ method: 'POST', url: '/auth/key-login', payload: { key: 'nope' } })
    expect(bad.statusCode).toBe(401)
    expect(cookies(bad).jar.anode_session).toBeUndefined()

    const ok = await app.inject({ method: 'POST', url: '/auth/key-login', payload: { key: ADMIN_KEY } })
    expect(ok.json()).toMatchObject({ role: 'admin', csrf_token: expect.any(String) })
    const sid = cookies(ok).jar.anode_session
    expect((await session(sid)).json()).toMatchObject({ authenticated: true, role: 'admin', user: { method: 'api_key' } })
  })

  it('cookies are Secure behind an HTTPS proxy', async () => {
    await start()
    const ok = await app.inject({ method: 'POST', url: '/auth/key-login', headers: { 'x-forwarded-proto': 'https' }, payload: { key: ADMIN_KEY } })
    expect(cookies(ok).raw[0]).toMatch(/; Secure$/)
  })
})

describe('auth disabled', () => {
  it('reports open access so the UI skips the login screen', async () => {
    for (const k of ['ADMIN_KEY', 'OIDC_ISSUER', 'OIDC_CLIENT_ID']) process.env[k] = ''
    vi.resetModules()
    const { buildServer } = await import('../../api/server.js')
    app = buildServer(db)
    await app.ready()
    expect((await app.inject({ method: 'GET', url: '/auth/session' })).json()).toMatchObject({ auth_enabled: false, authenticated: true })
  })
})

describe('UI assets and browser security headers', () => {
  it('index.html loads only same-origin scripts, under a CSP without unsafe-inline/eval', async () => {
    await start()
    const page = await app.inject({ method: 'GET', url: '/' })
    expect(page.statusCode).toBe(200)
    expect(page.body).not.toMatch(/https?:\/\//)
    expect(page.body).not.toMatch(/text\/babel/)
    const csp = String(page.headers['content-security-policy'])
    expect(csp).toContain("script-src 'self'")
    expect(csp).toContain("frame-ancestors 'none'")
    expect(csp).not.toMatch(/unsafe-(inline|eval)/)
    expect(page.headers['x-content-type-options']).toBe('nosniff')
  })

  it('serves React from node_modules and the UI compiled from JSX, without authentication', async () => {
    await start()
    const react = await app.inject({ method: 'GET', url: '/vendor/react.js' })
    expect(react.statusCode).toBe(200)
    expect(react.body).toContain('react.production.min.js')
    const ui = await app.inject({ method: 'GET', url: '/app.js' })
    expect(ui.statusCode).toBe(200)
    expect(ui.headers['content-type']).toMatch(/javascript/)
    expect(ui.body).toContain('React.createElement')
    expect((await app.inject({ method: 'GET', url: '/vendor/../package.json' })).statusCode).not.toBe(200)
  })
})
