/** authHook with OIDC enabled, public-path hardening, and the /openapi.json index. */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { generateKeyPairSync, createSign } from 'node:crypto'

const ISS = 'https://idp.example.com'
const AUD = 'airflow-nodejs'
const rsa = generateKeyPairSync('rsa', { modulusLength: 2048 })
const b64 = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url')
function jwt(claims: Record<string, unknown>): string {
  const input = `${b64({ alg: 'RS256', kid: 'k1' })}.${b64({ iss: ISS, aud: AUD, exp: Math.floor(Date.now() / 1000) + 300, ...claims })}`
  return `${input}.${createSign('RSA-SHA256').update(input).sign(rsa.privateKey).toString('base64url')}`
}

const ENV_KEYS = ['API_KEYS', 'ADMIN_KEY', 'OIDC_ISSUER', 'OIDC_AUDIENCE', 'OIDC_JWKS_URI', 'OIDC_ADMIN_VALUES', 'OIDC_EDITOR_VALUES', 'OIDC_VIEWER_VALUES', 'OIDC_DEFAULT_ROLE']
let saved: Record<string, string | undefined>

beforeEach(() => {
  vi.resetModules()
  saved = Object.fromEntries(ENV_KEYS.map(k => [k, process.env[k]]))
  for (const k of ENV_KEYS) delete process.env[k]
  vi.spyOn(console, 'log').mockImplementation(() => {})
  vi.spyOn(console, 'error').mockImplementation(() => {})
  vi.stubGlobal('fetch', vi.fn(async () => ({ ok: true, status: 200, json: async () => ({ keys: [{ ...rsa.publicKey.export({ format: 'jwk' }), kid: 'k1' }] }) })))
})
afterEach(() => {
  for (const k of ENV_KEYS) { if (saved[k] === undefined) delete process.env[k]; else process.env[k] = saved[k] }
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

function enableOidc(extra: Record<string, string> = {}) {
  Object.assign(process.env, {
    OIDC_ISSUER: ISS, OIDC_AUDIENCE: AUD, OIDC_JWKS_URI: `${ISS}/jwks`,
    OIDC_ADMIN_VALUES: 'admins', OIDC_EDITOR_VALUES: 'editors', OIDC_VIEWER_VALUES: 'viewers', ...extra,
  })
}

async function call(url: string, token?: string, method = 'GET') {
  const mod = await import('../index.js')
  const req = { url, method, headers: token ? { authorization: `Bearer ${token}` } : {}, routeOptions: {} } as never
  const send = vi.fn()
  const reply = { status: vi.fn().mockReturnThis(), send } as never
  await mod.authHook(req, reply)
  const r = reply as unknown as { status: ReturnType<typeof vi.fn> }
  return { status: r.status.mock.calls[0]?.[0] as number | undefined, body: send.mock.calls[0]?.[0], authRole: (req as { authRole?: string }).authRole }
}

describe('authHook + OIDC', () => {
  it('turns auth on when OIDC_ISSUER is set', async () => {
    enableOidc()
    expect((await import('../index.js')).AUTH_ENABLED).toBe(true)
  })

  it.each([
    ['admins', 'DELETE', 200, 'admin'],
    ['editors', 'POST', 200, 'editor'],
    ['viewers', 'GET', 200, 'viewer'],
    ['viewers', 'POST', 403, undefined],
    ['editors', 'GET', 200, 'editor'],
  ])('group %s doing %s → %s', async (group, method, status, role) => {
    enableOidc()
    const r = await call('/dags/x', jwt({ roles: [group] }), method)
    expect(r.status ?? 200).toBe(status)
    if (status === 200) expect(r.authRole).toBe(role)
  })

  it('admin-only /api-keys needs the admin role', async () => {
    enableOidc()
    expect((await call('/api-keys', jwt({ roles: ['editors'] }))).status).toBe(403)
    expect((await call('/api-keys', jwt({ roles: ['admins'] }))).status).toBeUndefined()
  })

  it('valid token without a mapped role → 403 (not 401)', async () => {
    enableOidc()
    const r = await call('/dags', jwt({ roles: ['randos'] }))
    expect(r.status).toBe(403)
  })

  it('OIDC_DEFAULT_ROLE grants a fallback role', async () => {
    enableOidc({ OIDC_DEFAULT_ROLE: 'viewer' })
    expect((await call('/dags', jwt({ roles: ['randos'] }))).authRole).toBe('viewer')
  })

  it('invalid / expired / foreign tokens → 401', async () => {
    enableOidc()
    expect((await call('/dags', jwt({ roles: ['admins'], exp: 1 }))).status).toBe(401)
    expect((await call('/dags', jwt({ roles: ['admins'], aud: 'other' }))).status).toBe(401)
    expect((await call('/dags', 'a.b.c')).status).toBe(401)
    expect((await call('/dags')).status).toBe(401)
  })

  it('API keys keep working alongside OIDC', async () => {
    enableOidc({ API_KEYS: 'legacy-key' })
    expect((await call('/api-keys', 'legacy-key')).authRole).toBe('admin')
  })

  it('a JWT-shaped string is not accepted when OIDC is off', async () => {
    process.env.API_KEYS = 'only-key'
    expect((await call('/dags', jwt({ roles: ['admins'] }))).status).toBe(401)
  })

  it('issuer without audience: auth stays ON and no token is accepted', async () => {
    enableOidc(); delete process.env.OIDC_AUDIENCE
    const mod = await import('../index.js')
    expect(mod.AUTH_ENABLED).toBe(true)
    expect((await call('/dags', jwt({ roles: ['admins'] }))).status).toBe(401)
  })
})

describe('isPublicPath', () => {
  it.each(['/', '/health', '/health?x=1', '/assets/app.js', '/index.js', '/app.css', '/ui-utils.js?v=2'])('%s is public', async (u) => {
    expect((await import('../index.js')).isPublicPath(u)).toBe(true)
  })
  it.each(['/api-keys?x=.js', '/variables?file=a.css', '/dags/my.js', '/dags/x.css', '/assetsfoo', '/api-keys', '/dags'])('%s is NOT public', async (u) => {
    expect((await import('../index.js')).isPublicPath(u)).toBe(false)
  })

  it('a query string ending in .js no longer bypasses auth', async () => {
    process.env.API_KEYS = 'k'
    expect((await call('/api-keys?x=.js')).status).toBe(401)
    expect((await call('/dags/some.css')).status).toBe(401)
  })
})
