import { describe, it, expect, vi } from 'vitest'
import { generateKeyPairSync, createSign, createHmac, sign as cryptoSign, type KeyObject } from 'node:crypto'
import { loadOidcConfig, createOidcVerifier, type OidcConfig, type FetchLike } from '../oidc.js'

const ISS = 'https://idp.example.com'
const AUD = 'airflow-nodejs'
const NOW = 1_800_000_000_000   // fixed clock (ms)
const nowSec = NOW / 1000

const rsa = generateKeyPairSync('rsa', { modulusLength: 2048 })
const rsa2 = generateKeyPairSync('rsa', { modulusLength: 2048 })
const ec = generateKeyPairSync('ec', { namedCurve: 'P-256' })

const b64 = (o: unknown) => Buffer.from(JSON.stringify(o)).toString('base64url')
const jwkOf = (pub: KeyObject, kid: string, extra: Record<string, unknown> = {}) => ({ ...pub.export({ format: 'jwk' }), kid, ...extra })

function signJwt(
  priv: KeyObject, header: Record<string, unknown>, claims: Record<string, unknown>,
): string {
  const input = `${b64(header)}.${b64(claims)}`
  const alg = header.alg as string
  const sig = alg.startsWith('ES')
    ? cryptoSign('sha256', Buffer.from(input), { key: priv, dsaEncoding: 'ieee-p1363' })
    : createSign('RSA-SHA256').update(input).sign(priv)
  return `${input}.${sig.toString('base64url')}`
}

const goodClaims = (over: Record<string, unknown> = {}) => ({ iss: ISS, aud: AUD, exp: nowSec + 600, roles: ['airflow-admin'], ...over })
const config: OidcConfig = {
  issuer: ISS, audience: AUD, jwksUri: `${ISS}/jwks`, roleClaim: 'roles',
  roleValues: { admin: ['airflow-admin'], editor: ['airflow-editor'], viewer: ['airflow-viewer'] }, defaultRole: null,
}

function jwksFetch(keys: unknown[]) {
  return vi.fn<FetchLike>(async () => ({ ok: true, status: 200, json: async () => ({ keys }) }))
}
const verifier = (fetchFn: FetchLike, over: Partial<OidcConfig> = {}, opts = {}) =>
  createOidcVerifier({ ...config, ...over }, { fetchFn, now: () => NOW, ...opts })

describe('loadOidcConfig', () => {
  it('disabled without OIDC_ISSUER', () => {
    expect(loadOidcConfig({})).toEqual({ enabled: false })
  })
  it('issuer without audience is an error (auth stays on, nothing accepted)', () => {
    const r = loadOidcConfig({ OIDC_ISSUER: ISS })
    expect(r).toMatchObject({ enabled: true, error: expect.stringContaining('OIDC_AUDIENCE') })
  })
  it('rejects an unknown default role', () => {
    expect(loadOidcConfig({ OIDC_ISSUER: ISS, OIDC_AUDIENCE: AUD, OIDC_DEFAULT_ROLE: 'root' })).toMatchObject({ error: expect.any(String) })
  })
  it('parses role values and defaults', () => {
    const r = loadOidcConfig({ OIDC_ISSUER: ISS, OIDC_AUDIENCE: AUD, OIDC_ADMIN_VALUES: 'a, b', OIDC_VIEWER_VALUES: 'v' })
    expect(r).toMatchObject({ enabled: true, config: { roleClaim: 'roles', jwksUri: null, defaultRole: null, roleValues: { admin: ['a', 'b'], editor: [], viewer: ['v'] } } })
  })
})

describe('createOidcVerifier — accepts valid tokens', () => {
  it('RS256', async () => {
    const v = verifier(jwksFetch([jwkOf(rsa.publicKey, 'k1')]))
    const claims = await v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims()))
    expect(claims).toMatchObject({ iss: ISS })
  })
  it('ES256 (raw r||s signature)', async () => {
    const v = verifier(jwksFetch([jwkOf(ec.publicKey, 'e1')]))
    expect(await v.verify(signJwt(ec.privateKey, { alg: 'ES256', kid: 'e1' }, goodClaims()))).not.toBeNull()
  })
  it('aud as an array containing the audience', async () => {
    const v = verifier(jwksFetch([jwkOf(rsa.publicKey, 'k1')]))
    expect(await v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ aud: ['other', AUD] })))).not.toBeNull()
  })
  it('tolerates small clock skew on exp, not large', async () => {
    const v = verifier(jwksFetch([jwkOf(rsa.publicKey, 'k1')]))
    const t = (exp: number) => signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ exp }))
    expect(await v.verify(t(nowSec - 30))).not.toBeNull()
    expect(await v.verify(t(nowSec - 120))).toBeNull()
  })
})

describe('createOidcVerifier — rejects', () => {
  const k = jwkOf(rsa.publicKey, 'k1')
  const cases: Array<[string, (v: ReturnType<typeof verifier>) => Promise<unknown>]> = [
    ['expired token', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ exp: nowSec - 3600 })))],
    ['missing exp', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, (({ exp: _e, ...r }) => r)(goodClaims())))],
    ['non-numeric exp', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ exp: '9999999999' })))],
    ['not-yet-valid (nbf)', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ nbf: nowSec + 3600 })))],
    ['wrong issuer', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ iss: 'https://evil.example.com' })))],
    ['issuer with trailing slash (exact match)', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ iss: `${ISS}/` })))],
    ['wrong audience', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ aud: 'someone-else' })))],
    ['missing audience', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, (({ aud: _a, ...r }) => r)(goodClaims())))],
    ['signed by a different key (same kid)', v => v.verify(signJwt(rsa2.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims()))],
    ['tampered payload', async v => {
      const [h, , s] = signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims({ roles: ['airflow-viewer'] })).split('.')
      return v.verify(`${h}.${b64(goodClaims({ roles: ['airflow-admin'] }))}.${s}`)
    }],
    ['alg none', v => v.verify(`${b64({ alg: 'none', kid: 'k1' })}.${b64(goodClaims())}.`)],
    ['alg none with junk signature', v => v.verify(`${b64({ alg: 'none', kid: 'k1' })}.${b64(goodClaims())}.AAAA`)],
    ['HS256 signed with the public key as secret (alg confusion)', async v => {
      const input = `${b64({ alg: 'HS256', kid: 'k1' })}.${b64(goodClaims())}`
      const secret = rsa.publicKey.export({ type: 'spki', format: 'pem' })
      return v.verify(`${input}.${createHmac('sha256', secret).update(input).digest('base64url')}`)
    }],
    ['RS256 header but the JWKS key is EC', v => v.verify(signJwt(ec.privateKey, { alg: 'RS256', kid: 'e1' }, goodClaims()))],
    ['no kid', v => v.verify(signJwt(rsa.privateKey, { alg: 'RS256' }, goodClaims()))],
    ['garbage', v => v.verify('not-a-jwt')],
    ['empty segments', v => v.verify('..')],
    ['non-JSON header', v => v.verify(`${Buffer.from('xx').toString('base64url')}.${b64(goodClaims())}.sig`)],
  ]
  it.each(cases)('%s', async (_name, run) => {
    const v = verifier(jwksFetch([k, jwkOf(ec.publicKey, 'e1')]))
    expect(await run(v)).toBeNull()
  })

  it('a key marked use=enc is not used for signatures', async () => {
    const v = verifier(jwksFetch([jwkOf(rsa.publicKey, 'k1', { use: 'enc' })]))
    expect(await v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims()))).toBeNull()
  })

  it('returns null (never throws) when the JWKS endpoint is down', async () => {
    const fetchFn = vi.fn<FetchLike>(async () => { throw new Error('ECONNREFUSED') })
    vi.spyOn(console, 'warn').mockImplementation(() => {})
    const v = verifier(fetchFn)
    expect(await v.verify(signJwt(rsa.privateKey, { alg: 'RS256', kid: 'k1' }, goodClaims()))).toBeNull()
  })
})

describe('JWKS caching and rotation', () => {
  const tok = (kid: string, priv = rsa.privateKey) => signJwt(priv, { alg: 'RS256', kid }, goodClaims())

  it('fetches once for repeated valid tokens', async () => {
    const f = jwksFetch([jwkOf(rsa.publicKey, 'k1')])
    const v = verifier(f)
    await v.verify(tok('k1')); await v.verify(tok('k1')); await v.verify(tok('k1'))
    expect(f).toHaveBeenCalledTimes(1)
  })

  it('refetches for an unknown kid once the refresh interval passed (key rotation)', async () => {
    let t = NOW
    let keys = [jwkOf(rsa.publicKey, 'old')]
    const f = vi.fn<FetchLike>(async () => ({ ok: true, status: 200, json: async () => ({ keys }) }))
    const v = createOidcVerifier(config, { fetchFn: f, now: () => t })
    expect(await v.verify(tok('old'))).not.toBeNull()

    keys = [jwkOf(rsa2.publicKey, 'new')]
    t += 61_000
    expect(await v.verify(tok('new', rsa2.privateKey))).not.toBeNull()
    expect(f).toHaveBeenCalledTimes(2)
  })

  it('does not refetch on every unknown kid (rate limited) — junk tokens cannot hammer the IdP', async () => {
    const f = jwksFetch([jwkOf(rsa.publicKey, 'k1')])
    const v = verifier(f)
    await v.verify(tok('k1'))
    for (let i = 0; i < 20; i++) await v.verify(tok(`bogus${i}`))
    expect(f).toHaveBeenCalledTimes(1)
  })

  it('coalesces concurrent first requests into one fetch', async () => {
    const f = jwksFetch([jwkOf(rsa.publicKey, 'k1')])
    const v = verifier(f)
    await Promise.all(Array.from({ length: 10 }, () => v.verify(tok('k1'))))
    expect(f).toHaveBeenCalledTimes(1)
  })

  it('discovers jwks_uri from the issuer when OIDC_JWKS_URI is not set', async () => {
    const urls: string[] = []
    const f = vi.fn<FetchLike>(async (url) => {
      urls.push(url)
      return { ok: true, status: 200, json: async () => (url.endsWith('openid-configuration') ? { jwks_uri: `${ISS}/keys` } : { keys: [jwkOf(rsa.publicKey, 'k1')] }) }
    })
    const v = verifier(f, { jwksUri: null })
    expect(await v.verify(tok('k1'))).not.toBeNull()
    expect(urls).toEqual([`${ISS}/.well-known/openid-configuration`, `${ISS}/keys`])
  })
})

describe('roleFor', () => {
  const v = verifier(jwksFetch([]))
  it('maps claim values to roles, highest wins', () => {
    expect(v.roleFor({ roles: ['airflow-viewer', 'airflow-editor'] })).toBe('editor')
    expect(v.roleFor({ roles: ['airflow-viewer', 'airflow-admin', 'x'] })).toBe('admin')
    expect(v.roleFor({ roles: ['airflow-viewer'] })).toBe('viewer')
  })
  it('accepts a scalar claim', () => {
    expect(v.roleFor({ roles: 'airflow-editor' })).toBe('editor')
  })
  it('null when nothing matches or the claim is missing/odd', () => {
    expect(v.roleFor({ roles: ['other'] })).toBeNull()
    expect(v.roleFor({})).toBeNull()
    expect(v.roleFor({ roles: [1, null, { a: 1 }] })).toBeNull()
  })
  it('falls back to defaultRole when configured', () => {
    expect(verifier(jwksFetch([]), { defaultRole: 'viewer' }).roleFor({ roles: ['other'] })).toBe('viewer')
  })
  it('supports a dotted claim path', () => {
    const d = verifier(jwksFetch([]), { roleClaim: 'realm_access.roles' })
    expect(d.roleFor({ realm_access: { roles: ['airflow-admin'] } })).toBe('admin')
    expect(d.roleFor({ realm_access: 'nope' })).toBeNull()
  })
})
