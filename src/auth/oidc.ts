/**
 * OIDC bearer-token validation (resource-server mode).
 *
 * Verifies JWT access/ID tokens issued by an OIDC provider (PingFed, Azure AD, Okta, ...)
 * against its JWKS, then maps a claim (roles/groups) to an airflow-nodejs role.
 * There is NO browser login flow here — callers obtain a token from the IdP themselves.
 *
 * No dependencies: signature checks use node:crypto. Only RS*, ES* are accepted (never `none`
 * or HS*), and the JWK's key type must match the token's `alg`.
 *
 * Env:
 *   OIDC_ISSUER        required to enable; exact `iss` the token must carry
 *   OIDC_AUDIENCE      required; token `aud` (string or array) must contain it
 *   OIDC_JWKS_URI      optional; defaults to the `jwks_uri` in <issuer>/.well-known/openid-configuration
 *   OIDC_ROLE_CLAIM    claim holding roles/groups (default `roles`; dotted path ok, e.g. `realm_access.roles`)
 *   OIDC_ADMIN_VALUES / OIDC_EDITOR_VALUES / OIDC_VIEWER_VALUES   comma-separated claim values per role
 *   OIDC_DEFAULT_ROLE  optional role for valid tokens with no matching value (default: none → 403)
 */
import { createPublicKey, verify as cryptoVerify, type JsonWebKey, type KeyObject } from 'node:crypto'
import type { Role } from './keys.js'

export interface OidcConfig {
  issuer: string
  audience: string
  jwksUri: string | null
  roleClaim: string
  roleValues: Record<Role, string[]>
  defaultRole: Role | null
}

export type OidcConfigResult =
  | { enabled: false }
  | { enabled: true; config: OidcConfig }
  | { enabled: true; error: string }   // misconfigured → auth stays on, no token is valid

const ROLES: Role[] = ['viewer', 'editor', 'admin']
const csv = (v: string | undefined) => (v ?? '').split(',').map(s => s.trim()).filter(Boolean)

export function loadOidcConfig(env: NodeJS.ProcessEnv): OidcConfigResult {
  const issuer = env.OIDC_ISSUER?.trim()
  if (!issuer) return { enabled: false }
  const audience = env.OIDC_AUDIENCE?.trim()
  if (!audience) return { enabled: true, error: 'OIDC_ISSUER is set but OIDC_AUDIENCE is missing — refusing to accept any token' }
  const defaultRole = env.OIDC_DEFAULT_ROLE?.trim() as Role | undefined
  if (defaultRole && !ROLES.includes(defaultRole)) {
    return { enabled: true, error: `OIDC_DEFAULT_ROLE '${defaultRole}' is not one of ${ROLES.join('/')}` }
  }
  return {
    enabled: true,
    config: {
      issuer,
      audience,
      jwksUri: env.OIDC_JWKS_URI?.trim() || null,
      roleClaim: env.OIDC_ROLE_CLAIM?.trim() || 'roles',
      roleValues: { admin: csv(env.OIDC_ADMIN_VALUES), editor: csv(env.OIDC_EDITOR_VALUES), viewer: csv(env.OIDC_VIEWER_VALUES) },
      defaultRole: defaultRole ?? null,
    },
  }
}

// ── JWT verification ────────────────────────────────────────────────────────

const ALG_PARAMS: Record<string, { kty: 'RSA' | 'EC'; hash: string; crv?: string }> = {
  RS256: { kty: 'RSA', hash: 'sha256' },
  RS384: { kty: 'RSA', hash: 'sha384' },
  RS512: { kty: 'RSA', hash: 'sha512' },
  ES256: { kty: 'EC', hash: 'sha256', crv: 'P-256' },
  ES384: { kty: 'EC', hash: 'sha384', crv: 'P-384' },
}

export type FetchLike = (url: string, init?: { signal?: AbortSignal }) => Promise<{ ok: boolean; status: number; json(): Promise<unknown> }>

export interface OidcVerifierOptions {
  fetchFn?: FetchLike
  /** Epoch ms; injectable for tests. */
  now?: () => number
  /** Seconds of clock skew tolerated on exp/nbf. Default 60. */
  leewaySeconds?: number
  /** Min ms between JWKS refetches. Default 60_000. */
  jwksRefreshMs?: number
}

export interface OidcVerifier {
  /** Resolves the validated claims, or null when the token is invalid for ANY reason. */
  verify(token: string): Promise<Record<string, unknown> | null>
  /** Role granted by the claims, or null when none applies. */
  roleFor(claims: Record<string, unknown>): Role | null
}

const b64urlToBuf = (s: string) => Buffer.from(s, 'base64url')

function parseJson(buf: Buffer): Record<string, unknown> | null {
  try {
    const v = JSON.parse(buf.toString('utf8')) as unknown
    return v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : null
  } catch { return null }
}

function getPath(obj: Record<string, unknown>, path: string): unknown {
  return path.split('.').reduce<unknown>((cur, key) => (cur && typeof cur === 'object' ? (cur as Record<string, unknown>)[key] : undefined), obj)
}

export function createOidcVerifier(config: OidcConfig, opts: OidcVerifierOptions = {}): OidcVerifier {
  const fetchFn: FetchLike = opts.fetchFn ?? ((url, init) => fetch(url, init))
  const now = opts.now ?? Date.now
  const leeway = opts.leewaySeconds ?? 60
  const refreshMs = opts.jwksRefreshMs ?? 60_000

  let keys = new Map<string, JsonWebKey>()   // kid → jwk
  let lastFetch = -Infinity
  let inflight: Promise<void> | null = null
  let jwksUri = config.jwksUri

  async function getJson(url: string): Promise<unknown> {
    const ctl = new AbortController()
    const timer = setTimeout(() => ctl.abort(), 5_000)
    try {
      const res = await fetchFn(url, { signal: ctl.signal })
      if (!res.ok) throw new Error(`HTTP ${res.status}`)
      return await res.json()
    } finally { clearTimeout(timer) }
  }

  function refreshKeys(): Promise<void> {
    if (inflight) return inflight
    if (now() - lastFetch < refreshMs) return Promise.resolve()
    lastFetch = now()
    inflight = (async () => {
      try {
        if (!jwksUri) {
          const disco = await getJson(`${config.issuer.replace(/\/$/, '')}/.well-known/openid-configuration`) as { jwks_uri?: string }
          if (!disco?.jwks_uri) throw new Error('discovery document has no jwks_uri')
          jwksUri = disco.jwks_uri
        }
        const doc = await getJson(jwksUri) as { keys?: JsonWebKey[] }
        const next = new Map<string, JsonWebKey>()
        for (const k of doc.keys ?? []) if (typeof k.kid === 'string') next.set(k.kid, k)
        keys = next
      } catch (err) {
        console.warn(`[auth] OIDC JWKS fetch failed: ${err instanceof Error ? err.message : String(err)}`)
      } finally { inflight = null }
    })()
    return inflight
  }

  async function findKey(kid: string): Promise<JsonWebKey | undefined> {
    if (!keys.has(kid)) await refreshKeys()   // unknown kid → at most one refetch per refreshMs (key rotation)
    return keys.get(kid)
  }

  async function verifyToken(token: string): Promise<Record<string, unknown> | null> {
    const parts = token.split('.')
    if (parts.length !== 3 || parts.some(p => p.length === 0)) return null
    const header = parseJson(b64urlToBuf(parts[0]))
    const claims = parseJson(b64urlToBuf(parts[1]))
    if (!header || !claims) return null

    const alg = typeof header.alg === 'string' ? header.alg : ''
    const params = ALG_PARAMS[alg]
    const kid = typeof header.kid === 'string' ? header.kid : ''
    if (!params || !kid) return null

    const jwk = await findKey(kid)
    // The key type (and curve) must match the algorithm — blocks alg-confusion attacks
    if (!jwk || jwk.kty !== params.kty || (params.crv && jwk.crv !== params.crv)) return null
    if (jwk.use && jwk.use !== 'sig') return null

    let key: KeyObject
    try { key = createPublicKey({ key: jwk, format: 'jwk' }) } catch { return null }

    let valid = false
    try {
      valid = cryptoVerify(
        params.hash,
        Buffer.from(`${parts[0]}.${parts[1]}`),
        params.kty === 'EC' ? { key, dsaEncoding: 'ieee-p1363' } : key,
        b64urlToBuf(parts[2]),
      )
    } catch { return null }
    if (!valid) return null

    // Claims
    const nowSec = Math.floor(now() / 1000)
    if (claims.iss !== config.issuer) return null
    const aud = claims.aud
    if (!(typeof aud === 'string' ? aud === config.audience : Array.isArray(aud) && aud.includes(config.audience))) return null
    if (typeof claims.exp !== 'number' || nowSec > claims.exp + leeway) return null
    if (claims.nbf !== undefined && (typeof claims.nbf !== 'number' || nowSec < claims.nbf - leeway)) return null
    return claims
  }

  return {
    verify: token => verifyToken(token).catch(() => null),
    roleFor(claims) {
      const raw = getPath(claims, config.roleClaim)
      const values = new Set((Array.isArray(raw) ? raw : raw === undefined ? [] : [raw]).filter((v): v is string => typeof v === 'string'))
      // Highest matching role wins
      for (const role of [...ROLES].reverse()) {
        if (config.roleValues[role].some(v => values.has(v))) return role
      }
      return config.defaultRole
    },
  }
}
