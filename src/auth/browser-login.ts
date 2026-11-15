/**
 * OIDC browser login — authorization code flow with PKCE (RFC 7636, S256), as a relying party.
 *
 *   GET /auth/login     → state + nonce + code_verifier stored server-side, browser sent to the IdP
 *   GET /auth/callback  → code exchanged at the token endpoint (with the verifier), ID token verified
 *                         (signature, iss, aud = client id, exp, nonce), role mapped, session created
 *
 * The client may be public (PKCE only) or confidential (OIDC_CLIENT_SECRET, sent as HTTP Basic —
 * client_secret_basic, the spec default). Tokens never reach the browser: the UI gets a session cookie.
 */
import { createHash } from 'node:crypto'
import type { Role } from './keys.js'
import type { OidcConfig, OidcVerifier } from './oidc.js'

export const pkceChallenge = (verifier: string) => createHash('sha256').update(verifier).digest('base64url')

/** Only same-origin relative paths: blocks open redirects such as `//evil.com` or `https://evil.com`. */
export function safeReturnTo(raw: unknown): string {
  if (typeof raw !== 'string' || !raw.startsWith('/') || raw.startsWith('//') || raw.includes('\\')) return '/'
  return raw
}

export async function buildAuthorizationUrl(
  verifier: OidcVerifier, config: OidcConfig,
  p: { state: string; nonce: string; codeVerifier: string; redirectUri: string },
): Promise<string> {
  const disco = await verifier.discovery()
  if (!disco.authorization_endpoint) throw new Error('OIDC discovery document has no authorization_endpoint')
  const url = new URL(disco.authorization_endpoint)
  url.searchParams.set('response_type', 'code')
  url.searchParams.set('client_id', config.clientId!)
  url.searchParams.set('redirect_uri', p.redirectUri)
  url.searchParams.set('scope', config.scopes)
  url.searchParams.set('state', p.state)
  url.searchParams.set('nonce', p.nonce)
  url.searchParams.set('code_challenge', pkceChallenge(p.codeVerifier))
  url.searchParams.set('code_challenge_method', 'S256')
  return url.toString()
}

export interface TokenResponse { idToken: string; accessToken: string | null }

export async function exchangeCode(
  verifier: OidcVerifier, config: OidcConfig,
  p: { code: string; codeVerifier: string; redirectUri: string },
): Promise<TokenResponse> {
  const disco = await verifier.discovery()
  if (!disco.token_endpoint) throw new Error('OIDC discovery document has no token_endpoint')
  const body = new URLSearchParams({
    grant_type: 'authorization_code', code: p.code, redirect_uri: p.redirectUri,
    code_verifier: p.codeVerifier, client_id: config.clientId!,
  })
  const headers: Record<string, string> = { 'content-type': 'application/x-www-form-urlencoded', accept: 'application/json' }
  if (config.clientSecret) {
    const basic = `${encodeURIComponent(config.clientId!)}:${encodeURIComponent(config.clientSecret)}`
    headers.authorization = `Basic ${Buffer.from(basic).toString('base64')}`
  }
  const res = await fetch(disco.token_endpoint, { method: 'POST', headers, body, signal: AbortSignal.timeout(10_000) })
  const json = await res.json().catch(() => null) as { id_token?: string; access_token?: string; error?: string } | null
  if (!res.ok || !json?.id_token) {
    throw new Error(`token endpoint returned HTTP ${res.status}${json?.error ? ` (${json.error})` : ''}${json && !json.id_token && res.ok ? ' without an id_token' : ''}`)
  }
  return { idToken: json.id_token, accessToken: json.access_token ?? null }
}

export type LoginIdentity =
  | { ok: true; role: Role; subject: string; name: string | null }
  | { ok: false; error: 'invalid_id_token' | 'no_role' }

/**
 * Verify the ID token (aud = client id, nonce) and map its claims to a role. Providers that put roles
 * in the access token instead (Azure AD app roles, Keycloak) are covered: when the ID token carries no
 * mapped role and the access token is a JWT for the configured audience, its claims are used.
 */
export async function resolveLoginIdentity(
  verifier: OidcVerifier, config: OidcConfig, tokens: TokenResponse, nonce: string,
): Promise<LoginIdentity> {
  const claims = await verifier.verify(tokens.idToken, { audience: config.clientId!, nonce })
  if (!claims || typeof claims.sub !== 'string') return { ok: false, error: 'invalid_id_token' }

  let role = verifier.roleFor(claims)
  if (!role && tokens.accessToken && tokens.accessToken.split('.').length === 3) {
    const atClaims = await verifier.verify(tokens.accessToken)
    if (atClaims && atClaims.sub === claims.sub) role = verifier.roleFor(atClaims)
  }
  if (!role) return { ok: false, error: 'no_role' }

  const name = [claims.name, claims.preferred_username, claims.email].find(v => typeof v === 'string') as string | undefined
  return { ok: true, role, subject: claims.sub, name: name ?? null }
}

/** RP-initiated logout URL, or null when the provider has no end_session_endpoint. */
export async function buildLogoutUrl(verifier: OidcVerifier, config: OidcConfig, idToken: string | null): Promise<string | null> {
  const disco = await verifier.discovery().catch(() => null)
  if (!disco?.end_session_endpoint) return null
  const url = new URL(disco.end_session_endpoint)
  if (idToken) url.searchParams.set('id_token_hint', idToken)
  if (config.clientId) url.searchParams.set('client_id', config.clientId)
  if (config.postLogoutRedirectUri) url.searchParams.set('post_logout_redirect_uri', config.postLogoutRedirectUri)
  return url.toString()
}
