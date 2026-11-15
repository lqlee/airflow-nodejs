/**
 * Remote secrets backends: HashiCorp Vault and Akeyless.
 *
 * Both are consulted for connections / variables not found in the database, like the env and file
 * backends. Lookups that fail (backend unreachable, permission denied) throw, so the task fails with
 * the real cause instead of a misleading "connection not found". A missing secret returns null.
 *
 * ── Vault (SECRETS_BACKEND=vault) — same layout as Airflow's VaultBackend ──────────────────────────
 *   VAULT_ADDR               https://vault.example.com   (required)
 *   Auth, first match wins:
 *     VAULT_TOKEN            a token
 *     VAULT_ROLE_ID + VAULT_SECRET_ID   AppRole login (VAULT_AUTH_PATH, default approle)
 *     VAULT_ROLE             Kubernetes login with the pod's service-account token
 *                            (VAULT_AUTH_PATH default kubernetes; VAULT_JWT_PATH default
 *                            /var/run/secrets/kubernetes.io/serviceaccount/token)
 *   VAULT_MOUNT              KV mount (default secret); VAULT_KV_VERSION 1 or 2 (default 2)
 *   VAULT_CONNECTIONS_PATH   default connections  → secret {conn_uri} or {conn_type, host, login, password, …}
 *   VAULT_VARIABLES_PATH     default variables    → secret {value}
 *   VAULT_NAMESPACE          Vault Enterprise namespace (optional)
 *
 * ── Akeyless (SECRETS_BACKEND=akeyless) ───────────────────────────────────────────────────────────
 *   AKEYLESS_API_URL         default https://api.akeyless.io (or your gateway's /api/v2 URL)
 *   Auth, first match wins:
 *     AKEYLESS_TOKEN         a token (t-…)
 *     AKEYLESS_ACCESS_ID + AKEYLESS_ACCESS_KEY            API-key login
 *     AKEYLESS_ACCESS_ID + AKEYLESS_K8S_AUTH_CONFIG_NAME  Kubernetes login with the pod's service-account
 *                            token (AKEYLESS_GATEWAY_URL is sent along, as Akeyless requires for k8s auth)
 *   AKEYLESS_CONNECTIONS_PATH  default /airflow/connections → static secret holding JSON or a connection URI
 *   AKEYLESS_VARIABLES_PATH    default /airflow/variables   → static secret holding the value
 *
 * Shared: SECRETS_TIMEOUT_MS (default 10000) per request; SECRETS_CACHE_TTL_MS (default 0 = off) caches
 * found secrets in-process.
 */
import { readFile } from 'node:fs/promises'
import type { ConnectionRuntime } from '../connections/index.js'
import type { SecretsBackend } from './index.js'
import { connectionFromFields, connectionFromSecretString } from './conn-format.js'

const SA_TOKEN_PATH = '/var/run/secrets/kubernetes.io/serviceaccount/token'

const env = (name: string) => {
  const v = process.env[name]?.trim()
  return v ? v : undefined
}
const timeoutMs = () => Number(env('SECRETS_TIMEOUT_MS') ?? 10_000)
const joinPath = (base: string, id: string) => `${base.replace(/\/+$/, '')}/${encodeURIComponent(id)}`

class HttpError extends Error {
  constructor(public readonly status: number, message: string) { super(message) }
}

async function requestJson(url: string, init: RequestInit, what: string): Promise<{ status: number; body: unknown }> {
  let res: Response
  try {
    res = await fetch(url, { ...init, signal: AbortSignal.timeout(timeoutMs()) })
  } catch (err) {
    throw new Error(`${what}: request failed: ${(err as Error).message}`)
  }
  const text = await res.text()
  let body: unknown = null
  try { body = text ? JSON.parse(text) : null } catch { body = text }
  return { status: res.status, body }
}

/** Positive-result cache shared by the remote backends. */
class SecretCache {
  private entries = new Map<string, { value: unknown; expires: number }>()
  get<T>(key: string): T | undefined {
    const e = this.entries.get(key)
    if (!e) return undefined
    if (e.expires < Date.now()) { this.entries.delete(key); return undefined }
    return e.value as T
  }
  set(key: string, value: unknown): void {
    const ttl = Number(env('SECRETS_CACHE_TTL_MS') ?? 0)
    if (ttl > 0 && value !== null) this.entries.set(key, { value, expires: Date.now() + ttl })
  }
}

/** A login token with its expiry; refreshed once on 401/403. */
abstract class TokenAuth {
  private token: string | null = null
  private expiresAt = 0

  protected abstract login(): Promise<{ token: string; ttlMs: number }>
  protected abstract readonly staticToken: string | undefined

  protected async getToken(force = false): Promise<string> {
    if (this.staticToken) return this.staticToken
    if (!force && this.token && Date.now() < this.expiresAt) return this.token
    const { token, ttlMs } = await this.login()
    this.token = token
    // Renew a little early; a non-expiring token is kept for an hour
    this.expiresAt = Date.now() + Math.max(10_000, (ttlMs > 0 ? ttlMs : 3_600_000) - 60_000)
    return token
  }

  /** Run a request with the token, logging in again once if it was rejected. */
  protected async withToken<T>(fn: (token: string) => Promise<T>): Promise<T> {
    try {
      return await fn(await this.getToken())
    } catch (err) {
      if (err instanceof HttpError && (err.status === 401 || err.status === 403) && !this.staticToken) {
        return fn(await this.getToken(true))
      }
      throw err
    }
  }
}

// ── Vault ────────────────────────────────────────────────────────────────────

export interface VaultOptions {
  addr: string
  token?: string
  roleId?: string
  secretId?: string
  role?: string
  authPath?: string
  jwtPath?: string
  mount: string
  kvVersion: 1 | 2
  connectionsPath: string
  variablesPath: string
  namespace?: string
}

export function vaultOptionsFromEnv(): VaultOptions {
  const addr = env('VAULT_ADDR')
  if (!addr) throw new Error('SECRETS_BACKEND=vault needs VAULT_ADDR')
  return {
    addr: addr.replace(/\/+$/, ''),
    token: env('VAULT_TOKEN'),
    roleId: env('VAULT_ROLE_ID'),
    secretId: env('VAULT_SECRET_ID'),
    role: env('VAULT_ROLE'),
    authPath: env('VAULT_AUTH_PATH'),
    jwtPath: env('VAULT_JWT_PATH'),
    mount: env('VAULT_MOUNT') ?? 'secret',
    kvVersion: env('VAULT_KV_VERSION') === '1' ? 1 : 2,
    connectionsPath: env('VAULT_CONNECTIONS_PATH') ?? 'connections',
    variablesPath: env('VAULT_VARIABLES_PATH') ?? 'variables',
    namespace: env('VAULT_NAMESPACE'),
  }
}

export class VaultSecretsBackend extends TokenAuth implements SecretsBackend {
  readonly name = 'vault'
  protected readonly staticToken: string | undefined
  private readonly cache = new SecretCache()

  constructor(private readonly opts: VaultOptions = vaultOptionsFromEnv()) {
    super()
    this.staticToken = opts.token
    if (!opts.token && !opts.role && !(opts.roleId && opts.secretId)) {
      throw new Error('SECRETS_BACKEND=vault needs VAULT_TOKEN, VAULT_ROLE_ID + VAULT_SECRET_ID, or VAULT_ROLE')
    }
  }

  private headers(token?: string): Record<string, string> {
    const h: Record<string, string> = { 'content-type': 'application/json' }
    if (token) h['x-vault-token'] = token
    if (this.opts.namespace) h['x-vault-namespace'] = this.opts.namespace
    return h
  }

  protected async login(): Promise<{ token: string; ttlMs: number }> {
    let path: string
    let payload: Record<string, string>
    if (this.opts.roleId && this.opts.secretId) {
      path = this.opts.authPath ?? 'approle'
      payload = { role_id: this.opts.roleId, secret_id: this.opts.secretId }
    } else {
      path = this.opts.authPath ?? 'kubernetes'
      const jwt = (await readFile(this.opts.jwtPath ?? SA_TOKEN_PATH, 'utf8')).trim()
      payload = { role: this.opts.role!, jwt }
    }
    const { status, body } = await requestJson(`${this.opts.addr}/v1/auth/${path}/login`,
      { method: 'POST', headers: this.headers(), body: JSON.stringify(payload) }, `vault login (${path})`)
    const auth = (body as { auth?: { client_token?: string; lease_duration?: number } } | null)?.auth
    if (status !== 200 || !auth?.client_token) throw new Error(`vault login (${path}) failed: HTTP ${status}`)
    return { token: auth.client_token, ttlMs: (auth.lease_duration ?? 0) * 1000 }
  }

  /** The key/value data of one secret, or null when it does not exist. */
  private async read(path: string): Promise<Record<string, unknown> | null> {
    const cached = this.cache.get<Record<string, unknown>>(path)
    if (cached) return cached
    const url = this.opts.kvVersion === 2
      ? `${this.opts.addr}/v1/${this.opts.mount}/data/${path}`
      : `${this.opts.addr}/v1/${this.opts.mount}/${path}`
    const data = await this.withToken(async token => {
      const { status, body } = await requestJson(url, { headers: this.headers(token) }, `vault read ${path}`)
      if (status === 404) return null
      if (status !== 200) throw new HttpError(status, `vault read ${path}: HTTP ${status}`)
      const d = (body as { data?: Record<string, unknown> } | null)?.data ?? null
      return (this.opts.kvVersion === 2 ? (d?.data as Record<string, unknown> | undefined) ?? null : d)
    })
    this.cache.set(path, data)
    return data
  }

  async getConnection(connId: string): Promise<ConnectionRuntime | null> {
    const data = await this.read(joinPath(this.opts.connectionsPath, connId))
    return data ? connectionFromFields(connId, data) : null
  }

  async getVariable(key: string): Promise<string | null> {
    const data = await this.read(joinPath(this.opts.variablesPath, key))
    if (!data || data.value === undefined || data.value === null) return null
    return typeof data.value === 'string' ? data.value : JSON.stringify(data.value)
  }
}

// ── Akeyless ─────────────────────────────────────────────────────────────────

export interface AkeylessOptions {
  apiUrl: string
  token?: string
  accessId?: string
  accessKey?: string
  k8sAuthConfigName?: string
  gatewayUrl?: string
  jwtPath?: string
  connectionsPath: string
  variablesPath: string
}

export function akeylessOptionsFromEnv(): AkeylessOptions {
  return {
    apiUrl: (env('AKEYLESS_API_URL') ?? 'https://api.akeyless.io').replace(/\/+$/, ''),
    token: env('AKEYLESS_TOKEN'),
    accessId: env('AKEYLESS_ACCESS_ID'),
    accessKey: env('AKEYLESS_ACCESS_KEY'),
    k8sAuthConfigName: env('AKEYLESS_K8S_AUTH_CONFIG_NAME'),
    gatewayUrl: env('AKEYLESS_GATEWAY_URL'),
    jwtPath: env('AKEYLESS_JWT_PATH'),
    connectionsPath: env('AKEYLESS_CONNECTIONS_PATH') ?? '/airflow/connections',
    variablesPath: env('AKEYLESS_VARIABLES_PATH') ?? '/airflow/variables',
  }
}

/** Akeyless tokens live 60 minutes by default; the auth response does not say. */
const AKEYLESS_TOKEN_TTL_MS = 55 * 60_000

export class AkeylessSecretsBackend extends TokenAuth implements SecretsBackend {
  readonly name = 'akeyless'
  protected readonly staticToken: string | undefined
  private readonly cache = new SecretCache()

  constructor(private readonly opts: AkeylessOptions = akeylessOptionsFromEnv()) {
    super()
    this.staticToken = opts.token
    if (!opts.token && !(opts.accessId && (opts.accessKey || opts.k8sAuthConfigName))) {
      throw new Error('SECRETS_BACKEND=akeyless needs AKEYLESS_TOKEN, or AKEYLESS_ACCESS_ID with AKEYLESS_ACCESS_KEY or AKEYLESS_K8S_AUTH_CONFIG_NAME')
    }
  }

  private async post(path: string, payload: Record<string, unknown>, what: string) {
    return requestJson(`${this.opts.apiUrl}/${path}`,
      { method: 'POST', headers: { 'content-type': 'application/json', accept: 'application/json' }, body: JSON.stringify(payload) }, what)
  }

  protected async login(): Promise<{ token: string; ttlMs: number }> {
    const payload: Record<string, unknown> = { 'access-id': this.opts.accessId }
    if (this.opts.accessKey) {
      Object.assign(payload, { 'access-type': 'access_key', 'access-key': this.opts.accessKey })
    } else {
      const jwt = (await readFile(this.opts.jwtPath ?? SA_TOKEN_PATH, 'utf8')).trim()
      Object.assign(payload, {
        'access-type': 'k8s',
        'k8s-auth-config-name': this.opts.k8sAuthConfigName,
        'k8s-service-account-token': Buffer.from(jwt).toString('base64'),
        ...(this.opts.gatewayUrl ? { 'gateway-url': this.opts.gatewayUrl } : {}),
      })
    }
    const { status, body } = await this.post('auth', payload, 'akeyless auth')
    const token = (body as { token?: string } | null)?.token
    if (status !== 200 || !token) throw new Error(`akeyless auth failed: HTTP ${status}`)
    return { token, ttlMs: AKEYLESS_TOKEN_TTL_MS }
  }

  private async read(name: string): Promise<string | null> {
    const cached = this.cache.get<string>(name)
    if (cached !== undefined) return cached
    const value = await this.withToken(async token => {
      const { status, body } = await this.post('get-secret-value', { names: [name], token }, `akeyless read ${name}`)
      // A missing item is a 404, or a 400 whose error says so (depends on the Akeyless version)
      if (status === 404 || (status === 400 && /not\s*found/i.test(JSON.stringify(body ?? '')))) return null
      if (status !== 200) throw new HttpError(status, `akeyless read ${name}: HTTP ${status}`)
      const v = (body as Record<string, unknown> | null)?.[name]
      return v === undefined || v === null ? null : typeof v === 'string' ? v : JSON.stringify(v)
    })
    this.cache.set(name, value)
    return value
  }

  async getConnection(connId: string): Promise<ConnectionRuntime | null> {
    const raw = await this.read(`${this.opts.connectionsPath.replace(/\/+$/, '')}/${connId}`)
    return raw === null ? null : connectionFromSecretString(connId, raw)
  }

  async getVariable(key: string): Promise<string | null> {
    return this.read(`${this.opts.variablesPath.replace(/\/+$/, '')}/${key}`)
  }
}
