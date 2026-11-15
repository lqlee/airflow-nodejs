/**
 * Connection formats shared by the secrets backends: Airflow connection URIs and JSON field objects.
 *
 * URI (Airflow format): `postgres://user:p%40ss@db.example.com:5432/app?sslmode=require`
 *   scheme → conn_type ('-' becomes '_'), user/password URL-decoded, path → schema,
 *   query params → extra (or a JSON `__extra__` param that becomes the whole extra).
 * JSON: `{ "conn_type": "postgres", "host": "…", "port": 5432, "login": "…", "password": "…", "schema": "…", "extra": {…} }`
 *   An object with a `conn_uri` field is read as a URI (Airflow's Vault layout).
 */
import type { ConnectionRuntime } from '../connections/index.js'

// '+' stays literal in user, password and path (Airflow uses unquote, not unquote_plus, for them)
const decode = (s: string) => decodeURIComponent(s)

export function parseConnectionUri(connId: string, uri: string): ConnectionRuntime {
  const m = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/(.*)$/.exec(uri.trim())
  if (!m) throw new Error(`connection '${connId}' is not a URI (expected scheme://…)`)
  // Parse with a fixed scheme: WHATWG URL treats unknown schemes as opaque and drops host/port parsing
  const url = new URL(`http://${m[2]}`)
  const extra: Record<string, unknown> = {}
  for (const [k, v] of url.searchParams) {
    if (k === '__extra__') {
      const parsed = JSON.parse(v) as unknown
      if (typeof parsed === 'object' && parsed !== null) Object.assign(extra, parsed)
    } else {
      extra[k] = v
    }
  }
  const path = url.pathname.replace(/^\//, '')
  return {
    conn_id: connId,
    conn_type: m[1].replace(/-/g, '_'),
    host: url.hostname ? decode(url.hostname) : null,
    port: url.port ? Number(url.port) : null,
    schema: path ? decode(path) : null,
    login: url.username ? decode(url.username) : null,
    password: url.password ? decode(url.password) : null,
    extra: Object.keys(extra).length ? extra : null,
  }
}

const str = (v: unknown) => (v === null || v === undefined || v === '' ? null : String(v))

function parseExtra(v: unknown): Record<string, unknown> | null {
  if (v === null || v === undefined || v === '') return null
  if (typeof v === 'string') {
    const parsed = JSON.parse(v) as unknown
    return typeof parsed === 'object' && parsed !== null ? parsed as Record<string, unknown> : null
  }
  return typeof v === 'object' ? v as Record<string, unknown> : null
}

export function connectionFromFields(connId: string, f: Record<string, unknown>): ConnectionRuntime {
  if (typeof f.conn_uri === 'string') return parseConnectionUri(connId, f.conn_uri)
  return {
    conn_id: connId,
    conn_type: str(f.conn_type) ?? 'generic',
    host: str(f.host),
    port: f.port !== null && f.port !== undefined && f.port !== '' ? Number(f.port) : null,
    schema: str(f.schema),
    login: str(f.login),
    password: str(f.password),
    extra: parseExtra(f.extra),
  }
}

/** A secret value as stored in a backend: a JSON object string, or a connection URI. */
export function connectionFromSecretString(connId: string, raw: string): ConnectionRuntime {
  const trimmed = raw.trim()
  const json = trimmed.startsWith('json:') ? trimmed.slice(5) : trimmed
  if (json.startsWith('{')) return connectionFromFields(connId, JSON.parse(json) as Record<string, unknown>)
  return parseConnectionUri(connId, trimmed)
}
