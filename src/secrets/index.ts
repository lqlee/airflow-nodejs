/**
 * Secrets Backend — pluggable secret store for connections and variables.
 *
 * When a connection or variable is not found in the database, the configured
 * secrets backend is consulted as a fallback. This mirrors Airflow's
 * secrets backends (Vault, AWS Secrets Manager, GCP Secret Manager, etc.)
 *
 * Built-in backends:
 *   'none'  — no fallback (default; DB-only, existing behaviour)
 *   'env'   — read from environment variables (12-factor apps)
 *   'file'  — read from a JSON file at SECRETS_FILE_PATH
 *
 * Select backend via SECRETS_BACKEND env var:
 *   SECRETS_BACKEND=env    → EnvSecretsBackend
 *   SECRETS_BACKEND=file   → FileSecretsBackend (also set SECRETS_FILE_PATH)
 *
 * File format (SECRETS_FILE_PATH):
 * {
 *   "connections": {
 *     "my_db": {
 *       "conn_type": "postgres",
 *       "host": "db.example.com",
 *       "port": 5432,
 *       "login": "user",
 *       "password": "secret",
 *       "schema": "public"
 *     }
 *   },
 *   "variables": {
 *     "api_key": "my-secret-api-key",
 *     "config": "some-value"
 *   }
 * }
 *
 * Env variable format for connections:
 *   AIRFLOW_CONN_<CONN_ID_UPPER>=json:{...}
 *   e.g. AIRFLOW_CONN_MY_DB='{"conn_type":"postgres","host":"localhost","port":5432}'
 *
 * Env variable format for variables:
 *   AIRFLOW_VAR_<KEY_UPPER>=value
 *   e.g. AIRFLOW_VAR_API_KEY=my-secret-api-key
 */

import { readFile } from 'node:fs/promises'
import type { ConnectionRuntime } from '../connections/index.js'

export interface SecretsBackend {
  /** Fetch a connection by conn_id. Returns null if not found. */
  getConnection(connId: string): Promise<ConnectionRuntime | null>
  /** Fetch a variable by key. Returns null if not found. */
  getVariable(key: string): Promise<string | null>
  /** Backend name for logging/health. */
  readonly name: string
}

// ── Null backend (default) ────────────────────────────────────────────────────

class NullSecretsBackend implements SecretsBackend {
  readonly name = 'none'
  async getConnection(_connId: string): Promise<null> { return null }
  async getVariable(_key: string): Promise<null> { return null }
}

// ── Environment backend ───────────────────────────────────────────────────────

/**
 * Reads secrets from environment variables.
 *
 * Connections: AIRFLOW_CONN_<CONN_ID_UPPER>=json:{...}
 * Variables:   AIRFLOW_VAR_<KEY_UPPER>=<value>
 *
 * Connection JSON shape: { conn_type, host?, port?, login?, password?, schema?, extra? }
 */
class EnvSecretsBackend implements SecretsBackend {
  readonly name = 'env'

  async getConnection(connId: string): Promise<ConnectionRuntime | null> {
    const envKey = `AIRFLOW_CONN_${connId.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`
    const raw = process.env[envKey]
    if (!raw) return null

    try {
      const json = raw.startsWith('json:') ? raw.slice(5) : raw
      const parsed = JSON.parse(json) as Record<string, unknown>
      return {
        conn_id:   connId,
        conn_type: String(parsed.conn_type ?? 'generic'),
        host:      parsed.host != null ? String(parsed.host) : null,
        port:      parsed.port != null ? Number(parsed.port) : null,
        schema:    parsed.schema != null ? String(parsed.schema) : null,
        login:     parsed.login != null ? String(parsed.login) : null,
        password:  parsed.password != null ? String(parsed.password) : null,
        extra:     parsed.extra != null ? (parsed.extra as Record<string, unknown>) : null,
      }
    } catch {
      console.warn(`[secrets/env] failed to parse AIRFLOW_CONN_${connId.toUpperCase()} as JSON`)
      return null
    }
  }

  async getVariable(key: string): Promise<string | null> {
    const envKey = `AIRFLOW_VAR_${key.toUpperCase().replace(/[^A-Z0-9]/g, '_')}`
    return process.env[envKey] ?? null
  }
}

// ── File backend ──────────────────────────────────────────────────────────────

interface SecretsFile {
  connections?: Record<string, Partial<ConnectionRuntime> & { conn_type: string }>
  variables?: Record<string, string>
}

/**
 * Reads secrets from a JSON file at SECRETS_FILE_PATH (or the path passed to the constructor).
 * File is read once and cached; reload by restarting the process.
 */
export class FileSecretsBackend implements SecretsBackend {
  readonly name = 'file'
  private _cache: SecretsFile | null = null
  private readonly filePath: string

  constructor(filePath?: string) {
    this.filePath = filePath ?? process.env.SECRETS_FILE_PATH ?? ''
  }

  private async load(): Promise<SecretsFile> {
    if (this._cache) return this._cache
    if (!this.filePath) {
      console.warn('[secrets/file] SECRETS_FILE_PATH not set — file backend returns no secrets')
      return {}
    }
    try {
      const raw = await readFile(this.filePath, 'utf8')
      this._cache = JSON.parse(raw) as SecretsFile
      console.log(`[secrets/file] loaded secrets from ${this.filePath}`)
      return this._cache
    } catch (err) {
      console.warn(`[secrets/file] failed to load ${this.filePath}:`, err)
      return {}
    }
  }

  /** Invalidate cache (for testing / hot-reload). */
  invalidate(): void { this._cache = null }

  async getConnection(connId: string): Promise<ConnectionRuntime | null> {
    const data = await this.load()
    const conn = data.connections?.[connId]
    if (!conn) return null
    return {
      conn_id:   connId,
      conn_type: conn.conn_type,
      host:      conn.host ?? null,
      port:      conn.port ?? null,
      schema:    conn.schema ?? null,
      login:     conn.login ?? null,
      password:  conn.password ?? null,
      extra:     conn.extra ?? null,
    }
  }

  async getVariable(key: string): Promise<string | null> {
    const data = await this.load()
    return data.variables?.[key] ?? null
  }
}

// ── Backend factory ───────────────────────────────────────────────────────────

let _backend: SecretsBackend | null = null

/** Get the configured secrets backend (lazy singleton). */
export function getSecretsBackend(): SecretsBackend {
  if (_backend) return _backend

  const name = (process.env.SECRETS_BACKEND ?? 'none').toLowerCase().trim()
  switch (name) {
    case 'env':
      _backend = new EnvSecretsBackend()
      console.log('[secrets] backend: env (AIRFLOW_CONN_* / AIRFLOW_VAR_*)')
      break
    case 'file':
      _backend = new FileSecretsBackend()
      console.log(`[secrets] backend: file (${process.env.SECRETS_FILE_PATH ?? 'SECRETS_FILE_PATH not set'})`)
      break
    default:
      _backend = new NullSecretsBackend()
      break
  }

  return _backend
}

/** Override the backend (testing / DI). */
export function setSecretsBackend(backend: SecretsBackend): void {
  _backend = backend
}

/** Reset to default (re-reads SECRETS_BACKEND env). */
export function resetSecretsBackend(): void {
  _backend = null
}
