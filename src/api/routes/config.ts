/**
 * Config API — expose effective runtime configuration.
 *
 * Sensitive values (credentials, keys) are masked as null.
 * Values are resolved at request time from process.env + defaults,
 * matching what the running process actually uses.
 *
 * Requires admin role (auth is enabled) — config exposes internal settings.
 * In RBAC terms: GET is normally viewer, but this route overrides to admin
 * via config.requiredRole to match the sensitivity of the data.
 */

import type { FastifyInstance } from 'fastify'

export interface ConfigSection {
  section: string
  entries: ConfigEntry[]
}

export interface ConfigEntry {
  key: string
  value: string | number | boolean | null
  description: string
  /** True if the env var is explicitly set; false = using default */
  is_default: boolean
  /** True if the value is masked for security */
  is_sensitive: boolean
}

function entry(
  key: string,
  envVar: string,
  defaultVal: string | number | boolean,
  description: string,
  sensitive = false,
): ConfigEntry {
  const raw = process.env[envVar]
  const isDefault = raw === undefined

  let value: string | number | boolean | null
  if (sensitive) {
    value = raw !== undefined ? '***' : null
  } else if (typeof defaultVal === 'number') {
    value = raw !== undefined ? Number(raw) : defaultVal
  } else if (typeof defaultVal === 'boolean') {
    value = raw !== undefined ? Boolean(raw) : defaultVal
  } else {
    value = raw ?? defaultVal
  }

  return { key, value, description, is_default: isDefault, is_sensitive: sensitive }
}

function buildConfig(): ConfigSection[] {
  return [
    {
      section: 'api',
      entries: [
        entry('port', 'PORT', 3000, 'HTTP listen port'),
        entry('host', 'HOST', '0.0.0.0', 'HTTP bind host'),
        entry('rate_limit_max', 'RATE_LIMIT_MAX', 120, 'Max requests per minute per IP (global)'),
        entry('rate_limit_auth_max', 'RATE_LIMIT_AUTH_MAX', 10, 'Max requests per minute for unauthenticated endpoints'),
      ],
    },
    {
      section: 'auth',
      entries: [
        entry('admin_key_set', 'ADMIN_KEY', false, 'Whether ADMIN_KEY is configured', true),
        entry('api_keys_count', 'API_KEYS', 0, 'Number of env-based API_KEYS configured — value masked', true),
      ],
    },
    {
      section: 'database',
      entries: [
        entry('mongo_url', 'MONGO_URL', 'mongodb://localhost:27017', 'MongoDB connection URL', true),
        entry('db_name', 'DB_NAME', 'airflow', 'MongoDB database name'),
      ],
    },
    {
      section: 'scheduler',
      entries: [
        entry('max_workers', 'MAX_WORKERS', 8, 'Max concurrent task worker processes'),
        entry('drain_timeout_ms', 'DRAIN_TIMEOUT_MS', 20000, 'Graceful shutdown worker drain timeout (ms)'),
        entry('redis_url_set', 'REDIS_URL', false, 'Whether REDIS_URL is set (BullMQ mode)', true),
        entry('worker_concurrency', 'WORKER_CONCURRENCY', 4, 'BullMQ worker concurrency (only applies in BullMQ mode)'),
      ],
    },
    {
      section: 'encryption',
      entries: [
        entry('encryption_key_set', 'ENCRYPTION_KEY', false, 'Whether ENCRYPTION_KEY is configured (required for secrets)', true),
      ],
    },
  ]
}

export async function configRoutes(app: FastifyInstance): Promise<void> {
  // GET /config — full runtime configuration (admin only)
  app.get(
    '/config',
    { config: { requiredRole: 'admin' } },
    async (_req, reply) => {
      const sections = buildConfig()
      return reply.send({
        sections,
        total_entries: sections.reduce((s, sec) => s + sec.entries.length, 0),
      })
    },
  )

  // GET /config/:section — single section (admin only)
  app.get<{ Params: { section: string } }>(
    '/config/:section',
    { config: { requiredRole: 'admin' } },
    async (req, reply) => {
      const sections = buildConfig()
      const sec = sections.find(s => s.section === req.params.section)
      if (!sec) {
        return reply.status(404).send({ error: `Config section '${req.params.section}' not found` })
      }
      return reply.send(sec)
    },
  )
}
