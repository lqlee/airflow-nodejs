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
import { getEncryptionKeys, isEncryptionConfigured, keyId } from '../../crypto/index.js'

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

/** Key ids are short hashes — safe to show, and what an operator needs to follow a rotation. */
function encryptionEntries(): ConfigEntry[] {
  const configured = isEncryptionConfigured()
  const ids = configured ? getEncryptionKeys().map(keyId) : []
  const isDefault = process.env.ENCRYPTION_KEYS === undefined && process.env.ENCRYPTION_KEY === undefined
  const fixed = (key: string, value: ConfigEntry['value'], description: string): ConfigEntry =>
    ({ key, value, description, is_default: isDefault, is_sensitive: false })
  return [
    // Same shape as other secret-backed entries: '***' when set, null when not
    { ...fixed('encryption_key_set', configured ? '***' : null, 'Whether ENCRYPTION_KEYS / ENCRYPTION_KEY is configured (required for secrets)'), is_sensitive: true },
    fixed('encryption_key_count', ids.length, 'Configured keys; more than one while a rotation is in progress'),
    fixed('encryption_primary_key_id', ids[0] ?? null, 'Id of the key new secrets are encrypted with'),
  ]
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
        entry('sandbox_call_timeout_ms', 'SANDBOX_CALL_TIMEOUT_MS', 30000, 'Limit for one callback/timetable/trigger call in the sandbox process (ms)'),
      ],
    },
    {
      section: 'dag_processor',
      entries: [
        entry('dag_processor', 'DAG_PROCESSOR', 'embedded', 'embedded: the leader scheduler parses Dag files; external: a separate `npm run dag-processor`'),
        entry('dag_parse_timeout_ms', 'DAG_PARSE_TIMEOUT_MS', 30000, 'Per-file import timeout; a file that hangs becomes an import error (ms)'),
        entry('dag_reparse_interval_ms', 'DAG_REPARSE_INTERVAL_MS', 300000, 'Re-parse this often even when no file changed (ms)'),
        entry('dag_processor_poll_ms', 'DAG_PROCESSOR_POLL_MS', 5000, 'External processor: how often it checks the Dag folder for changes (ms)'),
      ],
    },
    {
      section: 'execution_api',
      entries: [
        entry('execution_api_url', 'EXECUTION_API_URL', '', 'This deployment as reached from Kubernetes task pods'),
        entry('execution_api_secret_set', 'EXECUTION_API_SECRET', false, 'Task-token signing secret (else derived from ENCRYPTION_KEYS)', true),
        entry('execution_token_ttl_ms', 'EXECUTION_TOKEN_TTL_MS', 1800000, 'Task-token lifetime; workers refresh it while the try runs (ms)'),
      ],
    },
    {
      section: 'encryption',
      entries: encryptionEntries(),
    },
    {
      section: 'secrets',
      entries: [
        entry('secrets_backend', 'SECRETS_BACKEND', 'none', 'Fallback store for connections/variables not in the DB: none, env, file, vault, akeyless'),
        entry('mask_min_length', 'MASK_MIN_LENGTH', 5, 'Secrets shorter than this are not masked in task logs'),
        entry('sensitive_field_names', 'SENSITIVE_FIELD_NAMES', '', 'Extra field/variable name fragments treated as secret (comma-separated)'),
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
