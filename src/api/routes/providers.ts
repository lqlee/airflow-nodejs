/**
 * Providers API — three kinds of providers:
 *
 * 0. Built-in providers (providers/builtin) — SQL, Google Cloud, Kafka and SMTP operators that ship
 *    with airflow-nodejs; their drivers are optional peer dependencies.
 *
 * 1. npm packages (runtime dependencies) — the Node.js equivalent of pip providers.
 *    Static list read from package.json at startup.
 *
 * 2. Local providers — JS files in dags/providers/ that register reusable operator
 *    factories. Auto-discovered at startup by the provider loader.
 *    These are the direct equivalent of Airflow community providers.
 */

import type { FastifyInstance } from 'fastify'
import { readFile } from 'node:fs/promises'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'
import { listAllProviders } from '../../providers/registry.js'
import { CONNECTION_TYPES } from '../../providers/builtin/catalog.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PKG_PATH = resolve(__dirname, '../../../package.json')

export interface NpmProviderInfo {
  package_name: string
  version: string
  description: string
  role: string
  provider_type: 'npm'
}

export interface LocalProviderInfo {
  package_name: string
  version: string
  description: string
  operator_names: string[]
  connection_types: string[]
  role: string
  provider_type: 'local' | 'builtin'
  source: string
}

export type ProviderInfo = NpmProviderInfo | LocalProviderInfo

// Human-readable role descriptions for known scheduler dependencies
const ROLE_MAP: Record<string, string> = {
  'fastify': 'HTTP API server',
  '@fastify/rate-limit': 'API rate limiting',
  '@fastify/static': 'Static file serving (UI)',
  'mongodb': 'Metadata database driver',
  'node-cron': 'Cron expression scheduling',
  'bullmq': 'Distributed task queue (BullMQ mode)',
  'ioredis': 'Redis client for BullMQ',
  'pino': 'Structured logging',
  'cron-parser': 'Cron expression parsing (backfill)',
  'google-auth-library': 'Google Cloud credentials (google provider)',
}

let _npmProviders: NpmProviderInfo[] | null = null

async function loadNpmProviders(): Promise<NpmProviderInfo[]> {
  if (_npmProviders) return _npmProviders

  const raw = await readFile(PKG_PATH, 'utf8')
  const pkg = JSON.parse(raw) as {
    dependencies?: Record<string, string>
  }

  _npmProviders = Object.entries(pkg.dependencies ?? {}).map(([name, version]) => ({
    package_name: name,
    version: version.replace(/^[\^~>=<]+/, ''),
    description: ROLE_MAP[name] ?? 'npm dependency',
    role: ROLE_MAP[name] ?? 'dependency',
    provider_type: 'npm' as const,
  }))

  return _npmProviders
}

function toInfo(p: ReturnType<typeof listAllProviders>[number]): LocalProviderInfo {
  return {
    package_name: p.name,
    version: p.version,
    description: p.description,
    operator_names: p.operator_names,
    connection_types: p.connection_types,
    role: p.source === 'builtin' ? 'built-in operators' : 'local operators',
    provider_type: p.source === 'builtin' ? 'builtin' : 'local',
    source: p.source,
  }
}

export async function providersRoutes(app: FastifyInstance): Promise<void> {
  // GET /connection-types — connection types the built-in providers read: fields, extra keys, driver
  app.get('/connection-types', async (_req, reply) => reply.send(CONNECTION_TYPES))

  // GET /providers — list npm packages AND local providers from dags/providers/
  app.get('/providers', async (_req, reply) => {
    const [npmProviders, localProviders] = await Promise.all([
      loadNpmProviders(),
      Promise.resolve(listAllProviders()),
    ])

    const records: LocalProviderInfo[] = localProviders.map(toInfo)
    const builtin = records.filter(p => p.provider_type === 'builtin')
    const local = records.filter(p => p.provider_type === 'local')

    const all: ProviderInfo[] = [...builtin, ...local, ...npmProviders]

    return reply.send({
      providers: all,
      builtin_providers: builtin,
      local_providers: local,
      npm_providers: npmProviders,
      total_entries: all.length,
    })
  })

  // GET /providers/:packageName — single provider (local or npm)
  app.get<{ Params: { packageName: string } }>('/providers/:packageName', async (req, reply) => {
    const name = decodeURIComponent(req.params.packageName)

    // Built-in and local providers first
    const localDef = listAllProviders().find(p => p.name === name)
    if (localDef) return reply.send(toInfo(localDef))

    // Fall back to npm providers
    const npmProviders = await loadNpmProviders()
    const npm = npmProviders.find(p => p.package_name === name)
    if (npm) return reply.send(npm)

    return reply.status(404).send({ error: `Provider '${name}' not found` })
  })
}
