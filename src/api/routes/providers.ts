/**
 * Providers API — exposes the installed npm packages that power the scheduler.
 *
 * In Apache Airflow, "providers" are pip packages extending operator/hook support.
 * The Node.js equivalent is the set of npm runtime dependencies.
 * Reads from package.json at startup — static thereafter.
 */

import type { FastifyInstance } from 'fastify'
import { readFile } from 'node:fs/promises'
import { resolve, dirname } from 'node:path'
import { fileURLToPath } from 'node:url'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PKG_PATH = resolve(__dirname, '../../../package.json')

export interface ProviderInfo {
  package_name: string
  version: string
  description: string
  role: string       // what this package does in the scheduler
}

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
}

let _providers: ProviderInfo[] | null = null

async function loadProviders(): Promise<ProviderInfo[]> {
  if (_providers) return _providers

  const raw = await readFile(PKG_PATH, 'utf8')
  const pkg = JSON.parse(raw) as {
    dependencies?: Record<string, string>
    devDependencies?: Record<string, string>
  }

  _providers = Object.entries(pkg.dependencies ?? {}).map(([name, version]) => ({
    package_name: name,
    version: version.replace(/^[\^~>=<]+/, ''),  // strip semver range prefix
    description: ROLE_MAP[name] ?? 'npm dependency',
    role: ROLE_MAP[name] ?? 'dependency',
  }))

  return _providers
}

export async function providersRoutes(app: FastifyInstance): Promise<void> {
  // GET /providers — list all runtime npm packages
  app.get('/providers', async (_req, reply) => {
    const providers = await loadProviders()
    return reply.send({
      providers,
      total_entries: providers.length,
    })
  })

  // GET /providers/:packageName — single provider details
  app.get<{ Params: { packageName: string } }>('/providers/:packageName', async (req, reply) => {
    const providers = await loadProviders()
    // package names may contain @ and / — decode URI component
    const name = decodeURIComponent(req.params.packageName)
    const provider = providers.find(p => p.package_name === name)
    if (!provider) {
      return reply.status(404).send({ error: `Provider '${name}' not found` })
    }
    return reply.send(provider)
  })
}
