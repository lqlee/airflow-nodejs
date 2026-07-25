import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { buildServer } from '../server.js'
import { clearRegistry } from '../../dag/registry.js'
import { PLUGIN_REGISTRY } from '../routes/plugins.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_providers_plugins')
  clearRegistry()
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

// ── GET /providers ────────────────────────────────────────────────────────

describe('GET /providers', () => {
  it('returns 200 with providers array and total_entries', async () => {
    const res = await app.inject({ method: 'GET', url: '/providers' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(Array.isArray(body.providers)).toBe(true)
    expect(typeof body.total_entries).toBe('number')
    expect(body.total_entries).toBe(body.providers.length)
  })

  it('each provider has required fields', async () => {
    const res = await app.inject({ method: 'GET', url: '/providers' })
    const { providers } = res.json() as { providers: Array<Record<string, unknown>> }
    expect(providers.length).toBeGreaterThan(0)
    for (const p of providers) {
      expect(typeof p.package_name).toBe('string')
      expect(typeof p.version).toBe('string')
      expect(typeof p.description).toBe('string')
      expect(typeof p.role).toBe('string')
    }
  })

  it('includes known packages: fastify, mongodb, node-cron', async () => {
    const res = await app.inject({ method: 'GET', url: '/providers' })
    const { providers } = res.json() as { providers: Array<{ package_name: string }> }
    const names = providers.map(p => p.package_name)
    expect(names).toContain('fastify')
    expect(names).toContain('mongodb')
    expect(names).toContain('node-cron')
  })

  it('version strings have semver range prefix stripped', async () => {
    const res = await app.inject({ method: 'GET', url: '/providers' })
    const { providers } = res.json() as { providers: Array<{ version: string }> }
    for (const p of providers) {
      expect(p.version).not.toMatch(/^[\^~>=<]/)
    }
  })
})

describe('GET /providers/:packageName', () => {
  it('returns 200 for a known package', async () => {
    const res = await app.inject({ method: 'GET', url: '/providers/fastify' })
    expect(res.statusCode).toBe(200)
    expect(res.json().package_name).toBe('fastify')
    expect(res.json().description).toBe('HTTP API server')
  })

  it('returns 404 for unknown package', async () => {
    const res = await app.inject({ method: 'GET', url: '/providers/nonexistent-pkg' })
    expect(res.statusCode).toBe(404)
  })
})

// ── GET /plugins ──────────────────────────────────────────────────────────

describe('GET /plugins', () => {
  it('returns 200 with plugins array and total_entries', async () => {
    const res = await app.inject({ method: 'GET', url: '/plugins' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(Array.isArray(body.plugins)).toBe(true)
    expect(body.total_entries).toBe(body.plugins.length)
    expect(body.total_entries).toBe(PLUGIN_REGISTRY.length)
  })

  it('each plugin has name, description, routes[], category', async () => {
    const res = await app.inject({ method: 'GET', url: '/plugins' })
    const { plugins } = res.json() as { plugins: Array<Record<string, unknown>> }
    for (const p of plugins) {
      expect(typeof p.name).toBe('string')
      expect(typeof p.description).toBe('string')
      expect(Array.isArray(p.routes)).toBe(true)
      expect(['core', 'scheduling', 'auth', 'observability', 'lifecycle', 'discovery'])
        .toContain(p.category)
    }
  })

  it('includes expected plugin names', async () => {
    const res = await app.inject({ method: 'GET', url: '/plugins' })
    const names = (res.json().plugins as Array<{ name: string }>).map(p => p.name)
    for (const expected of ['dags', 'dag-runs', 'pools', 'backfills', 'hitl', 'event-logs', 'api-keys']) {
      expect(names).toContain(expected)
    }
  })
})

describe('GET /plugins/:name', () => {
  it('returns plugin details for a known plugin', async () => {
    const res = await app.inject({ method: 'GET', url: '/plugins/pools' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.name).toBe('pools')
    expect(body.category).toBe('scheduling')
    expect(body.routes).toContain('/pools')
  })

  it('returns 404 for unknown plugin', async () => {
    const res = await app.inject({ method: 'GET', url: '/plugins/nonexistent' })
    expect(res.statusCode).toBe(404)
  })
})

describe('GET /plugins/categories', () => {
  it('returns sorted unique categories', async () => {
    const res = await app.inject({ method: 'GET', url: '/plugins/categories' })
    expect(res.statusCode).toBe(200)
    const { categories } = res.json() as { categories: string[] }
    expect(Array.isArray(categories)).toBe(true)
    // All expected categories present
    for (const cat of ['auth', 'core', 'discovery', 'lifecycle', 'observability', 'scheduling']) {
      expect(categories).toContain(cat)
    }
    // Sorted
    const sorted = [...categories].sort()
    expect(categories).toEqual(sorted)
    // Unique
    expect(new Set(categories).size).toBe(categories.length)
  })
})
