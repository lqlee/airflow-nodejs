/**
 * Config API tests.
 *
 * Auth is disabled in these tests (no API_KEYS / ADMIN_KEY set), so the
 * admin-only requiredRole is bypassed — consistent with all other tests.
 * The RBAC enforcement itself is tested in rbac.test.ts.
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { buildServer } from '../server.js'
import { clearRegistry } from '../../dag/registry.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_config')
  clearRegistry()
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

describe('GET /config', () => {
  it('returns 200 with sections and total_entries', async () => {
    const res = await app.inject({ method: 'GET', url: '/config' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(Array.isArray(body.sections)).toBe(true)
    expect(typeof body.total_entries).toBe('number')
    expect(body.total_entries).toBeGreaterThan(0)
  })

  it('includes expected sections: api, auth, database, scheduler, encryption', async () => {
    const res = await app.inject({ method: 'GET', url: '/config' })
    const { sections } = res.json() as { sections: Array<{ section: string }> }
    const names = sections.map(s => s.section)
    for (const expected of ['api', 'auth', 'database', 'scheduler', 'encryption']) {
      expect(names).toContain(expected)
    }
  })

  it('each entry has key, value, description, is_default, is_sensitive', async () => {
    const res = await app.inject({ method: 'GET', url: '/config' })
    const { sections } = res.json() as { sections: Array<{ entries: Array<Record<string, unknown>> }> }
    for (const sec of sections) {
      for (const e of sec.entries) {
        expect(typeof e.key).toBe('string')
        expect('value' in e).toBe(true)
        expect(typeof e.description).toBe('string')
        expect(typeof e.is_default).toBe('boolean')
        expect(typeof e.is_sensitive).toBe('boolean')
      }
    }
  })

  it('sensitive entries have masked value (*** or null)', async () => {
    const res = await app.inject({ method: 'GET', url: '/config' })
    const { sections } = res.json() as { sections: Array<{ entries: Array<Record<string, unknown>> }> }
    const allEntries = sections.flatMap(s => s.entries)
    const sensitive = allEntries.filter(e => e.is_sensitive)
    expect(sensitive.length).toBeGreaterThan(0)
    for (const e of sensitive) {
      // Sensitive values are either null (not set) or '***' (set but masked)
      expect(e.value === null || e.value === '***').toBe(true)
    }
  })

  it('non-sensitive entries have readable values', async () => {
    const res = await app.inject({ method: 'GET', url: '/config' })
    const { sections } = res.json() as { sections: Array<{ section: string; entries: Array<Record<string, unknown>> }> }
    const apiSection = sections.find(s => s.section === 'api')!
    const portEntry = apiSection.entries.find(e => e.key === 'port')!
    // PORT not set in test env → uses default 3000
    expect(portEntry.value).toBe(3000)
    expect(portEntry.is_default).toBe(true)
    expect(portEntry.is_sensitive).toBe(false)
  })

  it('total_entries matches sum of all section entry counts', async () => {
    const res = await app.inject({ method: 'GET', url: '/config' })
    const { sections, total_entries } = res.json() as {
      sections: Array<{ entries: unknown[] }>
      total_entries: number
    }
    const computed = sections.reduce((s, sec) => s + sec.entries.length, 0)
    expect(total_entries).toBe(computed)
  })
})

describe('GET /config/:section', () => {
  it('returns the api section', async () => {
    const res = await app.inject({ method: 'GET', url: '/config/api' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.section).toBe('api')
    expect(Array.isArray(body.entries)).toBe(true)
    expect(body.entries.some((e: { key: string }) => e.key === 'port')).toBe(true)
  })

  it('returns the scheduler section', async () => {
    const res = await app.inject({ method: 'GET', url: '/config/scheduler' })
    expect(res.statusCode).toBe(200)
    const { entries } = res.json() as { entries: Array<{ key: string }> }
    expect(entries.some(e => e.key === 'max_workers')).toBe(true)
    expect(entries.some(e => e.key === 'drain_timeout_ms')).toBe(true)
  })

  it('returns 404 for unknown section', async () => {
    const res = await app.inject({ method: 'GET', url: '/config/nonexistent' })
    expect(res.statusCode).toBe(404)
  })

  it('encryption section reports encryption_key_set as sensitive', async () => {
    const res = await app.inject({ method: 'GET', url: '/config/encryption' })
    const { entries } = res.json() as { entries: Array<{ key: string; is_sensitive: boolean }> }
    const keyEntry = entries.find(e => e.key === 'encryption_key_set')!
    expect(keyEntry.is_sensitive).toBe(true)
  })
})
