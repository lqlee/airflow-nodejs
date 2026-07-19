import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { buildServer } from '../server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

const rlDag: DagDefinition = {
  id: 'rl_dag',
  schedule: null,
  tasks: { step: { run: async () => {} } },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_ratelimit')
  clearRegistry()
  register(rlDag)
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

describe('Rate limiting', () => {
  /**
   * All tests share ONE server so counters accumulate realistically.
   * We verify observable HTTP behaviour, not internal counter state.
   *
   * Server is built with rateLimitMax=10, rateLimitAuthMax=5 so there's
   * plenty of headroom for each individual sub-test while still triggering
   * limits when we deliberately exhaust them.
   */
  let app: Awaited<ReturnType<typeof buildServer>>

  beforeAll(async () => {
    app = buildServer(db, { rateLimitMax: 10, rateLimitAuthMax: 5 })
    await app.ready()
  })

  afterAll(async () => {
    await app.close()
  })

  it('includes x-ratelimit-limit and x-ratelimit-remaining headers', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags' })
    expect(res.statusCode).toBe(200)
    expect(res.headers['x-ratelimit-limit']).toBeDefined()
    expect(res.headers['x-ratelimit-remaining']).toBeDefined()
    // remaining < limit after one request
    const limit = Number(res.headers['x-ratelimit-limit'])
    const remaining = Number(res.headers['x-ratelimit-remaining'])
    expect(remaining).toBeLessThan(limit)
  })

  it('x-ratelimit-remaining decreases with each request', async () => {
    const r1 = await app.inject({ method: 'GET', url: '/dags' })
    const r2 = await app.inject({ method: 'GET', url: '/dags' })
    const rem1 = Number(r1.headers['x-ratelimit-remaining'])
    const rem2 = Number(r2.headers['x-ratelimit-remaining'])
    expect(rem2).toBeLessThan(rem1)
  })

  it('/health has a lower limit than /dags (different x-ratelimit-limit)', async () => {
    const healthRes = await app.inject({ method: 'GET', url: '/health' })
    const dagsRes   = await app.inject({ method: 'GET', url: '/dags' })
    const healthLimit = Number(healthRes.headers['x-ratelimit-limit'])
    const dagsLimit   = Number(dagsRes.headers['x-ratelimit-limit'])
    expect(healthLimit).toBeLessThan(dagsLimit)
  })

  it('returns 429 with structured error body after exceeding limit', async () => {
    // Build a dedicated app with max=3 to reliably hit the limit
    const tight = buildServer(db, { rateLimitMax: 3, rateLimitAuthMax: 2 })
    await tight.ready()
    try {
      for (let i = 0; i < 3; i++) {
        await tight.inject({ method: 'GET', url: '/dags' })
      }
      const res = await tight.inject({ method: 'GET', url: '/dags' })
      expect(res.statusCode).toBe(429)
      const body = res.json()
      expect(body.error).toBe('Too Many Requests')
      expect(body.retryAfter).toBeDefined()
    } finally {
      await tight.close()
    }
  })
})
