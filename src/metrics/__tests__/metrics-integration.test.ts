/** /metrics wired to the real scheduler, worker semaphore, pools and the static-asset cache hook. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'
import { buildServer } from '../../api/server.js'
import { startScheduler, stopScheduler } from '../../scheduler/index.js'
import { acquire, release } from '../../scheduler/pool.js'
import { acquirePool, releasePool, createPool, resetAllPools } from '../../pools/index.js'
import { clearRegistry } from '../../dag/registry.js'
import { clearMetricsCache } from '../index.js'
import { getSchedulerStats, resetMetrics } from '../registry.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.METRICS_CACHE_SECONDS = '0'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_metrics_integration')
  app = buildServer(db, { rateLimitMax: 0 })
  await app.ready()
})
afterAll(async () => {
  stopScheduler()
  await app.close(); await db.dropDatabase(); await client.close()
  delete process.env.METRICS_CACHE_SECONDS
})
afterEach(async () => {
  stopScheduler(); resetAllPools(); resetMetrics(); clearMetricsCache(); clearRegistry()
  await db.collection('pools').deleteMany({})
})

const metric = async (name: string) => {
  const text = (await app.inject({ method: 'GET', url: '/metrics' })).body
  const m = new RegExp(`^${name.replace(/[{}"|.()]/g, '\\$&')} (\\S+)$`, 'm').exec(text)
  return m ? Number(m[1]) : undefined
}
const until = async (fn: () => boolean | Promise<boolean>, ms = 8000) => {
  const end = Date.now() + ms
  while (!(await fn()) && Date.now() < end) await new Promise(r => setTimeout(r, 50))
}

describe('scheduler tick instrumentation', () => {
  it('a real scheduler tick increments ticks_total and stamps the last-tick time', async () => {
    const before = Date.now() / 1000
    startScheduler(db)
    await until(() => getSchedulerStats().ticks >= 1)
    stopScheduler()

    expect(await metric('airflow_scheduler_ticks_total')).toBeGreaterThanOrEqual(1)
    expect(await metric('airflow_scheduler_tick_errors_total')).toBe(0)
    expect((await metric('airflow_scheduler_last_tick_timestamp_seconds'))!).toBeGreaterThanOrEqual(before)
    expect((await metric('airflow_scheduler_last_tick_duration_seconds'))!).toBeGreaterThan(0)
  }, 20000)

  it('a tick that throws is still counted, as an error', async () => {
    const broken = { collection: () => { throw new Error('db down') } } as unknown as Db
    startScheduler(broken)
    await until(() => getSchedulerStats().ticks >= 1)
    stopScheduler()

    const s = getSchedulerStats()
    expect(s.ticks).toBeGreaterThanOrEqual(1)
    expect(s.errors).toBeGreaterThanOrEqual(1)
    expect(await metric('airflow_scheduler_tick_errors_total')).toBeGreaterThanOrEqual(1)
  }, 20000)
})

describe('live gauges follow the real semaphores', () => {
  it('airflow_workers_active / queued track the global worker semaphore', async () => {
    const base = (await metric('airflow_workers_active'))!
    await acquire()
    expect(await metric('airflow_workers_active')).toBe(base + 1)
    release()
    expect(await metric('airflow_workers_active')).toBe(base)
  })

  it('pool occupied/open slots track acquirePool / releasePool, weighted by slots', async () => {
    await createPool(db, 'live_pool', 5)
    await acquirePool(db, 'live_pool', 3)
    expect(await metric('airflow_pool_occupied_slots{pool="live_pool"}')).toBe(3)
    expect(await metric('airflow_pool_open_slots{pool="live_pool"}')).toBe(2)
    releasePool('live_pool', 3)
    expect(await metric('airflow_pool_occupied_slots{pool="live_pool"}')).toBe(0)
    expect(await metric('airflow_pool_open_slots{pool="live_pool"}')).toBe(5)
  })
})

describe('Cache-Control hook', () => {
  it('still serves the UI shell as no-store', async () => {
    const res = await app.inject({ method: 'GET', url: '/' })
    expect(res.headers['cache-control']).toMatch(/no-store/)
  })
  it('lets a route-set Cache-Control win over the static-asset default', async () => {
    const res = await app.inject({ method: 'GET', url: '/metrics' })
    expect(res.headers['cache-control']).toBe('no-store')
  })
})
