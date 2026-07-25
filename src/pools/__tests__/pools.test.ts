/**
 * Resource Pools tests.
 *
 * Enforcement tests use acquirePool/releasePool directly with injected
 * counters — no real workers needed. Discriminating check: a full pool
 * genuinely blocks the second acquire promise until release is called.
 *
 * Integration tests verify pool field flows end-to-end: task.pool → task_instance.pool.
 * Tasks without pool are unaffected (regression guard).
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import {
  acquirePool, releasePool, poolActiveCount, poolQueueDepth,
  resetAllPools, createPool, getPool, listPools, updatePool, deletePool,
} from '../index.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_pools'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_pools')
  clearRegistry()
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  delete process.env.DB_NAME
})

afterEach(async () => {
  await db.collection('pools').deleteMany({})
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('event_logs').deleteMany({})
  resetAllPools()
  clearRegistry()
})

// ── acquirePool / releasePool — pure enforcement ──────────────────────────

describe('acquirePool / releasePool — slot enforcement', () => {
  it('resolves immediately when pool has capacity', async () => {
    await createPool(db, 'fast_pool', 3)
    await acquirePool(db, 'fast_pool')
    expect(poolActiveCount('fast_pool')).toBe(1)
    releasePool('fast_pool')
    expect(poolActiveCount('fast_pool')).toBe(0)
  })

  it('BLOCKS second acquire when pool has 1 slot and first is held', async () => {
    await createPool(db, 'tight_pool', 1)

    // First acquire — gets the slot
    await acquirePool(db, 'tight_pool')
    expect(poolActiveCount('tight_pool')).toBe(1)

    // Second acquire — pool full, should stay pending
    let secondResolved = false
    const secondDone = acquirePool(db, 'tight_pool').then(() => { secondResolved = true })

    // Give microtasks a cycle — second should NOT have resolved
    await new Promise(r => setTimeout(r, 10))
    expect(secondResolved).toBe(false)
    expect(poolQueueDepth('tight_pool')).toBe(1)

    // Release first slot — second should now resolve
    releasePool('tight_pool')
    await secondDone
    expect(secondResolved).toBe(true)
    expect(poolActiveCount('tight_pool')).toBe(1)
    expect(poolQueueDepth('tight_pool')).toBe(0)

    releasePool('tight_pool')
    expect(poolActiveCount('tight_pool')).toBe(0)
  })

  it('unblocks waiters in FIFO order', async () => {
    await createPool(db, 'fifo_pool', 1)
    await acquirePool(db, 'fifo_pool')  // fill the slot

    const order: number[] = []
    const p1 = acquirePool(db, 'fifo_pool').then(() => order.push(1))
    const p2 = acquirePool(db, 'fifo_pool').then(() => order.push(2))

    releasePool('fifo_pool')
    await p1
    releasePool('fifo_pool')
    await p2
    releasePool('fifo_pool')

    expect(order).toEqual([1, 2])
  })

  it('falls through (no block) for unknown pool — logs warning', async () => {
    // Pool does not exist in DB
    const warnSpy: string[] = []
    const orig = console.warn
    console.warn = (...args: unknown[]) => { warnSpy.push(String(args[0])) }
    await acquirePool(db, 'nonexistent_pool')
    console.warn = orig
    expect(warnSpy.some(m => m.includes('nonexistent_pool'))).toBe(true)
    expect(poolActiveCount('nonexistent_pool')).toBe(0)
  })

  it('multiple pools are independent — one full does not block another', async () => {
    await createPool(db, 'pool_a', 1)
    await createPool(db, 'pool_b', 1)

    await acquirePool(db, 'pool_a')  // fill pool_a

    let bResolved = false
    acquirePool(db, 'pool_b').then(() => { bResolved = true })

    await new Promise(r => setTimeout(r, 10))
    expect(bResolved).toBe(true)  // pool_b unaffected

    releasePool('pool_a')
    releasePool('pool_b')
  })
})

// ── DB CRUD ────────────────────────────────────────────────────────────────

describe('Pool CRUD', () => {
  it('createPool stores pool and returns summary', async () => {
    const p = await createPool(db, 'etl', 5, 'ETL tasks')
    expect(p.name).toBe('etl')
    expect(p.slots).toBe(5)
    expect(p.description).toBe('ETL tasks')
    expect(p.open_slots).toBe(5)
    expect(p.occupied_slots).toBe(0)
  })

  it('getPool returns null for unknown pool', async () => {
    expect(await getPool(db, 'nope')).toBeNull()
  })

  it('listPools returns all pools sorted by name', async () => {
    await createPool(db, 'z_pool', 2)
    await createPool(db, 'a_pool', 4)
    const pools = await listPools(db)
    expect(pools.map(p => p.name)).toEqual(['a_pool', 'z_pool'])
  })

  it('updatePool changes slots', async () => {
    await createPool(db, 'resize_pool', 3)
    const updated = await updatePool(db, 'resize_pool', { slots: 10 })
    expect(updated?.slots).toBe(10)
  })

  it('deletePool returns true and removes it', async () => {
    await createPool(db, 'del_pool', 1)
    expect(await deletePool(db, 'del_pool')).toBe(true)
    expect(await getPool(db, 'del_pool')).toBeNull()
  })

  it('deletePool returns false for unknown pool', async () => {
    expect(await deletePool(db, 'ghost')).toBe(false)
  })
})

// ── API routes ─────────────────────────────────────────────────────────────

describe('GET /pools', () => {
  it('returns empty array when no pools', async () => {
    const res = await app.inject({ method: 'GET', url: '/pools' })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  it('returns created pool with open_slots field', async () => {
    await createPool(db, 'api_pool', 4)
    const res = await app.inject({ method: 'GET', url: '/pools' })
    const pools = res.json() as Array<{ name: string; slots: number; open_slots: number }>
    expect(pools).toHaveLength(1)
    expect(pools[0].name).toBe('api_pool')
    expect(pools[0].open_slots).toBe(4)
  })
})

describe('POST /pools', () => {
  it('creates a pool and returns 201', async () => {
    const res = await app.inject({
      method: 'POST', url: '/pools',
      payload: { name: 'new_pool', slots: 3, description: 'test' },
    })
    expect(res.statusCode).toBe(201)
    const body = res.json()
    expect(body.name).toBe('new_pool')
    expect(body.slots).toBe(3)
  })

  it('returns 409 for duplicate name', async () => {
    await createPool(db, 'dup', 1)
    const res = await app.inject({
      method: 'POST', url: '/pools',
      payload: { name: 'dup', slots: 2 },
    })
    expect(res.statusCode).toBe(409)
  })

  it('returns 400 when name missing', async () => {
    const res = await app.inject({
      method: 'POST', url: '/pools',
      payload: { slots: 3 },
    })
    expect(res.statusCode).toBe(400)
  })

  it('returns 400 when slots < 1', async () => {
    const res = await app.inject({
      method: 'POST', url: '/pools',
      payload: { name: 'bad', slots: 0 },
    })
    expect(res.statusCode).toBe(400)
  })
})

describe('PATCH /pools/:name', () => {
  it('updates slots', async () => {
    await createPool(db, 'patchable', 2)
    const res = await app.inject({
      method: 'PATCH', url: '/pools/patchable',
      payload: { slots: 8 },
    })
    expect(res.statusCode).toBe(200)
    expect(res.json().slots).toBe(8)
  })

  it('returns 404 for unknown pool', async () => {
    const res = await app.inject({
      method: 'PATCH', url: '/pools/nope',
      payload: { slots: 5 },
    })
    expect(res.statusCode).toBe(404)
  })
})

describe('DELETE /pools/:name', () => {
  it('returns 204 on success', async () => {
    await createPool(db, 'todelete', 1)
    const res = await app.inject({ method: 'DELETE', url: '/pools/todelete' })
    expect(res.statusCode).toBe(204)
  })

  it('returns 404 for unknown pool', async () => {
    const res = await app.inject({ method: 'DELETE', url: '/pools/ghost' })
    expect(res.statusCode).toBe(404)
  })
})

// ── pool field flows end-to-end ───────────────────────────────────────────

describe('pool field on task_instance', () => {
  it('task.pool is stamped on task_instance', async () => {
    const dag: DagDefinition = {
      id: 'pool_stamp_dag', schedule: null,
      tasks: { etl: { pool: 'my_pool', run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await db.collection('task_instances').findOne({ task_id: 'etl' })
    expect(ti?.pool).toBe('my_pool')
    void runId
  })

  it('task without pool has pool=null on task_instance', async () => {
    const dag: DagDefinition = {
      id: 'pool_null_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await db.collection('task_instances').findOne({ task_id: 'step' })
    expect(ti?.pool).toBeNull()
    void runId
  })

  it('task with unknown pool still runs (global-semaphore only)', async () => {
    // Pool is not in DB — acquirePool falls through, task still executes
    const dag: DagDefinition = {
      id: 'pool_unknown_dag', schedule: null,
      tasks: { step: { pool: 'nonexistent_pool', run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const run = await db.collection('dag_runs').findOne({ dag_id: 'pool_unknown_dag' })
    expect(run?.state).toBe('success')
  })

  it('tasks without pool advance normally while a separate pool exists', async () => {
    await createPool(db, 'other_pool', 1)
    const dag: DagDefinition = {
      id: 'pool_normal_dag', schedule: null,
      tasks: { step: { run: async () => {} } },  // no pool
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const run = await db.collection('dag_runs').findOne({ dag_id: 'pool_normal_dag' })
    expect(run?.state).toBe('success')
  })
})
