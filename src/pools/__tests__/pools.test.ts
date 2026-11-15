/**
 * Resource Pools tests.
 *
 * Enforcement happens when a task instance is claimed (scheduler/claim-limits.ts): occupancy is the
 * sum of pool_slots of the pool's RUNNING instances in the DB, so a slot held by another replica (or
 * a BullMQ worker) counts exactly like one held by this process. Claims for a pool are serialized by
 * a short claim lock, so two concurrent claimers can never both take the last slot.
 *
 * Integration tests verify pool field flows end-to-end: task.pool → task_instance.pool.
 * Tasks without pool are unaffected (regression guard).
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createPool, getPool, listPools, updatePool, deletePool } from '../index.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { claimReadyTasks } from '../../scheduler/claim.js'
import type { DagDefinition, TaskDefinition } from '../../dag/types.js'
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
  for (const c of ['pools', 'dag_runs', 'task_instances', 'event_logs', 'claim_locks']) await db.collection(c).deleteMany({})
  clearRegistry()
})

const noop = async () => {}

/** A Dag whose tasks all sit in `pool` (no dependencies, so every instance is ready at once). */
function poolDag(id: string, tasks: Record<string, Partial<TaskDefinition>>): DagDefinition {
  const dag: DagDefinition = {
    id, schedule: null,
    tasks: Object.fromEntries(Object.entries(tasks).map(([k, t]) => [k, { run: noop, ...t } as TaskDefinition])),
  }
  register(dag)
  return dag
}

const claimedIds = async (runId: string) => (await claimReadyTasks(db, runId)).map(t => `${t.task_id}${t.map_index ?? ''}`).sort()

// ── Claim-time enforcement ────────────────────────────────────────────────

describe('pool slots — enforced when claiming', () => {
  it('a full pool refuses further claims until the running instance finishes', async () => {
    await createPool(db, 'tight', 1)
    const dag = poolDag('tight_dag', { fan: { pool: 'tight', expand: [1, 2, 3] } })
    const runId = await createRun(db, dag)

    expect(await claimedIds(runId)).toEqual(['fan0'])
    expect(await claimedIds(runId)).toEqual([])            // still running → no free slot

    await db.collection('task_instances').updateOne({ dag_run_id: runId, map_index: 0 }, { $set: { state: 'success' } })
    expect(await claimedIds(runId)).toEqual(['fan1'])
  })

  it('counts slots held by another replica: a running instance it claimed blocks this one', async () => {
    await createPool(db, 'shared', 1)
    const dag = poolDag('shared_dag', { t: { pool: 'shared' } })
    const elsewhere = await createRun(db, dag)
    await db.collection('task_instances').updateOne({ dag_run_id: elsewhere }, { $set: { state: 'running', claimed_by: 'other-replica' } })
    const runId = await createRun(db, dag)

    expect(await claimedIds(runId)).toEqual([])
    expect(await getPool(db, 'shared')).toMatchObject({ occupied_slots: 1, open_slots: 0, queued_slots: 1 })
  })

  it('two concurrent claimers never both take the last slot', async () => {
    await createPool(db, 'race', 1)
    const dag = poolDag('race_dag', { t: { pool: 'race' } })
    const runs = await Promise.all(Array.from({ length: 6 }, () => createRun(db, dag)))

    const claimed = (await Promise.all(runs.map(r => claimReadyTasks(db, r)))).flat()
    expect(claimed).toHaveLength(1)
    expect(await db.collection('task_instances').countDocuments({ state: 'running' })).toBe(1)
    expect(await db.collection('claim_locks').countDocuments()).toBe(0)   // every claimer released its lock
  })

  it('a pool lock held by a live claimer elsewhere defers the claim; an expired one is taken over', async () => {
    await createPool(db, 'locked', 5)
    const dag = poolDag('locked_dag', { t: { pool: 'locked' } })
    const runId = await createRun(db, dag)
    await db.collection('claim_locks').updateOne({ _id: 'pool:locked' as never },
      { $set: { holder: 'other#1', expires_at: new Date(Date.now() + 60_000) } }, { upsert: true })

    expect(await claimedIds(runId)).toEqual([])
    expect((await db.collection('task_instances').findOne({ dag_run_id: runId }))!.state).toBe('queued')

    await db.collection('claim_locks').updateOne({ _id: 'pool:locked' as never }, { $set: { expires_at: new Date(Date.now() - 1) } })
    expect(await claimedIds(runId)).toEqual(['t'])
  })

  it('weights by poolSlots: a 2-slot task leaves room for a 1-slot task but not another 2-slot one', async () => {
    await createPool(db, 'wide', 3)
    const dag = poolDag('wide_dag', {
      a_wide: { pool: 'wide', poolSlots: 2, priority: 3 },
      b_wide: { pool: 'wide', poolSlots: 2, priority: 2 },
      c_narrow: { pool: 'wide', poolSlots: 1, priority: 1 },
    })
    const runId = await createRun(db, dag)
    expect(await claimedIds(runId)).toEqual(['a_wide', 'c_narrow'])
    expect(await getPool(db, 'wide')).toMatchObject({ occupied_slots: 3, open_slots: 0, queued_slots: 2 })
  })

  it('a request larger than the pool is clamped to the pool size instead of never running', async () => {
    await createPool(db, 'small', 2)
    const dag = poolDag('small_dag', { huge: { pool: 'small', poolSlots: 5 } })
    const runId = await createRun(db, dag)
    expect(await claimedIds(runId)).toEqual(['huge'])
    expect(await getPool(db, 'small')).toMatchObject({ occupied_slots: 2, open_slots: 0 })
  })

  it('deferred and up_for_retry instances hold no slots', async () => {
    await createPool(db, 'waiting', 1)
    const dag = poolDag('waiting_dag', { t: { pool: 'waiting' } })
    const parked = await createRun(db, dag)
    await db.collection('task_instances').updateOne({ dag_run_id: parked }, { $set: { state: 'deferred' } })
    const retrying = await createRun(db, dag)
    await db.collection('task_instances').updateOne({ dag_run_id: retrying }, { $set: { state: 'up_for_retry', next_retry_at: new Date(Date.now() + 60_000) } })
    const runId = await createRun(db, dag)
    expect(await claimedIds(runId)).toEqual(['t'])
  })

  it('a resized pool applies to the next claim', async () => {
    await createPool(db, 'resize', 1)
    const dag = poolDag('resize_dag', { fan: { pool: 'resize', expand: [1, 2, 3] } })
    const runId = await createRun(db, dag)
    expect(await claimedIds(runId)).toEqual(['fan0'])
    await updatePool(db, 'resize', { slots: 3 })
    expect(await claimedIds(runId)).toEqual(['fan1', 'fan2'])
  })

  it('an unknown pool does not limit the task', async () => {
    const dag = poolDag('unknown_dag', { fan: { pool: 'no_such_pool', expand: [1, 2, 3] } })
    const runId = await createRun(db, dag)
    expect(await claimedIds(runId)).toEqual(['fan0', 'fan1', 'fan2'])
  })

  it('pools are independent — one full pool does not block another', async () => {
    await createPool(db, 'full', 1)
    await createPool(db, 'free', 1)
    const dag = poolDag('two_pools', { a: { pool: 'full', priority: 2 }, b: { pool: 'full', priority: 1 }, c: { pool: 'free' } })
    const runId = await createRun(db, dag)
    expect(await claimedIds(runId)).toEqual(['a', 'c'])
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

  it('task with unknown pool still runs (no pool limit)', async () => {
    // Pool is not in DB — the claim falls through, task still executes
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

describe('poolSlots on task instances', () => {
  it('createRun stamps pool_slots (default 1, floor ≥ 1) and a run with a 2-slot task completes', async () => {
    await createPool(db, 'stamp_pool', 2)
    const dag: DagDefinition = {
      id: 'pool_slots_dag', schedule: null,
      tasks: {
        wide: { pool: 'stamp_pool', poolSlots: 2, run: async () => {} },
        plain: { pool: 'stamp_pool', run: async () => {} },
        bad: { pool: 'stamp_pool', poolSlots: 0, run: async () => {} },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const slots = Object.fromEntries(
      (await db.collection('task_instances').find({ dag_run_id: runId }).toArray()).map(t => [t.task_id, t.pool_slots]),
    )
    expect(slots).toEqual({ wide: 2, plain: 1, bad: 1 })

    for (let i = 0; i < 6; i++) await advanceRun(db, runId)
    const run = await db.collection('dag_runs').findOne({ dag_id: 'pool_slots_dag' })
    expect(run!.state).toBe('success')
    expect((await getPool(db, 'stamp_pool'))!.occupied_slots).toBe(0)   // nothing left running
  }, 60000)
})
