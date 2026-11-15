/**
 * maxActiveTis / maxActiveTisPerRun counted from the DB under claim locks — so the caps hold when
 * several claimers (replicas, overlapping ticks) claim at the same time. Pools: src/pools/__tests__.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { claimReadyTasks } from '../claim.js'
import { tryLock, unlock } from '../claim-locks.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_claim_limits')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'claim_locks']) await db.collection(c).deleteMany({})
})

const noop = async () => {}
const running = () => db.collection('task_instances').countDocuments({ state: 'running' })

describe('claim locks', () => {
  it('one holder at a time; an expired lock can be taken over; only the holder can release it', async () => {
    const now = new Date('2026-06-01T00:00:00Z')
    const first = await tryLock(db, 'k', 10_000, now)
    expect(first).toEqual(expect.any(String))
    expect(await tryLock(db, 'k', 10_000, new Date(now.getTime() + 5_000))).toBeNull()

    await unlock(db, 'k', 'someone-else')
    expect(await tryLock(db, 'k', 10_000, new Date(now.getTime() + 5_000))).toBeNull()

    const second = await tryLock(db, 'k', 10_000, new Date(now.getTime() + 10_001))   // first expired
    expect(second).not.toBe(first)
    await unlock(db, 'k', first!)                                                     // stale token: no effect
    expect(await db.collection('claim_locks').countDocuments()).toBe(1)
    await unlock(db, 'k', second!)
    expect(await db.collection('claim_locks').countDocuments()).toBe(0)
  })
})

describe('claim lock — a write the driver retried', () => {
  it('a duplicate-key error on our own committed upsert still means we hold the lock', async () => {
    // The first attempt commits, the driver retries, and the retry collides with that very row
    const retried = new Proxy(db, {
      get(target, prop) {
        if (prop !== 'collection') return Reflect.get(target, prop)
        return (name: string) => {
          const coll = target.collection(name)
          return new Proxy(coll, {
            get(c, p) {
              if (p === 'findOneAndUpdate') {
                return async (...args: Parameters<typeof c.findOneAndUpdate>) => {
                  await c.findOneAndUpdate(...args)
                  throw Object.assign(new Error('E11000 duplicate key'), { code: 11000 })
                }
              }
              const v = Reflect.get(c, p)
              return typeof v === 'function' ? v.bind(c) : v
            },
          })
        }
      },
    })
    const token = await tryLock(retried, 'retried-key')
    expect(token).toEqual(expect.any(String))
    expect((await db.collection('claim_locks').findOne({ _id: 'retried-key' as never }))!.holder).toBe(token)
  })
})

describe('maxActiveTis across concurrent claimers', () => {
  it('caps running instances of a task across the Dag even when every run claims at once', async () => {
    const dag: DagDefinition = { id: 'mat_dag', schedule: null, tasks: { t: { maxActiveTis: 2, run: noop } } }
    const runs = await Promise.all(Array.from({ length: 8 }, () => createRun(db, dag)))

    // Concurrent: claimers that find the lock busy skip for now — never more than the cap
    await Promise.all(runs.map(r => claimReadyTasks(db, r)))
    expect(await running()).toBeGreaterThanOrEqual(1)
    expect(await running()).toBeLessThanOrEqual(2)
    // Later passes fill the cap exactly
    for (const r of runs) await claimReadyTasks(db, r)
    expect(await running()).toBe(2)
  })

  it('counts an instance another replica is running', async () => {
    const dag: DagDefinition = { id: 'mat_other', schedule: null, tasks: { t: { maxActiveTis: 1, run: noop } } }
    const elsewhere = await createRun(db, dag)
    await db.collection('task_instances').updateOne({ dag_run_id: elsewhere }, { $set: { state: 'running', claimed_by: 'other-replica' } })
    const runId = await createRun(db, dag)
    expect(await claimReadyTasks(db, runId)).toHaveLength(0)
  })

  it('maxActiveTisPerRun holds when two claimers advance the same run at once', async () => {
    const dag: DagDefinition = { id: 'matpr_dag', schedule: null, tasks: { fan: { expand: [1, 2, 3, 4, 5, 6], maxActiveTisPerRun: 2, run: noop } } }
    const runId = await createRun(db, dag)

    await Promise.all([claimReadyTasks(db, runId), claimReadyTasks(db, runId)])
    expect(await running()).toBeLessThanOrEqual(2)
    await claimReadyTasks(db, runId)
    expect(await running()).toBe(2)
  })

  it('uncapped tasks need no lock', async () => {
    const dag: DagDefinition = { id: 'free_dag', schedule: null, tasks: { fan: { expand: [1, 2, 3], run: noop } } }
    const runId = await createRun(db, dag)
    await db.collection('claim_locks').updateOne({ _id: 'task:free_dag:fan' as never },
      { $set: { holder: 'x', expires_at: new Date(Date.now() + 60_000) } }, { upsert: true })
    expect(await claimReadyTasks(db, runId)).toHaveLength(3)
  })
})
