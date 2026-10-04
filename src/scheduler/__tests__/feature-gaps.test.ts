/**
 * Integration gaps for the run-control features: behaviour that spans the scheduler,
 * executor and pools together (the unit tests cover each piece in isolation).
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { scheduleDag, tickCatchup, stopAllCronJobs, activeCronJobCount, resetCatchupState } from '../cron.js'
import { createPool, resetAllPools, poolActiveCount } from '../../pools/index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB = 'airflow_test_feature_gaps'
let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.DB_NAME = DB   // forked workers read/write XCom here
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(DB)
  clearRegistry()
})
afterAll(async () => { stopAllCronJobs(); await db.dropDatabase(); await client.close(); delete process.env.DB_NAME })
afterEach(async () => {
  stopAllCronJobs(); resetCatchupState(); resetAllPools(); clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'xcoms', 'event_logs', 'pools', 'dag_paused']) await db.collection(c).deleteMany({})
})

const runDoc = async (id: string) => (await db.collection('dag_runs').findOne({ _id: new ObjectId(id) }))!
const ti = async (runId: string, taskId: string, mapIndex: number | null = null) =>
  (await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: taskId, map_index: mapIndex }))!

describe('runTimeout — between waves', () => {
  it('a slow task finishes, but the tasks after it are failed and the run fails', async () => {
    const dag: DagDefinition = {
      id: 'timeout_midrun', schedule: null, runTimeout: 150,
      tasks: {
        slow: { run: async () => { await new Promise(r => setTimeout(r, 600)) } },
        after: { dependsOn: ['slow'], run: async () => {} },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    expect((await ti(runId, 'slow')).state).toBe('success')   // not killed mid-flight (documented limitation)
    expect(await ti(runId, 'after')).toMatchObject({ state: 'failed', error: expect.stringMatching(/runTimeout \(150ms\)/) })
    expect((await runDoc(runId)).state).toBe('failed')
  }, 20000)
})

describe('poolSlots — real executor', () => {
  it('a 2-slot task and a 1-slot task in a 2-slot pool never overlap', async () => {
    await createPool(db, 'duo', 2)
    const body = (label: string) => new Function(`return async (ctx) => {
      await ctx.xcom.push('start', Date.now())
      await new Promise(r => setTimeout(r, 400))
      await ctx.xcom.push('end', Date.now())
    }`)() as () => Promise<void>
    const dag: DagDefinition = {
      id: 'pool_overlap', schedule: null,
      tasks: {
        wide: { pool: 'duo', poolSlots: 2, run: body('wide') as never },
        narrow: { pool: 'duo', poolSlots: 1, run: body('narrow') as never },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const get = async (task: string, key: string) =>
      (await db.collection('xcoms').findOne({ dag_run_id: runId, task_id: task, key }))!.value as number
    const w = { s: await get('wide', 'start'), e: await get('wide', 'end') }
    const n = { s: await get('narrow', 'start'), e: await get('narrow', 'end') }
    const overlap = Math.min(w.e, n.e) - Math.max(w.s, n.s)
    expect(overlap).toBeLessThanOrEqual(0)             // serialized by the pool
    expect((await runDoc(runId)).state).toBe('success')
    expect(poolActiveCount('duo')).toBe(0)
  }, 30000)
})

describe('depends_on_past — mapped tasks', () => {
  const dag: DagDefinition = {
    id: 'dop_mapped', schedule: null,
    tasks: { fan: { dependsOnPast: true, expand: ['a', 'b'], run: async () => {} } },
  }
  const day = (d: number) => new Date(Date.UTC(2026, 2, d))

  it('waits until EVERY mapped instance of the previous run has succeeded', async () => {
    register(dag)
    const r1 = await createRun(db, dag, { logicalDate: day(1) })
    const r2 = await createRun(db, dag, { logicalDate: day(2) })
    await db.collection('task_instances').updateOne({ dag_run_id: r1, task_id: 'fan', map_index: 0 }, { $set: { state: 'success' } })
    await db.collection('task_instances').updateOne({ dag_run_id: r1, task_id: 'fan', map_index: 1 }, { $set: { state: 'failed' } })

    await advanceRun(db, r2)
    expect((await ti(r2, 'fan', 0)).state).toBe('queued')
    expect((await ti(r2, 'fan', 1)).state).toBe('queued')

    await db.collection('task_instances').updateOne({ dag_run_id: r1, task_id: 'fan', map_index: 1 }, { $set: { state: 'success' } })
    await advanceRun(db, r2)
    expect((await ti(r2, 'fan', 0)).state).toBe('success')
    expect((await ti(r2, 'fan', 1)).state).toBe('success')
  }, 20000)
})

describe('catchup — interplay with other features', () => {
  const NOW = new Date('2026-03-10T12:30:00Z')
  const held = (extra: Partial<DagDefinition> = {}): DagDefinition => ({
    id: 'catchup_held', schedule: '0 * * * *', catchup: true, startDate: '2026-03-10T09:00:00Z',
    tasks: { gate: { requiresApproval: true, run: async () => {} } },   // keeps runs alive
    ...extra,
  })

  it('scheduleDag never registers a node-cron job for a catchup dag', () => {
    scheduleDag(db, held())
    expect(activeCronJobCount()).toBe(0)
  })

  it('maxActiveRuns throttles a catch-up replay: one run active, the rest stay queued', async () => {
    const dag = held({ maxActiveRuns: 1 })
    register(dag)
    expect(await tickCatchup(db, [dag], NOW)).toBe(4)   // 09,10,11,12

    const runs = await db.collection('dag_runs').find({ dag_id: 'catchup_held' }).sort({ ordering_date: 1 }).toArray()
    for (const r of runs) await advanceRun(db, r._id.toString())

    const states = (await db.collection('dag_runs').find({ dag_id: 'catchup_held' }).sort({ ordering_date: 1 }).toArray()).map(r => r.state)
    expect(states).toEqual(['running', 'queued', 'queued', 'queued'])   // oldest got the slot
  })
})
