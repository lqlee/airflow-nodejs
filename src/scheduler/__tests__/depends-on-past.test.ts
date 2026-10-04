import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { isPastSatisfied } from '../claim.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_depends_on_past')
  clearRegistry()
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'event_logs']) await db.collection(c).deleteMany({})
  clearRegistry()
})

const dag: DagDefinition = {
  id: 'dop_dag', schedule: null,
  tasks: {
    seq: { dependsOnPast: true, run: async () => {} },
    free: { run: async () => {} },
  },
}
const ti = async (runId: string, taskId: string) =>
  (await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: taskId }))!
const setTask = (runId: string, taskId: string, state: string) =>
  db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: taskId }, { $set: { state } })
const day = (d: number) => new Date(Date.UTC(2026, 2, d))

describe('isPastSatisfied', () => {
  it.each([
    [undefined, true], [[], true], [['success'], true], [['skipped'], true], [['success', 'skipped'], true],
    [['failed'], false], [['running'], false], [['queued'], false], [['success', 'failed'], false], [['cancelled'], false],
  ])('%j → %s', (states, expected) => {
    expect(isPastSatisfied(states as string[] | undefined)).toBe(expected)
  })
})

describe('depends_on_past', () => {
  it('the first run is not blocked', async () => {
    register(dag)
    const r1 = await createRun(db, dag, { logicalDate: day(1) })
    await advanceRun(db, r1)
    expect((await ti(r1, 'seq')).state).toBe('success')
  })

  it('stamps depends_on_past only on tasks that declare it', async () => {
    register(dag)
    const r = await createRun(db, dag)
    expect((await ti(r, 'seq')).depends_on_past).toBe(true)
    expect((await ti(r, 'free')).depends_on_past).toBe(false)
  })

  it('blocks while the previous run\'s task has not finished; other tasks still run', async () => {
    register(dag)
    const r1 = await createRun(db, dag, { logicalDate: day(1) })
    const r2 = await createRun(db, dag, { logicalDate: day(2) })
    await setTask(r1, 'seq', 'running')   // previous instance still in flight

    await advanceRun(db, r2)
    expect((await ti(r2, 'seq')).state).toBe('queued')
    expect((await ti(r2, 'free')).state).toBe('success')

    await setTask(r1, 'seq', 'success')
    await advanceRun(db, r2)
    expect((await ti(r2, 'seq')).state).toBe('success')
  })

  it('stays blocked when the previous instance failed, until it is fixed', async () => {
    register(dag)
    const r1 = await createRun(db, dag, { logicalDate: day(1) })
    const r2 = await createRun(db, dag, { logicalDate: day(2) })
    await setTask(r1, 'seq', 'failed')

    await advanceRun(db, r2)
    expect((await ti(r2, 'seq')).state).toBe('queued')

    await setTask(r1, 'seq', 'success')   // e.g. cleared and re-run
    await advanceRun(db, r2)
    expect((await ti(r2, 'seq')).state).toBe('success')
  })

  it('a skipped previous instance does not block', async () => {
    register(dag)
    const r1 = await createRun(db, dag, { logicalDate: day(1) })
    const r2 = await createRun(db, dag, { logicalDate: day(2) })
    await setTask(r1, 'seq', 'skipped')
    await advanceRun(db, r2)
    expect((await ti(r2, 'seq')).state).toBe('success')
  })

  it('compares against the previous run by logical date, not by creation order', async () => {
    register(dag)
    const later = await createRun(db, dag, { logicalDate: day(5) })
    const earlier = await createRun(db, dag, { logicalDate: day(1) })   // created second, dated first
    await advanceRun(db, earlier)
    expect((await ti(earlier, 'seq')).state).toBe('success')   // nothing before it
    await setTask(earlier, 'seq', 'failed')

    await advanceRun(db, later)
    expect((await ti(later, 'seq')).state).toBe('queued')       // previous = the day-1 run
  })

  it('ignores cancelled previous runs so they cannot block forever', async () => {
    register(dag)
    const r1 = await createRun(db, dag, { logicalDate: day(1) })
    const r2 = await createRun(db, dag, { logicalDate: day(2) })
    const r3 = await createRun(db, dag, { logicalDate: day(3) })
    await db.collection('dag_runs').updateOne({ _id: new ObjectId(r2) }, { $set: { state: 'cancelled' } })
    await setTask(r2, 'seq', 'cancelled')
    await advanceRun(db, r1)
    await advanceRun(db, r3)
    expect((await ti(r3, 'seq')).state).toBe('success')   // previous non-cancelled = r1 (success)
  })

  it('only looks at the same dag', async () => {
    register(dag)
    const other: DagDefinition = { ...dag, id: 'dop_other' }
    const o = await createRun(db, other, { logicalDate: day(1) })
    await setTask(o, 'seq', 'failed')
    const r = await createRun(db, dag, { logicalDate: day(2) })
    await advanceRun(db, r)
    expect((await ti(r, 'seq')).state).toBe('success')
  })

  it('a task without the flag is never blocked', async () => {
    register(dag)
    const r1 = await createRun(db, dag, { logicalDate: day(1) })
    const r2 = await createRun(db, dag, { logicalDate: day(2) })
    await setTask(r1, 'free', 'failed')
    await advanceRun(db, r2)
    expect((await ti(r2, 'free')).state).toBe('success')
  })
})

describe('depends_on_past + maxActiveRuns — tick ordering', () => {
  it('lets the earlier-dated run take the only slot even when it was created later', async () => {
    const limited: DagDefinition = { ...dag, id: 'dop_limited', maxActiveRuns: 1 }
    register(limited)
    const later = await createRun(db, limited, { logicalDate: day(5) })
    const earlier = await createRun(db, limited, { logicalDate: day(1) })   // created second, dated first

    // The scheduler tick advances runs in this order (see tick() in scheduler/index.ts)
    const ordered = (await db.collection('dag_runs')
      .find({ dag_id: 'dop_limited' }).sort({ ordering_date: 1, created_at: 1 }).toArray()).map(r => r._id.toString())
    expect(ordered).toEqual([earlier, later])
    for (const id of ordered) await advanceRun(db, id)

    expect((await ti(earlier, 'seq')).state).toBe('success')
    const e = await db.collection('dag_runs').findOne({ _id: new ObjectId(earlier) })
    expect(e!.state).toBe('success')
  })
})
