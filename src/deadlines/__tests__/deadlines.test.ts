import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'
import { checkDeadlines, type DeadlineMissInfo } from '../index.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun, cancelRun } from '../../scheduler/index.js'
import { clearTaskInstances, setRunState } from '../../scheduler/actions.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { buildServer } from '../../api/server.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_deadlines')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'deadlines', 'sla_alerts', 'event_logs']) await db.collection(c).deleteMany({})
  clearRegistry()
})

const MIN = 60_000
const noop = async () => {}
const deadlines = (runId?: string) => db.collection('deadlines').find(runId ? { dag_run_id: runId } : {}).sort({ name: 1 }).toArray()
const later = (ms: number) => new Date(Date.now() + ms)
const dag = (id: string, extra: Partial<DagDefinition> = {}): DagDefinition =>
  ({ id, schedule: null, tasks: { t: { run: noop } }, ...extra })

describe('deadlines are computed when the run is created', () => {
  it('logical_date, queued_at and fixed references', async () => {
    const fixed = new Date('2030-01-01T00:00:00Z')
    const d = dag('dl_refs', {
      deadline: [
        { name: 'by_logical', reference: 'logical_date', interval: 30 * MIN },
        { name: 'by_queue', reference: 'queued_at', interval: 5 * MIN },
        { name: 'by_fixed', reference: { fixed }, interval: -MIN },
      ],
    })
    const logicalDate = new Date('2026-06-01T00:00:00Z')
    const runId = await createRun(db, d, { logicalDate })
    const run = (await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) }))!

    const byName = Object.fromEntries((await deadlines(runId)).map(x => [x.name, x]))
    expect(byName.by_logical.deadline_time).toEqual(new Date(logicalDate.getTime() + 30 * MIN))
    expect(byName.by_queue.deadline_time).toEqual(new Date(run.queued_at.getTime() + 5 * MIN))
    expect(byName.by_fixed).toMatchObject({ deadline_time: new Date(fixed.getTime() - MIN), reference: 'fixed', missed: false })
  })

  it('a run without a logical date gets no logical_date deadline', async () => {
    const runId = await createRun(db, dag('dl_manual', { deadline: { reference: 'logical_date', interval: MIN } }))
    expect(await deadlines(runId)).toHaveLength(0)
  })

  it('averageRuntime waits for enough finished runs, then uses now + their mean duration', async () => {
    const d = dag('dl_avg', { deadline: { name: 'avg', reference: { averageRuntime: { maxRuns: 2 } }, interval: MIN } })
    const finished = (ms: number) => db.collection('dag_runs').insertOne({
      dag_id: 'dl_avg', state: 'success', started_at: new Date(0), ended_at: new Date(ms), ordering_date: new Date(), created_at: new Date(),
    })
    await finished(10_000)
    expect(await deadlines(await createRun(db, d))).toHaveLength(0)   // 1 of the 2 runs needed

    await finished(30_000)
    const before = Date.now()
    const [dl] = await deadlines(await createRun(db, d))
    expect(dl.deadline_time.getTime()).toBeGreaterThanOrEqual(before + 20_000 + MIN)
    expect(dl.deadline_time.getTime()).toBeLessThanOrEqual(Date.now() + 20_000 + MIN)
  })
})

describe('checkDeadlines', () => {
  it('marks a passed deadline missed, records an event and fires its callback once', async () => {
    const calls: DeadlineMissInfo[] = []
    const d = dag('dl_miss', { deadline: { name: 'fast', reference: 'queued_at', interval: MIN, callback: i => { calls.push(i) } } })
    register(d)
    const runId = await createRun(db, d)

    await Promise.all(await checkDeadlines(db, [d], later(30_000)))
    expect(calls).toHaveLength(0)

    const [a, b] = await Promise.all([checkDeadlines(db, [d], later(2 * MIN)), checkDeadlines(db, [d], later(2 * MIN))])
    await Promise.all([...a, ...b])
    await Promise.all(await checkDeadlines(db, [d], later(3 * MIN)))
    expect(calls).toEqual([expect.objectContaining({ dagId: 'dl_miss', runId, name: 'fast', reference: 'queued_at', runState: 'queued' })])
    expect((await deadlines(runId))[0]).toMatchObject({ missed: true, missed_at: expect.any(Date) })
    await expect.poll(() => db.collection('event_logs').countDocuments({ event_type: 'deadline_missed', dag_run_id: runId })).toBe(1)
  })

  it('a successful run drops its pending deadlines; a failed run keeps them and still alerts', async () => {
    const calls: string[] = []
    const ok = dag('dl_ok', { deadline: { reference: 'queued_at', interval: MIN, callback: i => { calls.push(i.dagId) } } })
    const bad = dag('dl_bad', { tasks: { t: { run: async () => { throw new Error('x') } } }, deadline: { reference: 'queued_at', interval: MIN, callback: i => { calls.push(`${i.dagId}:${i.runState}`) } } })
    register(ok); register(bad)
    const okRun = await createRun(db, ok)
    const badRun = await createRun(db, bad)
    for (let i = 0; i < 10; i++) { await advanceRun(db, okRun); await advanceRun(db, badRun) }

    expect(await deadlines(okRun)).toHaveLength(0)
    await Promise.all(await checkDeadlines(db, [ok, bad], later(2 * MIN)))
    expect(calls).toEqual(['dl_bad:failed'])
  })

  it('cancelling or marking a run success drops its pending deadlines', async () => {
    const d = dag('dl_cancel', { deadline: { reference: 'queued_at', interval: MIN } })
    register(d)
    const a = await createRun(db, d)
    const b = await createRun(db, d)
    await cancelRun(db, a)
    await setRunState(db, b, 'success')
    expect(await deadlines()).toHaveLength(0)
  })

  it('clearing a run restarts its queued_at deadlines from the clear; missed ones stay as history', async () => {
    const d = dag('dl_clear', { tasks: { t: { run: async () => { throw new Error('x') } } }, deadline: { name: 'q', reference: 'queued_at', interval: MIN } })
    register(d)
    const runId = await createRun(db, d)
    for (let i = 0; i < 5; i++) await advanceRun(db, runId)
    await checkDeadlines(db, [d], later(2 * MIN))   // missed

    const cleared = Date.now()
    await clearTaskInstances(db, runId)
    const all = await deadlines(runId)
    expect(all.map(x => x.missed).sort()).toEqual([false, true])
    expect(all.find(x => !x.missed)!.deadline_time.getTime()).toBeGreaterThanOrEqual(cleared + MIN)
  })

  it('active runs created without deadlines (older versions) get them on the next check', async () => {
    const d = dag('dl_backfill', { deadline: { reference: 'queued_at', interval: MIN } })
    const { insertedId } = await db.collection('dag_runs').insertOne({ dag_id: 'dl_backfill', state: 'running', created_at: new Date() })
    await db.collection('dag_runs').insertOne({ dag_id: 'dl_backfill', state: 'success', created_at: new Date() })

    await checkDeadlines(db, [d])
    await checkDeadlines(db, [d])
    expect((await deadlines()).map(x => x.dag_run_id)).toEqual([insertedId.toString()])
  })
})

describe('checkDeadlines — deleted runs', () => {
  it('drops the deadline of a run that no longer exists, without alerting', async () => {
    let n = 0
    const d = dag('dl_gone', { deadline: { reference: 'queued_at', interval: MIN, callback: () => { n++ } } })
    const runId = await createRun(db, d)
    await db.collection('dag_runs').deleteOne({ _id: new ObjectId(runId) })
    await Promise.all(await checkDeadlines(db, [d], later(2 * MIN)))
    expect(n).toBe(0)
    expect(await deadlines()).toHaveLength(0)
  })
})

describe('sla — a deadline alias', () => {
  it('a run still unfinished sla ms after it was queued gets one SLA alert', async () => {
    const d = dag('dl_sla', { sla: MIN })
    const runId = await createRun(db, d)
    expect(await deadlines(runId)).toEqual([expect.objectContaining({ name: 'sla', reference: 'queued_at' })])

    await checkDeadlines(db, [d], later(2 * MIN))
    const alerts = await db.collection('sla_alerts').find().toArray()
    expect(alerts).toEqual([expect.objectContaining({ dag_id: 'dl_sla', dag_run_id: runId, sla_ms: MIN, acked: false })])
    expect(alerts[0].elapsed_ms).toBeGreaterThanOrEqual(2 * MIN - 1_000)
  })
})

describe('GET /deadlines', () => {
  let app: FastifyInstance
  beforeAll(async () => { app = buildServer(db); await app.ready() })
  afterAll(async () => { await app.close() })

  it('lists deadlines filtered by run and missed', async () => {
    const d = dag('dl_api', { deadline: [{ name: 'soon', reference: 'queued_at', interval: MIN }, { name: 'late', reference: 'queued_at', interval: 60 * MIN }] })
    const runId = await createRun(db, d)
    await createRun(db, d)
    await checkDeadlines(db, [d], later(2 * MIN))

    const res = await app.inject({ method: 'GET', url: `/deadlines?dag_run_id=${runId}&missed=true` })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([expect.objectContaining({ dag_id: 'dl_api', dag_run_id: runId, name: 'soon', missed: true })])
    expect((await app.inject({ method: 'GET', url: '/deadlines?dag_id=dl_api' })).json()).toHaveLength(4)
    expect((await app.inject({ method: 'GET', url: '/deadlines?missed=maybe' })).statusCode).toBe(400)
  })
})
