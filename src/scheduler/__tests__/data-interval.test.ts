/** Logical date + data interval per schedule mode, and what task code / templates see of them. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { ensureIndexes } from '../../db/indexes.js'
import { runInfoForFire, runInfoForLogicalDate } from '../data-interval.js'
import { createRun } from '../runs.js'
import { fireCronRun, tickCatchup, resetCatchupState } from '../cron.js'
import { backfill } from '../backfill.js'
import { advanceRun } from '../index.js'
import { setElectionActive } from '../lease.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.LOG_BACKEND = 'mongodb'   // read shell output from task_logs
  process.env.DB_NAME = 'airflow_test_data_interval'   // forked workers connect here
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_data_interval')
  await ensureIndexes(db)
})
afterAll(async () => { delete process.env.LOG_BACKEND; delete process.env.DB_NAME; await db.dropDatabase(); await client.close() })
afterEach(async () => {
  setElectionActive(false)
  resetCatchupState()
  clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'backfills', 'xcoms', 'task_logs', 'task_instance_tries', 'event_logs']) await db.collection(c).deleteMany({})
})

const D = (s: string) => new Date(s)
const HOURLY = '0 * * * *'
const DAILY = '0 0 * * *'
const noop = async () => {}
const dag = (id: string, extra: Partial<DagDefinition> = {}): DagDefinition =>
  ({ id, schedule: DAILY, tasks: { t: { run: noop } }, ...extra })
const runs = (dagId: string) => db.collection('dag_runs').find({ dag_id: dagId }).sort({ logical_date: 1 }).toArray()
const iso = (d: Date | null | undefined) => d?.toISOString() ?? null

describe('runInfoForFire / runInfoForLogicalDate', () => {
  const T = D('2026-03-10T08:00:00Z')

  it("'trigger' (default): logical date = fire time, interval reaches back triggerInterval", () => {
    expect(runInfoForFire(dag('a', { schedule: HOURLY }), T, 'UTC')).toEqual({ logicalDate: T, dataInterval: { start: T, end: T } })
    const info = runInfoForFire(dag('a', { schedule: HOURLY, triggerInterval: 3_600_000 }), T, 'UTC')
    expect(info).toEqual({ logicalDate: T, dataInterval: { start: D('2026-03-10T07:00:00Z'), end: T } })
  })

  it("'interval': the run fired at T covers the period that just ended", () => {
    const d = dag('b', { schedule: HOURLY, scheduleMode: 'interval' })
    expect(runInfoForFire(d, T, 'UTC')).toEqual({ logicalDate: D('2026-03-10T07:00:00Z'), dataInterval: { start: D('2026-03-10T07:00:00Z'), end: T } })
    // A backfill names the period by its start
    expect(runInfoForLogicalDate(d, T)).toEqual({ logicalDate: T, dataInterval: { start: T, end: D('2026-03-10T09:00:00Z') } })
  })
})

describe('runs carry their data interval', () => {
  it('manual runs have none; runs with a logical date get one from the schedule mode', async () => {
    const manual = await createRun(db, dag('m'))
    const dated = await createRun(db, dag('m2', { scheduleMode: 'interval' }), { logicalDate: D('2026-01-05T00:00:00Z') })
    const [m] = await db.collection('dag_runs').find({ _id: new ObjectId(manual) }).toArray()
    const [d] = await db.collection('dag_runs').find({ _id: new ObjectId(dated) }).toArray()
    expect(m).toMatchObject({ logical_date: null, data_interval_start: null, data_interval_end: null })
    expect([iso(d.data_interval_start), iso(d.data_interval_end)]).toEqual(['2026-01-05T00:00:00.000Z', '2026-01-06T00:00:00.000Z'])
  })

  it('a cron firing in interval mode creates the run of the previous period', async () => {
    const d = dag('cf_int', { schedule: HOURLY, scheduleMode: 'interval' })
    const fired = D('2026-03-10T08:00:00.300Z')
    await fireCronRun(db, d, HOURLY, fired, { advance: false })
    const [run] = await runs('cf_int')
    // Local clock: the previous hourly tick is one hour back in any zone with whole-hour offsets
    expect(iso(run.data_interval_end)).toBe('2026-03-10T08:00:00.000Z')
    expect(iso(run.logical_date)).toBe('2026-03-10T07:00:00.000Z')
    expect(iso(run.data_interval_start)).toBe('2026-03-10T07:00:00.000Z')
  })

  it('catch-up in interval mode: one run per completed day since startDate, none for the period in progress', async () => {
    const d = dag('cu_int', { catchup: true, scheduleMode: 'interval', startDate: '2026-01-01T00:00:00Z' })
    await tickCatchup(db, [d], D('2026-01-04T00:00:30Z'))
    let all = await runs('cu_int')
    expect(all.map(r => [iso(r.logical_date), iso(r.data_interval_end)])).toEqual([
      ['2026-01-01T00:00:00.000Z', '2026-01-02T00:00:00.000Z'],
      ['2026-01-02T00:00:00.000Z', '2026-01-03T00:00:00.000Z'],
      ['2026-01-03T00:00:00.000Z', '2026-01-04T00:00:00.000Z'],
    ])

    await tickCatchup(db, [d], D('2026-01-04T12:00:00Z'))   // same day: nothing new
    expect(await runs('cu_int')).toHaveLength(3)
    await tickCatchup(db, [d], D('2026-01-05T00:00:01Z'))
    all = await runs('cu_int')
    expect(all.map(r => iso(r.logical_date)).at(-1)).toBe('2026-01-04T00:00:00.000Z')
    expect(all).toHaveLength(4)
  })

  it('backfill in interval mode names each run by the start of its period', async () => {
    const d = dag('bf_int', { scheduleMode: 'interval' })
    await backfill(db, d, { start: D('2026-02-01T00:00:00Z'), end: D('2026-02-02T00:00:00Z') })
    expect((await runs('bf_int')).map(r => [iso(r.logical_date), iso(r.data_interval_end)])).toEqual([
      ['2026-02-01T00:00:00.000Z', '2026-02-02T00:00:00.000Z'],
      ['2026-02-02T00:00:00.000Z', '2026-02-03T00:00:00.000Z'],
    ])
  })
})

describe('task code and templates see the run dates', () => {
  it('ctx exposes logicalDate, the data interval, ds and params; shell templates render macros', async () => {
    const d: DagDefinition = {
      id: 'ctx_dates', schedule: DAILY, scheduleMode: 'interval',
      tasks: {
        fn: {
          run: new Function(`return async (ctx) => {
            await ctx.xcom.push('seen', {
              logical: ctx.logicalDate && new Date(ctx.logicalDate).toISOString(),
              start: ctx.dataIntervalStart && new Date(ctx.dataIntervalStart).toISOString(),
              end: ctx.dataIntervalEnd && new Date(ctx.dataIntervalEnd).toISOString(),
              ds: ctx.ds, region: ctx.params.region,
            })
          }`)() as () => Promise<void>,
        },
        sh: { shell: { command: "echo \"[{{ macros.ds_add(ds, 7) }}|{{ data_interval_end | ds_nodash }}|{{ params.region | upper }}]\"" } },
      },
    }
    register(d)
    const runId = await createRun(db, d, { logicalDate: D('2026-04-01T00:00:00Z'), conf: { region: 'eu' } })
    for (let i = 0; i < 10; i++) await advanceRun(db, runId)

    const x = await db.collection('xcoms').findOne({ dag_run_id: runId, task_id: 'fn', key: 'seen' })
    const seen = typeof x!.value === 'string' ? JSON.parse(x!.value) : x!.value
    expect(seen).toEqual({
      logical: '2026-04-01T00:00:00.000Z', start: '2026-04-01T00:00:00.000Z', end: '2026-04-02T00:00:00.000Z', ds: '2026-04-01', region: 'eu',
    })
    await expect.poll(async () => (await db.collection('task_logs').find({ dag_run_id: runId, task_id: 'sh' }).toArray())
      .map(l => l.line).join('\n')).toContain('[2026-04-08|20260402|EU]')
  })
})
