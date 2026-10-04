import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { tickCatchup, syncCronJobs, stopAllCronJobs, activeCronJobCount, resetCatchupState, CATCHUP_MAX_RUNS_PER_TICK } from '../cron.js'
import { createRun } from '../runs.js'
import { pauseDag } from '../../dag/pause.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_catchup')
})
afterAll(async () => { stopAllCronJobs(); await db.dropDatabase(); await client.close() })
afterEach(async () => {
  stopAllCronJobs()
  resetCatchupState()
  for (const c of ['dag_runs', 'task_instances', 'dag_pauses', 'event_logs']) await db.collection(c).deleteMany({})
})

const NOW = new Date('2026-03-10T12:30:00Z')
const hourly = (extra: Partial<DagDefinition> = {}): DagDefinition => ({
  id: 'catchup_dag', schedule: '0 * * * *', catchup: true,
  startDate: '2026-03-10T08:00:00Z',
  tasks: { t: { run: async () => {} } },
  ...extra,
})
const runs = () => db.collection('dag_runs').find({ dag_id: 'catchup_dag' }).sort({ logical_date: 1 }).toArray()
const hours = (rs: { logical_date: Date }[]) => rs.map(r => new Date(r.logical_date).getUTCHours())

describe('tickCatchup', () => {
  it('creates a run for every missed occurrence since startDate, oldest first', async () => {
    expect(await tickCatchup(db, [hourly()], NOW)).toBe(5)   // 08,09,10,11,12
    const rs = await runs()
    expect(hours(rs as never)).toEqual([8, 9, 10, 11, 12])
    expect(rs.every(r => r.trigger_type === 'catchup' && r.state === 'queued')).toBe(true)
  })

  it('is idempotent — a second tick creates nothing', async () => {
    await tickCatchup(db, [hourly()], NOW)
    expect(await tickCatchup(db, [hourly()], NOW)).toBe(0)
    expect(await runs()).toHaveLength(5)
  })

  it('picks up new occurrences as time advances', async () => {
    await tickCatchup(db, [hourly()], NOW)
    expect(await tickCatchup(db, [hourly()], new Date('2026-03-10T14:05:00Z'))).toBe(2)   // 13, 14
    expect(hours(await runs() as never)).toEqual([8, 9, 10, 11, 12, 13, 14])
  })

  it('does not duplicate a date already covered by another run (e.g. a backfill)', async () => {
    await createRun(db, hourly(), { logicalDate: new Date('2026-03-10T10:00:00Z'), triggerType: 'backfill' })
    expect(await tickCatchup(db, [hourly()], NOW)).toBe(4)
    expect(hours(await runs() as never)).toEqual([8, 9, 10, 11, 12])
  })

  it('resumes from the latest catch-up run after downtime when no startDate is set', async () => {
    const dag = hourly({ startDate: undefined })
    await createRun(db, dag, { logicalDate: new Date('2026-03-10T09:00:00Z'), triggerType: 'catchup' })
    expect(await tickCatchup(db, [dag], NOW)).toBe(3)   // 10, 11, 12
  })

  it('without startDate or history it only starts counting from first sight', async () => {
    const dag = hourly({ startDate: undefined })
    expect(await tickCatchup(db, [dag], NOW)).toBe(0)
    expect(await tickCatchup(db, [dag], new Date('2026-03-10T14:00:00Z'))).toBe(2)   // 13, 14
  })

  it('caps runs created per tick and replays the rest on later ticks', async () => {
    const dag = hourly({ schedule: '* * * * *', startDate: '2026-03-10T08:00:00Z' })   // every minute → 271 due
    expect(await tickCatchup(db, [dag], NOW)).toBe(CATCHUP_MAX_RUNS_PER_TICK)
    expect(await tickCatchup(db, [dag], NOW)).toBe(CATCHUP_MAX_RUNS_PER_TICK)
    expect(await tickCatchup(db, [dag], NOW)).toBe(271 - 2 * CATCHUP_MAX_RUNS_PER_TICK)
    expect(await runs()).toHaveLength(271)
  })

  it('skips paused dags, then catches everything up after resume', async () => {
    await pauseDag(db, 'catchup_dag')
    expect(await tickCatchup(db, [hourly()], NOW)).toBe(0)
    const { resumeDag } = await import('../../dag/pause.js')
    await resumeDag(db, 'catchup_dag')
    expect(await tickCatchup(db, [hourly()], NOW)).toBe(5)
  })

  it('ignores dags without catchup, without a schedule, or with an invalid startDate', async () => {
    const dags = [
      hourly({ id: 'no_catchup', catchup: false }),
      hourly({ id: 'no_schedule', schedule: null }),
      hourly({ id: 'bad_start', startDate: 'not-a-date' }),
    ]
    expect(await tickCatchup(db, dags, NOW)).toBe(0)
    expect(await db.collection('dag_runs').countDocuments()).toBe(0)
  })
})

describe('syncCronJobs and catchup', () => {
  it('does not register a node-cron job for catchup dags (tickCatchup owns them)', () => {
    syncCronJobs(db, [hourly()])
    expect(activeCronJobCount()).toBe(0)
  })
  it('still registers normal scheduled dags', () => {
    syncCronJobs(db, [hourly({ id: 'plain', catchup: false })])
    expect(activeCronJobCount()).toBe(1)
  })
})

describe('tickCatchup — overlapping ticks', () => {
  it('concurrent calls never create duplicate runs for the same date', async () => {
    await Promise.all([tickCatchup(db, [hourly()], NOW), tickCatchup(db, [hourly()], NOW), tickCatchup(db, [hourly()], NOW)])
    const rs = await runs()
    expect(rs).toHaveLength(5)
    expect(new Set(rs.map(r => new Date(r.logical_date).getTime())).size).toBe(5)
  })
})
