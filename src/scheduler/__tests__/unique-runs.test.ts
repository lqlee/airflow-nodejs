import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { ensureIndexes } from '../../db/indexes.js'
import { createRun, DuplicateRunError } from '../runs.js'
import { cronLogicalDate, tickCatchup, resetCatchupState } from '../cron.js'
import { backfill } from '../backfill.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_unique_runs')
  await ensureIndexes(db)
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  resetCatchupState()
  for (const c of ['dag_runs', 'task_instances', 'backfills', 'event_logs']) await db.collection(c).deleteMany({})
})

const dag = (id = 'u_dag', extra: Partial<DagDefinition> = {}): DagDefinition => ({
  id, schedule: '0 * * * *', tasks: { t: { run: async () => {} } }, ...extra,
})
const D = new Date('2026-03-10T08:00:00Z')

/**
 * A Db whose `dag_runs.find()` sees nothing — simulates another replica inserting a run between the
 * caller's "which dates already exist?" query and its insert. Everything else hits the real DB.
 */
function blindToExistingRuns(real: Db): Db {
  return new Proxy(real, {
    get(target, prop) {
      if (prop !== 'collection') return Reflect.get(target, prop)
      return (name: string) => {
        const coll = target.collection(name)
        if (name !== 'dag_runs') return coll
        return new Proxy(coll, {
          get(c, p) {
            if (p === 'find') return () => ({ toArray: async () => [] })
            const v = Reflect.get(c, p)
            return typeof v === 'function' ? v.bind(c) : v
          },
        })
      }
    },
  })
}

describe('one run per (dag_id, logical_date)', () => {
  it('rejects a second run for the same Dag and date, and creates no task instances for it', async () => {
    await createRun(db, dag(), { logicalDate: D, triggerType: 'cron' })
    const second = createRun(db, dag(), { logicalDate: D, triggerType: 'backfill' })
    await expect(second).rejects.toBeInstanceOf(DuplicateRunError)
    await expect(second).rejects.toThrow(/u_dag.*2026-03-10T08:00:00.000Z/)
    expect(await db.collection('dag_runs').countDocuments()).toBe(1)
    expect(await db.collection('task_instances').countDocuments()).toBe(1)
  })

  it('allows any number of runs without a logical date (manual triggers)', async () => {
    await createRun(db, dag())
    await createRun(db, dag())
    expect(await db.collection('dag_runs').countDocuments()).toBe(2)
  })

  it('the same date is fine for different Dags', async () => {
    await createRun(db, dag('a'), { logicalDate: D })
    await createRun(db, dag('b'), { logicalDate: D })
    expect(await db.collection('dag_runs').countDocuments()).toBe(2)
  })

  it('concurrent creators of one date: exactly one run survives', async () => {
    const results = await Promise.allSettled(Array.from({ length: 5 }, () => createRun(db, dag(), { logicalDate: D, triggerType: 'catchup' })))
    expect(results.filter(r => r.status === 'fulfilled')).toHaveLength(1)
    expect(results.filter(r => r.status === 'rejected').every(r => (r as PromiseRejectedResult).reason instanceof DuplicateRunError)).toBe(true)
    expect(await db.collection('dag_runs').countDocuments()).toBe(1)
  })

  it('catch-up skips a date another scheduler created first instead of failing the tick', async () => {
    const d = dag('cu', { catchup: true, startDate: '2026-03-10T08:00:00Z' })
    const NOW = new Date('2026-03-10T10:30:00Z')   // due: 08, 09, 10
    // 09:00 was inserted by another replica after catch-up's existence check (hidden from it here)
    await createRun(db, d, { logicalDate: new Date('2026-03-10T09:00:00Z'), triggerType: 'cron' })
    expect(await tickCatchup(blindToExistingRuns(db), [d], NOW)).toBe(2)
    expect(await db.collection('dag_runs').countDocuments({ dag_id: 'cu' })).toBe(3)
  })

  it('backfill counts a date created after its existence check (a racing scheduler) as skipped', async () => {
    const d = dag('bf')
    await createRun(db, d, { logicalDate: new Date('2026-03-10T09:00:00Z'), triggerType: 'cron' })
    // Hide existing runs from backfill's pre-check so the insert itself hits the constraint, as in a race
    const res = await backfill(blindToExistingRuns(db), d, { start: new Date('2026-03-10T08:00:00Z'), end: new Date('2026-03-10T10:00:00Z') })
    expect(res.created).toHaveLength(2)
    expect(res.skipped).toBe(1)
    expect(await db.collection('dag_runs').countDocuments({ dag_id: 'bf' })).toBe(3)
  })
})

describe('cronLogicalDate', () => {
  it('truncates 5-field expressions to the minute', () => {
    expect(cronLogicalDate('0 * * * *', new Date('2026-03-10T08:00:00.731Z')).toISOString()).toBe('2026-03-10T08:00:00.000Z')
    expect(cronLogicalDate('*/5 * * * *', new Date('2026-03-10T08:05:42.000Z')).toISOString()).toBe('2026-03-10T08:05:00.000Z')
  })
  it('truncates 6-field (seconds) expressions to the second', () => {
    expect(cronLogicalDate('*/10 * * * * *', new Date('2026-03-10T08:00:10.420Z')).toISOString()).toBe('2026-03-10T08:00:10.000Z')
  })
})
