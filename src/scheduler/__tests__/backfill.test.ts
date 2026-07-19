import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { enumerateDates, backfill, BACKFILL_MAX_RUNS } from '../backfill.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const scheduledDag: DagDefinition = {
  id: 'daily_etl',
  schedule: '0 9 * * *', // 09:00 every day
  tasks: { load: { run: async () => {} } },
}

const manualDag: DagDefinition = {
  id: 'manual_only',
  schedule: null,
  tasks: { step: { run: async () => {} } },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_backfill')
  clearRegistry()
  register(scheduledDag)
  register(manualDag)
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
})

// ── enumerateDates ──────────────────────────────────────────────────────────

describe('enumerateDates', () => {
  it('returns one date per day for a daily cron in a 3-day range', () => {
    const dates = enumerateDates(
      '0 9 * * *',
      new Date('2025-01-01T00:00:00Z'),
      new Date('2025-01-03T23:59:59Z'),
    )
    expect(dates).toHaveLength(3)
    // All should be at 09:00 UTC
    for (const d of dates) {
      expect(d.getUTCHours()).toBe(9)
      expect(d.getUTCMinutes()).toBe(0)
    }
  })

  it('is inclusive of the start boundary', () => {
    // The first scheduled time exactly on start should be included
    const start = new Date('2025-06-01T09:00:00Z')
    const end = new Date('2025-06-01T23:59:59Z')
    const dates = enumerateDates('0 9 * * *', start, end)
    expect(dates).toHaveLength(1)
    expect(dates[0].getTime()).toBe(start.getTime())
  })

  it('returns empty array when no cron fires in range', () => {
    // 1-minute range with no scheduled firing
    const start = new Date('2025-01-01T00:00:00Z')
    const end = new Date('2025-01-01T00:00:30Z')
    const dates = enumerateDates('0 9 * * *', start, end)
    expect(dates).toHaveLength(0)
  })

  it('returns dates in ascending order', () => {
    const dates = enumerateDates(
      '0 * * * *', // every hour
      new Date('2025-01-01T00:00:00Z'),
      new Date('2025-01-01T05:59:59Z'),
    )
    expect(dates.length).toBeGreaterThan(1)
    for (let i = 1; i < dates.length; i++) {
      expect(dates[i].getTime()).toBeGreaterThan(dates[i - 1].getTime())
    }
  })
})

// ── backfill() ─────────────────────────────────────────────────────────────

describe('backfill()', () => {
  it('creates one queued run per scheduled date', async () => {
    const result = await backfill(db, scheduledDag, {
      start: new Date('2025-01-01T00:00:00Z'),
      end: new Date('2025-01-03T23:59:59Z'),
    })
    expect(result.created).toHaveLength(3)
    expect(result.skipped).toBe(0)
    expect(result.dates).toHaveLength(3)

    const runs = await db.collection('dag_runs').find({ dag_id: scheduledDag.id }).toArray()
    expect(runs).toHaveLength(3)
    for (const run of runs) {
      expect(run.state).toBe('queued')
      expect(run.logical_date).toBeInstanceOf(Date)
    }
  })

  it('stamps distinct logical_dates on each run', async () => {
    const result = await backfill(db, scheduledDag, {
      start: new Date('2025-02-01T00:00:00Z'),
      end: new Date('2025-02-03T23:59:59Z'),
    })
    const runs = await db
      .collection('dag_runs')
      .find({ dag_id: scheduledDag.id })
      .toArray()
    const logicalDates = runs.map(r => new Date(r.logical_date as Date).getTime())
    const unique = new Set(logicalDates)
    expect(unique.size).toBe(result.created.length)
  })

  it('skips (dag_id, logical_date) pairs that already exist', async () => {
    // First backfill
    await backfill(db, scheduledDag, {
      start: new Date('2025-03-01T00:00:00Z'),
      end: new Date('2025-03-03T23:59:59Z'),
    })
    // Second backfill over the same range
    const result = await backfill(db, scheduledDag, {
      start: new Date('2025-03-01T00:00:00Z'),
      end: new Date('2025-03-03T23:59:59Z'),
    })
    expect(result.created).toHaveLength(0)
    expect(result.skipped).toBe(3)
  })

  it('throws RangeError when dag has no schedule', async () => {
    await expect(
      backfill(db, manualDag, {
        start: new Date('2025-01-01'),
        end: new Date('2025-01-07'),
      }),
    ).rejects.toThrow(RangeError)
  })

  it('throws RangeError when start > end', async () => {
    await expect(
      backfill(db, scheduledDag, {
        start: new Date('2025-06-10'),
        end: new Date('2025-06-01'),
      }),
    ).rejects.toThrow(RangeError)
  })

  it(`throws RangeError when date count exceeds ${BACKFILL_MAX_RUNS}`, async () => {
    // every minute * ~525 minutes > 500
    const minuteDag: DagDefinition = { ...scheduledDag, id: 'minutely', schedule: '* * * * *' }
    register(minuteDag)
    await expect(
      backfill(db, minuteDag, {
        start: new Date('2025-01-01T00:00:00Z'),
        end: new Date('2025-01-02T00:00:00Z'), // 1441 minutes
      }),
    ).rejects.toThrow(/exceeds limit/)
  })
})

// ── API ─────────────────────────────────────────────────────────────────────

describe('POST /dags/:dagId/backfill — API', () => {
  it('returns 201 with created run ids and counts', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/daily_etl/backfill',
      payload: {
        start: '2025-04-01T00:00:00Z',
        end: '2025-04-03T23:59:59Z',
      },
    })
    expect(res.statusCode).toBe(201)
    const body = res.json()
    expect(body.dag_id).toBe('daily_etl')
    expect(body.created_count).toBe(3)
    expect(body.skipped).toBe(0)
    expect(body.total_dates).toBe(3)
    expect(Array.isArray(body.created)).toBe(true)
  })

  it('is idempotent — second backfill returns skipped=N, created=0', async () => {
    const opts = { start: '2025-05-01T00:00:00Z', end: '2025-05-02T23:59:59Z' }
    await app.inject({ method: 'POST', url: '/dags/daily_etl/backfill', payload: opts })
    const res = await app.inject({ method: 'POST', url: '/dags/daily_etl/backfill', payload: opts })
    const body = res.json()
    expect(res.statusCode).toBe(201)
    expect(body.created_count).toBe(0)
    expect(body.skipped).toBe(2)
  })

  it('returns 400 for a dag with no schedule', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/manual_only/backfill',
      payload: { start: '2025-01-01', end: '2025-01-07' },
    })
    expect(res.statusCode).toBe(400)
    expect(res.json().error).toMatch(/no schedule/)
  })

  it('returns 400 when start > end', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/daily_etl/backfill',
      payload: { start: '2025-06-10', end: '2025-06-01' },
    })
    expect(res.statusCode).toBe(400)
  })

  it('returns 400 when body fields are missing', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/daily_etl/backfill',
      payload: { start: '2025-01-01' }, // no end
    })
    expect(res.statusCode).toBe(400)
  })

  it('returns 404 for an unknown dag', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/does_not_exist/backfill',
      payload: { start: '2025-01-01', end: '2025-01-07' },
    })
    expect(res.statusCode).toBe(404)
  })

  it('backfill runs appear in GET /dags/:dagId/runs with logical_date set', async () => {
    await app.inject({
      method: 'POST',
      url: '/dags/daily_etl/backfill',
      payload: { start: '2025-07-01T00:00:00Z', end: '2025-07-01T23:59:59Z' },
    })
    const res = await app.inject({ method: 'GET', url: '/dags/daily_etl/runs' })
    const body = res.json()
    const backfillRun = body.items.find((r: { logical_date: string | null }) => r.logical_date !== null)
    expect(backfillRun).toBeDefined()
    expect(typeof backfillRun.logical_date).toBe('string') // serialised as ISO
  })

  it('manual trigger run has logical_date null', async () => {
    await app.inject({ method: 'POST', url: '/dags/daily_etl/trigger' })
    const res = await app.inject({ method: 'GET', url: '/dags/daily_etl/runs' })
    const body = res.json()
    const manualRun = body.items.find((r: { logical_date: string | null }) => r.logical_date === null)
    expect(manualRun).toBeDefined()
  })
})
