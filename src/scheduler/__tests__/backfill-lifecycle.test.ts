/**
 * Backfill Lifecycle tests.
 *
 * Discriminating tests verify that pause actually stalls advancement (not just
 * that the state field changed), resume actually resumes, and cancel cascades
 * to the individual runs. A non-backfill run still advances while a backfill
 * is paused (regression guard for the $nin filter).
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { backfill } from '../backfill.js'
import { buildActiveRunFilter, getPausedBackfillIds } from '../backfill-filter.js'
import { advanceRun } from '../index.js'
import { createRun } from '../runs.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const dailyDag: DagDefinition = {
  id: 'lifecycle_daily',
  schedule: '0 9 * * *',
  tasks: { step: { run: async () => {} } },
}

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_bf_lifecycle'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_bf_lifecycle')
  clearRegistry()
  register(dailyDag)
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
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('backfills').deleteMany({})
  await db.collection('event_logs').deleteMany({})
  clearRegistry()
  register(dailyDag)
})

// ── buildActiveRunFilter — pure unit ──────────────────────────────────────

describe('buildActiveRunFilter', () => {
  it('with no paused backfills: just state filter', () => {
    const f = buildActiveRunFilter([])
    expect(f['state']).toEqual({ $in: ['queued', 'running'] })
    expect(f['$or']).toBeUndefined()
  })

  it('with paused backfill ids: adds $or to exclude them', () => {
    const f = buildActiveRunFilter(['id1', 'id2'])
    expect(f['state']).toEqual({ $in: ['queued', 'running'] })
    expect(f['$or']).toBeDefined()
    // null backfill_id (normal runs) must be included
    const orClauses = f['$or'] as Array<Record<string, unknown>>
    expect(orClauses.some(c => c['backfill_id'] === null)).toBe(true)
    // paused ids must be excluded via $nin
    const ninClause = orClauses.find(c => {
      const bf = c['backfill_id']
      return bf && typeof bf === 'object' && '$nin' in (bf as object)
    })
    expect((ninClause?.['backfill_id'] as Record<string, unknown>)?.['$nin']).toEqual(['id1', 'id2'])
  })
})

// ── backfill() creates entity ─────────────────────────────────────────────

describe('backfill() creates a backfills document', () => {
  it('returns a backfill_id and the entity is stored in DB', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-01-01T00:00:00Z'),
      end: new Date('2025-01-03T23:59:59Z'),
    })

    expect(result.backfill_id).toBeDefined()
    expect(ObjectId.isValid(result.backfill_id)).toBe(true)

    const doc = await db.collection('backfills').findOne({ _id: new ObjectId(result.backfill_id) })
    expect(doc?.dag_id).toBe('lifecycle_daily')
    expect(doc?.state).toBe('active')
    expect(doc?.created_count).toBe(3)
    expect(doc?.run_ids).toHaveLength(3)
  })

  it('stamps backfill_id on each created dag_run', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-02-01T00:00:00Z'),
      end: new Date('2025-02-01T23:59:59Z'),
    })

    const runs = await db.collection('dag_runs').find({ dag_id: 'lifecycle_daily' }).toArray()
    expect(runs).toHaveLength(1)
    expect(runs[0].backfill_id).toBe(result.backfill_id)
  })
})

// ── Pause gating ──────────────────────────────────────────────────────────

describe('GET /backfills + POST /backfills/:id/pause', () => {
  it('paused backfill runs are excluded from getPausedBackfillIds', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-03-01T00:00:00Z'),
      end: new Date('2025-03-01T23:59:59Z'),
    })

    // Pause via API
    const pauseRes = await app.inject({
      method: 'POST', url: `/backfills/${result.backfill_id}/pause`,
    })
    expect(pauseRes.statusCode).toBe(200)
    expect(pauseRes.json().state).toBe('paused')

    // getPausedBackfillIds should now include it
    const paused = await getPausedBackfillIds(db)
    expect(paused.has(result.backfill_id)).toBe(true)
  })

  it('paused backfill run does NOT advance (buildActiveRunFilter excludes it)', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-04-01T00:00:00Z'),
      end: new Date('2025-04-01T23:59:59Z'),
    })
    const [runId] = result.created

    // Pause the backfill
    await app.inject({ method: 'POST', url: `/backfills/${result.backfill_id}/pause` })

    // Try to advance — the run should be excluded by the tick filter
    const pausedIds = [...(await getPausedBackfillIds(db))]
    const filter = buildActiveRunFilter(pausedIds)

    // The paused run should NOT match the filter
    const matching = await db.collection('dag_runs')
      .find({ _id: new ObjectId(runId), ...filter })
      .toArray()
    expect(matching).toHaveLength(0)
  })

  it('non-backfill run still advances while a backfill is paused', async () => {
    // Create and pause a backfill
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-05-01T00:00:00Z'),
      end: new Date('2025-05-01T23:59:59Z'),
    })
    await app.inject({ method: 'POST', url: `/backfills/${result.backfill_id}/pause` })

    // Create a regular (non-backfill) run
    const normalDag: DagDefinition = {
      id: 'normal_dag_lifecycle', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(normalDag)
    const normalRunId = await createRun(db, normalDag)

    // The paused filter should include the normal run
    const pausedIds = [...(await getPausedBackfillIds(db))]
    const filter = buildActiveRunFilter(pausedIds)
    const matching = await db.collection('dag_runs')
      .find({ _id: new ObjectId(normalRunId), ...filter })
      .toArray()
    expect(matching).toHaveLength(1)  // normal run IS included

    // Advance it — should succeed
    await advanceRun(db, normalRunId)
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(normalRunId) })
    expect(run?.state).toBe('success')
  })
})

// ── Resume ────────────────────────────────────────────────────────────────

describe('POST /backfills/:id/resume', () => {
  it('resume re-enables the run for tick advancement', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-06-01T00:00:00Z'),
      end: new Date('2025-06-01T23:59:59Z'),
    })
    const [runId] = result.created

    // Pause then resume
    await app.inject({ method: 'POST', url: `/backfills/${result.backfill_id}/pause` })
    const resumeRes = await app.inject({ method: 'POST', url: `/backfills/${result.backfill_id}/resume` })
    expect(resumeRes.statusCode).toBe(200)
    expect(resumeRes.json().state).toBe('active')

    // After resume, run should match the active filter
    const pausedIds = [...(await getPausedBackfillIds(db))]
    expect(pausedIds).not.toContain(result.backfill_id)

    const filter = buildActiveRunFilter(pausedIds)
    const matching = await db.collection('dag_runs')
      .find({ _id: new ObjectId(runId), ...filter })
      .toArray()
    expect(matching).toHaveLength(1)

    // Advance it — should now complete
    await advanceRun(db, runId)
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.state).toBe('success')
  })

  it('returns 409 when resuming an active (not paused) backfill', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-07-01T00:00:00Z'),
      end: new Date('2025-07-01T23:59:59Z'),
    })
    const res = await app.inject({ method: 'POST', url: `/backfills/${result.backfill_id}/resume` })
    expect(res.statusCode).toBe(409)
  })
})

// ── Cancel ────────────────────────────────────────────────────────────────

describe('POST /backfills/:id/cancel', () => {
  it('cancels the backfill and all its queued runs', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-08-01T00:00:00Z'),
      end: new Date('2025-08-03T23:59:59Z'),
    })
    expect(result.created).toHaveLength(3)

    const cancelRes = await app.inject({ method: 'POST', url: `/backfills/${result.backfill_id}/cancel` })
    expect(cancelRes.statusCode).toBe(200)
    expect(cancelRes.json().state).toBe('cancelled')
    expect(cancelRes.json().runs_cancelled).toBe(3)

    // All runs should be cancelled
    const runs = await db.collection('dag_runs').find({ dag_id: 'lifecycle_daily' }).toArray()
    expect(runs.every(r => r.state === 'cancelled')).toBe(true)

    // Backfill entity should be cancelled
    const doc = await db.collection('backfills').findOne({ _id: new ObjectId(result.backfill_id) })
    expect(doc?.state).toBe('cancelled')
  })

  it('returns 409 on double cancel', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-09-01T00:00:00Z'),
      end: new Date('2025-09-01T23:59:59Z'),
    })
    await app.inject({ method: 'POST', url: `/backfills/${result.backfill_id}/cancel` })
    const res = await app.inject({ method: 'POST', url: `/backfills/${result.backfill_id}/cancel` })
    expect(res.statusCode).toBe(409)
  })
})

// ── GET /backfills & GET /backfills/:id ───────────────────────────────────

describe('GET /backfills', () => {
  it('lists backfills for a dag_id', async () => {
    await backfill(db, dailyDag, {
      start: new Date('2025-10-01T00:00:00Z'),
      end: new Date('2025-10-01T23:59:59Z'),
    })
    await backfill(db, dailyDag, {
      start: new Date('2025-10-02T00:00:00Z'),
      end: new Date('2025-10-02T23:59:59Z'),
    })

    const res = await app.inject({ method: 'GET', url: '/backfills?dag_id=lifecycle_daily' })
    expect(res.statusCode).toBe(200)
    const { items } = res.json()
    expect(items).toHaveLength(2)
    expect(items.every((b: { dag_id: string }) => b.dag_id === 'lifecycle_daily')).toBe(true)
    expect(items[0].backfill_id).toBeDefined()
    expect(items[0].state).toBe('active')
  })

  it('?state=paused returns only paused backfills', async () => {
    const r1 = await backfill(db, dailyDag, {
      start: new Date('2025-11-01T00:00:00Z'),
      end: new Date('2025-11-01T23:59:59Z'),
    })
    await backfill(db, dailyDag, {
      start: new Date('2025-11-02T00:00:00Z'),
      end: new Date('2025-11-02T23:59:59Z'),
    })
    await app.inject({ method: 'POST', url: `/backfills/${r1.backfill_id}/pause` })

    const res = await app.inject({ method: 'GET', url: '/backfills?state=paused' })
    const { items } = res.json()
    expect(items.every((b: { state: string }) => b.state === 'paused')).toBe(true)
  })

  it('returns 400 for invalid state value', async () => {
    const res = await app.inject({ method: 'GET', url: '/backfills?state=unknown' })
    expect(res.statusCode).toBe(400)
  })
})

describe('GET /backfills/:id', () => {
  it('returns 404 for unknown id', async () => {
    const fakeId = new ObjectId().toString()
    const res = await app.inject({ method: 'GET', url: `/backfills/${fakeId}` })
    expect(res.statusCode).toBe(404)
  })

  it('returns backfill with all fields', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-12-01T00:00:00Z'),
      end: new Date('2025-12-01T23:59:59Z'),
    })

    const res = await app.inject({ method: 'GET', url: `/backfills/${result.backfill_id}` })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.backfill_id).toBe(result.backfill_id)
    expect(body.dag_id).toBe('lifecycle_daily')
    expect(body.state).toBe('active')
    expect(body.created_count).toBe(1)
    expect(body.total_dates).toBe(1)
    expect(Array.isArray(body.run_ids)).toBe(true)
    expect('completed' in body).toBe(true)
  })

  it('completed=true after all runs finish', async () => {
    const result = await backfill(db, dailyDag, {
      start: new Date('2025-12-02T00:00:00Z'),
      end: new Date('2025-12-02T23:59:59Z'),
    })
    const [runId] = result.created
    await advanceRun(db, runId)

    const res = await app.inject({ method: 'GET', url: `/backfills/${result.backfill_id}` })
    expect(res.json().completed).toBe(true)
  })
})
