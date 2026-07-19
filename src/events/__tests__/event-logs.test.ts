/**
 * Audit / Event Log tests.
 *
 * Discriminating tests perform real actions and verify the event was emitted —
 * NOT just "insert a doc, read it back." The read-back shape is tested with
 * one seed test to verify the GET endpoint format.
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { pauseDag, resumeDag } from '../../dag/pause.js'
import { recordEvent, buildEventFilter } from '../index.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_event_logs'
  process.env.ENCRYPTION_KEY = 'd'.repeat(64)
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_event_logs')
  clearRegistry()
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  delete process.env.DB_NAME
  delete process.env.ENCRYPTION_KEY
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('xcoms').deleteMany({})
  await db.collection('event_logs').deleteMany({})
  await db.collection('dag_paused').deleteMany({})
  clearRegistry()
})

// ── buildEventFilter — pure unit ──────────────────────────────────────────

describe('buildEventFilter', () => {
  it('returns empty filter when no params', () => {
    expect(buildEventFilter({})).toEqual({})
  })

  it('includes dag_id when provided', () => {
    expect(buildEventFilter({ dag_id: 'my_dag' })).toEqual({ dag_id: 'my_dag' })
  })

  it('includes all provided filters', () => {
    const f = buildEventFilter({ dag_id: 'd', dag_run_id: 'r', task_id: 't', event_type: 'run_success' })
    expect(f).toEqual({ dag_id: 'd', dag_run_id: 'r', task_id: 't', event_type: 'run_success' })
  })

  it('omits undefined/missing filters', () => {
    const f = buildEventFilter({ dag_id: 'x', event_type: undefined })
    expect(Object.keys(f)).toEqual(['dag_id'])
  })
})

// ── GET /event-logs — endpoint shape ─────────────────────────────────────

describe('GET /event-logs — shape', () => {
  it('returns 200 with items and next_cursor fields', async () => {
    await recordEvent(db, 'dag_paused', { dag_id: 'shape_dag' })
    const res = await app.inject({ method: 'GET', url: '/event-logs?dag_id=shape_dag' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(Array.isArray(body.items)).toBe(true)
    expect('next_cursor' in body).toBe(true)
    const e = body.items[0]
    expect(e.id).toBeDefined()
    expect(e.event_type).toBe('dag_paused')
    expect(e.dag_id).toBe('shape_dag')
    expect(e.created_at).toBeDefined()
    expect('metadata' in e).toBe(true)
  })

  it('returns 400 for unknown event_type', async () => {
    const res = await app.inject({ method: 'GET', url: '/event-logs?event_type=unknown' })
    expect(res.statusCode).toBe(400)
  })

  it('returns 400 for malformed dag_run_id', async () => {
    const res = await app.inject({ method: 'GET', url: '/event-logs?dag_run_id=not-an-id' })
    expect(res.statusCode).toBe(400)
  })

  it('returns empty items when no events match', async () => {
    const res = await app.inject({ method: 'GET', url: '/event-logs?dag_id=nonexistent' })
    expect(res.statusCode).toBe(200)
    expect(res.json().items).toHaveLength(0)
    expect(res.json().next_cursor).toBeNull()
  })
})

// ── Emission: real actions produce events ─────────────────────────────────

describe('run_triggered event on createRun', () => {
  it('POST /trigger produces a run_triggered event with correct dag_id and run_id', async () => {
    const dag: DagDefinition = { id: 'audit_trigger_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)

    const triggerRes = await app.inject({
      method: 'POST', url: '/dags/audit_trigger_dag/trigger', payload: {},
    })
    expect(triggerRes.statusCode).toBe(201)
    const { run_id } = triggerRes.json()

    // Give fire-and-forget a tick
    await new Promise(r => setTimeout(r, 50))

    const events = await db.collection('event_logs').find({ event_type: 'run_triggered', dag_run_id: run_id }).toArray()
    expect(events).toHaveLength(1)
    expect(events[0].dag_id).toBe('audit_trigger_dag')
    expect(events[0].metadata.trigger_type).toBe('manual')
  })

  it('backfill createRun emits trigger_type=backfill', async () => {
    const dag: DagDefinition = { id: 'audit_backfill_dag', schedule: '0 9 * * *', tasks: { s: { run: async () => {} } } }
    register(dag)

    // Use backfill API
    await app.inject({
      method: 'POST', url: '/dags/audit_backfill_dag/backfill',
      payload: { start: '2024-01-01', end: '2024-01-02' },
    })

    await new Promise(r => setTimeout(r, 80))

    const events = await db.collection('event_logs').find({ dag_id: 'audit_backfill_dag', event_type: 'run_triggered' }).toArray()
    expect(events.length).toBeGreaterThanOrEqual(1)
    expect(events.every(e => e.metadata.trigger_type === 'backfill')).toBe(true)
  })
})

describe('run_success / run_failed events on advanceRun terminal state', () => {
  it('successful run produces run_success event — exactly once even with double advanceRun', async () => {
    const dag: DagDefinition = { id: 'audit_success_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)

    // Advance twice — CAS ensures only one transition
    await advanceRun(db, runId)
    await advanceRun(db, runId)
    await new Promise(r => setTimeout(r, 50))

    const events = await db.collection('event_logs').find({ event_type: 'run_success', dag_run_id: runId }).toArray()
    expect(events).toHaveLength(1)
    expect(events[0].dag_id).toBe('audit_success_dag')
  })

  it('failed run produces run_failed event', async () => {
    const dag: DagDefinition = { id: 'audit_fail_dag', schedule: null, tasks: { boom: { run: async () => { throw new Error('fail') } } } }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    await new Promise(r => setTimeout(r, 50))

    const events = await db.collection('event_logs').find({ event_type: 'run_failed', dag_run_id: runId }).toArray()
    expect(events).toHaveLength(1)
  })

  it('no run_success event when run has not completed', async () => {
    const dag: DagDefinition = { id: 'audit_noterm_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    // Don't advance
    await new Promise(r => setTimeout(r, 30))

    const events = await db.collection('event_logs').find({ event_type: 'run_success', dag_run_id: runId }).toArray()
    expect(events).toHaveLength(0)
  })
})

describe('dag_paused / dag_resumed events', () => {
  it('POST pause produces dag_paused event with correct dag_id', async () => {
    const dag: DagDefinition = { id: 'audit_pause_dag', schedule: '0 * * * *', tasks: { s: { run: async () => {} } } }
    register(dag)

    await app.inject({ method: 'POST', url: '/dags/audit_pause_dag/pause' })
    await new Promise(r => setTimeout(r, 50))

    const events = await db.collection('event_logs').find({ event_type: 'dag_paused', dag_id: 'audit_pause_dag' }).toArray()
    expect(events).toHaveLength(1)
  })

  it('POST resume after pause produces dag_resumed event', async () => {
    const dag: DagDefinition = { id: 'audit_resume_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)

    await pauseDag(db, 'audit_resume_dag')
    await resumeDag(db, 'audit_resume_dag')
    await new Promise(r => setTimeout(r, 50))

    const pausedEvents = await db.collection('event_logs').find({ event_type: 'dag_paused', dag_id: 'audit_resume_dag' }).toArray()
    const resumedEvents = await db.collection('event_logs').find({ event_type: 'dag_resumed', dag_id: 'audit_resume_dag' }).toArray()
    expect(pausedEvents).toHaveLength(1)
    expect(resumedEvents).toHaveLength(1)
  })
})

describe('run_cancelled event on POST cancel', () => {
  it('cancelling a run produces run_cancelled event', async () => {
    const dag: DagDefinition = { id: 'audit_cancel_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)

    await app.inject({ method: 'POST', url: `/dag-runs/${runId}/cancel` })
    await new Promise(r => setTimeout(r, 50))

    const events = await db.collection('event_logs').find({ event_type: 'run_cancelled', dag_run_id: runId }).toArray()
    expect(events).toHaveLength(1)
    expect(events[0].dag_id).toBe('audit_cancel_dag')
  })
})

describe('task_cleared event on POST clear', () => {
  it('clearing a failed task produces task_cleared event with task_id', async () => {
    const dag: DagDefinition = { id: 'audit_clear_dag', schedule: null, tasks: { boom: { run: async () => { throw new Error('fail') } } } }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    await app.inject({ method: 'POST', url: `/dag-runs/${runId}/tasks/boom/clear` })
    await new Promise(r => setTimeout(r, 50))

    const events = await db.collection('event_logs').find({ event_type: 'task_cleared', dag_run_id: runId }).toArray()
    expect(events.length).toBeGreaterThanOrEqual(1)
    expect(events[0].task_id).toBe('boom')
    expect(events[0].metadata.cleared_count).toBe(1)
  })
})

// ── GET /event-logs — filtering ───────────────────────────────────────────

describe('GET /event-logs — filters', () => {
  it('?event_type= returns only matching events', async () => {
    const dag: DagDefinition = { id: 'audit_filter_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    await pauseDag(db, 'audit_filter_dag')
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    await new Promise(r => setTimeout(r, 60))

    const res = await app.inject({ method: 'GET', url: '/event-logs?dag_id=audit_filter_dag&event_type=dag_paused' })
    const { items } = res.json()
    expect(items.every((e: { event_type: string }) => e.event_type === 'dag_paused')).toBe(true)
    expect(items.some((e: { event_type: string }) => e.event_type === 'run_triggered')).toBe(false)
  })

  it('?dag_run_id= returns only events for that run', async () => {
    const dag: DagDefinition = { id: 'audit_runid_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const runId1 = await createRun(db, dag)
    const dag2: DagDefinition = { ...dag, id: 'audit_runid_dag2' }
    register(dag2)
    const runId2 = await createRun(db, dag2)
    await advanceRun(db, runId1)
    await advanceRun(db, runId2)
    await new Promise(r => setTimeout(r, 60))

    const res = await app.inject({ method: 'GET', url: `/event-logs?dag_run_id=${runId1}` })
    const { items } = res.json()
    expect(items.every((e: { dag_run_id: string }) => e.dag_run_id === runId1)).toBe(true)
  })

  it('pagination: next_cursor is set when results fill the page', async () => {
    // Insert 5 events, request limit=3 → should get next_cursor
    for (let i = 0; i < 5; i++) {
      await recordEvent(db, 'dag_paused', { dag_id: 'paged_dag' })
    }

    const res = await app.inject({ method: 'GET', url: '/event-logs?dag_id=paged_dag&limit=3' })
    const body = res.json()
    expect(body.items).toHaveLength(3)
    expect(body.next_cursor).not.toBeNull()

    // Use cursor to fetch next page
    const res2 = await app.inject({ method: 'GET', url: `/event-logs?dag_id=paged_dag&limit=3&cursor=${body.next_cursor}` })
    const body2 = res2.json()
    expect(body2.items).toHaveLength(2)
    expect(body2.next_cursor).toBeNull()  // no more pages
  })
})
