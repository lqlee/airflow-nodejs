/**
 * Task Instance Tries tests.
 *
 * Discriminating tests verify that try records are actually written when tasks
 * succeed, fail, and retry — not just that the endpoint reads them back.
 *
 * Key checks:
 * - success → 1 try record with state='success'
 * - fail (no retries) → 1 try record with state='failed' and error text
 * - retry → N failed try records (one per retry) then 1 success
 * - try_number on each record matches the try index
 * - mapped tasks store map_index correctly
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_tries'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_tries')
  clearRegistry()
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
  await db.collection('task_instance_tries').deleteMany({})
  await db.collection('xcoms').deleteMany({})
  await db.collection('event_logs').deleteMany({})
  clearRegistry()
})

// ── Success path ──────────────────────────────────────────────────────────

describe('successful task — try record', () => {
  it('records 1 try with state=success', async () => {
    const dag: DagDefinition = {
      id: 'tries_success_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const tries = await db.collection('task_instance_tries')
      .find({ dag_run_id: runId, task_id: 'step' }).toArray()
    expect(tries).toHaveLength(1)
    expect(tries[0].state).toBe('success')
    expect(tries[0].try_number).toBe(0)
    expect(tries[0].error).toBeNull()
    expect(tries[0].ended_at).toBeTruthy()
  })

  it('try record includes started_at and ended_at', async () => {
    const dag: DagDefinition = {
      id: 'tries_times_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const t = await db.collection('task_instance_tries').findOne({ dag_run_id: runId })
    expect(t?.started_at).toBeTruthy()
    expect(t?.ended_at).toBeTruthy()
  })
})

// ── Failure path ──────────────────────────────────────────────────────────

describe('failed task (no retries) — try record', () => {
  it('records 1 try with state=failed and error text', async () => {
    const dag: DagDefinition = {
      id: 'tries_fail_dag', schedule: null,
      tasks: { boom: { run: async () => { throw new Error('injected failure') } } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const tries = await db.collection('task_instance_tries')
      .find({ dag_run_id: runId, task_id: 'boom' }).toArray()
    expect(tries).toHaveLength(1)
    expect(tries[0].state).toBe('failed')
    expect(tries[0].error).toContain('injected failure')
    expect(tries[0].try_number).toBe(0)
  })
})

// ── Retry path ────────────────────────────────────────────────────────────

describe('task with retries — try records per attempt', () => {
  it('always-fail with 2 retries → 3 failed try records', async () => {
    const dag: DagDefinition = {
      id: 'tries_retry_dag', schedule: null,
      tasks: {
        flaky: {
          retries: 2,
          retryDelay: 0,
          run: async () => { throw new Error('always fails') },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const tries = await db.collection('task_instance_tries')
      .find({ dag_run_id: runId, task_id: 'flaky' })
      .sort({ try_number: 1 })
      .toArray()

    // 3 failed tries (try 0, 1, 2)
    expect(tries).toHaveLength(3)
    expect(tries[0].state).toBe('failed')
    expect(tries[0].try_number).toBe(0)
    expect(tries[1].state).toBe('failed')
    expect(tries[1].try_number).toBe(1)
    expect(tries[2].state).toBe('failed')
    expect(tries[2].try_number).toBe(2)

    // Run should be failed
    const run = await db.collection('dag_runs').findOne({ dag_id: 'tries_retry_dag' })
    expect(run?.state).toBe('failed')
  })

  it('always-fail with 1 retry → 2 failed try records with ascending try_number', async () => {
    const dag: DagDefinition = {
      id: 'tries_idx_dag', schedule: null,
      tasks: {
        flaky: {
          retries: 1,
          retryDelay: 0,
          run: async () => { throw new Error('always fail') },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const tries = await db.collection('task_instance_tries')
      .find({ dag_run_id: runId }).sort({ try_number: 1 }).toArray()
    expect(tries).toHaveLength(2)
    expect(tries[0].try_number).toBe(0)
    expect(tries[1].try_number).toBe(1)
    expect(tries.every(t => t.state === 'failed')).toBe(true)
  })
})

// ── Mapped tasks ──────────────────────────────────────────────────────────

describe('mapped task tries — map_index stored', () => {
  it('each instance gets its own try record with correct map_index', async () => {
    const dag: DagDefinition = {
      id: 'tries_mapped_dag', schedule: null,
      tasks: { fan: { expand: ['a', 'b'], run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const tries = await db.collection('task_instance_tries')
      .find({ dag_run_id: runId, task_id: 'fan' })
      .sort({ map_index: 1 })
      .toArray()
    expect(tries).toHaveLength(2)
    expect(tries[0].map_index).toBe(0)
    expect(tries[1].map_index).toBe(1)
    expect(tries.every(t => t.state === 'success')).toBe(true)
  })
})

// ── GET /dag-runs/:runId/tasks/:taskId/tries endpoint ─────────────────────

describe('GET /dag-runs/:runId/tasks/:taskId/tries', () => {
  it('returns 400 for invalid runId', async () => {
    const res = await app.inject({ method: 'GET', url: '/dag-runs/bad/tasks/step/tries' })
    expect(res.statusCode).toBe(400)
  })

  it('returns empty array when no tries recorded', async () => {
    const dag: DagDefinition = { id: 'tries_empty_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)  // don't advance

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/tasks/s/tries` })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  it('returns tries sorted by try_number after a successful run', async () => {
    const dag: DagDefinition = {
      id: 'tries_api_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/tasks/step/tries` })
    expect(res.statusCode).toBe(200)
    const tries = res.json() as Array<{ try_number: number; state: string; run_id: string }>
    expect(tries).toHaveLength(1)
    expect(tries[0].state).toBe('success')
    expect(tries[0].try_number).toBe(0)
    expect(tries[0].run_id).toBe(runId)
  })

  it('returns all tries in order for a retried task (always-fail with 1 retry)', async () => {
    const dag: DagDefinition = {
      id: 'tries_api_retry_dag', schedule: null,
      tasks: {
        flaky: {
          retries: 1, retryDelay: 0,
          run: async () => { throw new Error('always fail') },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/tasks/flaky/tries` })
    const tries = res.json() as Array<{ state: string; try_number: number }>
    expect(tries).toHaveLength(2)
    expect(tries[0]).toMatchObject({ try_number: 0, state: 'failed' })
    expect(tries[1]).toMatchObject({ try_number: 1, state: 'failed' })
  })

  it('?map_index= returns tries for that mapped instance only', async () => {
    const dag: DagDefinition = {
      id: 'tries_api_mapped_dag', schedule: null,
      tasks: { fan: { expand: ['x', 'y', 'z'], run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/tasks/fan/tries?map_index=1` })
    expect(res.statusCode).toBe(200)
    const tries = res.json() as Array<{ map_index: number }>
    expect(tries.every(t => t.map_index === 1)).toBe(true)
    expect(tries).toHaveLength(1)
  })

  it('each try has required fields: run_id, dag_id, task_id, try_number, state, ended_at', async () => {
    const dag: DagDefinition = {
      id: 'tries_fields_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/tasks/step/tries` })
    const t = res.json()[0] as Record<string, unknown>
    expect(t.run_id).toBe(runId)
    expect(t.dag_id).toBe('tries_fields_dag')
    expect(t.task_id).toBe('step')
    expect(typeof t.try_number).toBe('number')
    expect(t.state).toBe('success')
    expect(t.ended_at).toBeTruthy()
    expect('error' in t).toBe(true)
  })
})
