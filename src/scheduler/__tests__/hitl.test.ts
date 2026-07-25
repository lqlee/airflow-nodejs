/**
 * Human-in-the-Loop (HITL) tests.
 *
 * Discriminating tests verify the gate works:
 * - pending → claimReadyTasks excludes it → run parks (not terminal)
 * - approve → task runs → run succeeds
 * - reject → run reaches 'failed' (not hung)
 * - downstream task waits while HITL pending, runs after approval
 * - non-HITL dag still completes normally (regression guard)
 * - cancel with pending HITL → run cancelled (queued task covered by cancelRun)
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
  process.env.DB_NAME = 'airflow_test_hitl'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_hitl')
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
  await db.collection('xcoms').deleteMany({})
  await db.collection('event_logs').deleteMany({})
  clearRegistry()
})

// ── HITL fields stamped on task_instance ─────────────────────────────────

describe('createRun — HITL fields', () => {
  it('requiresApproval task has is_hitl=true and hitl_state=pending', async () => {
    const dag: DagDefinition = {
      id: 'hitl_stamp_dag', schedule: null,
      tasks: {
        gate: { requiresApproval: true, hitlPrompt: 'Please review', run: async () => {} },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await db.collection('task_instances').findOne({ task_id: 'gate' })
    expect(ti?.is_hitl).toBe(true)
    expect(ti?.hitl_state).toBe('pending')
    expect(ti?.hitl_prompt).toBe('Please review')
    expect(ti?.hitl_note).toBeNull()
    void runId
  })

  it('normal task has is_hitl=false and hitl_state=null', async () => {
    const dag: DagDefinition = {
      id: 'hitl_normal_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await db.collection('task_instances').findOne({ task_id: 'step' })
    expect(ti?.is_hitl).toBe(false)
    expect(ti?.hitl_state).toBeNull()
    void runId
  })
})

// ── Claim gate — pending HITL parks the run ───────────────────────────────

describe('HITL gate — claim exclusion', () => {
  it('pending HITL task is NOT claimed — run parks (not terminal after advance)', async () => {
    const dag: DagDefinition = {
      id: 'hitl_park_dag', schedule: null,
      tasks: { gate: { requiresApproval: true } },
    }
    register(dag)
    const runId = await createRun(db, dag)

    // Advance — the HITL task is excluded from claim
    await advanceRun(db, runId)

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    // Run should be running/queued (parked), NOT terminal
    expect(run?.state).toMatch(/^(queued|running)$/)

    // The gate task should still be queued
    const ti = await db.collection('task_instances').findOne({ task_id: 'gate' })
    expect(ti?.state).toBe('queued')
    expect(ti?.hitl_state).toBe('pending')
  })

  it('non-HITL dag still completes normally when HITL gate exists for another run', async () => {
    // HITL run parks
    const hitlDag: DagDefinition = {
      id: 'hitl_coexist_dag', schedule: null,
      tasks: { gate: { requiresApproval: true } },
    }
    register(hitlDag)
    await createRun(db, hitlDag)

    // Normal run completes independently
    const normalDag: DagDefinition = {
      id: 'hitl_normal_coexist', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(normalDag)
    const normalRunId = await createRun(db, normalDag)
    await advanceRun(db, normalRunId)

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(normalRunId) })
    expect(run?.state).toBe('success')
  })
})

// ── POST /hitl/:runId/:taskId — approve ───────────────────────────────────

describe('HITL approve', () => {
  it('approve no-run task → task succeeds immediately, run succeeds', async () => {
    const dag: DagDefinition = {
      id: 'hitl_approve_noop', schedule: null,
      tasks: {
        gate: { requiresApproval: true },  // no run body — succeeds immediately on approval
        after: { dependsOn: ['gate'], run: async (ctx) => { await ctx.xcom.push('done', true) } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)  // parks at gate

    const approveRes = await app.inject({
      method: 'POST',
      url: `/hitl/${runId}/gate`,
      payload: { decision: 'approve', note: 'looks good' },
    })
    expect(approveRes.statusCode).toBe(200)
    expect(approveRes.json().decision).toBe('approve')

    // advanceRun was called by approve route — run should be success
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.state).toBe('success')

    // 'after' downstream ran
    const xcom = await db.collection('xcoms').findOne({ task_id: 'after', key: 'done' })
    expect(xcom?.value).toBe(true)

    // Gate task has hitl metadata stored
    const gate = await db.collection('task_instances').findOne({ task_id: 'gate' })
    expect(gate?.hitl_state).toBe('approved')
    expect(gate?.hitl_note).toBe('looks good')
    expect(gate?.hitl_responded_at).toBeTruthy()
  })

  it('approve task with run body — task actually executes', async () => {
    const dag: DagDefinition = {
      id: 'hitl_approve_run', schedule: null,
      tasks: {
        gate: {
          requiresApproval: true,
          run: async (ctx) => { await ctx.xcom.push('executed', true) },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    await app.inject({
      method: 'POST', url: `/hitl/${runId}/gate`,
      payload: { decision: 'approve' },
    })

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.state).toBe('success')

    const xcom = await db.collection('xcoms').findOne({ key: 'executed' })
    expect(xcom?.value).toBe(true)
  })

  it('downstream task does not run until HITL approved', async () => {
    const dag: DagDefinition = {
      id: 'hitl_downstream_dag', schedule: null,
      tasks: {
        gate: { requiresApproval: true },
        compute: {
          dependsOn: ['gate'],
          run: async (ctx) => { await ctx.xcom.push('ran', true) },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)  // parks — neither task ran

    // compute has not run yet
    const computeBefore = await db.collection('task_instances').findOne({ task_id: 'compute' })
    expect(computeBefore?.state).toBe('queued')

    // Approve → both tasks complete
    await app.inject({ method: 'POST', url: `/hitl/${runId}/gate`, payload: { decision: 'approve' } })

    const xcom = await db.collection('xcoms').findOne({ key: 'ran' })
    expect(xcom?.value).toBe(true)
  })
})

// ── POST /hitl/:runId/:taskId — reject ────────────────────────────────────

describe('HITL reject', () => {
  it('reject → task failed, run reaches failed (not hung)', async () => {
    const dag: DagDefinition = {
      id: 'hitl_reject_dag', schedule: null,
      tasks: { gate: { requiresApproval: true } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const rejectRes = await app.inject({
      method: 'POST', url: `/hitl/${runId}/gate`,
      payload: { decision: 'reject', note: 'not ready' },
    })
    expect(rejectRes.statusCode).toBe(200)
    expect(rejectRes.json().decision).toBe('reject')

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.state).toBe('failed')

    const gate = await db.collection('task_instances').findOne({ task_id: 'gate' })
    expect(gate?.state).toBe('failed')
    expect(gate?.hitl_state).toBe('rejected')
    expect(gate?.hitl_note).toBe('not ready')
    expect(gate?.error).toContain('Rejected')
  })
})

// ── Cancel with pending HITL ──────────────────────────────────────────────

describe('cancel run with pending HITL', () => {
  it('cancel covers the queued HITL task', async () => {
    const dag: DagDefinition = {
      id: 'hitl_cancel_dag', schedule: null,
      tasks: { gate: { requiresApproval: true } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const cancelRes = await app.inject({ method: 'POST', url: `/dag-runs/${runId}/cancel` })
    expect(cancelRes.statusCode).toBe(200)

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.state).toBe('cancelled')

    const gate = await db.collection('task_instances').findOne({ task_id: 'gate' })
    expect(gate?.state).toBe('cancelled')
  })
})

// ── API validation ────────────────────────────────────────────────────────

describe('HITL API validation', () => {
  it('returns 400 for invalid decision', async () => {
    const dag: DagDefinition = {
      id: 'hitl_val_dag', schedule: null,
      tasks: { gate: { requiresApproval: true } },
    }
    register(dag)
    const runId = await createRun(db, dag)

    const res = await app.inject({
      method: 'POST', url: `/hitl/${runId}/gate`,
      payload: { decision: 'maybe' },
    })
    expect(res.statusCode).toBe(400)
  })

  it('returns 404 for non-HITL task', async () => {
    const dag: DagDefinition = {
      id: 'hitl_not_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)

    const res = await app.inject({
      method: 'POST', url: `/hitl/${runId}/step`,
      payload: { decision: 'approve' },
    })
    expect(res.statusCode).toBe(404)
  })

  it('returns 404 on double-approve (already responded)', async () => {
    const dag: DagDefinition = {
      id: 'hitl_double_dag', schedule: null,
      tasks: { gate: { requiresApproval: true } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    await app.inject({ method: 'POST', url: `/hitl/${runId}/gate`, payload: { decision: 'approve' } })
    const res = await app.inject({ method: 'POST', url: `/hitl/${runId}/gate`, payload: { decision: 'approve' } })
    expect(res.statusCode).toBe(404)  // hitl_state is no longer 'pending'
  })

  it('GET /hitl lists pending tasks', async () => {
    const dag: DagDefinition = {
      id: 'hitl_list_dag', schedule: null,
      tasks: { gate: { requiresApproval: true, hitlPrompt: 'Review this' } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const res = await app.inject({ method: 'GET', url: '/hitl' })
    expect(res.statusCode).toBe(200)
    const items = res.json() as Array<{ task_id: string; hitl_state: string; hitl_prompt: string }>
    const item = items.find(i => i.task_id === 'gate')
    expect(item).toBeDefined()
    expect(item?.hitl_state).toBe('pending')
    expect(item?.hitl_prompt).toBe('Review this')
    void runId
  })

  it('GET /hitl/:runId/:taskId returns task detail', async () => {
    const dag: DagDefinition = {
      id: 'hitl_detail_dag', schedule: null,
      tasks: { gate: { requiresApproval: true, hitlPrompt: 'Check me' } },
    }
    register(dag)
    const runId = await createRun(db, dag)

    const res = await app.inject({ method: 'GET', url: `/hitl/${runId}/gate` })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.hitl_state).toBe('pending')
    expect(body.hitl_prompt).toBe('Check me')
    expect(body.task_id).toBe('gate')
  })
})
