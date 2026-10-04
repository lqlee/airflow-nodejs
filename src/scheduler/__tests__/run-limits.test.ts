/**
 * Dag-level run limits: maxActiveRuns and runTimeout.
 *
 * A requiresApproval (HITL) task is used to keep a run alive — it is never claimed
 * until approved, so the run stays `running` and nothing races the assertions.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun, cancelRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_run_limits')
  clearRegistry()
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  clearRegistry()
})

const runState = async (id: string) =>
  (await db.collection('dag_runs').findOne({ _id: new ObjectId(id) }))!

function heldDag(extra: Partial<DagDefinition>): DagDefinition {
  return {
    id: 'limits_dag',
    schedule: null,
    tasks: { gate: { requiresApproval: true, run: async () => {} } },
    ...extra,
  }
}

describe('maxActiveRuns', () => {
  it('keeps extra runs queued until a running run frees its slot', async () => {
    const dag = heldDag({ maxActiveRuns: 1 })
    register(dag)
    const r1 = await createRun(db, dag)
    const r2 = await createRun(db, dag)

    await advanceRun(db, r1)
    await advanceRun(db, r2)

    expect((await runState(r1)).state).toBe('running')
    const second = await runState(r2)
    expect(second.state).toBe('queued')
    expect(second.started_at ?? null).toBeNull()

    await cancelRun(db, r1)
    await advanceRun(db, r2)
    expect((await runState(r2)).state).toBe('running')
  })

  it('allows up to N concurrent runs', async () => {
    const dag = heldDag({ maxActiveRuns: 2 })
    register(dag)
    const ids = [await createRun(db, dag), await createRun(db, dag), await createRun(db, dag)]
    for (const id of ids) await advanceRun(db, id)

    expect((await Promise.all(ids.map(async i => (await runState(i)).state)))).toEqual(['running', 'running', 'queued'])
  })

  it('does not limit other dags', async () => {
    const limited = heldDag({ maxActiveRuns: 1 })
    const other = heldDag({ id: 'other_dag' })
    register(limited); register(other)
    const a = await createRun(db, limited)
    const b = await createRun(db, other)
    await advanceRun(db, a)
    await advanceRun(db, b)
    expect((await runState(b)).state).toBe('running')
  })

  it('is unlimited when unset', async () => {
    const dag = heldDag({})
    register(dag)
    const ids = [await createRun(db, dag), await createRun(db, dag), await createRun(db, dag)]
    for (const id of ids) await advanceRun(db, id)
    for (const id of ids) expect((await runState(id)).state).toBe('running')
  })
})

describe('runTimeout', () => {
  it('fails the run and its unfinished tasks once running longer than runTimeout', async () => {
    const dag = heldDag({ runTimeout: 100 })
    register(dag)
    const runId = await createRun(db, dag)

    await advanceRun(db, runId)
    expect((await runState(runId)).state).toBe('running')
    expect((await runState(runId)).started_at).toBeInstanceOf(Date)

    await new Promise(r => setTimeout(r, 150))
    await advanceRun(db, runId)

    expect((await runState(runId)).state).toBe('failed')
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'gate' })
    expect(ti!.state).toBe('failed')
    expect(ti!.error).toMatch(/runTimeout \(100ms\)/)
  })

  it('leaves a run alone while within runTimeout', async () => {
    const dag = heldDag({ runTimeout: 60_000 })
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    await advanceRun(db, runId)
    expect((await runState(runId)).state).toBe('running')
  })

  it('does not count time spent queued behind maxActiveRuns', async () => {
    const dag = heldDag({ maxActiveRuns: 1, runTimeout: 200 })
    register(dag)
    const r1 = await createRun(db, dag)
    const r2 = await createRun(db, dag)
    await advanceRun(db, r1)
    await advanceRun(db, r2)               // stays queued

    await new Promise(r => setTimeout(r, 250))   // queued longer than runTimeout
    await cancelRun(db, r1)
    await advanceRun(db, r2)               // starts now

    expect((await runState(r2)).state).toBe('running')
  })
})
