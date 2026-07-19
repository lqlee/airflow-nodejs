/**
 * Graceful shutdown tests.
 *
 * drainPool is tested with injected counters — no real workers, no MongoDB.
 * isShuttingDown / setShuttingDown are pure in-memory; tested synchronously.
 * advanceRun bail-on-shutdown is tested end-to-end: a run in-progress bails
 * mid-wave when the flag is set, leaving tasks as queued (recovered on boot).
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from 'vitest'
import { drainPool, resetPool, activeWorkers, queueDepth } from '../pool.js'
import { setShuttingDown, isShuttingDown, startScheduler, stopScheduler, advanceRun } from '../index.js'
import { MongoClient, type Db } from 'mongodb'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../runs.js'
import type { DagDefinition } from '../../dag/types.js'

// ── drainPool — pure unit tests with injected counters ─────────────────────

describe('drainPool', () => {
  it('resolves immediately when both counters are 0', async () => {
    const result = await drainPool({
      getActive: () => 0,
      getQueued: () => 0,
      pollMs: 10,
      timeoutMs: 500,
    })
    expect(result.drained).toBe(true)
    expect(result.remaining).toBe(0)
  })

  it('waits while active > 0 and resolves when it drops to 0', async () => {
    let count = 3
    // Count decreases every 30ms — drain should catch it within timeout
    const id = setInterval(() => { if (count > 0) count-- }, 30)

    const result = await drainPool({
      getActive: () => count,
      getQueued: () => 0,
      pollMs: 15,
      timeoutMs: 500,
    })

    clearInterval(id)
    expect(result.drained).toBe(true)
    expect(result.remaining).toBe(0)
  })

  it('waits while queued > 0 and resolves when both reach 0', async () => {
    // active=1 for first 60ms, then queued=1 for next 60ms, then both 0
    let active = 1
    let queued = 0
    const t1 = setTimeout(() => { active = 0; queued = 1 }, 60)
    const t2 = setTimeout(() => { queued = 0 }, 120)

    const result = await drainPool({
      getActive: () => active,
      getQueued: () => queued,
      pollMs: 20,
      timeoutMs: 500,
    })

    clearTimeout(t1)
    clearTimeout(t2)
    expect(result.drained).toBe(true)
    expect(result.remaining).toBe(0)
  })

  it('returns drained=false and remaining>0 when timeout elapses', async () => {
    // Counters never reach 0
    const result = await drainPool({
      getActive: () => 2,
      getQueued: () => 1,
      pollMs: 10,
      timeoutMs: 80,
    })
    expect(result.drained).toBe(false)
    expect(result.remaining).toBe(3)
  })

  it('resolves without waiting when both counters start at 0 (no poll needed)', async () => {
    const start = Date.now()
    await drainPool({
      getActive: () => 0,
      getQueued: () => 0,
      pollMs: 200,
      timeoutMs: 1000,
    })
    // Should complete well before pollMs elapsed — no unnecessary sleep
    expect(Date.now() - start).toBeLessThan(150)
  })
})

// ── isShuttingDown / setShuttingDown ──────────────────────────────────────

describe('isShuttingDown / setShuttingDown', () => {
  afterEach(() => {
    // Always reset so other tests start clean
    setShuttingDown(false)
  })

  it('starts false', () => {
    expect(isShuttingDown()).toBe(false)
  })

  it('can be set to true', () => {
    setShuttingDown(true)
    expect(isShuttingDown()).toBe(true)
  })

  it('startScheduler resets the flag to false', () => {
    setShuttingDown(true)
    // startScheduler needs a db-like object — we pass a minimal stub
    // It only stores the interval; the db arg is captured by tick() closures
    const fakeDb = {} as never
    startScheduler(fakeDb)
    expect(isShuttingDown()).toBe(false)
    stopScheduler()  // clean up the interval
  })

  it('stopScheduler sets the flag to true', () => {
    setShuttingDown(false)
    stopScheduler()
    expect(isShuttingDown()).toBe(true)
  })
})

// ── advanceRun bails when shuttingDown=true ────────────────────────────────

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'

let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_shutdown'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_shutdown')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
  delete process.env.DB_NAME
})

afterEach(async () => {
  setShuttingDown(false)
  resetPool()
  clearRegistry()
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
})

describe('advanceRun bails on shutdown flag', () => {
  it('does not execute tasks when shuttingDown=true before first wave', async () => {
    const dag: DagDefinition = {
      id: 'shutdown_bail_dag',
      schedule: null,
      tasks: {
        step: { run: async () => { /* would succeed */ } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    // Set flag before advancing — advanceRun should bail without forking
    setShuttingDown(true)
    await advanceRun(db, runId)

    // Run should still be in non-terminal state (queued or running)
    // recoverOrphanedRuns() would re-claim it on next boot
    const run = await db.collection('dag_runs').findOne({ _id: { $exists: true } })
    expect(run?.state).toMatch(/^(queued|running)$/)

    // Task should be queued (not started/finished) since we bailed
    const tasks = await db.collection('task_instances').find({}).toArray()
    const taskStates = tasks.map(t => t.state)
    // Either still queued (bail before claim) or still running (bail after claim but no fork)
    expect(taskStates.every(s => s === 'queued' || s === 'running')).toBe(true)
  })

  it('normal run completes when shuttingDown=false', async () => {
    const dag: DagDefinition = {
      id: 'shutdown_ok_dag',
      schedule: null,
      tasks: {
        step: { run: async () => { /* ok */ } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    // Flag is false — run should complete normally
    setShuttingDown(false)
    await advanceRun(db, runId)

    const run = await db.collection('dag_runs').findOne({ _id: { $exists: true } })
    expect(run?.state).toBe('success')
  })
})

// ── Pool real counters (smoke test) ────────────────────────────────────────

describe('drainPool with real pool counters', () => {
  afterEach(() => resetPool())

  it('resolves immediately when pool is empty', async () => {
    // Pool starts at 0 after reset
    expect(activeWorkers()).toBe(0)
    expect(queueDepth()).toBe(0)
    const result = await drainPool({ timeoutMs: 500, pollMs: 20 })
    expect(result.drained).toBe(true)
  })
})
