/**
 * Tests for task priority weights.
 *
 * Tasks with higher `priority` are claimed before lower-priority tasks
 * when multiple tasks are ready simultaneously.
 *
 * What each test answers:
 *  - Default priority (0) when not set?
 *  - Higher priority task starts before lower under 1-slot pool?
 *  - Negative priority runs last?
 *  - Equal priority → FIFO order?
 *  - Priority does NOT affect tasks with dependencies (only queued+ready tasks)?
 *  - Priority respected across different DAG runs sharing a pool?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { resetAllPools } from '../../pools/index.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_DB = 'airflow_test_priority'

let client: MongoClient
let db: Db

// ── helpers ───────────────────────────────────────────────────────────────────

async function taskState(runId: string, taskId: string) {
  return db.collection('task_instances').findOne(
    { dag_run_id: runId, task_id: taskId },
    { projection: { state: 1, started_at: 1, priority: 1, _id: 0 } }
  )
}

async function runDag(dag: DagDefinition, maxTicks = 15): Promise<string> {
  register(dag)
  const runId = await createRun(db, dag)
  for (let i = 0; i < maxTicks; i++) await advanceRun(db, runId)
  return runId
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(TEST_DB)
  clearRegistry()

  // 1-slot pool for priority ordering tests
  await db.collection('pools').deleteMany({ name: 'priority_pool' })
  await db.collection('pools').insertOne({ name: 'priority_pool', slots: 1, description: 'priority test' })
})

afterAll(async () => {
  await new Promise(r => setTimeout(r, 200))
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('task_logs').deleteMany({})
  clearRegistry()
  resetAllPools()  // clear in-memory semaphore state between tests
})

// ══════════════════════════════════════════════════════════════════════════════
// PRIORITY TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('task priority weights', () => {

  it('default priority is 0 when not specified', async () => {
    const dag: DagDefinition = {
      id: 'prio_default',
      schedule: null,
      tasks: { step: { run: async () => 'ok' } },
    }
    const runId = await runDag(dag)
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'step' })
    expect(ti?.priority).toBe(0)
  })

  it('priority field stored correctly on task instance', async () => {
    const dag: DagDefinition = {
      id: 'prio_stored',
      schedule: null,
      tasks: {
        high: { priority: 10, run: async () => 'high' },
        low:  { priority: -5, run: async () => 'low' },
        mid:  { priority: 3,  run: async () => 'mid' },
      },
    }
    const runId = await runDag(dag)
    const high = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'high' })
    const low  = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'low' })
    const mid  = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'mid' })
    expect(high?.priority).toBe(10)
    expect(low?.priority).toBe(-5)
    expect(mid?.priority).toBe(3)
  })

  it('high-priority task starts before low-priority under 1-slot pool', async () => {
    // Both tasks use the 1-slot pool — only one can run at a time.
    // high_prio (priority: 10) should be claimed before low_prio (priority: 1).
    // We verify by checking started_at timestamps after both complete.
    const dag: DagDefinition = {
      id: 'prio_order',
      schedule: null,
      tasks: {
        low_prio: {
          pool: 'priority_pool',
          priority: 1,
          run: async () => 'low',
        },
        high_prio: {
          pool: 'priority_pool',
          priority: 10,
          run: async () => 'high',
        },
      },
    }
    const runId = await runDag(dag, 30)

    // Both should succeed
    expect((await taskState(runId, 'high_prio'))?.state).toBe('success')
    expect((await taskState(runId, 'low_prio'))?.state).toBe('success')

    // high_prio started earlier
    const high = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'high_prio' })
    const low  = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'low_prio' })
    expect(high?.started_at).not.toBeNull()
    expect(low?.started_at).not.toBeNull()
    // high_prio started_at should be <= low_prio started_at
    expect(new Date(high!.started_at!).getTime()).toBeLessThanOrEqual(new Date(low!.started_at!).getTime())
  })

  it('negative priority runs last', async () => {
    const dag: DagDefinition = {
      id: 'prio_negative',
      schedule: null,
      tasks: {
        normal:   { pool: 'priority_pool', priority: 0,  run: async () => 'normal' },
        negative: { pool: 'priority_pool', priority: -10, run: async () => 'negative' },
      },
    }
    const runId = await runDag(dag, 30)

    const normal   = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'normal' })
    const negative = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'negative' })

    // normal (priority: 0) should start before negative (priority: -10)
    expect(new Date(normal!.started_at!).getTime()).toBeLessThanOrEqual(new Date(negative!.started_at!).getTime())
  })

  it('all tasks complete regardless of priority order', async () => {
    const dag: DagDefinition = {
      id: 'prio_complete',
      schedule: null,
      tasks: {
        a: { priority: 100, run: async () => 'a' },
        b: { priority: 50,  run: async () => 'b' },
        c: { priority: 0,   run: async () => 'c' },
        d: { priority: -50, run: async () => 'd' },
      },
    }
    const runId = await runDag(dag)
    for (const id of ['a', 'b', 'c', 'd']) {
      expect((await taskState(runId, id))?.state).toBe('success')
    }
  })

  it('priority does not affect tasks with unmet dependencies', async () => {
    // high_prio depends on gatekeeper — must not run before gatekeeper succeeds
    const dag: DagDefinition = {
      id: 'prio_deps',
      schedule: null,
      tasks: {
        gatekeeper: { priority: 0,   run: async () => 'gate' },
        high_prio:  { priority: 999, dependsOn: ['gatekeeper'], run: async () => 'high' },
      },
    }
    const runId = await runDag(dag)

    const gate = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'gatekeeper' })
    const high = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'high_prio' })

    // gatekeeper must have started first despite lower priority
    expect(new Date(gate!.started_at!).getTime()).toBeLessThanOrEqual(new Date(high!.started_at!).getTime())
    expect(gate?.state).toBe('success')
    expect(high?.state).toBe('success')
  })

  it('run succeeds and all tasks complete with priority set', async () => {
    const dag: DagDefinition = {
      id: 'prio_run_success',
      schedule: null,
      tasks: {
        extract: { priority: 10, run: async (ctx) => { await ctx.xcom.push('data', [1, 2, 3]); return 'extracted' } },
        transform: { priority: 5, dependsOn: ['extract'], run: async (ctx) => {
          const data = await ctx.xcom.pull('extract', 'data')
          return { transformed: data }
        }},
        load: { priority: 1, dependsOn: ['transform'], run: async () => 'loaded' },
      },
    }
    const runId = await runDag(dag)
    for (const id of ['extract', 'transform', 'load']) {
      expect((await taskState(runId, id))?.state).toBe('success')
    }
    const { ObjectId } = await import('mongodb')
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.state).toBe('success')
  })
})
