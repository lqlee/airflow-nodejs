/**
 * Tests for branch tasks (@task.branch equivalent).
 *
 * A branch task returns task_id(s) to activate; all other direct dependents
 * are automatically skipped. The cascade then propagates skips downstream.
 *
 * What each test answers:
 *  - Does the selected branch run and others get skipped?
 *  - Does selecting multiple branches work?
 *  - Does selecting no branches (null / []) skip all?
 *  - Does a join task (none_failed) still run after branch skip?
 *  - Does a chain downstream of the skipped branch also get skipped?
 *  - Does an invalid task_id in branch decision get ignored (not crash)?
 *  - Does a failing branch task → branch skipped (not stuck running)?
 *  - Is the decision stored as XCom '_branch_decision'?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_DB = 'airflow_test_branching'

let client: MongoClient
let db: Db

// ── helpers ───────────────────────────────────────────────────────────────────

async function runDag(dag: DagDefinition, maxTicks = 20): Promise<string> {
  register(dag)
  const runId = await createRun(db, dag)
  for (let i = 0; i < maxTicks; i++) await advanceRun(db, runId)
  return runId
}

async function taskState(runId: string, taskId: string): Promise<string | undefined> {
  const ti = await db.collection('task_instances').findOne(
    { dag_run_id: runId, task_id: taskId },
    { projection: { state: 1 } }
  )
  return ti?.state
}

async function runState(runId: string): Promise<string | undefined> {
  const { ObjectId } = await import('mongodb')
  const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
  return run?.state
}

async function xcomValue(runId: string, taskId: string, key: string): Promise<unknown> {
  const doc = await db.collection('xcoms').findOne({ dag_run_id: runId, task_id: taskId, key })
  return doc?.value
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  // Branch tasks run in forked workers which inherit process.env.
  // Set DB_NAME so workers write XCom to the same test DB as this test.
  process.env.DB_NAME = TEST_DB
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(TEST_DB)
  clearRegistry()
})

afterAll(async () => {
  await new Promise(r => setTimeout(r, 200))
  delete process.env.DB_NAME
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('task_logs').deleteMany({})
  await db.collection('xcoms').deleteMany({})
  clearRegistry()
})

// ══════════════════════════════════════════════════════════════════════════════
// BRANCHING TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('branch tasks', () => {

  it('selected branch runs; non-selected branch is skipped', async () => {
    const dag: DagDefinition = {
      id: 'br_basic',
      schedule: null,
      tasks: {
        decide: {
          branch: async () => 'path_a',  // always pick path_a
        },
        path_a: { dependsOn: ['decide'], run: async () => 'a done' },
        path_b: { dependsOn: ['decide'], run: async () => 'b done' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'path_a')).toBe('success')
    expect(await taskState(runId, 'path_b')).toBe('skipped')
    expect(await runState(runId)).toBe('success')
  })

  it('branch decision is stored as XCom _branch_decision', async () => {
    const dag: DagDefinition = {
      id: 'br_xcom',
      schedule: null,
      tasks: {
        decide: { branch: async () => 'path_a' },
        path_a: { dependsOn: ['decide'], run: async () => 'ok' },
        path_b: { dependsOn: ['decide'], run: async () => 'ok' },
      }
    }
    const runId = await runDag(dag)
    const decision = await xcomValue(runId, 'decide', '_branch_decision')
    expect(decision).toEqual(['path_a'])
  })

  it('branch can select multiple paths', async () => {
    const dag: DagDefinition = {
      id: 'br_multi',
      schedule: null,
      tasks: {
        decide: { branch: async () => ['path_a', 'path_b'] },  // both
        path_a: { dependsOn: ['decide'], run: async () => 'a' },
        path_b: { dependsOn: ['decide'], run: async () => 'b' },
        path_c: { dependsOn: ['decide'], run: async () => 'c — should skip' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'path_a')).toBe('success')
    expect(await taskState(runId, 'path_b')).toBe('success')
    expect(await taskState(runId, 'path_c')).toBe('skipped')
    expect(await runState(runId)).toBe('success')
  })

  it('returning null skips all direct dependents', async () => {
    const dag: DagDefinition = {
      id: 'br_null',
      schedule: null,
      tasks: {
        decide: { branch: async () => null },
        path_a: { dependsOn: ['decide'], run: async () => 'a' },
        path_b: { dependsOn: ['decide'], run: async () => 'b' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'path_a')).toBe('skipped')
    expect(await taskState(runId, 'path_b')).toBe('skipped')
    const state = await runState(runId)
    // Run must be terminal — not stuck
    expect(['success', 'failed']).toContain(state)
  })

  it('returning [] skips all direct dependents', async () => {
    const dag: DagDefinition = {
      id: 'br_empty',
      schedule: null,
      tasks: {
        decide: { branch: async () => [] },
        path_a: { dependsOn: ['decide'], run: async () => 'a' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'path_a')).toBe('skipped')
    const state = await runState(runId)
    expect(['success', 'failed']).toContain(state)
  })

  it('join task with none_failed runs after branch (skipped branch is not failed)', async () => {
    const dag: DagDefinition = {
      id: 'br_join',
      schedule: null,
      tasks: {
        decide:  { branch: async () => 'path_a' },
        path_a:  { dependsOn: ['decide'], run: async () => 'a' },
        path_b:  { dependsOn: ['decide'], run: async () => 'b' },
        // Join: none_failed so it runs even when path_b is skipped
        join: {
          dependsOn: ['path_a', 'path_b'],
          triggerRule: 'none_failed',
          run: async () => 'join complete',
        },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'path_a')).toBe('success')
    expect(await taskState(runId, 'path_b')).toBe('skipped')
    expect(await taskState(runId, 'join')).toBe('success')
    expect(await runState(runId)).toBe('success')
  })

  it('chain downstream of skipped branch is also skipped (cascade)', async () => {
    const dag: DagDefinition = {
      id: 'br_cascade',
      schedule: null,
      tasks: {
        decide:    { branch: async () => 'path_a' },
        path_a:    { dependsOn: ['decide'], run: async () => 'a' },
        // path_b is skipped; path_b_child depends on it → also skipped
        path_b:    { dependsOn: ['decide'], run: async () => 'b' },
        path_b_child: { dependsOn: ['path_b'], run: async () => 'b child' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'path_a')).toBe('success')
    expect(await taskState(runId, 'path_b')).toBe('skipped')
    expect(await taskState(runId, 'path_b_child')).toBe('skipped')
    expect(await runState(runId)).toBe('success')
  })

  it('invalid task_id in branch decision is ignored; run still terminates', async () => {
    const dag: DagDefinition = {
      id: 'br_invalid',
      schedule: null,
      tasks: {
        decide: { branch: async () => ['path_a', 'nonexistent_task_xyz'] },
        path_a: { dependsOn: ['decide'], run: async () => 'a' },
        path_b: { dependsOn: ['decide'], run: async () => 'b' },
      }
    }
    const runId = await runDag(dag)
    // 'nonexistent_task_xyz' is ignored; path_a selected, path_b skipped
    expect(await taskState(runId, 'path_a')).toBe('success')
    expect(await taskState(runId, 'path_b')).toBe('skipped')
    const state = await runState(runId)
    expect(state).not.toBe('running')  // must terminate
  })

  it('failing branch task marks branch failed; run terminates (not stuck)', async () => {
    const dag: DagDefinition = {
      id: 'br_fail',
      schedule: null,
      tasks: {
        decide: {
          branch: async () => { throw new Error('branch exploded') }
        },
        path_a: { dependsOn: ['decide'], run: async () => 'a' },
        path_b: { dependsOn: ['decide'], run: async () => 'b' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'decide')).toBe('failed')
    // Both paths skipped (all_success → upstream failed)
    expect(await taskState(runId, 'path_a')).toBe('skipped')
    expect(await taskState(runId, 'path_b')).toBe('skipped')
    const state = await runState(runId)
    expect(state).toBe('failed')
    expect(state).not.toBe('running')
  })

  it('branch uses ctx (conf) to make dynamic decision', async () => {
    const dag: DagDefinition = {
      id: 'br_dynamic',
      schedule: null,
      tasks: {
        decide: {
          branch: async (ctx) => {
            // Decision based on trigger-time conf
            return ctx.conf.env === 'prod' ? 'prod_path' : 'dev_path'
          }
        },
        prod_path: { dependsOn: ['decide'], run: async () => 'production!' },
        dev_path:  { dependsOn: ['decide'], run: async () => 'development!' },
      }
    }
    register(dag)
    const runId = await createRun(db, dag, { conf: { env: 'prod' } })
    for (let i = 0; i < 20; i++) await advanceRun(db, runId)

    expect(await taskState(runId, 'prod_path')).toBe('success')
    expect(await taskState(runId, 'dev_path')).toBe('skipped')
    expect(await runState(runId)).toBe('success')
  })

  it('full branching pipeline: prepare → branch → (fast|slow) → join → notify', async () => {
    const dag: DagDefinition = {
      id: 'br_full',
      schedule: null,
      tasks: {
        prepare:   { run: async () => ({ ready: true }) },
        route: {
          dependsOn: ['prepare'],
          branch: async () => 'fast_path',
        },
        fast_path: { dependsOn: ['route'], run: async () => 'fast done' },
        slow_path: { dependsOn: ['route'], run: async () => 'slow done' },
        join: {
          dependsOn: ['fast_path', 'slow_path'],
          triggerRule: 'none_failed',
          run: async () => 'all branches resolved',
        },
        notify: {
          dependsOn: ['join'],
          run: async () => 'notified',
        },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'prepare')).toBe('success')
    expect(await taskState(runId, 'fast_path')).toBe('success')
    expect(await taskState(runId, 'slow_path')).toBe('skipped')
    expect(await taskState(runId, 'join')).toBe('success')
    expect(await taskState(runId, 'notify')).toBe('success')
    expect(await runState(runId)).toBe('success')
  })
})
