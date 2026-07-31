/**
 * Tests for trigger rules — controls when a task runs based on upstream states.
 *
 * Rules: all_success (default), all_failed, all_done, one_success, one_failed, none_failed
 *
 * Each test drives a full DAG run through MongoDB + executor and asserts:
 *  - The downstream task's final state (running vs skipped)
 *  - The run's final state (success/failed — never stuck 'running')
 *
 * Pure unit tests for isSatisfied/isUnsatisfiable are also included.
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import { isSatisfied, isUnsatisfiable } from '../claim.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'

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
  const run = await db.collection('dag_runs').findOne({ _id: new (await import('mongodb')).ObjectId(runId) })
  return run?.state
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_trigger_rules')
  clearRegistry()
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
})

// ══════════════════════════════════════════════════════════════════════════════
// Pure unit tests — isSatisfied / isUnsatisfiable
// ══════════════════════════════════════════════════════════════════════════════

describe('isSatisfied', () => {
  it('all_success: true when all upstream succeeded', () => {
    expect(isSatisfied('all_success', ['success', 'success'])).toBe(true)
  })
  it('all_success: false when any upstream failed', () => {
    expect(isSatisfied('all_success', ['success', 'failed'])).toBe(false)
  })
  it('all_success: false when any upstream still pending', () => {
    expect(isSatisfied('all_success', ['success', 'pending'])).toBe(false)
  })

  it('all_failed: true when all failed or skipped', () => {
    expect(isSatisfied('all_failed', ['failed', 'skipped'])).toBe(true)
  })
  it('all_failed: false when any succeeded', () => {
    expect(isSatisfied('all_failed', ['failed', 'success'])).toBe(false)
  })

  it('all_done: true when all terminal regardless of outcome', () => {
    expect(isSatisfied('all_done', ['success', 'failed', 'skipped'])).toBe(true)
  })
  it('all_done: false when any pending', () => {
    expect(isSatisfied('all_done', ['success', 'pending'])).toBe(false)
  })

  it('one_success: true when at least one succeeded', () => {
    expect(isSatisfied('one_success', ['failed', 'success', 'failed'])).toBe(true)
  })
  it('one_success: false when none succeeded', () => {
    expect(isSatisfied('one_success', ['failed', 'failed'])).toBe(false)
  })

  it('one_failed: true when at least one failed', () => {
    expect(isSatisfied('one_failed', ['success', 'failed'])).toBe(true)
  })
  it('one_failed: false when none failed', () => {
    expect(isSatisfied('one_failed', ['success', 'success'])).toBe(false)
  })

  it('none_failed: true when all success or skipped', () => {
    expect(isSatisfied('none_failed', ['success', 'skipped'])).toBe(true)
  })
  it('none_failed: false when any failed', () => {
    expect(isSatisfied('none_failed', ['success', 'failed'])).toBe(false)
  })

  it('no upstreams (empty array): all rules satisfied immediately', () => {
    for (const rule of ['all_success', 'all_failed', 'all_done', 'one_success', 'one_failed', 'none_failed']) {
      // Empty upstreams = no constraints; vacuously true (except one_* which need ≥1)
      // all_success([]) = true; all_failed([]) = true; all_done([]) = true
      // one_success([]) = false (no successes); one_failed([]) = false
      // none_failed([]) = true
    }
    expect(isSatisfied('all_success', [])).toBe(true)
    expect(isSatisfied('all_done', [])).toBe(true)
    expect(isSatisfied('none_failed', [])).toBe(true)
    expect(isSatisfied('one_success', [])).toBe(false)
    expect(isSatisfied('one_failed', [])).toBe(false)
  })
})

describe('isUnsatisfiable', () => {
  it('all_success becomes unsatisfiable when any upstream failed', () => {
    expect(isUnsatisfiable('all_success', ['success', 'failed'])).toBe(true)
  })
  it('all_success is NOT unsatisfiable when pending', () => {
    expect(isUnsatisfiable('all_success', ['success', 'pending'])).toBe(false)
  })
  it('one_success becomes unsatisfiable when all upstream failed', () => {
    expect(isUnsatisfiable('one_success', ['failed', 'failed'])).toBe(true)
  })
  it('one_failed becomes unsatisfiable when all upstream succeeded', () => {
    expect(isUnsatisfiable('one_failed', ['success', 'success'])).toBe(true)
  })
  it('all_done is never unsatisfiable once all terminal', () => {
    expect(isUnsatisfiable('all_done', ['success', 'failed'])).toBe(false)
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// Integration tests — full DAG runs
// ══════════════════════════════════════════════════════════════════════════════

describe('trigger rules — integration', () => {

  // ── all_success (default) ────────────────────────────────────────────────

  it('all_success (default): downstream runs when upstream succeeds', async () => {
    const dag: DagDefinition = {
      id: 'tr_all_success_pass',
      schedule: null,
      tasks: {
        up:   { run: async () => 'ok' },
        down: { dependsOn: ['up'], run: async () => 'ok' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('success')
    expect(await runState(runId)).toBe('success')
  })

  it('all_success: downstream is skipped when upstream fails', async () => {
    const dag: DagDefinition = {
      id: 'tr_all_success_skip',
      schedule: null,
      tasks: {
        up:   { run: async () => { throw new Error('upstream failed') } },
        down: { dependsOn: ['up'], run: async () => 'ok' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'up')).toBe('failed')
    expect(await taskState(runId, 'down')).toBe('skipped')
    // Run must be terminal — never stuck 'running'
    expect(await runState(runId)).toBe('failed')
  })

  // ── all_failed ────────────────────────────────────────────────────────────

  it('all_failed: downstream runs when upstream fails', async () => {
    const dag: DagDefinition = {
      id: 'tr_all_failed_pass',
      schedule: null,
      tasks: {
        up:   { run: async () => { throw new Error('expected') } },
        down: { dependsOn: ['up'], triggerRule: 'all_failed', run: async () => 'cleanup' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('success')
    expect(await runState(runId)).not.toBe('running')  // must be terminal
  })

  it('all_failed: downstream is skipped when upstream succeeds', async () => {
    const dag: DagDefinition = {
      id: 'tr_all_failed_skip',
      schedule: null,
      tasks: {
        up:   { run: async () => 'ok' },
        down: { dependsOn: ['up'], triggerRule: 'all_failed', run: async () => 'should not run' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('skipped')
    // Run must reach terminal state
    const state = await runState(runId)
    expect(['success', 'failed']).toContain(state)
  })

  // ── all_done ──────────────────────────────────────────────────────────────

  it('all_done: downstream runs regardless of upstream outcome (success)', async () => {
    const dag: DagDefinition = {
      id: 'tr_all_done_success',
      schedule: null,
      tasks: {
        up:   { run: async () => 'ok' },
        down: { dependsOn: ['up'], triggerRule: 'all_done', run: async () => 'always runs' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('success')
  })

  it('all_done: downstream runs regardless of upstream outcome (failure)', async () => {
    const dag: DagDefinition = {
      id: 'tr_all_done_fail',
      schedule: null,
      tasks: {
        up:   { run: async () => { throw new Error('fail') } },
        down: { dependsOn: ['up'], triggerRule: 'all_done', run: async () => 'always runs' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('success')
    expect(await runState(runId)).not.toBe('running')
  })

  // ── one_success ───────────────────────────────────────────────────────────

  it('one_success: downstream runs if at least one upstream succeeded', async () => {
    const dag: DagDefinition = {
      id: 'tr_one_success_pass',
      schedule: null,
      tasks: {
        a:    { run: async () => { throw new Error('a fails') } },
        b:    { run: async () => 'b ok' },
        down: { dependsOn: ['a', 'b'], triggerRule: 'one_success', run: async () => 'ok' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('success')
    expect(await runState(runId)).not.toBe('running')
  })

  it('one_success: downstream is skipped if all upstreams failed', async () => {
    const dag: DagDefinition = {
      id: 'tr_one_success_skip',
      schedule: null,
      tasks: {
        a:    { run: async () => { throw new Error('a') } },
        b:    { run: async () => { throw new Error('b') } },
        down: { dependsOn: ['a', 'b'], triggerRule: 'one_success', run: async () => 'should skip' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('skipped')
    const state = await runState(runId)
    expect(['success', 'failed']).toContain(state)
  })

  // ── one_failed ────────────────────────────────────────────────────────────

  it('one_failed: downstream runs if at least one upstream failed', async () => {
    const dag: DagDefinition = {
      id: 'tr_one_failed_pass',
      schedule: null,
      tasks: {
        a:    { run: async () => 'ok' },
        b:    { run: async () => { throw new Error('b fails') } },
        down: { dependsOn: ['a', 'b'], triggerRule: 'one_failed', run: async () => 'alert sent' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('success')
    expect(await runState(runId)).not.toBe('running')
  })

  it('one_failed: downstream is skipped if all upstreams succeeded', async () => {
    const dag: DagDefinition = {
      id: 'tr_one_failed_skip',
      schedule: null,
      tasks: {
        a:    { run: async () => 'ok' },
        b:    { run: async () => 'ok' },
        down: { dependsOn: ['a', 'b'], triggerRule: 'one_failed', run: async () => 'skip me' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('skipped')
    const state = await runState(runId)
    expect(['success', 'failed']).toContain(state)
  })

  // ── none_failed ───────────────────────────────────────────────────────────

  it('none_failed: downstream runs when no upstream failed (all success)', async () => {
    const dag: DagDefinition = {
      id: 'tr_none_failed_pass',
      schedule: null,
      tasks: {
        a:    { run: async () => 'ok' },
        b:    { run: async () => 'ok' },
        down: { dependsOn: ['a', 'b'], triggerRule: 'none_failed', run: async () => 'all good' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('success')
    expect(await runState(runId)).toBe('success')
  })

  it('none_failed: downstream is skipped when any upstream failed', async () => {
    const dag: DagDefinition = {
      id: 'tr_none_failed_skip',
      schedule: null,
      tasks: {
        a:    { run: async () => 'ok' },
        b:    { run: async () => { throw new Error('b fails') } },
        down: { dependsOn: ['a', 'b'], triggerRule: 'none_failed', run: async () => 'skip me' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'down')).toBe('skipped')
    const state = await runState(runId)
    expect(['success', 'failed']).toContain(state)
  })

  // ── cascade ───────────────────────────────────────────────────────────────

  it('skip cascades: skipping A (unsatisfied) makes B (all_success dep on A) also skip', async () => {
    const dag: DagDefinition = {
      id: 'tr_cascade',
      schedule: null,
      tasks: {
        root: { run: async () => { throw new Error('root fails') } },
        // A: all_success → skipped because root failed
        mid:  { dependsOn: ['root'], run: async () => 'mid' },
        // B: all_success dep on mid → skipped because mid is skipped
        leaf: { dependsOn: ['mid'], run: async () => 'leaf' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'root')).toBe('failed')
    expect(await taskState(runId, 'mid')).toBe('skipped')
    expect(await taskState(runId, 'leaf')).toBe('skipped')
    // Run must be terminal — not stuck 'running'
    expect(await runState(runId)).toBe('failed')
  })

  it('one_failed on skipped upstream: skipped counts as not-failed → one_failed unsatisfiable', async () => {
    const dag: DagDefinition = {
      id: 'tr_skipped_upstream',
      schedule: null,
      tasks: {
        root: { run: async () => { throw new Error('fail') } },
        mid:  { dependsOn: ['root'], run: async () => 'mid' },  // → skipped
        // one_failed on skipped: skipped ≠ failed → no failure to trigger on
        alert: {
          dependsOn: ['mid'],
          triggerRule: 'one_failed',
          run: async () => 'alert'
        },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'mid')).toBe('skipped')
    expect(await taskState(runId, 'alert')).toBe('skipped')
    const state = await runState(runId)
    expect(['success', 'failed']).toContain(state)
    expect(state).not.toBe('running')
  })

  // ── mixed rules in same DAG ───────────────────────────────────────────────

  it('mixed rules: cleanup runs on failure, report runs always', async () => {
    const dag: DagDefinition = {
      id: 'tr_mixed',
      schedule: null,
      tasks: {
        work:    { run: async () => { throw new Error('work fails') } },
        cleanup: { dependsOn: ['work'], triggerRule: 'all_failed',  run: async () => 'cleanup done' },
        report:  { dependsOn: ['work'], triggerRule: 'all_done',    run: async () => 'reported' },
        notify:  { dependsOn: ['work'], triggerRule: 'one_failed',  run: async () => 'notified' },
        skip_me: { dependsOn: ['work'], triggerRule: 'all_success', run: async () => 'should skip' },
      }
    }
    const runId = await runDag(dag)
    expect(await taskState(runId, 'work')).toBe('failed')
    expect(await taskState(runId, 'cleanup')).toBe('success')
    expect(await taskState(runId, 'report')).toBe('success')
    expect(await taskState(runId, 'notify')).toBe('success')
    expect(await taskState(runId, 'skip_me')).toBe('skipped')
    // Run must be terminal
    const state = await runState(runId)
    expect(state).not.toBe('running')
  })
})
