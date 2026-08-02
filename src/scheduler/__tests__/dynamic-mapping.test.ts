/**
 * Tests for XCom-driven dynamic task mapping.
 *
 * `expand: { from: 'sourceTask', key: 'items' }` fans out over a list
 * produced at runtime by an upstream task's XCom push.
 *
 * Advisor-specified verify cases:
 *  1. source pushes N items → N instances run, downstream runs once after all
 *  2. source pushes [] → mapped task skipped, run terminates (not hung)
 *  3. source fails → placeholder skipped via cascade, run terminates (not hung)
 *  4. instance count is derived at runtime, not authoring time
 *
 * Additional cases:
 *  - Non-array XCom value → mapped task skipped
 *  - Instance gets correct map_index and map_value
 *  - Literal expand still works (not broken by new code)
 *  - isDynamicMapped / isLiteralMapped type guards
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import { isDynamicMapped, isLiteralMapped } from '../mapping.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_DB = 'airflow_test_dynamic_mapping'

let client: MongoClient
let db: Db

// ── helpers ───────────────────────────────────────────────────────────────────

async function runDag(dag: DagDefinition, maxTicks = 25): Promise<string> {
  register(dag)
  const runId = await createRun(db, dag)
  for (let i = 0; i < maxTicks; i++) await advanceRun(db, runId)
  return runId
}

async function taskInstances(runId: string, taskId: string) {
  return db.collection('task_instances')
    .find({ dag_run_id: runId, task_id: taskId })
    .sort({ map_index: 1 })
    .toArray()
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

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  process.env.LOG_BACKEND = 'mongodb' // tests read from task_logs directly
  process.env.DB_NAME = TEST_DB  // workers inherit this for XCom writes
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
// Type guard unit tests
// ══════════════════════════════════════════════════════════════════════════════

describe('isDynamicMapped / isLiteralMapped', () => {
  it('isDynamicMapped recognizes { from, key } objects', () => {
    expect(isDynamicMapped({ from: 'upstream', key: 'items' })).toBe(true)
  })
  it('isDynamicMapped rejects arrays', () => {
    expect(isDynamicMapped(['a', 'b'])).toBe(false)
  })
  it('isDynamicMapped rejects undefined', () => {
    expect(isDynamicMapped(undefined)).toBe(false)
  })
  it('isLiteralMapped recognizes non-empty arrays', () => {
    expect(isLiteralMapped(['a', 'b'])).toBe(true)
  })
  it('isLiteralMapped rejects { from, key } objects', () => {
    expect(isLiteralMapped({ from: 'x', key: 'y' })).toBe(false)
  })
  it('isLiteralMapped rejects empty arrays', () => {
    expect(isLiteralMapped([])).toBe(false)
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// Integration tests — full DAG runs
// ══════════════════════════════════════════════════════════════════════════════

describe('XCom-driven dynamic mapping — integration', () => {

  // ── Case 1: source pushes N items → N instances run ──────────────────────

  it('source pushes 3 items → 3 mapped instances, all succeed', async () => {
    const dag: DagDefinition = {
      id: 'dm_basic',
      schedule: null,
      tasks: {
        source: {
          run: async (ctx) => {
            await ctx.xcom.push('files', ['/a.csv', '/b.csv', '/c.csv'])
            return 3
          }
        },
        process: {
          dependsOn: ['source'],
          expand: { from: 'source', key: 'files' },
          run: async (ctx) => `processed ${ctx.mapValue} at index ${ctx.mapIndex}`,
        },
      }
    }
    const runId = await runDag(dag)

    const instances = await taskInstances(runId, 'process')
    // Placeholder is deleted; only real instances remain
    const real = instances.filter(i => !i.is_dynamic_placeholder)
    expect(real.length).toBe(3)
    expect(real.every(i => i.state === 'success')).toBe(true)
    expect(real.map(i => i.map_index)).toEqual([0, 1, 2])
    expect(real.map(i => i.map_value)).toEqual(['/a.csv', '/b.csv', '/c.csv'])
    expect(await runState(runId)).toBe('success')
  })

  it('instance count derived at runtime — not known at authoring time', async () => {
    // The list size comes from conf, not hardcoded in the DAG
    const dag: DagDefinition = {
      id: 'dm_runtime_count',
      schedule: null,
      tasks: {
        list_items: {
          run: async (ctx) => {
            const count = Number(ctx.conf.count ?? 2)
            const items = Array.from({ length: count }, (_, i) => `item-${i}`)
            await ctx.xcom.push('items', items)
          }
        },
        process: {
          dependsOn: ['list_items'],
          expand: { from: 'list_items', key: 'items' },
          run: async (ctx) => `done: ${ctx.mapValue}`,
        },
      }
    }
    register(dag)
    const runId = await createRun(db, dag, { conf: { count: 4 } })
    for (let i = 0; i < 25; i++) await advanceRun(db, runId)

    const instances = await taskInstances(runId, 'process')
    const real = instances.filter(i => !i.is_dynamic_placeholder)
    expect(real.length).toBe(4)
    expect(await runState(runId)).toBe('success')
  })

  // ── Case 2: source pushes [] → mapped task skipped ────────────────────────

  it('source pushes [] → mapped task skipped, run terminates (not hung)', async () => {
    const dag: DagDefinition = {
      id: 'dm_empty',
      schedule: null,
      tasks: {
        source: {
          run: async (ctx) => {
            await ctx.xcom.push('items', [])  // empty!
          }
        },
        process: {
          dependsOn: ['source'],
          expand: { from: 'source', key: 'items' },
          run: async (ctx) => `should not run: ${ctx.mapValue}`,
        },
      }
    }
    const runId = await runDag(dag)

    const instances = await taskInstances(runId, 'process')
    // Placeholder should be marked skipped
    expect(instances.every(i => i.state === 'skipped')).toBe(true)
    const state = await runState(runId)
    expect(state).not.toBe('running')  // must terminate
    expect(['success', 'failed']).toContain(state)
  })

  // ── Case 3: source fails → placeholder skipped via cascade ───────────────

  it('source fails → placeholder skipped via all_success cascade, run terminates', async () => {
    const dag: DagDefinition = {
      id: 'dm_source_fail',
      schedule: null,
      tasks: {
        source: {
          run: async () => { throw new Error('source failed') }
        },
        process: {
          dependsOn: ['source'],
          expand: { from: 'source', key: 'items' },
          run: async (ctx) => `should not run: ${ctx.mapValue}`,
        },
      }
    }
    const runId = await runDag(dag)

    expect(await taskState(runId, 'source')).toBe('failed')
    const instances = await taskInstances(runId, 'process')
    expect(instances.every(i => i.state === 'skipped')).toBe(true)
    expect(await runState(runId)).toBe('failed')
  })

  // ── Non-array XCom value → skipped ────────────────────────────────────────

  it('source pushes non-array XCom → mapped task skipped, run terminates', async () => {
    const dag: DagDefinition = {
      id: 'dm_nonarr',
      schedule: null,
      tasks: {
        source: {
          run: async (ctx) => { await ctx.xcom.push('items', 'not-an-array') }
        },
        process: {
          dependsOn: ['source'],
          expand: { from: 'source', key: 'items' },
          run: async (ctx) => `should not run`,
        },
      }
    }
    const runId = await runDag(dag)
    const instances = await taskInstances(runId, 'process')
    expect(instances.every(i => i.state === 'skipped')).toBe(true)
    const state = await runState(runId)
    expect(state).not.toBe('running')
  })

  // ── Downstream after dynamic mapped task ──────────────────────────────────

  it('downstream task runs after ALL dynamic instances succeed', async () => {
    const dag: DagDefinition = {
      id: 'dm_downstream',
      schedule: null,
      tasks: {
        source: {
          run: async (ctx) => { await ctx.xcom.push('regions', ['us', 'eu', 'ap']) }
        },
        deploy: {
          dependsOn: ['source'],
          expand: { from: 'source', key: 'regions' },
          run: async (ctx) => `deployed to ${ctx.mapValue}`,
        },
        notify: {
          dependsOn: ['deploy'],
          run: async (ctx) => {
            // pull the array of results from all deploy instances
            const results = await ctx.xcom.pull('deploy', 'return_value') as unknown
            return `notified: all deployments done`
          }
        },
      }
    }
    const runId = await runDag(dag)

    const deployInstances = await taskInstances(runId, 'deploy')
    const real = deployInstances.filter(i => !i.is_dynamic_placeholder)
    expect(real.length).toBe(3)
    expect(real.every(i => i.state === 'success')).toBe(true)
    expect(await taskState(runId, 'notify')).toBe('success')
    expect(await runState(runId)).toBe('success')
  })

  // ── Literal expand still works ────────────────────────────────────────────

  it('literal expand (array form) still works correctly after changes', async () => {
    const dag: DagDefinition = {
      id: 'dm_literal',
      schedule: null,
      tasks: {
        process: {
          expand: ['x', 'y', 'z'],
          run: async (ctx) => `literal: ${ctx.mapValue}`,
        },
      }
    }
    const runId = await runDag(dag)
    const instances = await taskInstances(runId, 'process')
    expect(instances.length).toBe(3)
    expect(instances.every(i => i.state === 'success')).toBe(true)
    expect(await runState(runId)).toBe('success')
  })
})
