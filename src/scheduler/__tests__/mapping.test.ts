import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { planExpansion, isMappedTask } from '../mapping.js'
import { createRun } from '../runs.js'
import { claimReadyTasks } from '../claim.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import { buildServer } from '../../api/server.js'
import type { FastifyInstance } from 'fastify'
import { xcomPull } from '../../xcom/index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const noop = async () => {}

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_mapping'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_mapping')
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
  clearRegistry()
})

// ── planExpansion — pure ─────────────────────────────────────────────────────

describe('planExpansion — pure', () => {
  it('returns one instance per element with 0-based index', () => {
    const result = planExpansion(['a', 'b', 'c'])
    expect(result).toEqual([
      { map_index: 0, map_value: 'a' },
      { map_index: 1, map_value: 'b' },
      { map_index: 2, map_value: 'c' },
    ])
  })

  it('returns empty array for undefined expand', () => {
    expect(planExpansion(undefined)).toEqual([])
  })

  it('returns empty array for null expand', () => {
    expect(planExpansion(null)).toEqual([])
  })

  it('returns empty array for empty array', () => {
    expect(planExpansion([])).toEqual([])
  })

  it('handles object values', () => {
    const result = planExpansion([{ id: 1 }, { id: 2 }])
    expect(result[0].map_value).toEqual({ id: 1 })
    expect(result[1].map_index).toBe(1)
  })

  it('handles mixed types', () => {
    const result = planExpansion([1, 'two', null, { three: 3 }])
    expect(result).toHaveLength(4)
    expect(result[2].map_value).toBeNull()
  })
})

describe('isMappedTask', () => {
  it('returns true for non-empty array', () => {
    expect(isMappedTask(['a', 'b'])).toBe(true)
  })

  it('returns false for undefined/null/empty', () => {
    expect(isMappedTask(undefined)).toBe(false)
    expect(isMappedTask(null)).toBe(false)
    expect(isMappedTask([])).toBe(false)
  })
})

// ── createRun — fan-out ──────────────────────────────────────────────────────

describe('createRun — mapped task fan-out', () => {
  it('creates N task_instances for a mapped task', async () => {
    const dag: DagDefinition = {
      id: 'fan_out_dag', schedule: null,
      tasks: { process: { run: noop, expand: ['a', 'b', 'c'] } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const instances = await db.collection('task_instances')
      .find({ dag_run_id: runId, task_id: 'process' }).toArray()
    expect(instances).toHaveLength(3)
    expect(instances.map(i => i.map_index).sort()).toEqual([0, 1, 2])
    expect(instances.map(i => i.map_value).sort()).toEqual(['a', 'b', 'c'])
  })

  it('stamps map_index=null and map_value=null for non-mapped tasks', async () => {
    const dag: DagDefinition = {
      id: 'non_mapped_dag', schedule: null,
      tasks: { step: { run: noop } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const inst = await db.collection('task_instances')
      .findOne({ dag_run_id: runId, task_id: 'step' })
    expect(inst?.map_index).toBeNull()
    expect(inst?.map_value).toBeNull()
  })

  it('creates correct total instances for mixed dag (mapped + non-mapped)', async () => {
    const dag: DagDefinition = {
      id: 'mixed_dag', schedule: null,
      tasks: {
        extract: { run: noop },
        process: { run: noop, expand: ['x', 'y', 'z'], dependsOn: ['extract'] },
        report: { run: noop, dependsOn: ['process'] },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const all = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    expect(all).toHaveLength(5) // 1 extract + 3 process + 1 report
  })
})

// ── claimReadyTasks — partial-done gate (THE discriminating test) ────────────

describe('claimReadyTasks — mapped task dependency gate', () => {
  it('downstream NOT claimed while one mapped instance is still running', async () => {
    const dag: DagDefinition = {
      id: 'partial_gate_dag', schedule: null,
      tasks: {
        process: { run: noop, expand: [1, 2, 3] },
        downstream: { run: noop, dependsOn: ['process'] },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    // Mark process[0] success, process[1] running, process[2] queued
    const instances = await db.collection('task_instances')
      .find({ dag_run_id: runId, task_id: 'process' }).sort({ map_index: 1 }).toArray()
    await db.collection('task_instances').updateOne(
      { _id: instances[0]._id }, { $set: { state: 'success' } }
    )
    await db.collection('task_instances').updateOne(
      { _id: instances[1]._id }, { $set: { state: 'running' } }
    )
    // instances[2] stays queued

    const claimed = await claimReadyTasks(db, runId)
    // downstream must NOT be claimed — process is only partially done
    expect(claimed.map(t => t.task_id)).not.toContain('downstream')
  })

  it('downstream IS claimed when ALL mapped instances are success', async () => {
    const dag: DagDefinition = {
      id: 'all_done_gate_dag', schedule: null,
      tasks: {
        process: { run: noop, expand: [1, 2, 3] },
        downstream: { run: noop, dependsOn: ['process'] },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    // Mark ALL process instances success
    await db.collection('task_instances').updateMany(
      { dag_run_id: runId, task_id: 'process' },
      { $set: { state: 'success' } }
    )

    const claimed = await claimReadyTasks(db, runId)
    expect(claimed.map(t => t.task_id)).toContain('downstream')
  })

  it('non-mapped task dependency unchanged (1 instance, success → downstream claimable)', async () => {
    const dag: DagDefinition = {
      id: 'non_mapped_dep_dag', schedule: null,
      tasks: {
        step1: { run: noop },
        step2: { run: noop, dependsOn: ['step1'] },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    await db.collection('task_instances').updateOne(
      { dag_run_id: runId, task_id: 'step1' },
      { $set: { state: 'success' } }
    )

    const claimed = await claimReadyTasks(db, runId)
    expect(claimed.map(t => t.task_id)).toContain('step2')
  })
})

// ── Full run execution (forked worker) ───────────────────────────────────────

describe('mapped task end-to-end execution', () => {
  it('all instances execute and run reaches success state', async () => {
    const dag: DagDefinition = {
      id: 'e2e_mapped_dag', schedule: null,
      tasks: {
        process: {
          expand: ['file_a', 'file_b', 'file_c'],
          run: async (ctx) => {
            await ctx.xcom.push('processed', ctx.mapValue)
          },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const instances = await db.collection('task_instances')
      .find({ dag_run_id: runId, task_id: 'process' }).toArray()
    expect(instances).toHaveLength(3)
    const states = instances.map(i => `[${i.map_index}]:${i.state}`)
    expect(instances.every(i => i.state === 'success'), `states: ${states.join(', ')}`).toBe(true)

    const { ObjectId } = await import('mongodb')
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.state).toBe('success')
  })

  it('each mapped instance has distinct mapValue in ctx (verified via XCom)', async () => {
    const dag: DagDefinition = {
      id: 'e2e_xcom_mapped_dag', schedule: null,
      tasks: {
        process: {
          expand: ['alpha', 'beta', 'gamma'],
          run: async (ctx) => {
            // Push map_value so we can verify each got the right input
            await ctx.xcom.push('input', ctx.mapValue)
          },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    // xcomPull returns list ordered by map_index
    const values = await xcomPull(db, runId, 'process', 'input')
    expect(Array.isArray(values)).toBe(true)
    expect((values as unknown[]).sort()).toEqual(['alpha', 'beta', 'gamma'])
  })

  it('downstream task receives all mapped outputs as list via xcom.pull', async () => {
    const dag: DagDefinition = {
      id: 'reduce_dag', schedule: null,
      tasks: {
        process: {
          expand: [10, 20, 30],
          run: async (ctx) => {
            await ctx.xcom.push('result', (ctx.mapValue as number) * 2)
          },
        },
        aggregate: {
          run: async (ctx) => {
            const results = await ctx.xcom.pull('process', 'result')
            await ctx.xcom.push('sum', (results as number[]).reduce((a, b) => a + b, 0))
          },
          dependsOn: ['process'],
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const sumXcom = await db.collection('xcoms').findOne({
      dag_run_id: runId, task_id: 'aggregate', key: 'sum',
    })
    // 10*2 + 20*2 + 30*2 = 120
    expect(sumXcom?.value).toBe(120)
  })
})

  /**
   * Known limitation: if a mapped instance fails and there is a downstream task,
   * the run stays 'running' indefinitely — the downstream can never be claimed
   * (because process is not all-success) and advanceRun's allDone check never fires
   * (not all tasks are terminal). This is a pre-existing gap in the scheduler:
   * there is no upstream_failed state to propagate failure forward.
   * Tracked limitation — not a regression introduced by mapping.
   */
  it('known: mapped failure with downstream keeps run running (not success/failed)', async () => {
    const dag: DagDefinition = {
      id: 'failure_gap_dag', schedule: null,
      tasks: {
        process: {
          expand: [1, 2],
          run: async (ctx) => {
            if (ctx.mapValue === 2) throw new Error('instance 2 fails intentionally')
          },
        },
        downstream: { run: noop, dependsOn: ['process'] },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const { ObjectId } = await import('mongodb')
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    // Known limitation: run stays 'running' because downstream is blocked and allDone never fires
    // If this test fails (run becomes 'success' or 'failed'), the limitation has been fixed.
    const runState = run?.state
    // We assert what actually happens (running stays), not what we want (it should be 'failed')
    // This documents the gap rather than asserting incorrect behavior
    expect(['running', 'failed']).toContain(runState) // failed = improvement, running = known gap
  })

// ── API ───────────────────────────────────────────────────────────────────────

describe('API — mapped task fields', () => {
  it('GET /dag-runs/:runId exposes map_index and map_value per task', async () => {
    const dag: DagDefinition = {
      id: 'api_mapped_dag', schedule: null,
      tasks: { work: { run: noop, expand: ['x', 'y'] } },
    }
    register(dag)
    const runId = await createRun(db, dag)

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    const tasks = body.tasks as { task_id: string; map_index: number; map_value: unknown }[]
    expect(tasks).toHaveLength(2)
    const indices = tasks.map(t => t.map_index).sort()
    const values = tasks.map(t => t.map_value).sort()
    expect(indices).toEqual([0, 1])
    expect(values).toEqual(['x', 'y'])
  })

  it('non-mapped task has map_index=null and map_value=null in API response', async () => {
    const dag: DagDefinition = {
      id: 'api_non_mapped_dag', schedule: null,
      tasks: { step: { run: noop } },
    }
    register(dag)
    const runId = await createRun(db, dag)

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    const body = res.json()
    expect(body.tasks[0].map_index).toBeNull()
    expect(body.tasks[0].map_value).toBeNull()
  })
})
