import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_timeout')
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

describe('task timeout', () => {
  it('stores timeout_ms: 0 when no timeout is defined', async () => {
    const dag: DagDefinition = {
      id: 'no_timeout',
      schedule: null,
      tasks: {
        step: { run: async () => {} },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'step' })
    expect(ti!.timeout_ms).toBe(0)
  })

  it('stores timeout_ms from task definition', async () => {
    const dag: DagDefinition = {
      id: 'has_timeout',
      schedule: null,
      tasks: {
        step: { timeout: 5000, run: async () => {} },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'step' })
    expect(ti!.timeout_ms).toBe(5000)
  })

  it('marks task failed when it exceeds timeout', async () => {
    const dag: DagDefinition = {
      id: 'timeout_dag',
      schedule: null,
      tasks: {
        slow: {
          timeout: 500,  // 500ms timeout
          run: async () => {
            // Hang for 5 seconds — should be killed at 500ms
            await new Promise(res => setTimeout(res, 5000))
          },
        },
      },
    }
    register(dag)

    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'slow' })
    expect(ti!.state).toBe('failed')
    expect(ti!.error).toMatch(/timed out/i)
    expect(ti!.ended_at).toBeDefined()
  }, 10000)

  it('marks the dag_run failed when a timed-out task has no retries', async () => {
    const dag: DagDefinition = {
      id: 'timeout_run_failed',
      schedule: null,
      tasks: {
        slow: {
          timeout: 500,
          retries: 0,
          run: async () => { await new Promise(res => setTimeout(res, 5000)) },
        },
      },
    }
    register(dag)

    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run!.state).toBe('failed')
  }, 10000)

  it('task without timeout completes normally', async () => {
    const dag: DagDefinition = {
      id: 'no_timeout_completes',
      schedule: null,
      tasks: {
        fast: {
          // no timeout
          run: async () => {},
        },
      },
    }
    register(dag)

    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'fast' })
    expect(ti!.state).toBe('success')
  }, 10000)
})
