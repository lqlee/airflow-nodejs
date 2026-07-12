import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
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
  db = client.db('airflow_test_retries')
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

describe('task retries', () => {
  it('retries max_retries times then marks failed', async () => {
    // Task always fails — use try_number (from ctx) to track attempts
    // ctx is passed to the worker, so it's available inside new Function()
    const dag: DagDefinition = {
      id: 'retry_test',
      schedule: null,
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
    // Each advance: claims queued task, runs it, fails, requeues (if retries remain)
    for (let i = 0; i < 5; i++) {
      await advanceRun(db, runId)
    }

    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'flaky' })
    expect(ti!.state).toBe('failed')
    expect(ti!.try_number).toBe(2)   // incremented twice by retry logic
  }, 15000)

  it('succeeds on a later retry using try_number in ctx', async () => {
    // Use try_number passed in ctx to decide when to succeed
    // try_number starts at 0, incremented on each retry
    const dag: DagDefinition = {
      id: 'eventual_success',
      schedule: null,
      tasks: {
        flaky: {
          retries: 3,
          retryDelay: 0,
          // ctx.tryNumber is not in our current ctx — use a side-channel via XCom
          // Simpler: just fail twice, succeed on 3rd by checking a thrown count via XCom
          run: async (ctx) => {
            const prev = (await ctx.xcom.pull('flaky', 'count') as number | undefined) ?? 0
            await ctx.xcom.push('count', prev + 1)
            if (prev < 2) throw new Error(`not yet (prev=${prev})`)
            // succeeds when prev >= 2 (3rd execution)
          },
        },
      },
    }
    register(dag)

    const runId = await createRun(db, dag)
    for (let i = 0; i < 6; i++) {
      await advanceRun(db, runId)
    }

    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'flaky' })
    expect(ti!.state).toBe('success')
    expect(ti!.try_number).toBe(2)  // retried twice before success
  }, 15000)

  it('stores max_retries from task definition', async () => {
    const dag: DagDefinition = {
      id: 'store_retries',
      schedule: null,
      tasks: {
        step: { retries: 5, retryDelay: 100, run: async () => {} },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'step' })
    expect(ti!.max_retries).toBe(5)
    expect(ti!.retry_delay).toBe(100)
  })

  it('tasks with retries:0 fail immediately without retrying', async () => {
    const dag: DagDefinition = {
      id: 'no_retry',
      schedule: null,
      tasks: {
        step: {
          retries: 0,
          run: async () => { throw new Error('instant fail') },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'step' })
    expect(ti!.state).toBe('failed')
    expect(ti!.try_number).toBe(0)   // never retried — stays at 0
  }, 10000)
})
