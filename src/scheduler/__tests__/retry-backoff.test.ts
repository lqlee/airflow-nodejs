import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { computeRetryDelay, scheduleRetry } from '../executor.js'
import { createRun, type TaskInstance } from '../runs.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_retry_backoff')
  clearRegistry()
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  clearRegistry()
})

describe('computeRetryDelay', () => {
  const d = (retry_delay: number, retry_backoff: number, max_retry_delay: number, try_number: number) =>
    computeRetryDelay({ retry_delay, retry_backoff, max_retry_delay, try_number })

  it('fixed delay when no multiplier', () => {
    expect([0, 1, 2].map(n => d(1000, 0, 0, n))).toEqual([1000, 1000, 1000])
  })
  it('multiplier <= 1 means fixed delay', () => {
    expect(d(1000, 1, 0, 3)).toBe(1000)
  })
  it('grows geometrically from retry_delay', () => {
    expect([0, 1, 2, 3].map(n => d(1000, 2, 0, n))).toEqual([1000, 2000, 4000, 8000])
  })
  it('supports fractional multipliers', () => {
    expect(d(1000, 1.5, 0, 2)).toBe(2250)
  })
  it('is capped by max_retry_delay', () => {
    expect([0, 1, 2, 3].map(n => d(1000, 2, 3000, n))).toEqual([1000, 2000, 3000, 3000])
  })
  it('caps a fixed delay too', () => {
    expect(d(5000, 0, 2000, 0)).toBe(2000)
  })
  it('legacy task docs without the new fields keep the fixed delay', () => {
    expect(computeRetryDelay({ retry_delay: 750, try_number: 4 } as TaskInstance)).toBe(750)
  })
})

describe('createRun stamps backoff fields', () => {
  it('copies retryExponentialBackoff / maxRetryDelay onto the task instance', async () => {
    const dag: DagDefinition = {
      id: 'backoff_dag', schedule: null,
      tasks: { t: { retries: 3, retryDelay: 100, retryExponentialBackoff: 2, maxRetryDelay: 1000, run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId })
    expect(ti).toMatchObject({ retry_delay: 100, retry_backoff: 2, max_retry_delay: 1000 })
  })
})

describe('scheduleRetry', () => {
  async function seed(state: string) {
    const dag: DagDefinition = { id: 'sr_dag', schedule: null, tasks: { t: { retries: 2, run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    await db.collection('task_instances').updateOne({ dag_run_id: runId }, { $set: { state } })
    return (await db.collection<TaskInstance>('task_instances').findOne({ dag_run_id: runId }))!
  }

  it('requeues a running task and bumps try_number', async () => {
    const ti = await seed('running')
    await scheduleRetry(db, ti, 'boom')
    const after = await db.collection('task_instances').findOne({ dag_run_id: ti.dag_run_id })
    expect(after).toMatchObject({ state: 'queued', try_number: 1, error: 'boom' })
  })

  it('does not resurrect a task that was failed meanwhile (e.g. run timed out)', async () => {
    const ti = await seed('failed')
    await scheduleRetry(db, ti, 'boom')
    const after = await db.collection('task_instances').findOne({ dag_run_id: ti.dag_run_id })
    expect(after).toMatchObject({ state: 'failed', try_number: 0 })
  })
})
