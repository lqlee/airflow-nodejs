/**
 * up_for_retry, up_for_reschedule and removed — end to end through advanceRun / claimReadyTasks,
 * plus the guard that drops an executor's late result once the instance's state changed.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun, type TaskInstance } from '../runs.js'
import { advanceRun, cancelRun } from '../index.js'
import { claimReadyTasks } from '../claim.js'
import { markSuccess, markFailed } from '../ti-writes.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_new_states')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'xcoms', 'claim_locks']) await db.collection(c).deleteMany({})
})

const ti = (runId: string, taskId: string) =>
  db.collection<TaskInstance>('task_instances').findOne({ dag_run_id: runId, task_id: taskId }) as Promise<TaskInstance>
const runState = async (runId: string) => (await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) }))!.state

describe('up_for_retry', () => {
  const failing: DagDefinition = {
    id: 'ufr_dag', schedule: null,
    tasks: { flaky: { retries: 1, retryDelay: 60_000, run: async () => { throw new Error('nope') } } },
  }

  it('a failed try waits in up_for_retry until next_retry_at, keeping the run open', async () => {
    register(failing)
    const runId = await createRun(db, failing)
    const before = Date.now()
    await advanceRun(db, runId)

    const t = await ti(runId, 'flaky')
    expect(t).toMatchObject({ state: 'up_for_retry', try_number: 1, error: 'nope', claimed_by: null })
    expect(new Date(t.next_retry_at!).getTime()).toBeGreaterThanOrEqual(before + 60_000)
    expect(await runState(runId)).toBe('running')

    // Not due yet → nothing to claim; due → any process may claim the next try
    expect(await claimReadyTasks(db, runId, { now: new Date(before + 30_000) })).toHaveLength(0)
    const [claimed] = await claimReadyTasks(db, runId, { now: new Date(t.next_retry_at!.getTime() + 1) })
    expect(claimed).toMatchObject({ task_id: 'flaky', state: 'running', try_number: 1, ended_at: null })
  }, 20_000)

  it('cancelling the run cancels a task waiting to retry', async () => {
    register(failing)
    const runId = await createRun(db, failing)
    await advanceRun(db, runId)
    expect((await ti(runId, 'flaky')).state).toBe('up_for_retry')

    expect(await cancelRun(db, runId)).toBe(true)
    expect((await ti(runId, 'flaky')).state).toBe('cancelled')
  }, 20_000)

  it('without a delay the retry runs in the same pass and the run finishes', async () => {
    const dag: DagDefinition = {
      id: 'ufr_now', schedule: null,
      tasks: { flaky: { retries: 1, run: async () => { throw new Error('x') } } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    expect(await ti(runId, 'flaky')).toMatchObject({ state: 'failed', try_number: 1 })
    expect(await db.collection('task_instance_tries').countDocuments({ dag_run_id: runId })).toBe(2)
    expect(await runState(runId)).toBe('failed')
  }, 20_000)
})

describe('up_for_reschedule', () => {
  it('a sensor whose poke returns false is parked unclaimed until its next poke', async () => {
    const dag: DagDefinition = {
      id: 'ufrs_dag', schedule: null,
      tasks: { wait: { poke: async () => false, pokeInterval: 60_000 } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const t = await ti(runId, 'wait')
    expect(t).toMatchObject({ state: 'up_for_reschedule', claimed_by: null, poke_count: 1, try_number: 0 })
    expect(await claimReadyTasks(db, runId)).toHaveLength(0)
    const [claimed] = await claimReadyTasks(db, runId, { now: new Date(t.next_poke_at!.getTime() + 1) })
    expect(claimed.state).toBe('running')
  }, 20_000)
})

describe('removed', () => {
  const v1: DagDefinition = {
    id: 'rm_dag', schedule: null,
    tasks: {
      a: { run: async () => {} },
      gone: { run: async () => {} },
      after: { dependsOn: ['gone'], run: async () => {} },
    },
  }

  it('instances of a task deleted from the Dag become removed; downstream tasks ignore it and the run succeeds', async () => {
    register(v1)
    const runId = await createRun(db, v1)
    register({ ...v1, tasks: { a: v1.tasks.a, after: v1.tasks.after } })   // new Dag version without 'gone'

    await advanceRun(db, runId)
    await advanceRun(db, runId)
    expect((await ti(runId, 'gone')).state).toBe('removed')
    expect((await ti(runId, 'after')).state).toBe('success')
    expect(await runState(runId)).toBe('success')
  }, 20_000)

  it('a removed task that comes back is queued again while the run is active', async () => {
    register(v1)
    const runId = await createRun(db, v1)
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 'gone' }, { $set: { state: 'removed' } })
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 'a' }, { $set: { state: 'running' } })   // keeps the run open

    await advanceRun(db, runId)
    expect((await ti(runId, 'gone')).state).not.toBe('removed')
  }, 20_000)

  it('leaves running and finished instances of a deleted task alone', async () => {
    register(v1)
    const runId = await createRun(db, v1)
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 'gone' }, { $set: { state: 'running', claimed_by: 'other' } })
    register({ ...v1, tasks: { a: v1.tasks.a } })

    await advanceRun(db, runId)
    expect((await ti(runId, 'gone')).state).toBe('running')
    expect((await ti(runId, 'after')).state).toBe('removed')
  }, 20_000)
})

describe('late executor results', () => {
  it('are dropped once the instance left running (cancelled, marked, reaped)', async () => {
    const dag: DagDefinition = { id: 'late_dag', schedule: null, tasks: { t: { run: async () => {} }, u: { run: async () => {} } } }
    const runId = await createRun(db, dag)
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 't' }, { $set: { state: 'cancelled' } })
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 'u' }, { $set: { state: 'up_for_retry' } })

    await markSuccess(db, { dag_run_id: runId, task_id: 't', map_index: null })
    await markFailed(db, { dag_run_id: runId, task_id: 'u', map_index: null }, 'late')
    expect((await ti(runId, 't')).state).toBe('cancelled')
    expect((await ti(runId, 'u')).state).toBe('up_for_retry')
  })

  it('land while the instance is running or deferred', async () => {
    const dag: DagDefinition = { id: 'ontime_dag', schedule: null, tasks: { t: { run: async () => {} }, d: { run: async () => {} } } }
    const runId = await createRun(db, dag)
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 't' }, { $set: { state: 'running' } })
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 'd' }, { $set: { state: 'deferred' } })

    await markSuccess(db, { dag_run_id: runId, task_id: 't', map_index: null })
    await markFailed(db, { dag_run_id: runId, task_id: 'd', map_index: null }, 'trigger failed')
    expect((await ti(runId, 't')).state).toBe('success')
    expect(await ti(runId, 'd')).toMatchObject({ state: 'failed', error: 'trigger failed' })
  })
})

describe('claim capacity', () => {
  it('claims at most `limit` instances, highest priority first', async () => {
    const dag: DagDefinition = {
      id: 'limit_dag', schedule: null,
      tasks: { low: { priority: 1, run: async () => {} }, high: { priority: 9, run: async () => {} }, mid: { priority: 5, run: async () => {} } },
    }
    const runId = await createRun(db, dag)
    expect((await claimReadyTasks(db, runId, { limit: 2 })).map(t => t.task_id)).toEqual(['high', 'mid'])
    expect(await claimReadyTasks(db, runId, { limit: 0 })).toEqual([])
    expect((await claimReadyTasks(db, runId)).map(t => t.task_id)).toEqual(['low'])
  })
})
