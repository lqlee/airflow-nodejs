import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { cancelRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

const testDag: DagDefinition = {
  id: 'cancel_test_dag',
  schedule: null,
  tasks: {
    step1: { run: async () => {} },
    step2: { dependsOn: ['step1'], run: async () => {} },
  },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_cancel')
  clearRegistry()
  register(testDag)
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
})

describe('cancelRun', () => {
  it('cancels a queued run and returns true', async () => {
    const runId = await createRun(db, testDag)
    const result = await cancelRun(db, runId)
    expect(result).toBe(true)
  })

  it('marks dag_run state as cancelled', async () => {
    const runId = await createRun(db, testDag)
    await cancelRun(db, runId)
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run!.state).toBe('cancelled')
    expect(run!.ended_at).toBeDefined()
  })

  it('marks all queued tasks as cancelled', async () => {
    const runId = await createRun(db, testDag)
    await cancelRun(db, runId)
    const tasks = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    for (const t of tasks) {
      expect(t.state).toBe('cancelled')
      expect(t.error).toBe('Cancelled by user')
      expect(t.ended_at).toBeDefined()
    }
  })

  it('returns false when run is already cancelled', async () => {
    const runId = await createRun(db, testDag)
    await cancelRun(db, runId)
    const result = await cancelRun(db, runId)
    expect(result).toBe(false)
  })

  it('returns false when run does not exist', async () => {
    const fakeId = new ObjectId().toString()
    const result = await cancelRun(db, fakeId)
    expect(result).toBe(false)
  })

  it('does not affect already-successful tasks', async () => {
    const runId = await createRun(db, testDag)
    // Manually mark step1 as success before cancelling
    await db.collection('task_instances').updateOne(
      { dag_run_id: runId, task_id: 'step1' },
      { $set: { state: 'success', ended_at: new Date() } }
    )
    await cancelRun(db, runId)
    const step1 = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'step1' })
    expect(step1!.state).toBe('success')  // preserved
    const step2 = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'step2' })
    expect(step2!.state).toBe('cancelled')  // cancelled
  })
})
