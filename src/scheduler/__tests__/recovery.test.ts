import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { recoverOrphanedRuns } from '../recovery.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_recovery')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
})

describe('recoverOrphanedRuns', () => {
  it('resets running task_instances to queued', async () => {
    await db.collection('task_instances').insertMany([
      { dag_run_id: 'run1', task_id: 'extract', state: 'running', started_at: new Date(), try_number: 0 },
      { dag_run_id: 'run1', task_id: 'transform', state: 'running', started_at: new Date(), try_number: 0 },
    ])

    await recoverOrphanedRuns(db)

    const tasks = await db.collection('task_instances').find({}).toArray()
    expect(tasks.every(t => t.state === 'queued')).toBe(true)
  })

  it('clears started_at when resetting tasks', async () => {
    await db.collection('task_instances').insertOne({
      dag_run_id: 'run1', task_id: 'extract', state: 'running', started_at: new Date(), try_number: 0,
    })

    await recoverOrphanedRuns(db)

    const task = await db.collection('task_instances').findOne({ task_id: 'extract' })
    expect(task!.started_at).toBeNull()
  })

  it('increments try_number on reset', async () => {
    await db.collection('task_instances').insertOne({
      dag_run_id: 'run1', task_id: 'extract', state: 'running', started_at: new Date(), try_number: 1,
    })

    await recoverOrphanedRuns(db)

    const task = await db.collection('task_instances').findOne({ task_id: 'extract' })
    expect(task!.try_number).toBe(2)
  })

  it('resets running dag_runs to queued', async () => {
    await db.collection('dag_runs').insertMany([
      { dag_id: 'my_dag', state: 'running' },
      { dag_id: 'my_dag', state: 'running' },
    ])

    await recoverOrphanedRuns(db)

    const runs = await db.collection('dag_runs').find({}).toArray()
    expect(runs.every(r => r.state === 'queued')).toBe(true)
  })

  it('does not touch already queued or completed tasks', async () => {
    await db.collection('task_instances').insertMany([
      { dag_run_id: 'run1', task_id: 'a', state: 'queued', try_number: 0 },
      { dag_run_id: 'run1', task_id: 'b', state: 'success', try_number: 1 },
      { dag_run_id: 'run1', task_id: 'c', state: 'failed', try_number: 2 },
    ])

    await recoverOrphanedRuns(db)

    const tasks = await db.collection('task_instances').find({}).toArray()
    const byId = Object.fromEntries(tasks.map(t => [t.task_id, t]))
    expect(byId['a'].try_number).toBe(0)   // unchanged
    expect(byId['b'].state).toBe('success') // unchanged
    expect(byId['c'].state).toBe('failed')  // unchanged
  })

  it('does nothing when no orphaned runs exist', async () => {
    // Should not throw on empty collections
    await expect(recoverOrphanedRuns(db)).resolves.not.toThrow()
  })
})
