import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { claimNextTask } from '../claim.js'
import type { TaskInstance } from '../runs.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
const RUN_ID = 'test-run-claim'

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_claim')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('task_instances').deleteMany({})
})

const makeTask = (taskId: string, state = 'queued', dependsOn: string[] = []): TaskInstance => ({
  dag_run_id: RUN_ID,
  dag_id: 'test_dag',
  task_id: taskId,
  state: state as TaskInstance['state'],
  depends_on: dependsOn,
  try_number: 0,
  started_at: null,
  ended_at: null,
  error: null,
  created_at: new Date(),
})

describe('claimNextTask', () => {
  it('returns null when no tasks are queued', async () => {
    const result = await claimNextTask(db, RUN_ID)
    expect(result).toBeNull()
  })

  it('claims a task with no dependencies immediately', async () => {
    await db.collection('task_instances').insertOne(makeTask('extract'))
    const claimed = await claimNextTask(db, RUN_ID)
    expect(claimed).not.toBeNull()
    expect(claimed!.task_id).toBe('extract')
    expect(claimed!.state).toBe('running')
  })

  it('does not claim a task whose dependency is not yet done', async () => {
    await db.collection('task_instances').insertMany([
      makeTask('extract', 'queued'),
      makeTask('transform', 'queued', ['extract']),
    ])
    // Only extract has no deps — claim it
    const first = await claimNextTask(db, RUN_ID)
    expect(first!.task_id).toBe('extract')

    // transform still waiting — nothing else claimable
    const second = await claimNextTask(db, RUN_ID)
    expect(second).toBeNull()
  })

  it('claims downstream task once upstream is success', async () => {
    await db.collection('task_instances').insertMany([
      makeTask('extract', 'success'),
      makeTask('transform', 'queued', ['extract']),
    ])
    const claimed = await claimNextTask(db, RUN_ID)
    expect(claimed!.task_id).toBe('transform')
  })

  it('sets started_at when claiming', async () => {
    await db.collection('task_instances').insertOne(makeTask('extract'))
    const claimed = await claimNextTask(db, RUN_ID)
    expect(claimed!.started_at).toBeInstanceOf(Date)
  })
})
