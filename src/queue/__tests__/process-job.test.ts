/**
 * processJob (the BullMQ worker's job handler) records each try in task_instance_tries, like the
 * local executor — try history, its trace span and the UI's try list depend on it.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import type { Job } from 'bullmq'
import type { TaskJobData } from '../producer.js'
import type { TaskInstance } from '../../scheduler/runs.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME = 'airflow_test_process_job'
let client: MongoClient
let db: Db
let processJob: (job: Job<TaskJobData>) => Promise<void>

beforeAll(async () => {
  process.env.DB_NAME = DB_NAME   // read by consumer.ts when it loads
  ;({ processJob } = await import('../consumer.js'))
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(DB_NAME)
})
afterAll(async () => { await db.dropDatabase(); await client.close(); delete process.env.DB_NAME })
afterEach(async () => { for (const c of ['task_instances', 'task_instance_tries', 'xcoms']) await db.collection(c).deleteMany({}) })

async function runJob(fn: string, extra: Partial<TaskInstance> = {}) {
  const ti = {
    dag_run_id: new ObjectId().toString(), dag_id: 'd', task_id: 't', map_index: null, state: 'running', try_number: 0, max_retries: 0,
    claimed_by: 'scheduler-a', started_at: new Date('2026-03-02T06:00:00Z'), ...extra,
  } as TaskInstance
  await db.collection('task_instances').insertOne({ ...ti })
  // BullMQ hands the job over as JSON: dates arrive as strings
  await processJob({ id: '1', data: JSON.parse(JSON.stringify({ ti, fn })) } as Job<TaskJobData>)
  return db.collection('task_instance_tries').find({}).toArray()
}

describe('processJob try history', () => {
  it('records a successful try with its start time', async () => {
    const tries = await runJob('async () => {}')
    expect(tries).toHaveLength(1)
    expect(tries[0]).toMatchObject({ task_id: 't', try_number: 0, state: 'success', error: null, started_at: new Date('2026-03-02T06:00:00Z') })
  })

  it('records a failed try with its error', async () => {
    const tries = await runJob('async () => { throw new Error("disk full") }')
    expect(tries).toHaveLength(1)
    expect(tries[0]).toMatchObject({ state: 'failed', error: expect.stringContaining('disk full') })
  })
})
