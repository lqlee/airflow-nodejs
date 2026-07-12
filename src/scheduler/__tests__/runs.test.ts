import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME = 'airflow_test'

let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(DB_NAME)
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
})

const testDag: DagDefinition = {
  id: 'test_dag',
  schedule: null,
  tasks: {
    extract: { run: async () => {} },
    transform: { dependsOn: ['extract'], run: async () => {} },
    load: { dependsOn: ['transform'], run: async () => {} },
  },
}

describe('createRun', () => {
  it('inserts a dag_run with state queued', async () => {
    const runId = await createRun(db, testDag)
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run).not.toBeNull()
    expect(run!.dag_id).toBe('test_dag')
    expect(run!.state).toBe('queued')
  })

  it('inserts one task_instance per task', async () => {
    const runId = await createRun(db, testDag)
    const tasks = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    expect(tasks).toHaveLength(3)
    const taskIds = tasks.map(t => t.task_id)
    expect(taskIds).toContain('extract')
    expect(taskIds).toContain('transform')
    expect(taskIds).toContain('load')
  })

  it('all task_instances start with state queued', async () => {
    const runId = await createRun(db, testDag)
    const tasks = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    expect(tasks.every(t => t.state === 'queued')).toBe(true)
  })

  it('preserves depends_on for each task', async () => {
    const runId = await createRun(db, testDag)
    const tasks = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    const transform = tasks.find(t => t.task_id === 'transform')
    const load = tasks.find(t => t.task_id === 'load')
    expect(transform!.depends_on).toContain('extract')
    expect(load!.depends_on).toContain('transform')
  })

  it('extract has no dependencies', async () => {
    const runId = await createRun(db, testDag)
    const tasks = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    const extract = tasks.find(t => t.task_id === 'extract')
    expect(extract!.depends_on).toHaveLength(0)
  })

  it('returns a valid ObjectId string as runId', async () => {
    const runId = await createRun(db, testDag)
    expect(() => new ObjectId(runId)).not.toThrow()
  })
})
