import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { appendLog, getTaskLogs } from '../index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

const RUN_ID = 'test-run-logs'
const DAG_ID = 'test_dag'

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_logs')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('task_logs').deleteMany({})
})

describe('appendLog', () => {
  it('inserts a stdout log line', async () => {
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stdout', 'hello world')
    const docs = await db.collection('task_logs').find({ dag_run_id: RUN_ID }).toArray()
    expect(docs).toHaveLength(1)
    expect(docs[0].line).toBe('hello world')
    expect(docs[0].stream).toBe('stdout')
    expect(docs[0].task_id).toBe('task_a')
  })

  it('inserts a stderr log line', async () => {
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stderr', 'oh no')
    const docs = await db.collection('task_logs').find({ dag_run_id: RUN_ID }).toArray()
    expect(docs[0].stream).toBe('stderr')
    expect(docs[0].line).toBe('oh no')
  })

  it('sets a Date timestamp', async () => {
    const before = new Date()
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stdout', 'timestamped')
    const after = new Date()
    const docs = await db.collection('task_logs').find({ dag_run_id: RUN_ID }).toArray()
    expect(docs[0].ts.getTime()).toBeGreaterThanOrEqual(before.getTime())
    expect(docs[0].ts.getTime()).toBeLessThanOrEqual(after.getTime())
  })
})

describe('getTaskLogs', () => {
  it('returns empty array when no logs exist', async () => {
    const logs = await getTaskLogs(db, RUN_ID, 'task_x')
    expect(logs).toHaveLength(0)
  })

  it('returns only logs for the specified task', async () => {
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stdout', 'from a')
    await appendLog(db, RUN_ID, DAG_ID, 'task_b', 'stdout', 'from b')

    const logsA = await getTaskLogs(db, RUN_ID, 'task_a')
    expect(logsA).toHaveLength(1)
    expect(logsA[0].line).toBe('from a')

    const logsB = await getTaskLogs(db, RUN_ID, 'task_b')
    expect(logsB).toHaveLength(1)
    expect(logsB[0].line).toBe('from b')
  })

  it('returns multiple lines in insertion order', async () => {
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stdout', 'line 1')
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stdout', 'line 2')
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stderr', 'line 3')

    const logs = await getTaskLogs(db, RUN_ID, 'task_a')
    expect(logs).toHaveLength(3)
    expect(logs[0].line).toBe('line 1')
    expect(logs[1].line).toBe('line 2')
    expect(logs[2].line).toBe('line 3')
  })

  it('does not return logs from a different run_id', async () => {
    await appendLog(db, 'other-run', DAG_ID, 'task_a', 'stdout', 'other run')
    const logs = await getTaskLogs(db, RUN_ID, 'task_a')
    expect(logs).toHaveLength(0)
  })

  it('returns mixed stdout/stderr streams with correct labels', async () => {
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stdout', 'info')
    await appendLog(db, RUN_ID, DAG_ID, 'task_a', 'stderr', 'error')

    const logs = await getTaskLogs(db, RUN_ID, 'task_a')
    const streams = logs.map(l => l.stream)
    expect(streams).toContain('stdout')
    expect(streams).toContain('stderr')
  })
})
