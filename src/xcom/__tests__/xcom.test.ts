import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { xcomPush, xcomPull } from '../index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

const RUN_ID = 'test-run-xcom'
const DAG_ID = 'test_dag'

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_xcom')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('xcoms').deleteMany({})
})

describe('xcomPush + xcomPull', () => {
  it('pushes and pulls a value by key', async () => {
    await xcomPush(db, RUN_ID, DAG_ID, 'extract', 'result', { rows: 42 })
    const val = await xcomPull(db, RUN_ID, 'extract', 'result')
    expect(val).toEqual({ rows: 42 })
  })

  it('returns undefined for a missing key', async () => {
    const val = await xcomPull(db, RUN_ID, 'extract', 'nonexistent')
    expect(val).toBeUndefined()
  })

  it('returns undefined for a missing task', async () => {
    const val = await xcomPull(db, RUN_ID, 'ghost_task', 'result')
    expect(val).toBeUndefined()
  })

  it('overwrites when pushing same key twice', async () => {
    await xcomPush(db, RUN_ID, DAG_ID, 'extract', 'result', { rows: 1 })
    await xcomPush(db, RUN_ID, DAG_ID, 'extract', 'result', { rows: 99 })
    const val = await xcomPull(db, RUN_ID, 'extract', 'result')
    expect((val as { rows: number }).rows).toBe(99)
  })

  it('isolates xcoms by run_id', async () => {
    await xcomPush(db, 'run-A', DAG_ID, 'extract', 'result', 'from-A')
    await xcomPush(db, 'run-B', DAG_ID, 'extract', 'result', 'from-B')
    expect(await xcomPull(db, 'run-A', 'extract', 'result')).toBe('from-A')
    expect(await xcomPull(db, 'run-B', 'extract', 'result')).toBe('from-B')
  })

  it('supports different keys from the same task', async () => {
    await xcomPush(db, RUN_ID, DAG_ID, 'extract', 'count', 42)
    await xcomPush(db, RUN_ID, DAG_ID, 'extract', 'source', 'warehouse')
    expect(await xcomPull(db, RUN_ID, 'extract', 'count')).toBe(42)
    expect(await xcomPull(db, RUN_ID, 'extract', 'source')).toBe('warehouse')
  })

  it('supports complex nested values', async () => {
    const value = { rows: [{ id: 1, name: 'Alice' }, { id: 2, name: 'Bob' }], meta: { total: 2 } }
    await xcomPush(db, RUN_ID, DAG_ID, 'extract', 'dataset', value)
    const pulled = await xcomPull(db, RUN_ID, 'extract', 'dataset')
    expect(pulled).toEqual(value)
  })
})
