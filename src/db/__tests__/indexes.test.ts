import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { ensureIndexes } from '../indexes.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_indexes_upgrade')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })

// Mongo-only: on Spanner the constraint is DDL (see spanner/schema.ts), so duplicates can't be seeded
describe.skipIf(process.env.DB_BACKEND === 'spanner')('ensureIndexes on an existing database', () => {
  it('starts even when old runs already violate the new unique (dag_id, logical_date) index', async () => {
    const date = new Date('2026-01-01T00:00:00Z')
    await db.collection('dag_runs').insertMany([
      { dag_id: 'd', logical_date: date, state: 'success' },
      { dag_id: 'd', logical_date: date, state: 'success' },   // created by an older version
    ])

    await expect(ensureIndexes(db)).resolves.toBeUndefined()

    const names = (await db.collection('dag_runs').indexes()).map(i => i.name)
    expect(names).not.toContain('dag_runs_dag_id_logical_date_uq')
    expect(names).toContain('dag_id_1_state_1')   // the rest of the indexes were still created
  })

  it('once the duplicates are gone, the constraint is created and enforced', async () => {
    await db.collection('dag_runs').deleteMany({})
    await ensureIndexes(db)
    const date = new Date('2026-01-02T00:00:00Z')
    await db.collection('dag_runs').insertOne({ dag_id: 'd', logical_date: date })
    await expect(db.collection('dag_runs').insertOne({ dag_id: 'd', logical_date: date })).rejects.toMatchObject({ code: 11000 })
    // runs without a logical date are exempt
    await db.collection('dag_runs').insertMany([{ dag_id: 'd', logical_date: null }, { dag_id: 'd', logical_date: null }])
  })
})
