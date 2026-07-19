import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { pauseDag, resumeDag, isDagPaused, getPausedDagIds } from '../pause.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_pause')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_paused').deleteMany({})
})

describe('isDagPaused', () => {
  it('returns false for a dag with no record', async () => {
    expect(await isDagPaused(db, 'unknown_dag')).toBe(false)
  })

  it('returns true after pauseDag', async () => {
    await pauseDag(db, 'my_dag')
    expect(await isDagPaused(db, 'my_dag')).toBe(true)
  })

  it('returns false after resumeDag', async () => {
    await pauseDag(db, 'my_dag')
    await resumeDag(db, 'my_dag')
    expect(await isDagPaused(db, 'my_dag')).toBe(false)
  })

  it('pause is idempotent — calling twice stays paused', async () => {
    await pauseDag(db, 'my_dag')
    await pauseDag(db, 'my_dag')
    expect(await isDagPaused(db, 'my_dag')).toBe(true)
  })

  it('resume is idempotent — calling twice stays resumed', async () => {
    await pauseDag(db, 'my_dag')
    await resumeDag(db, 'my_dag')
    await resumeDag(db, 'my_dag')
    expect(await isDagPaused(db, 'my_dag')).toBe(false)
  })
})

describe('getPausedDagIds', () => {
  it('returns empty set when nothing is paused', async () => {
    const set = await getPausedDagIds(db)
    expect(set.size).toBe(0)
  })

  it('returns only paused dags', async () => {
    await pauseDag(db, 'dag_a')
    await pauseDag(db, 'dag_b')
    await pauseDag(db, 'dag_c')
    await resumeDag(db, 'dag_b')  // resumed — should not appear

    const set = await getPausedDagIds(db)
    expect(set.has('dag_a')).toBe(true)
    expect(set.has('dag_b')).toBe(false)
    expect(set.has('dag_c')).toBe(true)
    expect(set.size).toBe(2)
  })

  it('pause/resume cycle does not leave stale entries', async () => {
    await pauseDag(db, 'dag_x')
    await resumeDag(db, 'dag_x')
    const set = await getPausedDagIds(db)
    expect(set.has('dag_x')).toBe(false)
  })
})

describe('pause/resume API route behaviour (via server)', () => {
  it('does not affect other dags', async () => {
    await pauseDag(db, 'dag_1')
    // dag_2 untouched
    expect(await isDagPaused(db, 'dag_2')).toBe(false)
  })
})
