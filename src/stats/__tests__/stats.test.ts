import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { getDagStats } from '../index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_stats')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
})

function run(dagId: string, state: string, durationMs?: number) {
  const created_at = new Date(Date.now() - (durationMs ?? 0) - 5000)
  const ended_at = durationMs != null ? new Date(created_at.getTime() + durationMs) : undefined
  return { dag_id: dagId, state, created_at, ...(ended_at ? { ended_at } : {}) }
}

describe('getDagStats', () => {
  it('returns zero nulls when no runs exist', async () => {
    const stats = await getDagStats(db, 'empty_dag')
    expect(stats.total_runs).toBe(0)
    expect(stats.success_count).toBe(0)
    expect(stats.failed_count).toBe(0)
    expect(stats.success_rate).toBeNull()
    expect(stats.avg_duration_ms).toBeNull()
    expect(stats.p95_duration_ms).toBeNull()
    expect(stats.min_duration_ms).toBeNull()
    expect(stats.max_duration_ms).toBeNull()
  })

  it('counts states correctly', async () => {
    await db.collection('dag_runs').insertMany([
      run('dag1', 'success', 1000),
      run('dag1', 'success', 2000),
      run('dag1', 'failed', 500),
      run('dag1', 'cancelled'),
    ])
    const stats = await getDagStats(db, 'dag1')
    expect(stats.total_runs).toBe(4)
    expect(stats.success_count).toBe(2)
    expect(stats.failed_count).toBe(1)
    expect(stats.cancelled_count).toBe(1)
  })

  it('success_rate = success / terminal (excludes running)', async () => {
    // 2 success, 1 failed, 1 running (not terminal)
    await db.collection('dag_runs').insertMany([
      run('dag2', 'success', 1000),
      run('dag2', 'success', 2000),
      run('dag2', 'failed', 500),
      run('dag2', 'running'),
    ])
    const stats = await getDagStats(db, 'dag2')
    // success_rate = 2 / (2+1+0) = 0.666...
    expect(stats.success_rate).toBeCloseTo(2 / 3)
  })

  it('success_rate is 1.0 when all runs succeeded', async () => {
    await db.collection('dag_runs').insertMany([
      run('dag3', 'success', 1000),
      run('dag3', 'success', 2000),
    ])
    const stats = await getDagStats(db, 'dag3')
    expect(stats.success_rate).toBe(1)
  })

  it('success_rate is null when only running (no terminal) runs', async () => {
    await db.collection('dag_runs').insertMany([
      run('dag4', 'running'),
      run('dag4', 'queued'),
    ])
    const stats = await getDagStats(db, 'dag4')
    expect(stats.success_rate).toBeNull()
  })

  it('computes avg, min, max duration from ended runs only', async () => {
    await db.collection('dag_runs').insertMany([
      run('dag5', 'success', 1000),
      run('dag5', 'success', 3000),
      run('dag5', 'running'),   // no ended_at — excluded
    ])
    const stats = await getDagStats(db, 'dag5')
    expect(stats.avg_duration_ms).toBe(2000)
    expect(stats.min_duration_ms).toBe(1000)
    expect(stats.max_duration_ms).toBe(3000)
  })

  it('computes p95 correctly with 20 runs', async () => {
    // durations 100, 200, ..., 2000 ms (20 values sorted ascending)
    const docs = Array.from({ length: 20 }, (_, i) =>
      run('dag6', 'success', (i + 1) * 100),
    )
    await db.collection('dag_runs').insertMany(docs)
    const stats = await getDagStats(db, 'dag6')
    // p95 index = ceil(0.95 * 20) - 1 = ceil(19) - 1 = 18, value = 1900
    expect(stats.p95_duration_ms).toBe(1900)
  })

  it('p95 of a single run equals its duration', async () => {
    await db.collection('dag_runs').insertMany([run('dag7', 'success', 5000)])
    const stats = await getDagStats(db, 'dag7')
    expect(stats.p95_duration_ms).toBe(5000)
    expect(stats.avg_duration_ms).toBe(5000)
  })

  it('respects limit param — only last N runs counted', async () => {
    // Insert 10 runs for dag8
    const docs = Array.from({ length: 10 }, (_, i) =>
      run('dag8', i < 5 ? 'success' : 'failed', 1000),
    )
    await db.collection('dag_runs').insertMany(docs)
    // limit=5 — only sees the 5 most recent (sorted by created_at desc)
    const stats = await getDagStats(db, 'dag8', 5)
    expect(stats.total_runs).toBe(5)
  })

  it('only counts runs for the requested dag_id', async () => {
    await db.collection('dag_runs').insertMany([
      run('dag_a', 'success', 1000),
      run('dag_a', 'success', 2000),
      run('dag_b', 'failed', 500),
    ])
    const statsA = await getDagStats(db, 'dag_a')
    const statsB = await getDagStats(db, 'dag_b')
    expect(statsA.total_runs).toBe(2)
    expect(statsA.failed_count).toBe(0)
    expect(statsB.total_runs).toBe(1)
    expect(statsB.success_count).toBe(0)
  })

  it('cancelled runs are counted but excluded from success_rate numerator', async () => {
    await db.collection('dag_runs').insertMany([
      run('dag9', 'success', 1000),
      run('dag9', 'cancelled'),
    ])
    const stats = await getDagStats(db, 'dag9')
    expect(stats.cancelled_count).toBe(1)
    // success_rate = 1 / (1+0+1) = 0.5
    expect(stats.success_rate).toBeCloseTo(0.5)
  })
})
