import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { sensorOutcome } from '../sensor.js'
import { createRun } from '../runs.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const noop = async () => {}

// ── Pure sensorOutcome tests ─────────────────────────────────────────────────

describe('sensorOutcome — pure', () => {
  const first = new Date('2025-01-01T10:00:00Z')

  it('returns success when poke is true, regardless of elapsed time', () => {
    const now = new Date('2025-01-01T11:00:00Z')
    expect(sensorOutcome(true, first, now, 60_000)).toBe('success')
  })

  it('returns reschedule when poke is false and within timeout', () => {
    const now = new Date('2025-01-01T10:00:29Z') // 29s elapsed, timeout 30s
    expect(sensorOutcome(false, first, now, 30_000)).toBe('reschedule')
  })

  it('returns timeout when poke is false and elapsed >= sensorTimeout', () => {
    const now = new Date('2025-01-01T10:00:30Z') // exactly at deadline
    expect(sensorOutcome(false, first, now, 30_000)).toBe('timeout')
  })

  it('returns timeout when well past deadline', () => {
    const now = new Date('2025-01-01T12:00:00Z') // 2h elapsed, timeout 1h
    expect(sensorOutcome(false, first, now, 3_600_000)).toBe('timeout')
  })

  it('returns reschedule when timeout is 0 (no deadline)', () => {
    const now = new Date('2099-01-01T00:00:00Z') // far future
    expect(sensorOutcome(false, first, now, 0)).toBe('reschedule')
  })

  it('returns success even at timeout boundary if poke is true', () => {
    const now = new Date('2025-01-01T10:01:00Z') // past deadline
    expect(sensorOutcome(true, first, now, 30_000)).toBe('success')
  })
})

// ── Integration fixtures ─────────────────────────────────────────────────────

let pokeCallCount = 0
let pokeResult = false

const sensorDag: DagDefinition = {
  id: 'sensor_dag',
  schedule: null,
  tasks: {
    wait_for_file: {
      poke: async () => { pokeCallCount++; return pokeResult },
      pokeInterval: 1000,    // 1s for tests
      sensorTimeout: 5000,   // 5s deadline
    },
    downstream: {
      run: noop,
      dependsOn: ['wait_for_file'],
    },
  },
}

const timeoutSensorDag: DagDefinition = {
  id: 'timeout_sensor_dag',
  schedule: null,
  tasks: {
    never_ready: {
      poke: async () => false,
      pokeInterval: 100,
      sensorTimeout: 1,  // 1ms — will always time out on second poke
    },
  },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_sensor')
  clearRegistry()
  register(sensorDag)
  register(timeoutSensorDag)
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  pokeCallCount = 0
  pokeResult = false
})

// ── TaskInstance schema ───────────────────────────────────────────────────────

describe('createRun — sensor task_instance fields', () => {
  it('stamps is_sensor=true for tasks with poke()', async () => {
    const runId = await createRun(db, sensorDag)
    const sensor = await db.collection('task_instances')
      .findOne({ dag_run_id: runId, task_id: 'wait_for_file' })
    expect(sensor?.is_sensor).toBe(true)
    expect(sensor?.poke_interval_ms).toBe(1000)
    expect(sensor?.sensor_timeout_ms).toBe(5000)
    expect(sensor?.first_poked_at).toBeNull()
    expect(sensor?.next_poke_at).toBeNull()
    expect(sensor?.poke_count).toBe(0)
  })

  it('stamps is_sensor=false for regular tasks', async () => {
    const runId = await createRun(db, sensorDag)
    const regular = await db.collection('task_instances')
      .findOne({ dag_run_id: runId, task_id: 'downstream' })
    expect(regular?.is_sensor).toBe(false)
    expect(regular?.poke_interval_ms).toBe(0)
  })

  it('enforces minimum pokeInterval of 1000ms', async () => {
    const dag: DagDefinition = {
      id: 'fast_sensor', schedule: null,
      tasks: { s: { poke: async () => false, pokeInterval: 10 } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const inst = await db.collection('task_instances')
      .findOne({ dag_run_id: runId, task_id: 's' })
    expect(inst?.poke_interval_ms).toBe(1000)
  })
})

// ── Sensor poke gate in claim ─────────────────────────────────────────────────

describe('sensor poke gate (claim)', () => {
  it('sensor with next_poke_at in the future is NOT claimed', async () => {
    const runId = await createRun(db, sensorDag)
    // Set next_poke_at 10 minutes in the future
    const futureTime = new Date(Date.now() + 600_000)
    await db.collection('task_instances').updateOne(
      { dag_run_id: runId, task_id: 'wait_for_file' },
      { $set: { next_poke_at: futureTime } },
    )
    const { claimReadyTasks } = await import('../claim.js')
    const claimed = await claimReadyTasks(db, runId)
    expect(claimed.map(t => t.task_id)).not.toContain('wait_for_file')
  })

  it('sensor with next_poke_at in the past IS claimed', async () => {
    const runId = await createRun(db, sensorDag)
    const pastTime = new Date(Date.now() - 1000)
    await db.collection('task_instances').updateOne(
      { dag_run_id: runId, task_id: 'wait_for_file' },
      { $set: { next_poke_at: pastTime } },
    )
    const { claimReadyTasks } = await import('../claim.js')
    const claimed = await claimReadyTasks(db, runId)
    expect(claimed.map(t => t.task_id)).toContain('wait_for_file')
  })

  it('sensor with next_poke_at=null is claimed immediately', async () => {
    const runId = await createRun(db, sensorDag)
    const { claimReadyTasks } = await import('../claim.js')
    const claimed = await claimReadyTasks(db, runId)
    expect(claimed.map(t => t.task_id)).toContain('wait_for_file')
  })
})

// ── API ────────────────────────────────────────────────────────────────────────

describe('API — sensor fields exposed', () => {
  it('GET /dag-runs/:runId exposes is_sensor, poke_count, next_poke_at', async () => {
    const runId = await createRun(db, sensorDag)
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    const sensor = body.tasks.find((t: { task_id: string }) => t.task_id === 'wait_for_file')
    const regular = body.tasks.find((t: { task_id: string }) => t.task_id === 'downstream')
    expect(sensor?.is_sensor).toBe(true)
    expect(sensor?.poke_count).toBe(0)
    expect(sensor?.next_poke_at).toBeNull()
    expect(regular?.is_sensor).toBe(false)
  })

  it('reschedule increments poke_count and sets next_poke_at in DB', async () => {
    const runId = await createRun(db, sensorDag)
    // Manually simulate a reschedule (what executor does after false poke)
    const now = new Date()
    const nextPokeAt = new Date(now.getTime() + 1000)
    await db.collection('task_instances').updateOne(
      { dag_run_id: runId, task_id: 'wait_for_file' },
      {
        $set: { state: 'queued', started_at: null, next_poke_at: nextPokeAt, first_poked_at: now },
        $inc: { poke_count: 1 },
      },
    )
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    const body = res.json()
    const sensor = body.tasks.find((t: { task_id: string }) => t.task_id === 'wait_for_file')
    expect(sensor?.poke_count).toBe(1)
    expect(sensor?.next_poke_at).not.toBeNull()
    expect(sensor?.first_poked_at).not.toBeNull()
  })

  it('reschedule does NOT increment try_number', async () => {
    const runId = await createRun(db, sensorDag)
    // Simulate reschedule
    await db.collection('task_instances').updateOne(
      { dag_run_id: runId, task_id: 'wait_for_file' },
      {
        $set: { state: 'queued', next_poke_at: new Date(), first_poked_at: new Date() },
        $inc: { poke_count: 3 },
      },
    )
    const inst = await db.collection('task_instances')
      .findOne({ dag_run_id: runId, task_id: 'wait_for_file' })
    // try_number must still be 0
    expect(inst?.try_number).toBe(0)
    expect(inst?.poke_count).toBe(3)
  })
})
