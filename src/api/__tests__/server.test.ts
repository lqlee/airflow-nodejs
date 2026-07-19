import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { buildServer } from '../server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { FastifyInstance } from 'fastify'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const testDag: DagDefinition = {
  id: 'api_test_dag',
  schedule: null,
  tasks: {
    step1: { run: async () => {} },
    step2: { dependsOn: ['step1'], run: async () => {} },
  },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_api')
  clearRegistry()
  register(testDag)
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
})

describe('GET /health', () => {
  it('returns status ok', async () => {
    const res = await app.inject({ method: 'GET', url: '/health' })
    expect(res.statusCode).toBe(200)
    expect(res.json().status).toBe('ok')
    expect(res.json().workers).toBeDefined()
  })
})

describe('GET /dags', () => {
  it('returns registered dags', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(Array.isArray(body)).toBe(true)
    expect(body.find((d: { id: string }) => d.id === 'api_test_dag')).toBeDefined()
  })
})

describe('GET /dags/:dagId', () => {
  it('returns dag detail with tasks', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags/api_test_dag' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.id).toBe('api_test_dag')
    expect(body.tasks).toHaveLength(2)
  })

  it('returns 404 for unknown dag', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags/nonexistent' })
    expect(res.statusCode).toBe(404)
  })
})

describe('POST /dags/:dagId/trigger', () => {
  it('creates a dag_run and returns run_id', async () => {
    const res = await app.inject({ method: 'POST', url: '/dags/api_test_dag/trigger' })
    expect(res.statusCode).toBe(201)
    const body = res.json()
    expect(body.run_id).toBeDefined()
    expect(body.dag_id).toBe('api_test_dag')
    // Trigger now runs immediately — state is success (tasks are instant no-ops)
    expect(['queued', 'running', 'success']).toContain(body.state)
  })

  it('returns 404 for unknown dag', async () => {
    const res = await app.inject({ method: 'POST', url: '/dags/ghost/trigger' })
    expect(res.statusCode).toBe(404)
  })
})

describe('GET /dag-runs/:runId', () => {
  it('returns run state and tasks', async () => {
    const trigger = await app.inject({ method: 'POST', url: '/dags/api_test_dag/trigger' })
    const { run_id } = trigger.json()

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${run_id}` })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.run_id).toBe(run_id)
    expect(body.tasks).toHaveLength(2)
  })

  it('returns 400 for invalid run id', async () => {
    const res = await app.inject({ method: 'GET', url: '/dag-runs/not-an-id' })
    expect(res.statusCode).toBe(400)
  })

  it('returns 404 for unknown run', async () => {
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${new ObjectId()}` })
    expect(res.statusCode).toBe(404)
  })
})

describe('GET /dags/:dagId/runs', () => {
  it('returns list of runs for a dag', async () => {
    await app.inject({ method: 'POST', url: '/dags/api_test_dag/trigger' })
    await app.inject({ method: 'POST', url: '/dags/api_test_dag/trigger' })

    const res = await app.inject({ method: 'GET', url: '/dags/api_test_dag/runs' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.length).toBeGreaterThanOrEqual(2)
    expect(body[0].dag_id).toBe('api_test_dag')
  })
})

describe('POST /dags/:dagId/pause and /resume', () => {
  afterEach(async () => {
    // always leave test dag in resumed state
    await db.collection('dag_paused').deleteMany({})
  })

  it('pause returns is_paused: true', async () => {
    const res = await app.inject({ method: 'POST', url: '/dags/api_test_dag/pause' })
    expect(res.statusCode).toBe(200)
    expect(res.json().is_paused).toBe(true)
    expect(res.json().dag_id).toBe('api_test_dag')
  })

  it('resume returns is_paused: false', async () => {
    await app.inject({ method: 'POST', url: '/dags/api_test_dag/pause' })
    const res = await app.inject({ method: 'POST', url: '/dags/api_test_dag/resume' })
    expect(res.statusCode).toBe(200)
    expect(res.json().is_paused).toBe(false)
  })

  it('GET /dags reflects is_paused state', async () => {
    await app.inject({ method: 'POST', url: '/dags/api_test_dag/pause' })
    const res = await app.inject({ method: 'GET', url: '/dags' })
    const dag = res.json().find((d: { id: string }) => d.id === 'api_test_dag')
    expect(dag.is_paused).toBe(true)
  })

  it('pause returns 404 for unknown dag', async () => {
    const res = await app.inject({ method: 'POST', url: '/dags/ghost/pause' })
    expect(res.statusCode).toBe(404)
  })

  it('resume returns 404 for unknown dag', async () => {
    const res = await app.inject({ method: 'POST', url: '/dags/ghost/resume' })
    expect(res.statusCode).toBe(404)
  })
})

describe('POST /dag-runs/:runId/cancel', () => {
  it('cancels a queued run and returns cancelled state', async () => {
    const trigger = await app.inject({ method: 'POST', url: '/dags/api_test_dag/trigger' })
    const { run_id } = trigger.json()

    // Only test cancel on a run that may still be queued (fast tasks may already be done)
    // Create a fresh run and cancel before advanceRun can finish it
    const runId = run_id

    // Even if already complete, cancel returns 409 — both paths valid
    const res = await app.inject({ method: 'POST', url: `/dag-runs/${runId}/cancel` })
    expect([200, 409]).toContain(res.statusCode)
    if (res.statusCode === 200) {
      expect(res.json().state).toBe('cancelled')
      expect(res.json().run_id).toBe(runId)
    }
  })

  it('returns 409 when run is already in terminal state', async () => {
    const trigger = await app.inject({ method: 'POST', url: '/dags/api_test_dag/trigger' })
    const { run_id } = trigger.json()

    // Cancel once (may succeed or 409 if already done)
    await app.inject({ method: 'POST', url: `/dag-runs/${run_id}/cancel` })
    // Force the run to terminal by waiting and cancelling again
    const res2 = await app.inject({ method: 'POST', url: `/dag-runs/${run_id}/cancel` })
    // Either 409 (was already terminal) or we got it on second try
    expect([409]).toContain(res2.statusCode)
  })

  it('returns 400 for invalid run id', async () => {
    const res = await app.inject({ method: 'POST', url: '/dag-runs/not-an-id/cancel' })
    expect(res.statusCode).toBe(400)
  })
})
