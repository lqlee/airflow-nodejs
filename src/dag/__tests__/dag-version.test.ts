import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../../scheduler/runs.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../registry.js'
import type { DagDefinition } from '../types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const dagWithVersion: DagDefinition = {
  id: 'versioned_dag',
  schedule: null,
  version: 'abc123def456',
  tasks: { step: { run: async () => {} } },
}

const dagNoVersion: DagDefinition = {
  id: 'unversioned_dag',
  schedule: null,
  tasks: { step: { run: async () => {} } },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_dagversion')
  clearRegistry()
  register(dagWithVersion)
  register(dagNoVersion)
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

describe('Dag versioning — createRun', () => {
  it('stamps dag_version from DagDefinition onto the dag_run document', async () => {
    const runId = await createRun(db, dagWithVersion)
    const doc = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(doc?.dag_version).toBe('abc123def456')
  })

  it('stores null when dag has no version', async () => {
    const runId = await createRun(db, dagNoVersion)
    const doc = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(doc?.dag_version).toBeNull()
  })

  it('two runs from the same dag share the same version', async () => {
    const id1 = await createRun(db, dagWithVersion)
    const id2 = await createRun(db, dagWithVersion)
    const [r1, r2] = await Promise.all([
      db.collection('dag_runs').findOne({ _id: new ObjectId(id1) }),
      db.collection('dag_runs').findOne({ _id: new ObjectId(id2) }),
    ])
    expect(r1?.dag_version).toBe(r2?.dag_version)
  })

  it('different dag versions produce different run stamps', async () => {
    const dagV2: DagDefinition = { ...dagWithVersion, version: 'newversion123' }
    const id1 = await createRun(db, dagWithVersion)
    const id2 = await createRun(db, dagV2)
    const [r1, r2] = await Promise.all([
      db.collection('dag_runs').findOne({ _id: new ObjectId(id1) }),
      db.collection('dag_runs').findOne({ _id: new ObjectId(id2) }),
    ])
    expect(r1?.dag_version).not.toBe(r2?.dag_version)
  })
})

describe('Dag versioning — API', () => {
  it('GET /dag-runs/:runId exposes dag_version', async () => {
    const runId = await createRun(db, dagWithVersion)
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.dag_version).toBe('abc123def456')
  })

  it('GET /dag-runs/:runId returns null dag_version for unversioned dags', async () => {
    const runId = await createRun(db, dagNoVersion)
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    const body = res.json()
    expect(body.dag_version).toBeNull()
  })

  it('GET /dags/:dagId/runs includes dag_version in items', async () => {
    await createRun(db, dagWithVersion)
    const res = await app.inject({ method: 'GET', url: '/dags/versioned_dag/runs' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.items[0].dag_version).toBe('abc123def456')
  })
})
