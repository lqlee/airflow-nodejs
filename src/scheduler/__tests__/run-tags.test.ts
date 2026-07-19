import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { buildServer } from '../../api/server.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const noop = async () => {}

const testDag: DagDefinition = {
  id: 'tags_test_dag',
  schedule: null,
  tasks: { step: { run: noop } },
}

const confDag: DagDefinition = {
  id: 'conf_test_dag',
  schedule: null,
  tasks: {
    reader: {
      run: async (ctx) => {
        // Read conf and push to xcom so we can verify injection
        await ctx.xcom.push('batch_size', ctx.conf.batchSize ?? null)
        await ctx.xcom.push('env', ctx.conf.env ?? null)
      },
    },
  },
}

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_run_tags'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_run_tags')
  clearRegistry()
  register(testDag)
  register(confDag)
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  delete process.env.DB_NAME
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('xcoms').deleteMany({})
})

// ── createRun — conf/tags/note stored ────────────────────────────────────────

describe('createRun — conf, tags, note defaults', () => {
  it('stamps conf={} tags=[] note=null when no opts', async () => {
    const runId = await createRun(db, testDag)
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.conf).toEqual({})
    expect(run?.tags).toEqual([])
    expect(run?.note).toBeNull()
  })

  it('stamps provided conf and tags', async () => {
    const runId = await createRun(db, testDag, {
      conf: { batchSize: 100, env: 'prod' },
      tags: ['etl', 'prod'],
    })
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.conf).toEqual({ batchSize: 100, env: 'prod' })
    expect(run?.tags).toEqual(['etl', 'prod'])
  })
})

// ── conf injection (forked worker) ───────────────────────────────────────────

describe('conf injection in forked worker', () => {
  it('ctx.conf contains the trigger-time conf — verified through XCom', async () => {
    const runId = await createRun(db, confDag, {
      conf: { batchSize: 250, env: 'staging' },
    })
    await advanceRun(db, runId)

    const batchXcom = await db.collection('xcoms').findOne({
      dag_run_id: runId, task_id: 'reader', key: 'batch_size',
    })
    const envXcom = await db.collection('xcoms').findOne({
      dag_run_id: runId, task_id: 'reader', key: 'env',
    })
    expect(batchXcom?.value).toBe(250)
    expect(envXcom?.value).toBe('staging')
  })

  it('ctx.conf is empty object when no conf provided', async () => {
    const runId = await createRun(db, confDag)
    await advanceRun(db, runId)

    const batchXcom = await db.collection('xcoms').findOne({
      dag_run_id: runId, task_id: 'reader', key: 'batch_size',
    })
    expect(batchXcom?.value).toBeNull()
  })
})

// ── Tag filter in GET /dags/:dagId/runs ──────────────────────────────────────

describe('tag filter — GET /dags/:dagId/runs', () => {
  it('returns only tagged run when ?tag= filter applied (not both)', async () => {
    // Create 2 runs: one tagged, one not
    await createRun(db, testDag, { tags: ['prod'] })
    await createRun(db, testDag)  // untagged

    const res = await app.inject({ method: 'GET', url: '/dags/tags_test_dag/runs?tag=prod' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.items).toHaveLength(1)
    expect(body.items[0].tags).toContain('prod')
  })

  it('returns all runs when no tag filter applied', async () => {
    await createRun(db, testDag, { tags: ['prod'] })
    await createRun(db, testDag)

    const res = await app.inject({ method: 'GET', url: '/dags/tags_test_dag/runs' })
    const body = res.json()
    expect(body.items).toHaveLength(2)
  })

  it('tag filter ANDs with cursor pagination — tagged run still filtered correctly', async () => {
    const r1 = await createRun(db, testDag, { tags: ['debug'] })
    const r2 = await createRun(db, testDag, { tags: ['debug'] })
    const r3 = await createRun(db, testDag)  // no tag

    // First page with tag filter, limit=1
    const page1 = await app.inject({ method: 'GET', url: '/dags/tags_test_dag/runs?tag=debug&limit=1' })
    const p1body = page1.json()
    expect(p1body.items).toHaveLength(1)
    expect(p1body.next_cursor).not.toBeNull()

    // Second page — should return the other tagged run
    const page2 = await app.inject({
      method: 'GET',
      url: `/dags/tags_test_dag/runs?tag=debug&limit=1&cursor=${p1body.next_cursor}`,
    })
    const p2body = page2.json()
    expect(p2body.items).toHaveLength(1)
    expect(p2body.items[0].tags).toContain('debug')
    // Should not return r3 (untagged)
    expect(p2body.items[0].run_id).not.toBe(r3)
  })

  it('returns empty items for a tag with no matching runs', async () => {
    await createRun(db, testDag, { tags: ['prod'] })

    const res = await app.inject({ method: 'GET', url: '/dags/tags_test_dag/runs?tag=nonexistent' })
    expect(res.json().items).toHaveLength(0)
  })
})

// ── Note endpoint ─────────────────────────────────────────────────────────────

describe('POST /dag-runs/:runId/note', () => {
  it('adds a note to a run', async () => {
    const runId = await createRun(db, testDag)
    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/note`,
      payload: { note: 'Manual rerun after hotfix' },
    })
    expect(res.statusCode).toBe(200)
    expect(res.json().note).toBe('Manual rerun after hotfix')

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.note).toBe('Manual rerun after hotfix')
  })

  it('updates an existing note', async () => {
    const runId = await createRun(db, testDag)
    await app.inject({ method: 'POST', url: `/dag-runs/${runId}/note`, payload: { note: 'first' } })
    await app.inject({ method: 'POST', url: `/dag-runs/${runId}/note`, payload: { note: 'updated' } })

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    expect(run?.note).toBe('updated')
  })

  it('can add a note to a terminal (success) run', async () => {
    const runId = await createRun(db, testDag)
    await db.collection('dag_runs').updateOne({ _id: new ObjectId(runId) }, { $set: { state: 'success' } })

    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/note`,
      payload: { note: 'Reviewed by ops team' },
    })
    expect(res.statusCode).toBe(200)
    expect(res.json().note).toBe('Reviewed by ops team')
  })

  it('returns 400 when note field is missing', async () => {
    const runId = await createRun(db, testDag)
    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/note`,
      payload: { message: 'wrong field' },
    })
    expect(res.statusCode).toBe(400)
  })

  it('returns 404 for unknown run id', async () => {
    const fakeId = new ObjectId().toString()
    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${fakeId}/note`,
      payload: { note: 'hello' },
    })
    expect(res.statusCode).toBe(404)
  })
})

// ── API — /trigger with conf/tags ────────────────────────────────────────────

describe('POST /dags/:dagId/trigger — conf/tags/note', () => {
  it('triggers with conf and tags, returns them in response', async () => {
    const res = await app.inject({
      method: 'POST', url: '/dags/tags_test_dag/trigger',
      payload: { conf: { x: 1 }, tags: ['manual', 'test'] },
    })
    expect(res.statusCode).toBe(201)
    const body = res.json()
    expect(body.conf).toEqual({ x: 1 })
    expect(body.tags).toContain('manual')
    expect(body.tags).toContain('test')
  })

  it('triggers with note set at creation', async () => {
    const res = await app.inject({
      method: 'POST', url: '/dags/tags_test_dag/trigger',
      payload: { note: 'Emergency run' },
    })
    expect(res.statusCode).toBe(201)
    expect(res.json().note).toBe('Emergency run')
  })

  it('returns 400 when conf is not an object', async () => {
    const res = await app.inject({
      method: 'POST', url: '/dags/tags_test_dag/trigger',
      payload: { conf: 'not-an-object' },
    })
    expect(res.statusCode).toBe(400)
    expect(res.json().error).toMatch(/conf/)
  })

  it('returns 400 when conf is an array', async () => {
    const res = await app.inject({
      method: 'POST', url: '/dags/tags_test_dag/trigger',
      payload: { conf: [1, 2, 3] },
    })
    expect(res.statusCode).toBe(400)
  })

  it('trigger with no body → conf={} tags=[] note=null', async () => {
    const res = await app.inject({ method: 'POST', url: '/dags/tags_test_dag/trigger' })
    expect(res.statusCode).toBe(201)
    const body = res.json()
    expect(body.conf).toEqual({})
    expect(body.tags).toEqual([])
    expect(body.note).toBeNull()
  })
})

// ── GET /dag-runs/:runId exposes conf/tags/note ───────────────────────────────

describe('GET /dag-runs/:runId exposes conf, tags, note', () => {
  it('exposes conf and tags in run detail', async () => {
    const runId = await createRun(db, testDag, {
      conf: { region: 'us-east-1' },
      tags: ['scheduled'],
    })
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    const body = res.json()
    expect(body.conf).toEqual({ region: 'us-east-1' })
    expect(body.tags).toContain('scheduled')
    expect(body.note).toBeNull()
  })

  it('exposes note after it is set', async () => {
    const runId = await createRun(db, testDag)
    await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/note`,
      payload: { note: 'Post-hoc annotation' },
    })
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    expect(res.json().note).toBe('Post-hoc annotation')
  })
})
