/**
 * DAG Versions / Source tests.
 *
 * recordDagVersion is the key lever — tested directly (no dags/ dir needed).
 * Discriminating tests:
 *   - same source twice → ONE row (idempotent upsert)
 *   - changed source → second row; first_seen of original unchanged
 *   - run_count derived correctly
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { hashDagSource, recordDagVersion, listDagVersions, getDagSource } from '../version.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../registry.js'
import { createRun } from '../../scheduler/runs.js'
import type { DagDefinition } from '../types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_dag_versions'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_dag_versions')
  clearRegistry()
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
  await db.collection('dag_versions').deleteMany({})
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  clearRegistry()
})

// ── hashDagSource ─────────────────────────────────────────────────────────

describe('hashDagSource', () => {
  it('returns a 12-char hex string', () => {
    const h = hashDagSource('const x = 1')
    expect(h).toMatch(/^[0-9a-f]{12}$/)
  })

  it('same source → same hash', () => {
    expect(hashDagSource('abc')).toBe(hashDagSource('abc'))
  })

  it('different sources → different hashes', () => {
    expect(hashDagSource('version 1')).not.toBe(hashDagSource('version 2'))
  })
})

// ── recordDagVersion — dedup ──────────────────────────────────────────────

describe('recordDagVersion', () => {
  it('stores a new version row', async () => {
    await recordDagVersion(db, 'my_dag', 'abc123def456', 'source code', ['task_a'])
    const count = await db.collection('dag_versions').countDocuments({ dag_id: 'my_dag' })
    expect(count).toBe(1)
  })

  it('same (dag_id, version) twice → ONE row (idempotent)', async () => {
    const version = hashDagSource('const dag = {id: "test"}')
    await recordDagVersion(db, 'idempotent_dag', version, 'source v1', ['step'])
    await recordDagVersion(db, 'idempotent_dag', version, 'source v1', ['step'])

    const count = await db.collection('dag_versions').countDocuments({ dag_id: 'idempotent_dag' })
    expect(count).toBe(1)
  })

  it('changed source → new row; original first_seen unchanged', async () => {
    const v1 = hashDagSource('version 1 source')
    const v2 = hashDagSource('version 2 source — changed')

    await recordDagVersion(db, 'evolving_dag', v1, 'version 1 source', ['a'])
    const orig = await db.collection('dag_versions').findOne({ dag_id: 'evolving_dag', version: v1 })
    const origFirstSeen = orig?.first_seen as Date

    await new Promise(r => setTimeout(r, 5))  // ensure distinct timestamps

    await recordDagVersion(db, 'evolving_dag', v2, 'version 2 source — changed', ['a', 'b'])

    const count = await db.collection('dag_versions').countDocuments({ dag_id: 'evolving_dag' })
    expect(count).toBe(2)

    // Original first_seen must not be overwritten by the second insert
    const unchanged = await db.collection('dag_versions').findOne({ dag_id: 'evolving_dag', version: v1 })
    expect((unchanged?.first_seen as Date).getTime()).toBe(origFirstSeen.getTime())
  })

  it('multiple dags share the collection without interference', async () => {
    const v = hashDagSource('shared source')
    await recordDagVersion(db, 'dag_alpha', v, 'shared source', ['x'])
    await recordDagVersion(db, 'dag_beta',  v, 'shared source', ['x'])

    expect(await db.collection('dag_versions').countDocuments({ dag_id: 'dag_alpha' })).toBe(1)
    expect(await db.collection('dag_versions').countDocuments({ dag_id: 'dag_beta' })).toBe(1)
  })
})

// ── listDagVersions ───────────────────────────────────────────────────────

describe('listDagVersions', () => {
  it('returns empty array when no versions recorded', async () => {
    expect(await listDagVersions(db, 'unknown_dag')).toEqual([])
  })

  it('returns versions sorted newest-first', async () => {
    const v1 = hashDagSource('source v1')
    const v2 = hashDagSource('source v2')
    await recordDagVersion(db, 'multi_dag', v1, 'source v1', ['a'])
    await new Promise(r => setTimeout(r, 5))
    await recordDagVersion(db, 'multi_dag', v2, 'source v2', ['a', 'b'])

    const versions = await listDagVersions(db, 'multi_dag')
    expect(versions).toHaveLength(2)
    expect(versions[0].version).toBe(v2)  // newest first
    expect(versions[1].version).toBe(v1)
  })

  it('run_count is derived correctly', async () => {
    const version = hashDagSource('run_count source')
    await recordDagVersion(db, 'counted_dag', version, 'run_count source', ['step'])

    // Insert 3 runs with this version, 1 with a different version
    const dag: DagDefinition = { id: 'counted_dag', schedule: null, tasks: { step: { run: async () => {} } } }
    register(dag)
    dag.version = version
    await createRun(db, dag)
    await createRun(db, dag)
    await createRun(db, dag)
    dag.version = 'other000000'
    await createRun(db, dag)

    const versions = await listDagVersions(db, 'counted_dag')
    const v = versions.find(v => v.version === version)
    expect(v?.run_count).toBe(3)
  })

  it('includes task_ids snapshot', async () => {
    const v = hashDagSource('snapshot test')
    await recordDagVersion(db, 'snap_dag', v, 'snapshot test', ['extract', 'transform', 'load'])
    const versions = await listDagVersions(db, 'snap_dag')
    expect(versions[0].task_ids).toEqual(['extract', 'transform', 'load'])
  })
})

// ── getDagSource ──────────────────────────────────────────────────────────

describe('getDagSource', () => {
  it('returns null when no versions exist', async () => {
    expect(await getDagSource(db, 'no_dag')).toBeNull()
  })

  it('returns specific version when version param given', async () => {
    const v = hashDagSource('specific version source')
    await recordDagVersion(db, 'specific_dag', v, 'specific version source', ['a'])
    const doc = await getDagSource(db, 'specific_dag', v)
    expect(doc?.source).toBe('specific version source')
    expect(doc?.version).toBe(v)
  })

  it('returns null for unknown version', async () => {
    await recordDagVersion(db, 'partial_dag', 'abc123def456', 'real source', ['a'])
    expect(await getDagSource(db, 'partial_dag', 'nonexistent0')).toBeNull()
  })

  it('defaults to latest when version omitted', async () => {
    const v1 = hashDagSource('older source')
    const v2 = hashDagSource('newer source')
    await recordDagVersion(db, 'latest_dag', v1, 'older source', [])
    await new Promise(r => setTimeout(r, 5))
    await recordDagVersion(db, 'latest_dag', v2, 'newer source', [])

    const doc = await getDagSource(db, 'latest_dag')
    expect(doc?.version).toBe(v2)
    expect(doc?.source).toBe('newer source')
  })
})

// ── API routes ────────────────────────────────────────────────────────────

describe('GET /dags/:dagId/versions', () => {
  it('returns 404 for unregistered dag', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags/nope/versions' })
    expect(res.statusCode).toBe(404)
  })

  it('returns empty array when dag has no recorded versions', async () => {
    const dag: DagDefinition = { id: 'unversioned_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const res = await app.inject({ method: 'GET', url: '/dags/unversioned_dag/versions' })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  it('returns recorded versions with all fields', async () => {
    const dag: DagDefinition = { id: 'versioned_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const v = hashDagSource('my dag source')
    await recordDagVersion(db, 'versioned_dag', v, 'my dag source', ['s'])

    const res = await app.inject({ method: 'GET', url: '/dags/versioned_dag/versions' })
    expect(res.statusCode).toBe(200)
    const versions = res.json() as Array<{ version: string; run_count: number; task_ids: string[] }>
    expect(versions).toHaveLength(1)
    expect(versions[0].version).toBe(v)
    expect(versions[0].run_count).toBe(0)
    expect(versions[0].task_ids).toEqual(['s'])
  })
})

describe('GET /dags/:dagId/source', () => {
  it('returns 404 when no source recorded', async () => {
    const dag: DagDefinition = { id: 'nosource_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const res = await app.inject({ method: 'GET', url: '/dags/nosource_dag/source' })
    expect(res.statusCode).toBe(404)
  })

  it('returns 404 for unknown version', async () => {
    const dag: DagDefinition = { id: 'src_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    await recordDagVersion(db, 'src_dag', 'abc123def456', 'real source', ['s'])
    const res = await app.inject({ method: 'GET', url: '/dags/src_dag/source?version=nonexistent0' })
    expect(res.statusCode).toBe(404)
  })

  it('returns source for a specific version', async () => {
    const dag: DagDefinition = { id: 'src2_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const v = hashDagSource('the dag source code')
    await recordDagVersion(db, 'src2_dag', v, 'the dag source code', ['s'])

    const res = await app.inject({ method: 'GET', url: `/dags/src2_dag/source?version=${v}` })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.source).toBe('the dag source code')
    expect(body.version).toBe(v)
    expect(body.dag_id).toBe('src2_dag')
    expect('first_seen' in body).toBe(true)
  })

  it('returns latest source when version omitted', async () => {
    const dag: DagDefinition = { id: 'src3_dag', schedule: null, tasks: { s: { run: async () => {} } } }
    register(dag)
    const v1 = hashDagSource('old source')
    const v2 = hashDagSource('new source')
    await recordDagVersion(db, 'src3_dag', v1, 'old source', [])
    await new Promise(r => setTimeout(r, 5))
    await recordDagVersion(db, 'src3_dag', v2, 'new source', [])

    const res = await app.inject({ method: 'GET', url: '/dags/src3_dag/source' })
    expect(res.statusCode).toBe(200)
    expect(res.json().source).toBe('new source')
  })
})
