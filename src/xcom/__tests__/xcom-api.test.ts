import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { xcomPush } from '../index.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_xcom_api'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_xcom_api')
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
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('xcoms').deleteMany({})
  clearRegistry()
})

// ── Helper: run a dag with xcom-pushing tasks ─────────────────────────────

async function runDagWithXcom() {
  const dag: DagDefinition = {
    id: 'xcom_test_dag',
    schedule: null,
    tasks: {
      producer: {
        run: async (ctx) => {
          await ctx.xcom.push('result', 42)
          await ctx.xcom.push('label', 'hello')
        },
      },
      consumer: {
        dependsOn: ['producer'],
        run: async (ctx) => {
          const v = await ctx.xcom.pull('producer', 'result')
          await ctx.xcom.push('doubled', (v as number) * 2)
        },
      },
    },
  }
  register(dag)
  const runId = await createRun(db, dag)
  await advanceRun(db, runId)
  return runId
}

// ── GET /dag-runs/:runId/xcoms ─────────────────────────────────────────────

describe('GET /dag-runs/:runId/xcoms', () => {
  it('returns 400 for invalid runId', async () => {
    const res = await app.inject({ method: 'GET', url: '/dag-runs/not-an-id/xcoms' })
    expect(res.statusCode).toBe(400)
  })

  it('returns all xcoms for a completed run', async () => {
    const runId = await runDagWithXcom()
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms` })
    expect(res.statusCode).toBe(200)
    const xcoms = res.json() as Array<{ task_id: string; key: string; value: unknown }>
    expect(xcoms.length).toBe(3)  // result, label, doubled
    const keys = xcoms.map(x => x.key).sort()
    expect(keys).toEqual(['doubled', 'label', 'result'])
  })

  it('includes run_id, dag_id, map_index, pushed_at in each entry', async () => {
    const runId = await runDagWithXcom()
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms` })
    const x = res.json()[0] as Record<string, unknown>
    expect(x.run_id).toBe(runId)
    expect(x.dag_id).toBe('xcom_test_dag')
    expect('map_index' in x).toBe(true)
    expect('pushed_at' in x).toBe(true)
  })

  it('?task_id= filters to that task only', async () => {
    const runId = await runDagWithXcom()
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms?task_id=producer` })
    const xcoms = res.json() as Array<{ task_id: string }>
    expect(xcoms.every(x => x.task_id === 'producer')).toBe(true)
    expect(xcoms.some(x => x.task_id === 'consumer')).toBe(false)
  })

  it('?key= filters to that key only', async () => {
    const runId = await runDagWithXcom()
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms?key=result` })
    const xcoms = res.json() as Array<{ key: string; value: unknown }>
    expect(xcoms).toHaveLength(1)
    expect(xcoms[0].key).toBe('result')
    expect(xcoms[0].value).toBe(42)
  })

  it('returns empty array for run with no xcoms', async () => {
    const dag: DagDefinition = { id: 'no_xcom_dag', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms` })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })
})

// ── GET /dag-runs/:runId/xcoms/:taskId/:key ───────────────────────────────

describe('GET /dag-runs/:runId/xcoms/:taskId/:key', () => {
  it('returns the xcom value for a task+key', async () => {
    const runId = await runDagWithXcom()
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms/producer/result` })
    expect(res.statusCode).toBe(200)
    expect(res.json().value).toBe(42)
    expect(res.json().key).toBe('result')
    expect(res.json().task_id).toBe('producer')
  })

  it('returns 404 for unknown key', async () => {
    const runId = await runDagWithXcom()
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms/producer/nonexistent` })
    expect(res.statusCode).toBe(404)
  })

  it('?map_index= targets a specific mapped instance', async () => {
    // Push mapped xcoms directly
    const dag: DagDefinition = { id: 'xcom_mapped_get', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    await xcomPush(db, runId, 'xcom_mapped_get', 'fan', 0, 'score', 10)
    await xcomPush(db, runId, 'xcom_mapped_get', 'fan', 1, 'score', 20)
    await xcomPush(db, runId, 'xcom_mapped_get', 'fan', 2, 'score', 30)

    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms/fan/score?map_index=1` })
    expect(res.statusCode).toBe(200)
    expect(res.json().value).toBe(20)
    expect(res.json().map_index).toBe(1)
  })

  it('returns 400 for invalid map_index', async () => {
    const runId = await runDagWithXcom()
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/xcoms/producer/result?map_index=abc` })
    expect(res.statusCode).toBe(400)
  })
})

// ── POST /dag-runs/:runId/xcoms ────────────────────────────────────────────

describe('POST /dag-runs/:runId/xcoms', () => {
  it('returns 400 for invalid runId', async () => {
    const res = await app.inject({
      method: 'POST', url: '/dag-runs/bad-id/xcoms',
      payload: { task_id: 'x', key: 'k', value: 1 },
    })
    expect(res.statusCode).toBe(400)
  })

  it('returns 404 for unknown run', async () => {
    const { ObjectId } = await import('mongodb')
    const fakeId = new ObjectId().toString()
    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${fakeId}/xcoms`,
      payload: { task_id: 'x', key: 'k', value: 1 },
    })
    expect(res.statusCode).toBe(404)
  })

  it('pushes an xcom value and returns 201', async () => {
    const dag: DagDefinition = { id: 'xcom_post_dag', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)

    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/xcoms`,
      payload: { task_id: 'external', key: 'manual_result', value: { status: 'approved' } },
    })
    expect(res.statusCode).toBe(201)
    expect(res.json().key).toBe('manual_result')
    expect(res.json().value).toEqual({ status: 'approved' })
    expect(res.json().task_id).toBe('external')

    // Verify persisted
    const stored = await db.collection('xcoms').findOne({ dag_run_id: runId, key: 'manual_result' })
    expect(stored?.value).toEqual({ status: 'approved' })
  })

  it('upserts when pushing same task_id+key+map_index twice', async () => {
    const dag: DagDefinition = { id: 'xcom_upsert_dag', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)

    await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/xcoms`,
      payload: { task_id: 'op', key: 'count', value: 1 },
    })
    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/xcoms`,
      payload: { task_id: 'op', key: 'count', value: 2 },
    })
    expect(res.statusCode).toBe(201)

    const count = await db.collection('xcoms').countDocuments({ dag_run_id: runId, task_id: 'op', key: 'count' })
    expect(count).toBe(1)  // upserted, not duplicated

    const stored = await db.collection('xcoms').findOne({ dag_run_id: runId, key: 'count' })
    expect(stored?.value).toBe(2)
  })

  it('returns 400 when task_id missing', async () => {
    const dag: DagDefinition = { id: 'xcom_post_val', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/xcoms`,
      payload: { key: 'k', value: 1 },
    })
    expect(res.statusCode).toBe(400)
    expect(res.json().error).toContain('"task_id"')
  })

  it('returns 400 when key missing', async () => {
    const dag: DagDefinition = { id: 'xcom_post_key', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/xcoms`,
      payload: { task_id: 't', value: 1 },
    })
    expect(res.statusCode).toBe(400)
    expect(res.json().error).toContain('"key"')
  })

  it('accepts null and falsy values (value is required but can be null/0/false)', async () => {
    const dag: DagDefinition = { id: 'xcom_null_dag', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)

    for (const value of [null, 0, false, '']) {
      const res = await app.inject({
        method: 'POST', url: `/dag-runs/${runId}/xcoms`,
        payload: { task_id: 'x', key: `v_${JSON.stringify(value)}`, value },
      })
      expect(res.statusCode).toBe(201)
    }
  })

  it('supports map_index for mapped task xcoms', async () => {
    const dag: DagDefinition = { id: 'xcom_mapidx_dag', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)

    const res = await app.inject({
      method: 'POST', url: `/dag-runs/${runId}/xcoms`,
      payload: { task_id: 'fan', key: 'score', value: 99, map_index: 2 },
    })
    expect(res.statusCode).toBe(201)
    expect(res.json().map_index).toBe(2)

    const stored = await db.collection('xcoms').findOne({ dag_run_id: runId, task_id: 'fan', map_index: 2 })
    expect(stored?.value).toBe(99)
  })
})

// ── DELETE /dag-runs/:runId/xcoms/:taskId/:key ────────────────────────────

describe('DELETE /dag-runs/:runId/xcoms/:taskId/:key', () => {
  it('deletes a single xcom entry and returns 204', async () => {
    const runId = await runDagWithXcom()

    const res = await app.inject({ method: 'DELETE', url: `/dag-runs/${runId}/xcoms/producer/result` })
    expect(res.statusCode).toBe(204)

    const gone = await db.collection('xcoms').findOne({ dag_run_id: runId, task_id: 'producer', key: 'result' })
    expect(gone).toBeNull()
  })

  it('returns 404 for non-existent entry', async () => {
    const runId = await runDagWithXcom()
    const res = await app.inject({ method: 'DELETE', url: `/dag-runs/${runId}/xcoms/producer/ghost` })
    expect(res.statusCode).toBe(404)
  })

  it('?map_index= deletes only the specific mapped instance', async () => {
    const dag: DagDefinition = { id: 'xcom_del_mapped', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    await xcomPush(db, runId, 'xcom_del_mapped', 'fan', 0, 'score', 10)
    await xcomPush(db, runId, 'xcom_del_mapped', 'fan', 1, 'score', 20)
    await xcomPush(db, runId, 'xcom_del_mapped', 'fan', 2, 'score', 30)

    const res = await app.inject({ method: 'DELETE', url: `/dag-runs/${runId}/xcoms/fan/score?map_index=1` })
    expect(res.statusCode).toBe(204)

    const remaining = await db.collection('xcoms').find({ dag_run_id: runId, task_id: 'fan', key: 'score' }).toArray()
    expect(remaining).toHaveLength(2)
    expect(remaining.map(r => r.map_index)).not.toContain(1)
  })
})

// ── DELETE /dag-runs/:runId/xcoms — delete all ────────────────────────────

describe('DELETE /dag-runs/:runId/xcoms', () => {
  it('deletes all xcoms for a run and returns count', async () => {
    const runId = await runDagWithXcom()

    const before = await db.collection('xcoms').countDocuments({ dag_run_id: runId })
    expect(before).toBeGreaterThan(0)

    const res = await app.inject({ method: 'DELETE', url: `/dag-runs/${runId}/xcoms` })
    expect(res.statusCode).toBe(200)
    expect(res.json().deleted_count).toBe(before)

    const after = await db.collection('xcoms').countDocuments({ dag_run_id: runId })
    expect(after).toBe(0)
  })

  it('returns deleted_count=0 when no xcoms exist', async () => {
    const dag: DagDefinition = { id: 'xcom_delall_empty', schedule: null, tasks: { noop: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    const res = await app.inject({ method: 'DELETE', url: `/dag-runs/${runId}/xcoms` })
    expect(res.statusCode).toBe(200)
    expect(res.json().deleted_count).toBe(0)
  })

  it('only deletes xcoms for the specified run — other runs untouched', async () => {
    const runId1 = await runDagWithXcom()

    const dag2: DagDefinition = {
      id: 'xcom_other_dag',
      schedule: null,
      tasks: { step: { run: async (ctx) => { await ctx.xcom.push('x', 1) } } },
    }
    register(dag2)
    const runId2 = await createRun(db, dag2)
    await advanceRun(db, runId2)

    // Delete xcoms for run1 only
    await app.inject({ method: 'DELETE', url: `/dag-runs/${runId1}/xcoms` })

    const run1Count = await db.collection('xcoms').countDocuments({ dag_run_id: runId1 })
    const run2Count = await db.collection('xcoms').countDocuments({ dag_run_id: runId2 })
    expect(run1Count).toBe(0)
    expect(run2Count).toBeGreaterThan(0)
  })

  it('returns 400 for invalid runId', async () => {
    const res = await app.inject({ method: 'DELETE', url: '/dag-runs/bad-id/xcoms' })
    expect(res.statusCode).toBe(400)
  })
})
