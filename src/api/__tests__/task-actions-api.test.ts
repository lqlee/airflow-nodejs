import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'
import { buildServer } from '../server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { waitFor } from '../../scheduler/__tests__/wait-for.js'
import { setVariable } from '../../variables/index.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_task_actions_api'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_task_actions_api')
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
  clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'event_logs', 'xcoms', 'variables']) await db.collection(c).deleteMany({})
})

/** a fails until the variable `<id>_ok` is '1'; b depends on it. */
function flakyDag(id: string): DagDefinition {
  const dag: DagDefinition = {
    id, schedule: null,
    tasks: {
      a: {
        run: new Function(`return async (ctx) => {
          if ((await ctx.variables.get('${id}_ok')) !== '1') throw new Error('not yet')
        }`)() as () => Promise<void>,
      },
      b: { dependsOn: ['a'], run: async () => {} },
    },
  }
  register(dag)
  return dag
}

async function failedRun(): Promise<string> {
  const dag: DagDefinition = {
    id: 'api_act', schedule: null,
    tasks: { a: { run: async () => { throw new Error('boom') } }, b: { dependsOn: ['a'], run: async () => {} } },
  }
  register(dag)
  const runId = await createRun(db, dag)
  await advanceRun(db, runId)
  return runId
}

const states = async (runId: string) => Object.fromEntries(
  (await db.collection('task_instances').find({ dag_run_id: runId }).toArray()).map(t => [t.task_id, t.state]))
const runState = async (runId: string) => (await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) }))!.state
const inject = (method: 'POST' | 'PATCH', url: string, payload?: object) =>
  app.inject({ method, url, ...(payload ? { payload } : {}) })

describe('POST /dag-runs/:runId/tasks/:taskId/clear', () => {
  it('include_downstream clears the dependent task too; without a body only the task itself (back-compat)', async () => {
    const runId = await failedRun()
    expect(await states(runId)).toEqual({ a: 'failed', b: 'upstream_failed' })

    const dry = await inject('POST', `/dag-runs/${runId}/tasks/a/clear`, { include_downstream: true, dry_run: true })
    expect(dry.statusCode).toBe(200)
    expect(dry.json()).toMatchObject({ dry_run: true, cleared_count: 2 })
    expect(await states(runId)).toEqual({ a: 'failed', b: 'upstream_failed' })

    const solo = await inject('POST', `/dag-runs/${runId}/tasks/a/clear`, { dry_run: true })
    expect(solo.json().task_instances).toEqual([{ task_id: 'a', map_index: null, state: 'failed' }])
  }, 20_000)

  it('409 when nothing is finished; 400 for a non-boolean flag; 404 for an unknown task', async () => {
    const dag: DagDefinition = { id: 'api_q', schedule: null, tasks: { t: { run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    expect((await inject('POST', `/dag-runs/${runId}/tasks/t/clear`)).statusCode).toBe(409)
    expect((await inject('POST', `/dag-runs/${runId}/tasks/t/clear`, { include_downstream: 'yes' })).statusCode).toBe(400)
    expect((await inject('POST', `/dag-runs/${runId}/tasks/nope/clear`)).statusCode).toBe(404)
  })
})

describe('POST /dag-runs/:runId/clear — re-run', () => {
  it('only_failed re-runs the failed part and the run ends in success', async () => {
    const dag = flakyDag('api_rerun')
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    expect(await runState(runId)).toBe('failed')
    await setVariable(db, { key: 'api_rerun_ok', value: '1' })

    const res = await inject('POST', `/dag-runs/${runId}/clear`, { only_failed: true })
    expect(res.statusCode).toBe(200)
    expect(res.json().cleared_count).toBe(2)
    await waitFor(() => runState(runId), s => s === 'success', 15_000)
    expect(await states(runId)).toEqual({ a: 'success', b: 'success' })
    expect(await db.collection('event_logs').countDocuments({ event_type: 'run_cleared', dag_run_id: runId })).toBe(1)
  }, 30_000)

  it('404 for an unknown run, 400 for a bad id', async () => {
    expect((await inject('POST', `/dag-runs/${new ObjectId()}/clear`)).statusCode).toBe(404)
    expect((await inject('POST', '/dag-runs/not-an-id/clear')).statusCode).toBe(400)
  })
})

describe('PATCH /dag-runs/:runId/tasks/:taskId — mark state', () => {
  it('marking the failed task success (with downstream) turns the run into a success', async () => {
    const runId = await failedRun()
    const res = await inject('PATCH', `/dag-runs/${runId}/tasks/a`, { state: 'success', include_downstream: true })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({ state: 'success', changed_count: 2 })
    await waitFor(() => runState(runId), s => s === 'success', 15_000)
  }, 20_000)

  it('marking the failed task success alone lets the scheduler run the downstream task', async () => {
    const runId = await failedRun()
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 'b' }, { $set: { state: 'queued' } })
    await inject('PATCH', `/dag-runs/${runId}/tasks/a`, { state: 'success' })
    await waitFor(() => runState(runId), s => s === 'success', 15_000)
    expect(await states(runId)).toEqual({ a: 'success', b: 'success' })
  }, 20_000)

  it('validates state and map_index', async () => {
    const runId = await failedRun()
    expect((await inject('PATCH', `/dag-runs/${runId}/tasks/a`, { state: 'running' })).statusCode).toBe(400)
    expect((await inject('PATCH', `/dag-runs/${runId}/tasks/a`, { state: 'success', map_index: -1 })).statusCode).toBe(400)
    expect((await inject('PATCH', `/dag-runs/${runId}/tasks/zzz`, { state: 'success' })).statusCode).toBe(404)
  }, 20_000)
})

describe('PATCH /dag-runs/:runId — mark run', () => {
  it('marks a running run failed and stops its waiting tasks', async () => {
    const dag: DagDefinition = { id: 'api_mark_run', schedule: null, tasks: { a: { run: async () => {} }, b: { dependsOn: ['a'], run: async () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    await db.collection('dag_runs').updateOne({ _id: new ObjectId(runId) }, { $set: { state: 'running' } })

    const res = await inject('PATCH', `/dag-runs/${runId}`, { state: 'failed' })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toMatchObject({ state: 'failed', changed_count: 2 })
    expect(await runState(runId)).toBe('failed')
    expect(await states(runId)).toEqual({ a: 'skipped', b: 'skipped' })
  })

  it('rejects other states', async () => {
    const runId = await failedRun()
    expect((await inject('PATCH', `/dag-runs/${runId}`, { state: 'queued' })).statusCode).toBe(400)
  }, 20_000)
})

describe('GET /dag-runs/:runId/tasks?state=', () => {
  it('accepts the new states', async () => {
    const runId = await failedRun()
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}/tasks?state=upstream_failed` })
    expect(res.statusCode).toBe(200)
    expect(res.json().map((t: { task_id: string }) => t.task_id)).toEqual(['b'])
  }, 20_000)
})
