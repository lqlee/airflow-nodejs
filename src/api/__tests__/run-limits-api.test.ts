/** API exposure of maxActiveRuns / runTimeout / backoff / poolSlots, and the real retry timing. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { buildServer } from '../server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import type { FastifyInstance } from 'fastify'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_run_limits_api')
  clearRegistry()
  app = buildServer(db)
  await app.ready()
})
afterAll(async () => { await app.close(); await db.dropDatabase(); await client.close() })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'event_logs']) await db.collection(c).deleteMany({})
  clearRegistry()
})

describe('API exposes the new Dag/task settings', () => {
  const dag: DagDefinition = {
    id: 'api_limits_dag', schedule: null, maxActiveRuns: 3, runTimeout: 90_000,
    tasks: {
      limited: { pool: 'p', poolSlots: 2, retries: 2, retryDelay: 500, retryExponentialBackoff: 2, maxRetryDelay: 4000, run: async () => {} },
      plain: { run: async () => {} },
    },
  }

  it('GET /dags/:id returns max_active_runs and run_timeout_ms', async () => {
    register(dag)
    const body = (await app.inject({ method: 'GET', url: '/dags/api_limits_dag' })).json()
    expect(body).toMatchObject({ max_active_runs: 3, run_timeout_ms: 90_000 })
  })

  it('GET /dags/:id returns null limits when unset', async () => {
    register({ id: 'bare_dag', schedule: null, tasks: { t: { run: async () => {} } } })
    const body = (await app.inject({ method: 'GET', url: '/dags/bare_dag' })).json()
    expect(body).toMatchObject({ max_active_runs: null, run_timeout_ms: null })
  })

  it('GET /dags/:id/tasks returns backoff + pool settings per task', async () => {
    register(dag)
    const tasks = (await app.inject({ method: 'GET', url: '/dags/api_limits_dag/tasks' })).json()
    const by = Object.fromEntries(tasks.map((t: { task_id: string }) => [t.task_id, t]))
    expect(by.limited).toMatchObject({
      retry_delay_ms: 500, retry_exponential_backoff: 2, max_retry_delay_ms: 4000, pool: 'p', pool_slots: 2,
    })
    expect(by.plain).toMatchObject({ retry_exponential_backoff: null, max_retry_delay_ms: null, pool: null, pool_slots: null })
  })

  it('GET /dag-runs/:id/tasks/:taskId reports pool_slots on the instance', async () => {
    register(dag)
    const runId = await createRun(db, dag)
    const get = async (taskId: string) =>
      (await app.inject({ method: 'GET', url: `/dag-runs/${runId}/tasks/${taskId}` })).json()[0]
    expect((await get('limited')).pool_slots).toBe(2)
    expect((await get('plain')).pool_slots).toBeNull()
  })
})

describe('API exposes catchup / startDate / dependsOnPast', () => {
  it('GET /dags/:id and /dags/:id/tasks', async () => {
    register({
      id: 'api_catchup_dag', schedule: '0 * * * *', catchup: true, startDate: '2026-01-02T03:04:05Z',
      tasks: { seq: { dependsOnPast: true, run: async () => {} }, free: { run: async () => {} } },
    })
    const dagBody = (await app.inject({ method: 'GET', url: '/dags/api_catchup_dag' })).json()
    expect(dagBody).toMatchObject({ catchup: true, start_date: '2026-01-02T03:04:05.000Z' })
    const tasks = (await app.inject({ method: 'GET', url: '/dags/api_catchup_dag/tasks' })).json()
    const by = Object.fromEntries(tasks.map((t: { task_id: string }) => [t.task_id, t]))
    expect(by.seq.depends_on_past).toBe(true)
    expect(by.free.depends_on_past).toBe(false)
  })

  it('defaults to catchup=false / start_date=null, and tolerates an invalid startDate', async () => {
    register({ id: 'api_plain_dag', schedule: null, tasks: { t: { run: async () => {} } } })
    register({ id: 'api_badstart_dag', schedule: null, catchup: true, startDate: 'garbage', tasks: { t: { run: async () => {} } } })
    expect((await app.inject({ method: 'GET', url: '/dags/api_plain_dag' })).json()).toMatchObject({ catchup: false, start_date: null })
    const bad = await app.inject({ method: 'GET', url: '/dags/api_badstart_dag' })
    expect(bad.statusCode).toBe(200)
    expect(bad.json().start_date).toBeNull()
  })
})

describe('API exposes shortCircuit / triggerDag / externalTask', () => {
  it('GET /dags/:id/tasks reports the cross-dag task kinds', async () => {
    register({
      id: 'api_cross_dag', schedule: null,
      tasks: {
        gate: { shortCircuit: async () => true },
        fire: { triggerDag: { dagId: 'x', conf: { a: 1 }, waitForCompletion: true } },
        wait: { externalTask: { dagId: 'y', taskId: 'z', allowedStates: ['success'] } },
        plain: { run: async () => {} },
      },
    })
    const tasks = (await app.inject({ method: 'GET', url: '/dags/api_cross_dag/tasks' })).json()
    const by = Object.fromEntries(tasks.map((t: { task_id: string }) => [t.task_id, t]))
    expect(by.gate).toMatchObject({ is_short_circuit: true, trigger_dag: null, external_task: null })
    expect(by.fire.trigger_dag).toEqual({ dagId: 'x', conf: { a: 1 }, waitForCompletion: true })
    expect(by.wait.external_task).toEqual({ dagId: 'y', taskId: 'z', allowedStates: ['success'] })
    expect(by.plain).toMatchObject({ is_short_circuit: false, trigger_dag: null, external_task: null })
  })
})

describe('retry backoff — real timing', () => {
  it('waits retryDelay, then retryDelay×multiplier between successive attempts', async () => {
    const dag: DagDefinition = {
      id: 'backoff_timing', schedule: null,
      tasks: { flaky: { retries: 2, retryDelay: 150, retryExponentialBackoff: 2, run: async () => { throw new Error('nope') } } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const deadline = Date.now() + 20_000
    let state = 'queued'
    while (!['failed', 'success'].includes(state) && Date.now() < deadline) {
      await advanceRun(db, runId)
      state = (await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) }))!.state
      await new Promise(r => setTimeout(r, 25))
    }
    expect(state).toBe('failed')

    const tries = await db.collection('task_instance_tries').find({ dag_run_id: runId }).sort({ try_number: 1 }).toArray()
    expect(tries).toHaveLength(3)
    const gap = (a: number, b: number) =>
      new Date(tries[b].started_at as Date).getTime() - new Date(tries[a].ended_at as Date).getTime()
    expect(gap(0, 1)).toBeGreaterThanOrEqual(140)   // 150ms
    expect(gap(1, 2)).toBeGreaterThanOrEqual(290)   // 300ms
  }, 30000)
})

describe('runTimeout — finalization side effects', () => {
  it('records a run_failed event when a run times out', async () => {
    const dag: DagDefinition = {
      id: 'timeout_event', schedule: null, runTimeout: 50,
      tasks: { gate: { requiresApproval: true, run: async () => {} } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    await new Promise(r => setTimeout(r, 80))
    await advanceRun(db, runId)

    const deadline = Date.now() + 3000
    let ev = null
    while (!ev && Date.now() < deadline) {
      ev = await db.collection('event_logs').findOne({ event_type: 'run_failed', dag_run_id: runId })
      if (!ev) await new Promise(r => setTimeout(r, 25))
    }
    expect(ev).not.toBeNull()
  })
})

describe('GET /dags/:id/runs cursor — same-millisecond runs', () => {
  it('pages through runs sharing one created_at without skipping any', async () => {
    register({ id: 'same_ms_dag', schedule: null, tasks: { t: { run: async () => {} } } })
    const ts = new Date()
    const ids = (await db.collection('dag_runs').insertMany(
      Array.from({ length: 5 }, () => ({ dag_id: 'same_ms_dag', state: 'success', tags: [], created_at: ts })),
    )).insertedIds
    const expected = new Set(Object.values(ids).map(String))

    const seen: string[] = []
    let cursor: string | null = null
    for (let page = 0; page < 5; page++) {
      const body: { items: { run_id: string }[]; next_cursor: string | null } = (await app.inject({
        method: 'GET', url: `/dags/same_ms_dag/runs?limit=2${cursor ? `&cursor=${cursor}` : ''}`,
      })).json()
      seen.push(...body.items.map(i => i.run_id))
      cursor = body.next_cursor
      if (!cursor) break
    }
    expect(new Set(seen)).toEqual(expected)
    expect(seen).toHaveLength(5)
  })
})
