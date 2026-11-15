/** Task and Dag callbacks: onExecute / onSuccess / onFailure / onRetry, notifier forms, exactly-once firing. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun, type TaskInstance } from '../runs.js'
import { advanceRun } from '../index.js'
import { flushTaskCallbacks } from '../callbacks.js'
import { reapZombies } from '../zombies.js'
import { successUpdate, failedUpdate, retryUpdate } from '../ti-writes.js'
import { setTaskInstanceState } from '../actions.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition, RunCallbackInfo, TaskCallbackInfo } from '../../dag/types.js'
import { waitFor } from './wait-for.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_callbacks')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'task_logs', 'event_logs', 'scheduler_instances']) await db.collection(c).deleteMany({})
  clearRegistry()
})

const noop = async () => {}
const boom = async () => { throw new Error('boom') }
const tis = () => db.collection<TaskInstance>('task_instances')

async function runToEnd(runId: string): Promise<void> {
  for (let i = 0; i < 40; i++) {
    await advanceRun(db, runId)
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    if (run?.state === 'success' || run?.state === 'failed') break
  }
}

/** Put the run's only instance of `taskId` into running, as an executor would after claiming it. */
async function running(runId: string, taskId = 't'): Promise<TaskInstance> {
  return (await tis().findOneAndUpdate({ dag_run_id: runId, task_id: taskId }, { $set: { state: 'running', started_at: new Date() } }, { returnDocument: 'after' }))!
}

describe('task callbacks in a real run', () => {
  it('onExecute and onSuccess fire once each, with the attempt number', async () => {
    const calls: TaskCallbackInfo[] = []
    const dag: DagDefinition = {
      id: 'cb_ok', schedule: null,
      tasks: { t: { run: noop, onExecute: i => { calls.push(i) }, onSuccess: i => { calls.push(i) }, onFailure: i => { calls.push(i) } } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await runToEnd(runId)

    await waitFor(async () => calls.length, n => n >= 2)
    expect(calls.map(c => c.state)).toEqual(['running', 'success'])
    expect(calls[1]).toMatchObject({ dagId: 'cb_ok', runId, taskId: 't', mapIndex: null, tryNumber: 1, error: null })
    expect((await tis().findOne({ dag_run_id: runId }))!.callback_pending).toBeNull()
  })

  it('onRetry fires per retried try, onFailure only for the final failure', async () => {
    const calls: TaskCallbackInfo[] = []
    const dag: DagDefinition = {
      id: 'cb_fail', schedule: null,
      tasks: { t: { retries: 2, run: boom, onRetry: i => { calls.push(i) }, onFailure: i => { calls.push(i) }, onSuccess: i => { calls.push(i) } } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await runToEnd(runId)

    await waitFor(async () => calls.length, n => n >= 3)
    expect(calls.map(c => [c.state, c.tryNumber])).toEqual([['up_for_retry', 1], ['up_for_retry', 2], ['failed', 3]])
    expect(calls[2].error).toContain('boom')
  })

  it('callbacks of every instance of a mapped task carry their map index', async () => {
    const seen: number[] = []
    const dag: DagDefinition = { id: 'cb_mapped', schedule: null, tasks: { m: { expand: [1, 2, 3], run: noop, onSuccess: i => { seen.push(i.mapIndex!) } } } }
    register(dag)
    await runToEnd(await createRun(db, dag))
    await waitFor(async () => seen.length, n => n >= 3)
    expect(seen.sort()).toEqual([0, 1, 2])
  })
})

describe('results written outside the scheduler (BullMQ worker, zombie reaper)', () => {
  it('a result written by another process fires its callback on the next flush', async () => {
    const calls: TaskCallbackInfo[] = []
    const dag: DagDefinition = { id: 'cb_remote', schedule: null, tasks: { t: { run: noop, onFailure: i => { calls.push(i) } } } }
    register(dag)
    const runId = await createRun(db, dag)
    await running(runId)
    await tis().updateOne({ dag_run_id: runId }, failedUpdate('worker crashed'))   // what the BullMQ consumer writes

    await Promise.all(await flushTaskCallbacks(db))
    expect(calls).toEqual([expect.objectContaining({ state: 'failed', error: 'worker crashed', tryNumber: 1 })])
  })

  it('a zombie with retries left fires onRetry; without, onFailure', async () => {
    const calls: TaskCallbackInfo[] = []
    const dag: DagDefinition = {
      id: 'cb_zombie', schedule: null,
      tasks: {
        a: { retries: 1, run: noop, onRetry: i => { calls.push(i) } },
        b: { run: noop, onFailure: i => { calls.push(i) } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const old = new Date(Date.now() - 3_600_000)
    await tis().updateMany({ dag_run_id: runId }, { $set: { state: 'running', claimed_by: 'dead-replica', started_at: old } })

    await reapZombies(db, { thresholdMs: 1_000 })
    await Promise.all(await flushTaskCallbacks(db))
    expect(calls.map(c => [c.taskId, c.state, c.tryNumber]).sort()).toEqual([['a', 'up_for_retry', 1], ['b', 'failed', 1]])
    expect(calls.every(c => c.error?.includes('heartbeat lost'))).toBe(true)
  })
})

describe('flushTaskCallbacks', () => {
  it('fires each flag exactly once, even when two schedulers flush at the same time', async () => {
    let n = 0
    const dag: DagDefinition = { id: 'cb_once', schedule: null, tasks: { t: { run: noop, onSuccess: () => { n++ } } } }
    register(dag)
    const runId = await createRun(db, dag)
    await running(runId)
    await tis().updateOne({ dag_run_id: runId }, successUpdate())

    const [a, b] = await Promise.all([flushTaskCallbacks(db), flushTaskCallbacks(db)])
    await Promise.all([...a, ...b])
    await Promise.all(await flushTaskCallbacks(db))
    expect(n).toBe(1)
  })

  it('clears flags of tasks without that callback, so they are not read again', async () => {
    const dag: DagDefinition = { id: 'cb_none', schedule: null, tasks: { t: { run: noop, onFailure: () => {} } } }
    register(dag)
    const runId = await createRun(db, dag)
    await running(runId)
    await tis().updateOne({ dag_run_id: runId }, successUpdate())   // success, but only onFailure is set

    expect(await flushTaskCallbacks(db)).toHaveLength(0)
    expect((await tis().findOne({ dag_run_id: runId }))!.callback_pending).toBeNull()
  })

  it('a retry flag carries the failed attempt even after try_number moved on', async () => {
    const calls: TaskCallbackInfo[] = []
    const dag: DagDefinition = { id: 'cb_try', schedule: null, tasks: { t: { retries: 3, run: noop, onRetry: i => { calls.push(i) } } } }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = await running(runId)
    await tis().updateOne({ dag_run_id: runId }, retryUpdate({ ...ti, try_number: 1 }, 'second try failed'))

    await Promise.all(await flushTaskCallbacks(db))
    expect(calls[0]).toMatchObject({ tryNumber: 2, state: 'up_for_retry', error: 'second try failed' })
  })

  it('marking a task by hand fires no callback', async () => {
    let n = 0
    const dag: DagDefinition = { id: 'cb_mark', schedule: null, tasks: { t: { run: noop, onSuccess: () => { n++ }, onFailure: () => { n++ } } } }
    register(dag)
    const runId = await createRun(db, dag)
    await setTaskInstanceState(db, runId, 'failed', { taskIds: ['t'] })
    await Promise.all(await flushTaskCallbacks(db))
    expect(n).toBe(0)
  })
})

describe('notifier forms', () => {
  let server: Server
  let url: string
  const bodies: unknown[] = []
  beforeAll(async () => {
    server = createServer((req, res) => {
      let data = ''
      req.on('data', c => { data += c })
      req.on('end', () => { bodies.push(JSON.parse(data)); res.writeHead(204).end() })
    })
    await new Promise<void>(r => server.listen(0, '127.0.0.1', r))
    url = `http://127.0.0.1:${(server.address() as AddressInfo).port}/hook`
  })
  afterAll(() => new Promise<void>(r => server.close(() => r())))
  afterEach(() => { bodies.length = 0 })

  it('a list of a function, a { notify } object and a webhook URL all fire; a throwing one does not stop the others', async () => {
    const seen: string[] = []
    const dag: DagDefinition = {
      id: 'cb_forms', schedule: null,
      tasks: {
        t: {
          run: boom,
          onFailure: [
            () => { throw new Error('first notifier broke') },
            i => { seen.push(`fn:${i.taskId}`) },
            { notify: i => { seen.push(`obj:${i.state}`) } },
            url,
          ],
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await runToEnd(runId)

    await waitFor(async () => seen.length + bodies.length, n => n >= 3)
    expect(seen.sort()).toEqual(['fn:t', 'obj:failed'])
    expect(bodies).toEqual([expect.objectContaining({ event: 'task_failed', dag_id: 'cb_forms', run_id: runId, task_id: 't', try_number: 1 })])
  })

  it('Dag onSuccess / onFailure accept functions and objects; URLs keep the run webhook payload', async () => {
    const runCalls: RunCallbackInfo[] = []
    const ok: DagDefinition = { id: 'cb_dag_ok', schedule: null, tasks: { t: { run: noop } }, onSuccess: [i => { runCalls.push(i) }, url], onFailure: () => { throw new Error('not expected') } }
    const bad: DagDefinition = { id: 'cb_dag_bad', schedule: null, tasks: { t: { run: boom } }, onFailure: { notify: i => { runCalls.push(i) } } }
    register(ok); register(bad)
    const okRun = await createRun(db, ok, { conf: { k: 1 } })
    const badRun = await createRun(db, bad)
    await runToEnd(okRun)
    await runToEnd(badRun)

    await waitFor(async () => runCalls.length + bodies.length, n => n >= 3)
    expect(runCalls.map(c => [c.dagId, c.state]).sort()).toEqual([['cb_dag_bad', 'failed'], ['cb_dag_ok', 'success']])
    expect(runCalls.find(c => c.dagId === 'cb_dag_ok')).toMatchObject({ runId: okRun, conf: { k: 1 }, endedAt: expect.any(Date) })
    expect(bodies).toEqual([expect.objectContaining({ dag_id: 'cb_dag_ok', run_id: okRun, state: 'success', conf: { k: 1 } })])
  })
})
