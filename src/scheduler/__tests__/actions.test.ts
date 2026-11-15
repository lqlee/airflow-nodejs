import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { clearTaskInstances, setTaskInstanceState, setRunState } from '../actions.js'
import { markSuccess } from '../ti-writes.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_actions')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'event_logs']) await db.collection(c).deleteMany({})
})

const noop = async () => {}
// extract → transform → load, plus a side branch report ← transform, and an unrelated task
const dag: DagDefinition = {
  id: 'act_dag', schedule: null,
  tasks: {
    extract: { run: noop },
    transform: { dependsOn: ['extract'], retries: 2, run: noop },
    load: { dependsOn: ['transform'], run: noop },
    report: { dependsOn: ['transform'], run: noop },
    other: { run: noop },
  },
}

/** A finished run: states per task as given (started_at set for tasks that ran). */
async function finishedRun(states: Record<string, string>, runState = 'failed'): Promise<string> {
  register(dag)
  const runId = await createRun(db, dag)
  for (const [task, state] of Object.entries(states)) {
    const ran = state === 'success' || state === 'failed'
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: task }, {
      $set: { state, started_at: ran ? new Date() : null, ended_at: new Date(), try_number: ran ? 1 : 0 },
    })
  }
  await db.collection('dag_runs').updateOne({ _id: new ObjectId(runId) }, { $set: { state: runState, ended_at: new Date() } })
  return runId
}

const states = async (runId: string) => Object.fromEntries(
  (await db.collection('task_instances').find({ dag_run_id: runId }).toArray()).map(t => [t.task_id, t.state]))
const run = async (runId: string) => (await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) }))!
const ids = (r: Awaited<ReturnType<typeof clearTaskInstances>>) => r.ok ? r.changed.map(c => c.task_id).sort() : r
const events = (type: string) => db.collection('event_logs').find({ event_type: type }).toArray()
const waitForEvents = async (type: string, n: number) => {
  for (let i = 0; i < 50 && (await events(type)).length < n; i++) await new Promise(r => setTimeout(r, 20))
  return events(type)
}

const FAILED_RUN = { extract: 'success', transform: 'failed', load: 'upstream_failed', report: 'upstream_failed', other: 'success' }

describe('clearTaskInstances', () => {
  it('with downstream: clears the task and everything below it, re-opens the run, and records an event', async () => {
    const runId = await finishedRun(FAILED_RUN)
    const res = await clearTaskInstances(db, runId, { taskIds: ['transform'], includeDownstream: true })

    expect(ids(res)).toEqual(['load', 'report', 'transform'])
    expect(await states(runId)).toEqual({ ...FAILED_RUN, transform: 'queued', load: 'queued', report: 'queued' })
    expect(await run(runId)).toMatchObject({ state: 'queued', ended_at: null })
    const [ev] = await waitForEvents('task_cleared', 1)
    expect(ev).toMatchObject({ dag_run_id: runId, task_id: 'transform', metadata: { cleared_count: 3, include_downstream: true } })
  })

  it('with upstream: clears the task and its ancestors only', async () => {
    const runId = await finishedRun(FAILED_RUN)
    expect(ids(await clearTaskInstances(db, runId, { taskIds: ['load'], includeUpstream: true }))).toEqual(['extract', 'load', 'transform'])
  })

  it('only_failed leaves successful instances alone', async () => {
    const runId = await finishedRun(FAILED_RUN)
    expect(ids(await clearTaskInstances(db, runId, { onlyFailed: true }))).toEqual(['load', 'report', 'transform'])
    expect((await states(runId)).extract).toBe('success')
  })

  it('a whole-run clear (re-run) resets every finished instance and records run_cleared', async () => {
    const runId = await finishedRun(FAILED_RUN)
    expect(ids(await clearTaskInstances(db, runId))).toEqual(['extract', 'load', 'other', 'report', 'transform'])
    expect(await waitForEvents('run_cleared', 1)).toHaveLength(1)
  })

  it('never clears running or waiting instances', async () => {
    const runId = await finishedRun({ ...FAILED_RUN, load: 'running', report: 'up_for_retry' }, 'running')
    expect(ids(await clearTaskInstances(db, runId, { taskIds: ['transform'], includeDownstream: true }))).toEqual(['transform'])
    expect(await states(runId)).toMatchObject({ load: 'running', report: 'up_for_retry' })
  })

  it('keeps try_number counting and restores the full retry budget', async () => {
    const runId = await finishedRun(FAILED_RUN)
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 'transform' }, { $set: { try_number: 2, max_retries: 2 } })
    await clearTaskInstances(db, runId, { taskIds: ['transform', 'load'] })

    const tis = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    const t = tis.find(x => x.task_id === 'transform')!
    expect(t).toMatchObject({ try_number: 3, max_retries: 5, started_at: null, ended_at: null, error: null })  // 3 + retries: 2
    expect(tis.find(x => x.task_id === 'load')).toMatchObject({ try_number: 0, max_retries: 0 })               // never ran
  })

  it('a mapIndex restricts the named task to one instance', async () => {
    const mapped: DagDefinition = { id: 'act_mapped', schedule: null, tasks: { fan: { expand: [1, 2, 3], run: noop } } }
    register(mapped)
    const runId = await createRun(db, mapped)
    await db.collection('task_instances').updateMany({ dag_run_id: runId }, { $set: { state: 'failed' } })

    const res = await clearTaskInstances(db, runId, { taskIds: ['fan'], mapIndex: 1 })
    expect(res.ok && res.changed).toEqual([{ task_id: 'fan', map_index: 1, state: 'failed' }])
  })

  it('a cleared approval task needs a fresh approval', async () => {
    const hitl: DagDefinition = { id: 'act_hitl', schedule: null, tasks: { gate: { requiresApproval: true } } }
    register(hitl)
    const runId = await createRun(db, hitl)
    await db.collection('task_instances').updateOne({ dag_run_id: runId }, { $set: { state: 'failed', hitl_state: 'rejected', hitl_note: 'no' } })

    await clearTaskInstances(db, runId, { taskIds: ['gate'] })
    expect(await db.collection('task_instances').findOne({ dag_run_id: runId }))
      .toMatchObject({ state: 'queued', hitl_state: 'pending', hitl_note: null })
  })

  it('dry run reports without writing', async () => {
    const runId = await finishedRun(FAILED_RUN)
    expect(ids(await clearTaskInstances(db, runId, { taskIds: ['transform'], includeDownstream: true, dryRun: true })))
      .toEqual(['load', 'report', 'transform'])
    expect(await states(runId)).toEqual(FAILED_RUN)
    expect((await run(runId)).state).toBe('failed')
  })

  it('unknown run / task', async () => {
    expect(await clearTaskInstances(db, new ObjectId().toString())).toEqual({ ok: false, reason: 'run_not_found' })
    const runId = await finishedRun(FAILED_RUN)
    expect(await clearTaskInstances(db, runId, { taskIds: ['nope'] })).toEqual({ ok: false, reason: 'task_not_found', taskId: 'nope' })
  })
})

describe('setTaskInstanceState', () => {
  it('mark success with downstream re-opens a finished run so the scheduler re-evaluates it', async () => {
    const runId = await finishedRun(FAILED_RUN)
    const res = await setTaskInstanceState(db, runId, 'success', { taskIds: ['transform'], includeDownstream: true })

    expect(ids(res)).toEqual(['load', 'report', 'transform'])
    expect(await states(runId)).toEqual({ extract: 'success', transform: 'success', load: 'success', report: 'success', other: 'success' })
    expect(await run(runId)).toMatchObject({ state: 'running', ended_at: null })
    const [ev] = await waitForEvents('task_state_set', 1)
    expect(ev.metadata).toMatchObject({ state: 'success', changed_count: 3 })
  })

  it('marking a running instance failed wins over its late result', async () => {
    const runId = await finishedRun({ extract: 'running' }, 'running')
    await setTaskInstanceState(db, runId, 'failed', { taskIds: ['extract'] })
    await markSuccess(db, { dag_run_id: runId, task_id: 'extract', map_index: null })   // the executor finishing later

    expect(await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'extract' }))
      .toMatchObject({ state: 'failed', error: 'Marked failed by user', claimed_by: null })
  })

  it('leaves instances already in that state (and removed ones) alone', async () => {
    const runId = await finishedRun({ ...FAILED_RUN, report: 'removed' })
    expect(ids(await setTaskInstanceState(db, runId, 'success', { taskIds: ['extract', 'report'] }))).toEqual([])
    expect((await run(runId)).state).toBe('failed')   // nothing changed → run untouched
  })
})

describe('setRunState', () => {
  it('failed: running instances fail, waiting ones are skipped, finished ones keep their state', async () => {
    const runId = await finishedRun({ extract: 'success', transform: 'running', load: 'queued', report: 'up_for_retry' }, 'running')
    const res = await setRunState(db, runId, 'failed')

    expect(ids(res)).toEqual(['load', 'other', 'report', 'transform'])
    expect(await states(runId)).toEqual({ extract: 'success', transform: 'failed', load: 'skipped', report: 'skipped', other: 'skipped' })
    expect((await run(runId)).state).toBe('failed')
    const [ev] = await waitForEvents('run_state_set', 1)
    expect(ev.metadata).toMatchObject({ state: 'failed', previous_state: 'running' })
  })

  it('success: every instance that is not success / skipped / removed becomes success', async () => {
    const runId = await finishedRun({ ...FAILED_RUN, other: 'skipped' })
    await setRunState(db, runId, 'success')
    expect(await states(runId)).toEqual({ extract: 'success', transform: 'success', load: 'success', report: 'success', other: 'skipped' })
    expect((await run(runId)).state).toBe('success')
  })

  it('dry run changes nothing', async () => {
    const runId = await finishedRun(FAILED_RUN)
    expect(ids(await setRunState(db, runId, 'success', { dryRun: true }))).toEqual(['load', 'report', 'transform'])
    expect((await run(runId)).state).toBe('failed')
  })
})
