/** TriggerDagRun and ExternalTaskSensor. Both run in the scheduler process (no worker). */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun, cancelRun } from '../index.js'
import { pollDeferredTasks } from '../executor.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_cross_dag')
  clearRegistry()
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'xcoms', 'event_logs']) await db.collection(c).deleteMany({})
  clearRegistry()
})

const noop = { run: async () => {} }
const child: DagDefinition = { id: 'child_dag', schedule: null, tasks: { work: noop, other: noop } }

const ti = async (runId: string, taskId: string) =>
  (await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: taskId }))!
const runDoc = async (runId: string) => (await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) }))!
const setRunState = (runId: string, state: string) =>
  db.collection('dag_runs').updateOne({ _id: new ObjectId(runId) }, { $set: { state } })
/** Make deferred tasks due now, then poll once. */
async function poll() {
  await db.collection('task_instances').updateMany({ state: 'deferred' }, { $set: { next_poke_at: new Date(Date.now() - 1000) } })
  await pollDeferredTasks(db)
}
const parent = (task: DagDefinition['tasks'][string]): DagDefinition => ({ id: 'parent_dag', schedule: null, tasks: { t: task } })

describe('TriggerDagRun', () => {
  it('fire-and-forget: creates a run of the target with conf, links it, succeeds immediately', async () => {
    const dag = parent({ triggerDag: { dagId: 'child_dag', conf: { env: 'prod', n: 3 } } })
    register(dag); register(child)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    expect((await ti(runId, 't')).state).toBe('success')
    expect((await runDoc(runId)).state).toBe('success')

    const childRun = await db.collection('dag_runs').findOne({ dag_id: 'child_dag' })
    expect(childRun).toMatchObject({
      conf: { env: 'prod', n: 3 }, trigger_type: 'triggered',
      triggered_by: { dag_id: 'parent_dag', dag_run_id: runId, task_id: 't' },
    })
    expect(await db.collection('task_instances').countDocuments({ dag_run_id: childRun!._id.toString() })).toBe(2)

    const x = await db.collection('xcoms').findOne({ dag_run_id: runId, task_id: 't', key: 'triggered_run_id' })
    expect(x!.value).toBe(childRun!._id.toString())
  })

  it('fails when the target Dag is not registered', async () => {
    const dag = parent({ triggerDag: { dagId: 'ghost' } })
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    expect(await ti(runId, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/unknown Dag 'ghost'/) })
    expect(await db.collection('dag_runs').countDocuments({ dag_id: 'ghost' })).toBe(0)
  })

  it('refuses to trigger itself', async () => {
    const dag = parent({ triggerDag: { dagId: 'parent_dag' } })
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    expect(await ti(runId, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/cannot trigger itself/) })
    expect(await db.collection('dag_runs').countDocuments({ dag_id: 'parent_dag' })).toBe(1)
  })

  describe('waitForCompletion', () => {
    async function start(extra: Partial<DagDefinition['tasks'][string]> = {}) {
      const dag = parent({ triggerDag: { dagId: 'child_dag', waitForCompletion: true }, ...extra })
      register(dag); register(child)
      const runId = await createRun(db, dag)
      await advanceRun(db, runId)
      const childRunId = (await db.collection('dag_runs').findOne({ dag_id: 'child_dag' }))!._id.toString()
      return { runId, childRunId }
    }

    it('parks the task as deferred while the triggered run is in flight', async () => {
      const { runId } = await start()
      expect(await ti(runId, 't')).toMatchObject({ state: 'deferred', pool: null })
      expect((await runDoc(runId)).state).toBe('running')   // parent run not finalized
    })

    it('succeeds once the triggered run succeeds, then the parent run completes', async () => {
      const { runId, childRunId } = await start()
      await poll()
      expect((await ti(runId, 't')).state).toBe('deferred')   // child still queued → keep waiting

      await setRunState(childRunId, 'success')
      await poll()
      expect((await ti(runId, 't')).state).toBe('success')
      await advanceRun(db, runId)
      expect((await runDoc(runId)).state).toBe('success')
    })

    it.each(['failed', 'cancelled'])('fails the task when the triggered run is %s', async (terminal) => {
      const { runId, childRunId } = await start()
      await setRunState(childRunId, terminal)
      await poll()
      expect(await ti(runId, 't')).toMatchObject({ state: 'failed', error: expect.stringContaining(`${terminal}`) })
      await advanceRun(db, runId)
      expect((await runDoc(runId)).state).toBe('failed')
    })

    it('fails when the deadline (task timeout) passes before the triggered run finishes', async () => {
      const { runId } = await start({ timeout: 50 })
      await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 't' },
        { $set: { deferred_at: new Date(Date.now() - 5000) } })
      await poll()
      expect(await ti(runId, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/Timed out after 50ms/) })
    })

    it('fails if the triggered run was deleted', async () => {
      const { runId, childRunId } = await start()
      await db.collection('dag_runs').deleteOne({ _id: new ObjectId(childRunId) })
      await poll()
      expect(await ti(runId, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/no longer exists/) })
    })
  })
})

describe('ExternalTaskSensor', () => {
  const sensor = (cfg: NonNullable<DagDefinition['tasks'][string]['externalTask']>, extra = {}) => {
    const dag = parent({ externalTask: cfg, ...extra })
    register(dag); register(child)
    return dag
  }
  const start = async (dag: DagDefinition, opts = {}) => {
    const runId = await createRun(db, dag, opts)
    await advanceRun(db, runId)
    return runId
  }

  it('waits (deferred) when the external Dag has no run yet', async () => {
    const runId = await start(sensor({ dagId: 'child_dag' }))
    expect(await ti(runId, 't')).toMatchObject({ state: 'deferred' })
  })

  it('succeeds immediately when the latest external run already succeeded', async () => {
    const dag = sensor({ dagId: 'child_dag' })
    const ext = await createRun(db, child)
    await setRunState(ext, 'success')
    const runId = await start(dag)
    expect((await ti(runId, 't')).state).toBe('success')
  })

  it('keeps waiting while the external run is running, succeeds when it does', async () => {
    const dag = sensor({ dagId: 'child_dag' })
    const ext = await createRun(db, child)
    await setRunState(ext, 'running')
    const runId = await start(dag)
    await poll()
    expect((await ti(runId, 't')).state).toBe('deferred')

    await setRunState(ext, 'success')
    await poll()
    expect((await ti(runId, 't')).state).toBe('success')
  })

  it('a failed external run only fails the sensor when failedStates says so', async () => {
    const ext = await createRun(db, child)
    await setRunState(ext, 'failed')

    const lenient = await start(sensor({ dagId: 'child_dag' }))
    expect((await ti(lenient, 't')).state).toBe('deferred')   // Airflow default: keep waiting

    clearRegistry()
    const strict = await start(sensor({ dagId: 'child_dag', failedStates: ['failed'] }))
    expect(await ti(strict, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/'failed'/) })
  })

  it('task-level: waits on one task of the external run, ignoring others', async () => {
    const dag = sensor({ dagId: 'child_dag', taskId: 'work' })
    const ext = await createRun(db, child)
    await db.collection('task_instances').updateOne({ dag_run_id: ext, task_id: 'other' }, { $set: { state: 'failed' } })
    await db.collection('task_instances').updateOne({ dag_run_id: ext, task_id: 'work' }, { $set: { state: 'running' } })
    const runId = await start(dag)
    expect((await ti(runId, 't')).state).toBe('deferred')

    await db.collection('task_instances').updateOne({ dag_run_id: ext, task_id: 'work' }, { $set: { state: 'success' } })
    await poll()
    expect((await ti(runId, 't')).state).toBe('success')   // 'other' failing is irrelevant
  })

  it('honours allowedStates (e.g. accept skipped)', async () => {
    const dag = sensor({ dagId: 'child_dag', taskId: 'work', allowedStates: ['success', 'skipped'] })
    const ext = await createRun(db, child)
    await db.collection('task_instances').updateOne({ dag_run_id: ext, task_id: 'work' }, { $set: { state: 'skipped' } })
    const runId = await start(dag)
    expect((await ti(runId, 't')).state).toBe('success')
  })

  it("match 'logical_date' only looks at the external run with this run's logical_date", async () => {
    const d1 = new Date('2026-03-01T00:00:00Z'), d2 = new Date('2026-03-02T00:00:00Z')
    const dag = sensor({ dagId: 'child_dag', match: 'logical_date' })
    const other = await createRun(db, child, { logicalDate: d1 })
    await setRunState(other, 'success')                       // a different date succeeded — must not count
    const runId = await start(dag, { logicalDate: d2 })
    expect((await ti(runId, 't')).state).toBe('deferred')

    const same = await createRun(db, child, { logicalDate: d2 })
    await setRunState(same, 'success')
    await poll()
    expect((await ti(runId, 't')).state).toBe('success')
  })

  it("match 'logical_date' fails clearly when this run has no logical_date", async () => {
    const runId = await start(sensor({ dagId: 'child_dag', match: 'logical_date' }))
    expect(await ti(runId, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/needs this run to have a logical_date/) })
  })

  it('fails fast on an unregistered Dag or unknown task instead of timing out', async () => {
    const a = await start(sensor({ dagId: 'ghost' }))
    expect(await ti(a, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/Dag 'ghost' is not registered/) })
    clearRegistry()
    const b = await start(sensor({ dagId: 'child_dag', taskId: 'nope' }))
    expect(await ti(b, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/task 'nope' not found/) })
  })

  it('times out after sensorTimeout', async () => {
    const runId = await start(sensor({ dagId: 'child_dag' }, { sensorTimeout: 50 }))
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 't' },
      { $set: { deferred_at: new Date(Date.now() - 5000) } })
    await poll()
    expect(await ti(runId, 't')).toMatchObject({ state: 'failed', error: expect.stringMatching(/Timed out after 50ms/) })
  })
})

describe('cancelling a run while a cross-Dag task is waiting', () => {
  it('cancels the deferred task; later polls never flip it to success', async () => {
    const dag = parent({ triggerDag: { dagId: 'child_dag', waitForCompletion: true } })
    register(dag); register(child)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    expect((await ti(runId, 't')).state).toBe('deferred')

    expect(await cancelRun(db, runId)).toBe(true)
    const childRunId = (await db.collection('dag_runs').findOne({ dag_id: 'child_dag' }))!._id.toString()
    await setRunState(childRunId, 'success')   // would satisfy the wait if it were still polled
    await poll()

    expect((await ti(runId, 't')).state).toBe('cancelled')
    expect((await runDoc(runId)).state).toBe('cancelled')
  })
})
