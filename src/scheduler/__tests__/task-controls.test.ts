/** weightRule, maxActiveTis / maxActiveTisPerRun, onRetry. */
import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createRun, type TaskInstance } from '../runs.js'
import { claimReadyTasks, expandDynamicMapped } from '../claim.js'
import { advanceRun } from '../index.js'
import { scheduleRetry } from '../executor.js'
import { fireRetryCallback } from '../callbacks.js'
import { computeEffectiveWeights } from '../weights.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition, RetryCallbackInfo } from '../../dag/types.js'
import { waitFor } from './wait-for.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_task_controls')
  clearRegistry()
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'task_logs']) await db.collection(c).deleteMany({})
  clearRegistry()
})

const noop = async () => {}

describe('computeEffectiveWeights', () => {
  // a → b → d, a → c → d (diamond): d is reachable from a via two paths
  const diamond = (rule?: 'absolute' | 'downstream' | 'upstream') => ({
    a: { priority: 1, weightRule: rule },
    b: { priority: 2, weightRule: rule, dependsOn: ['a'] },
    c: { priority: 3, weightRule: rule, dependsOn: ['a'] },
    d: { priority: 4, weightRule: rule, dependsOn: ['b', 'c'] },
  })

  it('absolute (default) keeps the task priority', () => {
    expect([...computeEffectiveWeights(diamond())]).toEqual([['a', 1], ['b', 2], ['c', 3], ['d', 4]])
  })
  it('downstream adds each descendant once, not once per path', () => {
    const w = computeEffectiveWeights(diamond('downstream'))
    expect(w.get('a')).toBe(1 + 2 + 3 + 4)   // d counted once despite two paths
    expect(w.get('b')).toBe(2 + 4)
    expect(w.get('d')).toBe(4)
  })
  it('upstream adds each ancestor once', () => {
    const w = computeEffectiveWeights(diamond('upstream'))
    expect(w.get('d')).toBe(4 + 2 + 3 + 1)
    expect(w.get('a')).toBe(1)
  })
  it('ignores unknown dependency ids and tolerates cycles', () => {
    const w = computeEffectiveWeights({
      x: { priority: 1, weightRule: 'downstream', dependsOn: ['ghost', 'y'] },
      y: { priority: 2, weightRule: 'downstream', dependsOn: ['x'] },
    })
    expect(w.get('x')).toBe(1 + 2)
    expect(w.get('y')).toBe(2 + 1)
  })
})

describe('weightRule in claim order', () => {
  it('downstream weight makes the task with more dependents claim first', async () => {
    const dag: DagDefinition = {
      id: 'weight_dag', schedule: null,
      tasks: {
        // same own priority; `heavy` carries its two dependents' weight so it must outrank `light`
        light: { run: noop, weightRule: 'downstream' },
        heavy: { run: noop, weightRule: 'downstream' },
        h1: { priority: 1, dependsOn: ['heavy'], run: noop },
        h2: { priority: 1, dependsOn: ['heavy'], run: noop },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const claimed = await claimReadyTasks(db, runId)
    expect(claimed.map(t => t.task_id)).toEqual(['heavy', 'light'])
  })

  it('stamps the effective weight on the instance', async () => {
    const dag: DagDefinition = {
      id: 'weight_dag2', schedule: null,
      tasks: {
        root: { priority: 5, weightRule: 'downstream', run: noop },
        leaf: { priority: 7, dependsOn: ['root'], run: noop },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const root = await db.collection<TaskInstance>('task_instances').findOne({ dag_run_id: runId, task_id: 'root' })
    const leaf = await db.collection<TaskInstance>('task_instances').findOne({ dag_run_id: runId, task_id: 'leaf' })
    expect(root?.priority).toBe(12)
    expect(leaf?.priority).toBe(7)
  })
})

describe('maxActiveTisPerRun', () => {
  const dag: DagDefinition = {
    id: 'per_run_dag', schedule: null,
    tasks: { fan: { expand: [1, 2, 3, 4, 5], maxActiveTisPerRun: 2, run: noop } },
  }

  it('claims at most N mapped instances at once and releases slots as they finish', async () => {
    register(dag)
    const runId = await createRun(db, dag)
    expect(await claimReadyTasks(db, runId)).toHaveLength(2)
    expect(await claimReadyTasks(db, runId)).toHaveLength(0)  // both still running

    await db.collection('task_instances').updateOne({ dag_run_id: runId, state: 'running' }, { $set: { state: 'success' } })
    expect(await claimReadyTasks(db, runId)).toHaveLength(1)
  })

  it('limits survive dynamic expansion (expand: {from, key})', async () => {
    const dyn: DagDefinition = {
      id: 'per_run_dynamic', schedule: null,
      tasks: {
        src: { run: noop },
        fan: { dependsOn: ['src'], expand: { from: 'src', key: 'items' }, maxActiveTisPerRun: 2, priority: 4, run: noop },
      },
    }
    register(dyn)
    const runId = await createRun(db, dyn)
    await db.collection('xcoms').insertOne({ dag_run_id: runId, task_id: 'src', key: 'items', value: [1, 2, 3, 4] })
    await db.collection('task_instances').updateOne({ dag_run_id: runId, task_id: 'src' }, { $set: { state: 'success' } })

    expect(await expandDynamicMapped(db, runId)).toBe(4)
    const claimed = await claimReadyTasks(db, runId)
    expect(claimed).toHaveLength(2)
    expect(claimed[0]).toMatchObject({ max_active_tis_per_run: 2, priority: 4 })
    await db.collection('xcoms').deleteMany({ dag_run_id: runId })
  })

  it('does not cap tasks that set no limit', async () => {
    const free: DagDefinition = { id: 'per_run_free', schedule: null, tasks: { fan: { expand: [1, 2, 3], run: noop } } }
    register(free)
    const runId = await createRun(db, free)
    expect(await claimReadyTasks(db, runId)).toHaveLength(3)
  })
})

describe('maxActiveTis (across runs)', () => {
  const dag: DagDefinition = {
    id: 'per_dag_dag', schedule: null,
    tasks: { t: { maxActiveTis: 1, run: noop }, free: { run: noop } },
  }

  it('a second run waits while another run holds the only slot', async () => {
    register(dag)
    const run1 = await createRun(db, dag)
    const run2 = await createRun(db, dag)

    expect((await claimReadyTasks(db, run1)).map(t => t.task_id).sort()).toEqual(['free', 't'])
    // run2: `t` blocked by run1's running instance, uncapped `free` still claims
    expect((await claimReadyTasks(db, run2)).map(t => t.task_id)).toEqual(['free'])

    await db.collection('task_instances').updateOne({ dag_run_id: run1, task_id: 't' }, { $set: { state: 'success' } })
    expect((await claimReadyTasks(db, run2)).map(t => t.task_id)).toEqual(['t'])
  })

  it('ignores invalid limits (0, negative, NaN) as unlimited', async () => {
    const bad: DagDefinition = {
      id: 'bad_limits', schedule: null,
      tasks: { a: { maxActiveTis: 0, run: noop }, b: { maxActiveTis: -3, run: noop }, c: { maxActiveTisPerRun: NaN, run: noop } },
    }
    register(bad)
    const r1 = await createRun(db, bad)
    const r2 = await createRun(db, bad)
    expect(await claimReadyTasks(db, r1)).toHaveLength(3)
    expect(await claimReadyTasks(db, r2)).toHaveLength(3)
  })
})

describe('onRetry', () => {
  it('fires for each retried failure with the attempt number, not for the final failure', async () => {
    const calls: RetryCallbackInfo[] = []
    const dag: DagDefinition = {
      id: 'on_retry_dag', schedule: null,
      tasks: {
        flaky: { retries: 2, run: async () => { throw new Error('nope') }, onRetry: info => { calls.push(info) } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    for (let i = 0; i < 40; i++) await advanceRun(db, runId)

    await waitFor(async () => calls.length, n => n >= 2)
    await new Promise(r => setTimeout(r, 100))  // would catch a stray 3rd call
    expect(calls.map(c => c.tryNumber)).toEqual([1, 2])
    expect(calls[0]).toMatchObject({ dagId: 'on_retry_dag', runId, taskId: 'flaky', maxRetries: 2, mapIndex: null })
    expect(calls[0].error).toContain('nope')
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId })
    expect(ti?.state).toBe('failed')
  })

  it('a throwing or async-rejecting callback never blocks the retry', async () => {
    for (const cb of [
      () => { throw new Error('cb sync boom') },
      async () => { throw new Error('cb async boom') },
    ]) {
      const dag: DagDefinition = {
        id: 'on_retry_throws', schedule: null,
        tasks: { t: { retries: 1, run: noop, onRetry: cb } },
      }
      register(dag)
      const runId = await createRun(db, dag)
      const ti = (await db.collection<TaskInstance>('task_instances').findOneAndUpdate(
        { dag_run_id: runId }, { $set: { state: 'running' } }, { returnDocument: 'after' }))!
      await scheduleRetry(db, ti, 'failed once')
      const after = await db.collection('task_instances').findOne({ dag_run_id: runId })
      expect(after).toMatchObject({ state: 'queued', try_number: 1, error: 'failed once' })
      clearRegistry()
    }
  })

  it('tasks without onRetry retry normally', async () => {
    const dag: DagDefinition = { id: 'no_cb', schedule: null, tasks: { t: { retries: 1, run: noop } } }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = (await db.collection<TaskInstance>('task_instances').findOneAndUpdate(
      { dag_run_id: runId }, { $set: { state: 'running' } }, { returnDocument: 'after' }))!
    await scheduleRetry(db, ti, 'e')
    expect((await db.collection('task_instances').findOne({ dag_run_id: runId }))?.state).toBe('queued')
  })
})

describe('maxActiveTis — isolation and real execution', () => {
  it('a same-named task in a different Dag does not consume the slot', async () => {
    const a: DagDefinition = { id: 'iso_a', schedule: null, tasks: { t: { maxActiveTis: 1, run: noop } } }
    const b: DagDefinition = { id: 'iso_b', schedule: null, tasks: { t: { maxActiveTis: 1, run: noop } } }
    register(a); register(b)
    const ra = await createRun(db, a)
    const rb = await createRun(db, b)
    expect(await claimReadyTasks(db, ra)).toHaveLength(1)
    expect(await claimReadyTasks(db, rb)).toHaveLength(1)
  })

  it('a capped mapped task never has more than N instances running at the same time', async () => {
    const dag: DagDefinition = {
      id: 'cap_exec', schedule: null,
      tasks: { fan: { expand: [1, 2, 3, 4, 5], maxActiveTisPerRun: 2, run: async () => { await new Promise(r => setTimeout(r, 250)) } } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    for (let i = 0; i < 60; i++) {
      await advanceRun(db, runId)
      const done = await db.collection('task_instances').countDocuments({ dag_run_id: runId, state: 'success' })
      if (done === 5) break
    }
    const tis = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    expect(tis.map(t => t.state)).toEqual(Array(5).fill('success'))
    // Peak overlap from recorded start/end times
    const events = tis.flatMap(t => [[t.started_at.getTime(), 1], [t.ended_at.getTime(), -1]] as const)
      .sort((x, y) => x[0] - y[0] || x[1] - y[1])  // ends before starts at equal timestamps
    let cur = 0, peak = 0
    for (const [, d] of events) { cur += d; peak = Math.max(peak, cur) }
    expect(peak).toBeLessThanOrEqual(2)
    expect(peak).toBe(2)  // and the cap is actually used, not serialised
  })
})

describe('onRetry — details', () => {
  it('passes mapIndex for a mapped task instance', async () => {
    const calls: RetryCallbackInfo[] = []
    const dag: DagDefinition = {
      id: 'on_retry_mapped', schedule: null,
      tasks: { m: { expand: ['x', 'y'], retries: 1, run: noop, onRetry: i => { calls.push(i) } } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    const ti = (await db.collection<TaskInstance>('task_instances').findOneAndUpdate(
      { dag_run_id: runId, map_index: 1 }, { $set: { state: 'running' } }, { returnDocument: 'after' }))!
    await scheduleRetry(db, ti, 'e')
    await waitFor(async () => calls.length, n => n >= 1)
    expect(calls).toHaveLength(1)
    expect(calls[0]).toMatchObject({ mapIndex: 1, tryNumber: 1, maxRetries: 1 })
  })

  it('a hung callback is abandoned after the timeout without throwing', async () => {
    vi.useFakeTimers()
    try {
      const dag: DagDefinition = {
        id: 'on_retry_hang', schedule: null,
        tasks: { t: { retries: 1, run: noop, onRetry: () => new Promise<void>(() => { /* never settles */ }) } },
      }
      register(dag)
      const ti = { dag_id: 'on_retry_hang', dag_run_id: 'r', task_id: 't', map_index: null, try_number: 0, max_retries: 1 } as TaskInstance
      let settled = false
      const p = fireRetryCallback(ti, 'e').then(() => { settled = true })
      await vi.advanceTimersByTimeAsync(9_999)
      expect(settled).toBe(false)
      await vi.advanceTimersByTimeAsync(2)
      await p
      expect(settled).toBe(true)
    } finally {
      vi.useRealTimers()
    }
  })

  it('is a no-op for an unknown Dag or a task without a callback', async () => {
    const ti = { dag_id: 'nope', dag_run_id: 'r', task_id: 't', map_index: null, try_number: 0, max_retries: 1 } as TaskInstance
    await expect(fireRetryCallback(ti, 'e')).resolves.toBeUndefined()
  })
})
