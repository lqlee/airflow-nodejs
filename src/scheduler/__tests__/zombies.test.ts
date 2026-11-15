import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { reapZombies, releaseOwnedTasks } from '../zombies.js'
import { INSTANCE_ID, heartbeat, listLiveInstanceIds } from '../instance.js'
import { createRun } from '../runs.js'
import { claimReadyTasks } from '../claim.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_zombies')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  for (const c of ['task_instances', 'task_instance_tries', 'scheduler_instances', 'dag_runs']) await db.collection(c).deleteMany({})
})

const NOW = new Date('2026-06-01T12:00:00Z')
const THRESHOLD = 60_000
const ago = (ms: number) => new Date(NOW.getTime() - ms)

/** Register another process with a heartbeat at `at` (bypasses heartbeat(), which always writes INSTANCE_ID). */
const registerPeer = (id: string, at: Date) =>
  db.collection('scheduler_instances').insertOne({ _id: id as never, hostname: 'h', pid: 1, role: 'scheduler', started_at: at, heartbeat_at: at })

const runningTi = (taskId: string, extra: Record<string, unknown> = {}) => ({
  dag_run_id: 'r1', dag_id: 'd', task_id: taskId, map_index: null, state: 'running',
  try_number: 0, max_retries: 0, started_at: ago(5 * 60_000), ended_at: null, error: null, ...extra,
})
const ti = (taskId: string) => db.collection('task_instances').findOne({ task_id: taskId })
const reap = () => reapZombies(db, { now: NOW, thresholdMs: THRESHOLD })

describe('reapZombies', () => {
  it('retries a task whose owner stopped heartbeating, recording the lost try as failed', async () => {
    await registerPeer('dead-1', ago(2 * THRESHOLD))
    await db.collection('task_instances').insertOne(runningTi('t', { claimed_by: 'dead-1', max_retries: 2, retry_delay: 15_000 }))

    expect(await reap()).toEqual({ retried: 1, failed: 0, requeued: 0 })

    const after = await ti('t')
    expect(after).toMatchObject({ state: 'up_for_retry', try_number: 1, claimed_by: null, ended_at: NOW })
    expect(after!.next_retry_at).toEqual(new Date(NOW.getTime() + 15_000))   // the task's retry delay
    expect(after!.error).toMatch(/heartbeat lost.*dead-1/)
    const tries = await db.collection('task_instance_tries').find({ task_id: 't' }).toArray()
    expect(tries).toHaveLength(1)
    expect(tries[0]).toMatchObject({ try_number: 0, state: 'failed' })
  })

  it('fails the task when its retries are used up', async () => {
    await db.collection('task_instances').insertOne(runningTi('t', { claimed_by: 'gone', try_number: 1, max_retries: 1 }))

    expect(await reap()).toEqual({ retried: 0, failed: 1, requeued: 0 })
    const after = await ti('t')
    expect(after).toMatchObject({ state: 'failed', try_number: 1 })
    expect(after!.ended_at).toEqual(NOW)
  })

  it('leaves tasks of live owners, of this process, and ones claimed within the threshold', async () => {
    await registerPeer('alive', ago(THRESHOLD / 2))
    await db.collection('task_instances').insertMany([
      runningTi('live_owner', { claimed_by: 'alive' }),
      runningTi('mine', { claimed_by: INSTANCE_ID }),                                  // no heartbeat doc needed
      runningTi('fresh', { claimed_by: 'unknown', started_at: ago(THRESHOLD / 2) }),   // just claimed
    ])

    expect(await reap()).toEqual({ retried: 0, failed: 0, requeued: 0 })
    for (const id of ['live_owner', 'mine', 'fresh']) expect((await ti(id))!.state).toBe('running')
  })

  it('ignores tasks that are not running', async () => {
    await db.collection('task_instances').insertMany([
      runningTi('q', { state: 'queued', claimed_by: 'gone' }),
      runningTi('d', { state: 'deferred', claimed_by: 'gone' }),
    ])
    expect(await reap()).toEqual({ retried: 0, failed: 0, requeued: 0 })
  })

  it('requeues legacy rows with no owner without spending a retry check (old startup recovery)', async () => {
    await db.collection('task_instances').insertOne(runningTi('legacy', { max_retries: 0, try_number: 3 }))

    expect(await reap()).toEqual({ retried: 0, failed: 0, requeued: 1 })
    expect(await ti('legacy')).toMatchObject({ state: 'queued', try_number: 4, started_at: null })
    expect(await db.collection('task_instance_tries').countDocuments()).toBe(0)
  })

  it('leaves a task alone if it finished between the zombie scan and the update', async () => {
    await db.collection('task_instances').insertOne(runningTi('racing', { claimed_by: 'gone' }))
    // The owner was only slow: its success lands right after the reaper read the candidates
    const racing = new Proxy(db, {
      get(target, prop) {
        if (prop !== 'collection') return Reflect.get(target, prop)
        return (name: string) => {
          const coll = target.collection(name)
          if (name !== 'task_instances') return coll
          return new Proxy(coll, {
            get(c, p) {
              if (p === 'find') {
                return (...args: Parameters<typeof c.find>) => ({
                  toArray: async () => {
                    const docs = await c.find(...args).toArray()
                    await c.updateOne({ task_id: 'racing' }, { $set: { state: 'success' } })
                    return docs
                  },
                })
              }
              const v = Reflect.get(c, p)
              return typeof v === 'function' ? v.bind(c) : v
            },
          })
        }
      },
    })

    expect(await reapZombies(racing, { now: NOW, thresholdMs: THRESHOLD })).toEqual({ retried: 0, failed: 0, requeued: 0 })
    expect((await ti('racing'))!.state).toBe('success')
    expect(await db.collection('task_instance_tries').countDocuments()).toBe(0)
  })

  it('labels mapped instances and handles each one independently', async () => {
    await db.collection('task_instances').insertMany([
      runningTi('m', { map_index: 0, claimed_by: 'gone', max_retries: 1 }),
      runningTi('m', { map_index: 1, claimed_by: 'gone', max_retries: 0 }),
    ])
    expect(await reap()).toEqual({ retried: 1, failed: 1, requeued: 0 })
    expect((await db.collection('task_instances').findOne({ task_id: 'm', map_index: 0 }))!.state).toBe('up_for_retry')
    expect((await db.collection('task_instances').findOne({ task_id: 'm', map_index: 1 }))!.state).toBe('failed')
    const tries = await db.collection('task_instance_tries').find({ task_id: 'm' }).sort({ map_index: 1 }).toArray()
    expect(tries.map(t => t.map_index)).toEqual([0, 1])
  })

  it('forgets instances silent for more than 10× the threshold', async () => {
    await registerPeer('ancient', ago(11 * THRESHOLD))
    await registerPeer('recent', ago(2 * THRESHOLD))
    await reap()
    const ids = (await db.collection('scheduler_instances').find({}).toArray()).map(d => d._id)
    expect(ids).toEqual(['recent'])
  })
})

describe('claim ownership', () => {
  it('claimReadyTasks stamps claimed_by with this process id', async () => {
    const dag: DagDefinition = { id: 'own_dag', schedule: null, tasks: { a: { run: async () => {} } } }
    const runId = await createRun(db, dag)
    const [claimed] = await claimReadyTasks(db, runId)
    expect(claimed.claimed_by).toBe(INSTANCE_ID)
    expect((await db.collection('task_instances').findOne({ dag_run_id: runId }))!.claimed_by).toBe(INSTANCE_ID)
  })
})

describe('releaseOwnedTasks', () => {
  it('hands back only this process’s running tasks, without a retry check', async () => {
    await db.collection('task_instances').insertMany([
      runningTi('mine', { claimed_by: INSTANCE_ID, try_number: 0, max_retries: 0 }),
      runningTi('theirs', { claimed_by: 'other' }),
      runningTi('done', { claimed_by: INSTANCE_ID, state: 'success' }),
    ])

    expect(await releaseOwnedTasks(db)).toBe(1)
    expect(await ti('mine')).toMatchObject({ state: 'queued', try_number: 1, claimed_by: null })
    expect((await ti('theirs'))!.state).toBe('running')
    expect((await ti('done'))!.state).toBe('success')
  })
})

describe('instance registry', () => {
  it('heartbeat registers this process and listLiveInstanceIds respects the cutoff', async () => {
    await heartbeat(db, 'scheduler', NOW)
    await registerPeer('stale', ago(2 * THRESHOLD))
    expect(await listLiveInstanceIds(db, ago(THRESHOLD))).toEqual([INSTANCE_ID])

    await heartbeat(db, 'scheduler', new Date(NOW.getTime() + 1000))   // upsert, not a second doc
    expect(await db.collection('scheduler_instances').countDocuments({ _id: INSTANCE_ID as never })).toBe(1)
  })
})
