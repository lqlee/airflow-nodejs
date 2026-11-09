import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { adoptClaim } from '../consumer.js'
import { INSTANCE_ID } from '../../scheduler/instance.js'
import type { TaskInstance } from '../../scheduler/runs.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_adopt_claim')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => { await db.collection('task_instances').deleteMany({}) })

/** The task instance as it was when the scheduler claimed it and queued the job. */
const queuedJobTi = (extra: Partial<TaskInstance> = {}) => ({
  dag_run_id: 'r1', dag_id: 'd', task_id: 't', map_index: null, state: 'running', try_number: 0,
  claimed_by: 'scheduler-a', ...extra,
}) as TaskInstance
const row = (extra: Record<string, unknown> = {}) => db.collection('task_instances').findOne({ task_id: 't', ...extra })

describe('adoptClaim', () => {
  it('takes ownership of the running try and returns a filter scoped to it', async () => {
    await db.collection('task_instances').insertOne({ ...queuedJobTi() })

    const owned = await adoptClaim(db, queuedJobTi())

    expect(owned).toEqual({ dag_run_id: 'r1', task_id: 't', map_index: null, state: 'running', claimed_by: INSTANCE_ID })
    expect((await row())!.claimed_by).toBe(INSTANCE_ID)
    // The filter stops matching once the try is no longer ours (e.g. reaped and re-claimed)
    await db.collection('task_instances').updateOne({ task_id: 't' }, { $set: { claimed_by: 'scheduler-b' } })
    expect((await db.collection('task_instances').updateOne(owned!, { $set: { state: 'success' } })).matchedCount).toBe(0)
  })

  it.each([
    ['reaped and requeued', { state: 'queued', claimed_by: null }],
    ['cancelled', { state: 'cancelled' }],
    ['re-claimed by another scheduler', { claimed_by: 'scheduler-b' }],
    ['already on a later try', { try_number: 1 }],
  ])('rejects a stale job when the task was %s', async (_label, current) => {
    await db.collection('task_instances').insertOne({ ...queuedJobTi(), ...current })
    expect(await adoptClaim(db, queuedJobTi())).toBeNull()
    expect((await row())!.claimed_by).not.toBe(INSTANCE_ID)
  })

  it('only adopts the mapped instance the job is for', async () => {
    await db.collection('task_instances').insertMany([{ ...queuedJobTi({ map_index: 0 }) }, { ...queuedJobTi({ map_index: 1 }) }])
    expect(await adoptClaim(db, queuedJobTi({ map_index: 1 }))).not.toBeNull()
    expect((await row({ map_index: 0 }))!.claimed_by).toBe('scheduler-a')
    expect((await row({ map_index: 1 }))!.claimed_by).toBe(INSTANCE_ID)
  })

  it('adopts jobs queued before owner tracking (no claimed_by on either side)', async () => {
    const { claimed_by: _omit, ...legacy } = queuedJobTi()
    await db.collection('task_instances').insertOne({ ...legacy })
    expect(await adoptClaim(db, legacy as TaskInstance)).not.toBeNull()
  })
})
