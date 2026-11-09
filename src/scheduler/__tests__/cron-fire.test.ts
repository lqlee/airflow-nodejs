import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { ensureIndexes } from '../../db/indexes.js'
import { fireCronRun } from '../cron.js'
import { setElectionActive } from '../lease.js'
import { pauseDag } from '../../dag/pause.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_cron_fire')
  await ensureIndexes(db)
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  setElectionActive(false)
  for (const c of ['dag_runs', 'task_instances', 'dag_paused', 'event_logs']) await db.collection(c).deleteMany({})
})

const dag: DagDefinition = { id: 'cf_dag', schedule: '0 * * * *', tasks: { t: { run: async () => {} } } }
const FIRED = new Date('2026-03-10T08:00:00.412Z')
const fire = (at = FIRED) => fireCronRun(db, dag, '0 * * * *', at, { advance: false })

describe('fireCronRun', () => {
  it('creates a cron run whose logical date is the scheduled minute', async () => {
    const runId = await fire()
    expect(runId).toBeTruthy()
    const run = await db.collection('dag_runs').findOne({ dag_id: 'cf_dag' })
    expect(run).toMatchObject({ trigger_type: 'cron', state: 'queued' })
    expect(new Date(run!.logical_date).toISOString()).toBe('2026-03-10T08:00:00.000Z')
  })

  it('a second firing of the same occurrence (another replica) creates nothing and does not throw', async () => {
    await fire()
    expect(await fire(new Date('2026-03-10T08:00:00.950Z'))).toBeNull()
    expect(await db.collection('dag_runs').countDocuments({ dag_id: 'cf_dag' })).toBe(1)
  })

  it('the next occurrence gets its own run', async () => {
    await fire()
    expect(await fire(new Date('2026-03-10T09:00:00.100Z'))).toBeTruthy()
    expect(await db.collection('dag_runs').countDocuments({ dag_id: 'cf_dag' })).toBe(2)
  })

  it('does nothing on a replica that is not the leader', async () => {
    setElectionActive(true)   // in an election, holding no lease
    expect(await fire()).toBeNull()
    expect(await db.collection('dag_runs').countDocuments()).toBe(0)
  })

  it('skips a paused Dag', async () => {
    await pauseDag(db, 'cf_dag')
    expect(await fire()).toBeNull()
    expect(await db.collection('dag_runs').countDocuments()).toBe(0)
  })

  it('never throws when the database fails', async () => {
    const broken = { collection: () => { throw new Error('db down') } } as unknown as Db
    await expect(fireCronRun(broken, dag, '0 * * * *', FIRED, { advance: false })).resolves.toBeNull()
  })
})
