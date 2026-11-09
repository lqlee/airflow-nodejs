import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { runTickWork } from '../index.js'
import { renewLease, setElectionActive, isLeader } from '../lease.js'
import { createRun } from '../runs.js'
import { activeCronJobCount, activeTimetableCount, stopAllCronJobs, resetAllTimetables, resetCatchupState } from '../cron.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_leader_tick')
})
afterAll(async () => { setElectionActive(false); await db.dropDatabase(); await client.close() })
afterEach(async () => {
  setElectionActive(false)
  stopAllCronJobs()
  resetAllTimetables()
  resetCatchupState()
  for (const c of ['dag_runs', 'task_instances', 'scheduler_leases', 'scheduler_instances', 'event_logs']) await db.collection(c).deleteMany({})
})

const catchupDag: DagDefinition = {
  id: 'lt_catchup', schedule: '0 * * * *', catchup: true,
  startDate: new Date(Date.now() - 3 * 3_600_000).toISOString(),
  tasks: { t: { run: async () => {} } },
}
// A run whose only task waits for approval: advancing it moves the run to 'running' without forking anything
const gateDag: DagDefinition = { id: 'lt_gate', schedule: null, tasks: { gate: { requiresApproval: true, run: async () => {} } } }
const zombie = (taskId: string) => ({
  dag_run_id: 'zr', dag_id: 'zd', task_id: taskId, map_index: null, state: 'running', try_number: 0, max_retries: 0,
  claimed_by: 'dead-replica', started_at: new Date(Date.now() - 3_600_000), ended_at: null, error: null,
})

const becomeFollower = () => setElectionActive(true)   // election running, no lease held
const becomeLeader = async () => { setElectionActive(true); await renewLease(db, 'me', 30_000) }

describe('runTickWork — follower replica', () => {
  it('creates no scheduled runs and reaps nothing, but still advances existing runs', async () => {
    becomeFollower()
    expect(isLeader()).toBe(false)
    await createRun(db, gateDag)
    await db.collection('task_instances').insertOne(zombie('z'))

    await runTickWork(db, [catchupDag, gateDag])

    expect(await db.collection('dag_runs').countDocuments({ dag_id: 'lt_catchup' })).toBe(0)
    expect((await db.collection('task_instances').findOne({ task_id: 'z' }))!.state).toBe('running')
    expect((await db.collection('dag_runs').findOne({ dag_id: 'lt_gate' }))!.state).toBe('running')
  })
})

describe('runTickWork — leader replica', () => {
  it('creates catch-up runs and reaps zombie tasks', async () => {
    await becomeLeader()
    await db.collection('task_instances').insertOne(zombie('z'))

    await runTickWork(db, [catchupDag])

    expect(await db.collection('dag_runs').countDocuments({ dag_id: 'lt_catchup', trigger_type: 'catchup' })).toBeGreaterThanOrEqual(3)
    expect((await db.collection('task_instances').findOne({ task_id: 'z' }))!.state).toBe('failed')
  })

  it('losing leadership stops cron jobs and forgets timetable state', async () => {
    const cronDag: DagDefinition = { id: 'lt_cron', schedule: '0 0 1 1 *', tasks: { t: { run: async () => {} } } }
    const ttDag: DagDefinition = {
      id: 'lt_tt', schedule: null, timetable: () => new Date(Date.now() + 86_400_000), tasks: { t: { run: async () => {} } },
    }
    await becomeLeader()
    await runTickWork(db, [cronDag, ttDag])
    expect(activeCronJobCount()).toBe(1)
    expect(activeTimetableCount()).toBe(1)

    // Lease lost: re-entering the election clears this process's local leadership deadline
    setElectionActive(false)
    becomeFollower()
    await runTickWork(db, [cronDag, ttDag])

    expect(activeCronJobCount()).toBe(0)
    expect(activeTimetableCount()).toBe(0)
  })
})
