import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { checkSlaBreaches, listAlerts, ackAlert } from '../index.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_sla')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('sla_alerts').deleteMany({})
})

const dagWithSla: DagDefinition = {
  id: 'sla_dag',
  schedule: null,
  sla: 1000,  // 1 second SLA
  tasks: { step: { run: async () => {} } },
}

const dagNoSla: DagDefinition = {
  id: 'no_sla_dag',
  schedule: null,
  tasks: { step: { run: async () => {} } },
}

async function insertRun(dagId: string, ageMs: number, state = 'running') {
  const created_at = new Date(Date.now() - ageMs)
  const res = await db.collection('dag_runs').insertOne({ dag_id: dagId, state, created_at })
  return res.insertedId.toString()
}

describe('checkSlaBreaches', () => {
  it('does nothing when no dags have SLA configured', async () => {
    await insertRun('no_sla_dag', 5000)
    await checkSlaBreaches(db, [dagNoSla])
    const alerts = await db.collection('sla_alerts').find({}).toArray()
    expect(alerts).toHaveLength(0)
  })

  it('does nothing when run is within SLA window', async () => {
    await insertRun('sla_dag', 500)  // 500ms old, SLA is 1000ms
    await checkSlaBreaches(db, [dagWithSla])
    const alerts = await db.collection('sla_alerts').find({}).toArray()
    expect(alerts).toHaveLength(0)
  })

  it('fires an alert when run exceeds SLA', async () => {
    const runId = await insertRun('sla_dag', 2000)  // 2s old, SLA is 1s
    await checkSlaBreaches(db, [dagWithSla])
    const alerts = await db.collection('sla_alerts').find({}).toArray()
    expect(alerts).toHaveLength(1)
    expect(alerts[0].dag_id).toBe('sla_dag')
    expect(alerts[0].dag_run_id).toBe(runId)
    expect(alerts[0].sla_ms).toBe(1000)
    expect(alerts[0].elapsed_ms).toBeGreaterThanOrEqual(2000)
    expect(alerts[0].acked).toBe(false)
  })

  it('does not fire duplicate alerts for the same run', async () => {
    await insertRun('sla_dag', 2000)
    await checkSlaBreaches(db, [dagWithSla])
    await checkSlaBreaches(db, [dagWithSla])  // second tick
    const alerts = await db.collection('sla_alerts').find({}).toArray()
    expect(alerts).toHaveLength(1)
  })

  it('does not alert for terminal (success/failed) runs', async () => {
    await insertRun('sla_dag', 2000, 'success')
    await checkSlaBreaches(db, [dagWithSla])
    const alerts = await db.collection('sla_alerts').find({}).toArray()
    expect(alerts).toHaveLength(0)
  })

  it('alerts for multiple different breached runs', async () => {
    await insertRun('sla_dag', 2000)
    await insertRun('sla_dag', 3000)
    await checkSlaBreaches(db, [dagWithSla])
    const alerts = await db.collection('sla_alerts').find({}).toArray()
    expect(alerts).toHaveLength(2)
  })
})

describe('listAlerts', () => {
  it('returns empty array when no alerts exist', async () => {
    const alerts = await listAlerts(db)
    expect(alerts).toHaveLength(0)
  })

  it('returns all alerts by default', async () => {
    await insertRun('sla_dag', 2000)
    await checkSlaBreaches(db, [dagWithSla])
    const alerts = await listAlerts(db)
    expect(alerts).toHaveLength(1)
  })

  it('returns only unacked alerts when unackedOnly: true', async () => {
    await insertRun('sla_dag', 2000)
    await insertRun('sla_dag', 3000)
    await checkSlaBreaches(db, [dagWithSla])

    // Ack the first one
    const all = await listAlerts(db)
    await ackAlert(db, all[0]._id!.toString())

    const unacked = await listAlerts(db, { unackedOnly: true })
    expect(unacked).toHaveLength(1)
    expect(unacked[0].acked).toBe(false)
  })
})

describe('ackAlert', () => {
  it('returns false for non-existent id', async () => {
    const result = await ackAlert(db, new ObjectId().toString())
    expect(result).toBe(false)
  })

  it('returns false for invalid id', async () => {
    const result = await ackAlert(db, 'not-an-id')
    expect(result).toBe(false)
  })

  it('acknowledges an existing alert', async () => {
    await insertRun('sla_dag', 2000)
    await checkSlaBreaches(db, [dagWithSla])
    const all = await listAlerts(db)
    const id = all[0]._id!.toString()

    const result = await ackAlert(db, id)
    expect(result).toBe(true)

    const updated = await db.collection('sla_alerts').findOne({ _id: new ObjectId(id) })
    expect(updated!.acked).toBe(true)
    expect(updated!.acked_at).toBeDefined()
  })
})
