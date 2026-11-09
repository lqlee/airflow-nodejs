import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { INSTANCE_ID, startLiveness, stopLiveness, shutdownLiveness, listInstances, LEASE_TTL_MS, ZOMBIE_THRESHOLD_MS, HEARTBEAT_INTERVAL_MS } from '../instance.js'
import { isLeader, getLeaseHolder, renewLease, setElectionActive } from '../lease.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_liveness')
})
afterAll(async () => { stopLiveness(); await db.dropDatabase(); await client.close() })
afterEach(async () => {
  stopLiveness()
  setElectionActive(false)
  for (const c of ['scheduler_instances', 'scheduler_leases']) await db.collection(c).deleteMany({})
})

const broken = { collection: () => { throw new Error('db down') } } as unknown as Db

describe('startLiveness', () => {
  it('as a scheduler: registers this process and takes the free leader lease before resolving', async () => {
    await startLiveness(db, { lease: true })

    const [me] = await listInstances(db)
    expect(me).toMatchObject({ _id: INSTANCE_ID, role: 'scheduler', pid: process.pid })
    expect(await getLeaseHolder(db)).toBe(INSTANCE_ID)
    expect(isLeader()).toBe(true)
  })

  it('as a scheduler while another replica leads: registers but does not lead', async () => {
    await renewLease(db, 'other-replica', LEASE_TTL_MS)
    setElectionActive(false)   // that renewal was "another process" — reset this process's view

    await startLiveness(db, { lease: true })

    expect(await getLeaseHolder(db)).toBe('other-replica')
    expect(isLeader()).toBe(false)
  })

  it('as a worker: registers with role worker and never takes the lease', async () => {
    await startLiveness(db, { lease: false, role: 'worker' })
    expect((await listInstances(db))[0]).toMatchObject({ _id: INSTANCE_ID, role: 'worker' })
    expect(await getLeaseHolder(db)).toBeNull()
  })

  it('does not throw when the database is down, and does not claim leadership', async () => {
    await expect(startLiveness(broken, { lease: true })).resolves.toBeUndefined()
    expect(isLeader()).toBe(false)
  })
})

describe('shutdownLiveness', () => {
  it('releases the lease and deregisters, so another replica can lead at once', async () => {
    await startLiveness(db, { lease: true })
    await shutdownLiveness(db)

    expect(await listInstances(db)).toEqual([])
    expect(await getLeaseHolder(db)).toBeNull()
    setElectionActive(false)
    expect(await renewLease(db, 'other-replica', LEASE_TTL_MS)).toBe(true)
  })
})

describe('timing settings', () => {
  it('keep the zombie threshold and lease TTL safely above the heartbeat interval', () => {
    expect(ZOMBIE_THRESHOLD_MS).toBeGreaterThanOrEqual(3 * HEARTBEAT_INTERVAL_MS)
    expect(LEASE_TTL_MS).toBeGreaterThanOrEqual(2 * HEARTBEAT_INTERVAL_MS)
  })
})

describe('renewLease errors', () => {
  it('a database error propagates and drops leadership', async () => {
    setElectionActive(true)
    await renewLease(db, 'me', LEASE_TTL_MS)
    expect(isLeader()).toBe(true)
    await expect(renewLease(broken, 'me', LEASE_TTL_MS)).rejects.toThrow('db down')
    expect(isLeader()).toBe(false)
  })
})
