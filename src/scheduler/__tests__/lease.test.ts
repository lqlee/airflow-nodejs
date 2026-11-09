import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { renewLease, releaseLease, getLeaseHolder, isLeader, setElectionActive } from '../lease.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_lease')
})
afterAll(async () => { setElectionActive(false); await db.dropDatabase(); await client.close() })
afterEach(async () => { setElectionActive(false); await db.collection('scheduler_leases').deleteMany({}) })

const TTL = 30_000
const T0 = new Date('2026-06-01T12:00:00Z')
const at = (ms: number) => new Date(T0.getTime() + ms)

describe('renewLease', () => {
  it('the first holder wins; others lose while the lease is live', async () => {
    expect(await renewLease(db, 'a', TTL, T0)).toBe(true)
    expect(await renewLease(db, 'b', TTL, at(1_000))).toBe(false)
    expect(await getLeaseHolder(db, at(1_000))).toBe('a')
  })

  it('the holder can renew, pushing the expiry forward', async () => {
    await renewLease(db, 'a', TTL, T0)
    expect(await renewLease(db, 'a', TTL, at(20_000))).toBe(true)
    // Would have expired at T0+30s without the renewal
    expect(await renewLease(db, 'b', TTL, at(40_000))).toBe(false)
  })

  it('another replica takes over once the lease expires', async () => {
    await renewLease(db, 'a', TTL, T0)
    expect(await renewLease(db, 'b', TTL, at(TTL + 1))).toBe(true)
    expect(await renewLease(db, 'a', TTL, at(TTL + 2))).toBe(false)
    expect(await getLeaseHolder(db, at(TTL + 2))).toBe('b')
  })

  it('exactly one of several simultaneous contenders wins a free lease', async () => {
    const wins = await Promise.all(['a', 'b', 'c', 'd', 'e'].map(h => renewLease(db, h, TTL, T0)))
    expect(wins.filter(Boolean)).toHaveLength(1)
  })
})

describe('releaseLease', () => {
  it('only the holder can release; afterwards anyone can take it immediately', async () => {
    await renewLease(db, 'a', TTL, T0)
    await releaseLease(db, 'b')
    expect(await getLeaseHolder(db, T0)).toBe('a')

    await releaseLease(db, 'a')
    expect(await getLeaseHolder(db, T0)).toBeNull()
    expect(await renewLease(db, 'b', TTL, at(1))).toBe(true)
  })
})

describe('isLeader', () => {
  it('is true for a standalone process that never joined an election', () => {
    setElectionActive(false)
    expect(isLeader()).toBe(true)
  })

  it('follows the outcome of the last renewal once the election is active', async () => {
    setElectionActive(true)
    expect(isLeader()).toBe(false)
    await renewLease(db, 'me', TTL, new Date())
    expect(isLeader()).toBe(true)
    await renewLease(db, 'other', TTL, new Date())   // loses — `me` holds it
    expect(isLeader()).toBe(false)                   // this process's view tracks its latest attempt
  })
})
