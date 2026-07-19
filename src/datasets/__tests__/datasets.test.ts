import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import {
  evaluateConsumers,
  emitOutlets,
  triggerDatasetConsumers,
  listDatasetEvents,
  type LatestEvents,
  type Watermarks,
} from '../index.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const noop = async () => {}

// ── Fixtures ────────────────────────────────────────────────────────────────

const producerDag: DagDefinition = {
  id: 'raw_loader',
  schedule: '0 6 * * *',
  outlets: ['pg://warehouse/raw_users', 'pg://warehouse/raw_orders'],
  tasks: { load: { run: noop } },
}

const consumerA: DagDefinition = {
  id: 'user_report',
  schedule: null,
  datasets: ['pg://warehouse/raw_users'],
  tasks: { report: { run: noop } },
}

const consumerAnd: DagDefinition = {
  id: 'join_report',
  schedule: null,
  datasets: ['pg://warehouse/raw_users', 'pg://warehouse/raw_orders'],
  tasks: { join: { run: noop } },
}

const plainDag: DagDefinition = {
  id: 'plain',
  schedule: '0 * * * *',
  tasks: { step: { run: noop } },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_datasets')
  clearRegistry()
  register(producerDag)
  register(consumerA)
  register(consumerAnd)
  register(plainDag)
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('dataset_events').deleteMany({})
  await db.collection('dataset_watermarks').deleteMany({})
})

// ── evaluateConsumers (pure) ────────────────────────────────────────────────

describe('evaluateConsumers — pure logic', () => {
  const consumers = [consumerA, consumerAnd]

  it('fires when single-dataset consumer has a new event', () => {
    const events: LatestEvents = { 'pg://warehouse/raw_users': 'evt001' }
    const watermarks: Watermarks = { 'user_report::pg://warehouse/raw_users': 'evt000' }
    const decisions = evaluateConsumers(consumers, events, watermarks)
    expect(decisions.find(d => d.dag.id === 'user_report')).toBeDefined()
  })

  it('does not fire when watermark matches latest event (no new events)', () => {
    const events: LatestEvents = { 'pg://warehouse/raw_users': 'evt001' }
    const watermarks: Watermarks = { 'user_report::pg://warehouse/raw_users': 'evt001' }
    const decisions = evaluateConsumers(consumers, events, watermarks)
    expect(decisions.find(d => d.dag.id === 'user_report')).toBeUndefined()
  })

  it('fires AND-consumer only when BOTH datasets have new events', () => {
    const events: LatestEvents = {
      'pg://warehouse/raw_users': 'evt002',
      'pg://warehouse/raw_orders': 'evt003',
    }
    const watermarks: Watermarks = {
      'join_report::pg://warehouse/raw_users': 'evt001',
      'join_report::pg://warehouse/raw_orders': 'evt001',
    }
    const decisions = evaluateConsumers(consumers, events, watermarks)
    expect(decisions.find(d => d.dag.id === 'join_report')).toBeDefined()
  })

  it('does NOT fire AND-consumer when only one dataset is updated', () => {
    const events: LatestEvents = {
      'pg://warehouse/raw_users': 'evt002',  // new
      'pg://warehouse/raw_orders': 'evt001', // same as watermark
    }
    const watermarks: Watermarks = {
      'join_report::pg://warehouse/raw_users': 'evt001',
      'join_report::pg://warehouse/raw_orders': 'evt001',
    }
    const decisions = evaluateConsumers(consumers, events, watermarks)
    expect(decisions.find(d => d.dag.id === 'join_report')).toBeUndefined()
  })

  it('does not fire on first boot — returns seed decision with shouldFire=false', () => {
    const events: LatestEvents = { 'pg://warehouse/raw_users': 'evt001' }
    const watermarks: Watermarks = {}  // no watermark yet
    const decisions = evaluateConsumers(consumers, events, watermarks)
    // user_report should get a seed decision, not a fire decision
    const d = decisions.find(d => d.dag.id === 'user_report')
    expect(d).toBeDefined()
    expect((d as { shouldFire?: boolean }).shouldFire).toBe(false)
  })

  it('does not fire when no events exist for the dataset', () => {
    const events: LatestEvents = {}
    const watermarks: Watermarks = {}
    const decisions = evaluateConsumers(consumers, events, watermarks)
    expect(decisions).toHaveLength(0)
  })

  it('newWatermarks always jumps to latest event regardless of how many accumulated', () => {
    // 3 events accumulated on users, 1 on orders — watermark should jump to latest (evt003),
    // not step through each event one at a time
    const events: LatestEvents = {
      'pg://warehouse/raw_users': 'evt003',
      'pg://warehouse/raw_orders': 'evt001',
    }
    const watermarks: Watermarks = {
      'join_report::pg://warehouse/raw_users': 'evt000',
      'join_report::pg://warehouse/raw_orders': 'evt000',
    }
    const decisions = evaluateConsumers([consumerAnd], events, watermarks)
    const d = decisions[0]
    // Watermark advances to the latest, not evt001 or evt002
    expect(d.newWatermarks['pg://warehouse/raw_users']).toBe('evt003')
  })

  it('newWatermarks in decision points to latest event for each dataset', () => {
    const events: LatestEvents = {
      'pg://warehouse/raw_users': 'evt999',
      'pg://warehouse/raw_orders': 'evt888',
    }
    const watermarks: Watermarks = {
      'join_report::pg://warehouse/raw_users': 'evt001',
      'join_report::pg://warehouse/raw_orders': 'evt001',
    }
    const decisions = evaluateConsumers([consumerAnd], events, watermarks)
    const d = decisions[0]
    expect(d.newWatermarks['pg://warehouse/raw_users']).toBe('evt999')
    expect(d.newWatermarks['pg://warehouse/raw_orders']).toBe('evt888')
  })
})

// ── emitOutlets (DB) ────────────────────────────────────────────────────────

describe('emitOutlets', () => {
  it('inserts one dataset_event per outlet URI', async () => {
    await emitOutlets(db, producerDag, 'run_abc')
    const events = await db.collection('dataset_events').find({}).toArray()
    expect(events).toHaveLength(2)
    const uris = events.map(e => e.uri)
    expect(uris).toContain('pg://warehouse/raw_users')
    expect(uris).toContain('pg://warehouse/raw_orders')
  })

  it('stamps produced_by and run_id correctly', async () => {
    await emitOutlets(db, producerDag, 'run_xyz')
    const events = await db.collection('dataset_events').find({}).toArray()
    for (const e of events) {
      expect(e.produced_by).toBe('raw_loader')
      expect(e.run_id).toBe('run_xyz')
    }
  })

  it('no-ops when dag has no outlets', async () => {
    await emitOutlets(db, plainDag, 'run_noop')
    const events = await db.collection('dataset_events').find({}).toArray()
    expect(events).toHaveLength(0)
  })
})

// ── triggerDatasetConsumers (integration) ──────────────────────────────────

describe('triggerDatasetConsumers', () => {
  const paused = async () => false

  it('creates a run for a consumer when its dataset has a new event', async () => {
    await emitOutlets(db, producerDag, 'seed_run')
    // First call seeds watermarks (no trigger)
    await triggerDatasetConsumers(db, [consumerA], async (d, dag) => 'seeded', paused)

    // Emit another event
    await emitOutlets(db, producerDag, 'trigger_run')
    let triggered = ''
    await triggerDatasetConsumers(
      db,
      [consumerA],
      async (_db, dag) => { triggered = dag.id; return 'run1' },
      paused,
    )
    expect(triggered).toBe('user_report')
  })

  it('is idempotent — two consecutive calls with no new events trigger only once', async () => {
    await emitOutlets(db, producerDag, 'initial')
    // Seed
    await triggerDatasetConsumers(db, [consumerA], async () => 'seed', paused)
    // New event
    await emitOutlets(db, producerDag, 'second')
    let count = 0
    const inc = async () => { count++; return `r${count}` }
    await triggerDatasetConsumers(db, [consumerA], inc, paused)
    await triggerDatasetConsumers(db, [consumerA], inc, paused) // no new events → no trigger
    expect(count).toBe(1)
  })

  it('3 events accumulated on a dataset → exactly ONE run created, not three', async () => {
    // Seed watermark
    await emitOutlets(db, producerDag, 'evt1')
    await triggerDatasetConsumers(db, [consumerA], async () => 'seed', paused)
    // Emit 3 more events before evaluating (simulates backpressure / slow tick)
    for (const id of ['evt2', 'evt3', 'evt4']) {
      await emitOutlets(db, producerDag, id)
    }
    // Single evaluation — should create exactly 1 run (watermark jumps to latest)
    const { createRun } = await import('../../scheduler/runs.js')
    await triggerDatasetConsumers(db, [consumerA], createRun, paused)
    const runs = await db.collection('dag_runs').find({ dag_id: 'user_report' }).toArray()
    expect(runs).toHaveLength(1)
  })

  it('AND-consumer does not trigger when only one dataset updated', async () => {
    // Only emit users, not orders
    await db.collection('dataset_events').insertOne({
      uri: 'pg://warehouse/raw_users', produced_by: 'raw_loader', run_id: 'r1', emitted_at: new Date(),
    })
    // Seed
    await triggerDatasetConsumers(db, [consumerAnd], async () => 'seed', paused)
    // New users event
    await db.collection('dataset_events').insertOne({
      uri: 'pg://warehouse/raw_users', produced_by: 'raw_loader', run_id: 'r2', emitted_at: new Date(),
    })
    let triggered = false
    await triggerDatasetConsumers(db, [consumerAnd], async () => { triggered = true; return 'r3' }, paused)
    expect(triggered).toBe(false)
  })

  it('does not trigger paused consumers', async () => {
    await emitOutlets(db, producerDag, 'seed')
    await triggerDatasetConsumers(db, [consumerA], async () => 'seed', paused)
    await emitOutlets(db, producerDag, 'new')
    let triggered = false
    await triggerDatasetConsumers(
      db, [consumerA], async () => { triggered = true; return 'r' },
      async () => true, // always paused
    )
    expect(triggered).toBe(false)
  })
})

// ── API ─────────────────────────────────────────────────────────────────────

describe('GET /datasets API', () => {
  it('returns empty array when no events', async () => {
    const res = await app.inject({ method: 'GET', url: '/datasets' })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  it('returns emitted events with all fields', async () => {
    await emitOutlets(db, producerDag, 'run_api')
    const res = await app.inject({ method: 'GET', url: '/datasets' })
    const body = res.json()
    expect(body.length).toBe(2)
    expect(body[0]).toMatchObject({ produced_by: 'raw_loader', run_id: 'run_api' })
    expect(body[0].uri).toBeDefined()
    expect(body[0].emitted_at).toBeDefined()
  })

  it('filters by URI', async () => {
    await emitOutlets(db, producerDag, 'run_filter')
    const res = await app.inject({
      method: 'GET',
      url: '/datasets?uri=pg%3A%2F%2Fwarehouse%2Fraw_users',
    })
    const body = res.json()
    expect(body.every((e: { uri: string }) => e.uri === 'pg://warehouse/raw_users')).toBe(true)
  })

  it('GET /dags includes datasets and outlets fields', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags' })
    const dags = res.json()
    const producer = dags.find((d: { id: string }) => d.id === 'raw_loader')
    const consumer = dags.find((d: { id: string }) => d.id === 'user_report')
    expect(producer.outlets).toContain('pg://warehouse/raw_users')
    expect(consumer.datasets).toContain('pg://warehouse/raw_users')
  })
})
