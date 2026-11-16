/**
 * API behind the UI's Dag list, runs table, lineage and event-log views:
 * GET /dags filters + last_run, GET /dags/:id/runs?state=, GET /lineage, GET /event-logs/types.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'
import { buildServer } from '../server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { EVENT_TYPES } from '../../events/index.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME = 'airflow_test_ui_views_api'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.DB_NAME = DB_NAME
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(DB_NAME)
  app = buildServer(db)
  await app.ready()
})
afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  delete process.env.DB_NAME
})
afterEach(async () => {
  clearRegistry()
  for (const c of ['dag_runs', 'dag_paused', 'dataset_events', 'dataset_alias_links', 'event_logs']) await db.collection(c).deleteMany({})
})

function dag(id: string, over: Partial<DagDefinition> = {}): DagDefinition {
  const d: DagDefinition = { id, schedule: null, tasks: { t: { run: async () => {} } }, ...over }
  register(d)
  return d
}

async function insertRun(dagId: string, state: string, createdAt: string): Promise<string> {
  const { insertedId } = await db.collection('dag_runs').insertOne({
    dag_id: dagId, state, created_at: new Date(createdAt), logical_date: null, conf: {}, tags: [], note: null,
  })
  return insertedId.toString()
}

const get = async (url: string) => {
  const res = await app.inject({ method: 'GET', url })
  return { status: res.statusCode, body: res.json() }
}
const ids = (body: Array<{ id: string }>) => body.map(d => d.id).sort()

describe('GET /dags — last run and filters', () => {
  async function seed() {
    dag('sales_daily', { tags: ['finance', 'daily'], owner: 'ana' })
    dag('sales_hourly', { tags: ['finance'] })
    dag('ml_train', { owner: 'Sales-Science' })
    dag('never_ran')
    await insertRun('sales_daily', 'failed', '2026-03-01T00:00:00Z')
    const newest = await insertRun('sales_daily', 'success', '2026-03-02T00:00:00Z')
    await insertRun('sales_hourly', 'failed', '2026-03-02T00:00:00Z')
    await insertRun('ml_train', 'running', '2026-03-02T00:00:00Z')
    await app.inject({ method: 'POST', url: '/dags/sales_hourly/pause' })
    return newest
  }

  it('reports each Dag\'s newest run, or null when it never ran', async () => {
    const newest = await seed()
    const { body } = await get('/dags')
    const byId = Object.fromEntries(body.map((d: { id: string; last_run: unknown }) => [d.id, d.last_run]))
    expect(byId.sales_daily).toEqual({ run_id: newest, state: 'success', created_at: '2026-03-02T00:00:00.000Z', logical_date: null })
    expect(byId.never_ran).toBeNull()
  })

  it.each([
    ['search matches id, owner or tag, case-insensitively', '?search=SALES', ['ml_train', 'sales_daily', 'sales_hourly']],
    ['search by tag', '?search=daily', ['sales_daily']],
    ['paused=true', '?paused=true', ['sales_hourly']],
    ['paused=false', '?paused=false', ['ml_train', 'never_ran', 'sales_daily']],
    ['last_run_state uses only the newest run', '?last_run_state=failed', ['sales_hourly']],
    ['last_run_state=none', '?last_run_state=none', ['never_ran']],
    ['filters are ANDed', '?tag=finance&paused=false&search=sales', ['sales_daily']],
  ])('%s', async (_name, query, expected) => {
    await seed()
    const { status, body } = await get(`/dags${query}`)
    expect(status).toBe(200)
    expect(ids(body)).toEqual(expected)
  })

  it.each([
    ['/dags?paused=yes', '"paused" must be true or false'],
    ['/dags?last_run_state=bogus', '"last_run_state" must be one of: queued, running, success, failed, cancelled, none'],
  ])('rejects %s', async (url, error) => {
    expect(await get(url)).toEqual({ status: 400, body: { error } })
  })
})

describe('GET /dags/:dagId/runs?state=', () => {
  it('lists only runs in that state, newest first, and rejects unknown states', async () => {
    dag('d')
    await insertRun('d', 'failed', '2026-03-01T00:00:00Z')
    await insertRun('d', 'success', '2026-03-02T00:00:00Z')
    const newerFailed = await insertRun('d', 'failed', '2026-03-03T00:00:00Z')

    const { body } = await get('/dags/d/runs?state=failed')
    expect(body.items.map((r: { state: string }) => r.state)).toEqual(['failed', 'failed'])
    expect(body.items[0].run_id).toBe(newerFailed)
    expect((await get('/dags/d/runs?state=skipped')).status).toBe(400)
  })
})

describe('GET /lineage', () => {
  it('returns declared and runtime edges, each dataset\'s newest event, and honours the time window', async () => {
    dag('producer', { outlets: ['s3://out'] })
    dag('consumer', { datasets: ['s3://out'] })
    const now = Date.now()
    await db.collection('dataset_events').insertMany([
      { uri: 's3://out', produced_by: 'producer', run_id: 'r1', emitted_at: new Date(now - 3_600_000) },
      { uri: 's3://out', produced_by: 'producer', run_id: 'r2', emitted_at: new Date(now - 60_000) },
      { uri: 's3://recent', produced_by: 'producer', run_id: 'r2', task_id: 't', emitted_at: new Date(now - 60_000) },
      { uri: 's3://old', produced_by: 'producer', run_id: 'r0', task_id: 't', emitted_at: new Date(now - 40 * 86_400_000) },
    ])

    const { status, body } = await get('/lineage')
    expect(status).toBe(200)
    expect(body.edges.map((e: { from: string; to: string; runtime: boolean }) => `${e.from}>${e.to}${e.runtime ? '*' : ''}`).sort()).toEqual([
      'dag:producer>dataset:s3://out',
      'dag:producer>dataset:s3://recent*',
      'dataset:s3://out>dag:consumer',
    ])
    const out = body.nodes.find((n: { id: string }) => n.id === 'dataset:s3://out')
    expect(out).toMatchObject({ kind: 'dataset', layer: 1, last_event: { produced_by: 'producer', run_id: 'r2' } })

    const wide = await get('/lineage?days=60')
    expect(wide.body.nodes.map((n: { id: string }) => n.id)).toContain('dataset:s3://old')
  })

  it('focuses on one Dag, and 404s for an unknown one', async () => {
    dag('a', { outlets: ['x'] })
    dag('b', { datasets: ['x'] })
    dag('unrelated', { outlets: ['y'] })
    const { body } = await get('/lineage?dag_id=b')
    expect(body.nodes.map((n: { id: string }) => n.id).sort()).toEqual(['dag:a', 'dag:b', 'dataset:x'])
    expect(await get('/lineage?dag_id=nope')).toEqual({ status: 404, body: { error: "Dag 'nope' not found" } })
  })
})

describe('GET /event-logs/types', () => {
  it('lists every event type the log accepts as a filter', async () => {
    const { body } = await get('/event-logs/types')
    expect(body.items).toEqual([...EVENT_TYPES])
    for (const t of body.items) expect((await get(`/event-logs?event_type=${t}`)).status).toBe(200)
  })
})
