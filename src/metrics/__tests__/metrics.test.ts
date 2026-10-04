import { describe, it, expect, beforeAll, afterAll, afterEach, beforeEach, vi } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { createPool, resetAllPools } from '../../pools/index.js'
import { pauseDag } from '../../dag/pause.js'
import { setImportErrors } from '../../dag/import-errors.js'
import { clearMetricsCache } from '../index.js'
import {
  escapeLabelValue, formatLabels, sample, family, recordHttpRequest, renderHttpMetrics,
  recordSchedulerTick, resetMetrics, HTTP_BUCKETS,
} from '../registry.js'

describe('prometheus text helpers', () => {
  it('escapes backslash, double quote and newline in label values', () => {
    expect(escapeLabelValue('a\\b"c\nd')).toBe('a\\\\b\\"c\\nd')
  })
  it('formats labels in insertion order, and nothing when empty', () => {
    expect(formatLabels({ dag_id: 'x', state: 'ok' })).toBe('{dag_id="x",state="ok"}')
    expect(formatLabels({})).toBe('')
  })
  it('renders a sample line; non-finite values become 0', () => {
    expect(sample('m', 3, { a: 'b' })).toBe('m{a="b"} 3')
    expect(sample('m', NaN)).toBe('m 0')
    expect(sample('m', Infinity)).toBe('m 0')
  })
  it('family emits HELP and TYPE before the samples', () => {
    expect(family('m', 'gauge', 'help text', ['m 1'])).toEqual(['# HELP m help text', '# TYPE m gauge', 'm 1'])
  })
})

describe('http histogram', () => {
  beforeEach(() => resetMetrics())

  it('buckets are cumulative; +Inf, _sum and _count agree', () => {
    recordHttpRequest('GET', '/dags', 200, 0.02)   // ≤0.025 and above
    recordHttpRequest('GET', '/dags', 200, 0.4)    // ≤0.5 and above
    recordHttpRequest('GET', '/dags', 200, 20)     // beyond every finite bucket
    const text = renderHttpMetrics().join('\n')
    const bucket = (le: string) =>
      Number(new RegExp(`_bucket\\{method="GET",route="/dags",status="200",le="${le.replace('+', '\\+')}"\\} (\\d+)`).exec(text)![1])
    expect(bucket('0.01')).toBe(0)
    expect(bucket('0.025')).toBe(1)
    expect(bucket('0.5')).toBe(2)
    expect(bucket('10')).toBe(2)
    expect(bucket('+Inf')).toBe(3)
    expect(text).toMatch(/_count\{method="GET",route="\/dags",status="200"\} 3/)
    expect(text).toMatch(/_sum\{method="GET",route="\/dags",status="200"\} 20\.42/)
    expect(HTTP_BUCKETS).toEqual([...HTTP_BUCKETS].sort((a, b) => a - b))
  })

  it('keeps separate series per status code', () => {
    recordHttpRequest('GET', '/x', 200, 0.1)
    recordHttpRequest('GET', '/x', 500, 0.1)
    const text = renderHttpMetrics().join('\n')
    expect(text).toMatch(/_count\{method="GET",route="\/x",status="200"\} 1/)
    expect(text).toMatch(/_count\{method="GET",route="\/x",status="500"\} 1/)
  })
})

describe('GET /metrics', () => {
  const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
  let client: MongoClient
  let db: Db
  let app: FastifyInstance

  beforeAll(async () => {
    process.env.METRICS_CACHE_SECONDS = '0'
    client = new MongoClient(MONGO_URL)
    await client.connect()
    db = client.db('airflow_test_metrics')
    app = buildServer(db, { rateLimitMax: 0 })
    await app.ready()
  })
  afterAll(async () => { await app.close(); await db.dropDatabase(); await client.close(); delete process.env.METRICS_CACHE_SECONDS })
  afterEach(async () => {
    for (const c of ['dag_runs', 'task_instances', 'dag_paused', 'pools', 'sla_alerts']) await db.collection(c).deleteMany({})
    clearRegistry(); resetAllPools(); resetMetrics(); clearMetricsCache(); setImportErrors([])
  })

  const scrape = async () => {
    const res = await app.inject({ method: 'GET', url: '/metrics' })
    return { res, text: res.body }
  }
  const value = (text: string, series: string) => {
    const m = new RegExp(`^${series.replace(/[{}"|.()]/g, '\\$&')} (\\S+)$`, 'm').exec(text)
    return m ? Number(m[1]) : undefined
  }

  it('serves the Prometheus text content type', async () => {
    const { res } = await scrape()
    expect(res.statusCode).toBe(200)
    expect(res.headers['content-type']).toBe('text/plain; version=0.0.4; charset=utf-8')
  })

  it('is never cacheable — a stale scrape would hide an outage', async () => {
    const { res } = await scrape()
    expect(res.headers['cache-control']).toBe('no-store')
  })

  it('every line is a comment or a valid sample, and every family has HELP + TYPE', async () => {
    register({ id: 'fmt_dag', schedule: null, tasks: { t: { run: async () => {} } } })
    await createRun(db, { id: 'fmt_dag', schedule: null, tasks: { t: { run: async () => {} } } })
    const { text } = await scrape()
    const lines = text.trimEnd().split('\n')
    const sampleRe = /^[a-zA-Z_:][a-zA-Z0-9_:]*(\{([a-zA-Z_][a-zA-Z0-9_]*="([^"\\]|\\.)*",?)*\})? -?[0-9.eE+-]+$/
    for (const l of lines) expect(l.startsWith('#') || sampleRe.test(l), `bad line: ${l}`).toBe(true)

    const typed = new Set(lines.filter(l => l.startsWith('# TYPE ')).map(l => l.split(' ')[2]))
    const helped = new Set(lines.filter(l => l.startsWith('# HELP ')).map(l => l.split(' ')[2]))
    expect(typed).toEqual(helped)
    expect(text.endsWith('\n')).toBe(true)
  })

  it('reports run and task-instance counts per Dag and state, zero-filling the rest', async () => {
    const dag = { id: 'count_dag', schedule: null, tasks: { a: { run: async () => {} }, b: { dependsOn: ['a'], run: async () => {} } } }
    register(dag)
    await createRun(db, dag)
    const r2 = await createRun(db, dag)
    await db.collection('dag_runs').updateOne({ dag_id: 'count_dag', state: 'queued' }, { $set: { state: 'failed' } })
    void r2

    const { text } = await scrape()
    expect(value(text, 'airflow_dag_runs{dag_id="count_dag",state="queued"}')).toBe(1)
    expect(value(text, 'airflow_dag_runs{dag_id="count_dag",state="failed"}')).toBe(1)
    expect(value(text, 'airflow_dag_runs{dag_id="count_dag",state="running"}')).toBe(0)    // zero-filled
    expect(value(text, 'airflow_task_instances{dag_id="count_dag",state="queued"}')).toBe(4)
    expect(value(text, 'airflow_task_instances{dag_id="count_dag",state="deferred"}')).toBe(0)
  })

  it('includes Dags that only exist in the DB (removed from the registry) and escapes odd ids', async () => {
    await db.collection('dag_runs').insertOne({ dag_id: 'gone"dag\\x', state: 'success', created_at: new Date() })
    const { text } = await scrape()
    expect(text).toContain('airflow_dag_runs{dag_id="gone\\"dag\\\\x",state="success"} 1')
  })

  it('reports loaded / paused Dag counts', async () => {
    register({ id: 'p1', schedule: null, tasks: { t: { run: async () => {} } } })
    register({ id: 'p2', schedule: null, tasks: { t: { run: async () => {} } } })
    await pauseDag(db, 'p1')
    const { text } = await scrape()
    expect(value(text, 'airflow_dags')).toBe(2)
    expect(value(text, 'airflow_dags_paused')).toBe(1)
  })

  it('reports pool slots, hitl backlog, unacked SLA alerts and import errors', async () => {
    await createPool(db, 'etl', 4)
    const dag = { id: 'hitl_dag', schedule: null, tasks: { gate: { requiresApproval: true, run: async () => {} } } }
    register(dag)
    await createRun(db, dag)
    await db.collection('sla_alerts').insertMany([{ acked: false }, { acked: false }, { acked: true }])
    setImportErrors([{ filename: 'bad.js', error: 'x', imported_at: new Date() }])

    const { text } = await scrape()
    expect(value(text, 'airflow_pool_slots{pool="etl"}')).toBe(4)
    expect(value(text, 'airflow_pool_open_slots{pool="etl"}')).toBe(4)
    expect(value(text, 'airflow_pool_occupied_slots{pool="etl"}')).toBe(0)
    expect(value(text, 'airflow_hitl_pending_tasks')).toBe(1)
    expect(value(text, 'airflow_sla_alerts_unacknowledged')).toBe(2)
    expect(value(text, 'airflow_import_errors')).toBe(1)
  })

  it('exposes scheduler tick counters and the last-tick timestamp', async () => {
    recordSchedulerTick(0.25, true)
    recordSchedulerTick(0.5, false)
    const before = Date.now() / 1000
    const { text } = await scrape()
    expect(value(text, 'airflow_scheduler_ticks_total')).toBe(2)
    expect(value(text, 'airflow_scheduler_tick_errors_total')).toBe(1)
    expect(value(text, 'airflow_scheduler_last_tick_duration_seconds')).toBe(0.5)
    expect(before - value(text, 'airflow_scheduler_last_tick_timestamp_seconds')!).toBeLessThan(5)
  })

  it('records HTTP requests by route pattern — including 404s as "unmatched"', async () => {
    register({ id: 'route_dag', schedule: null, tasks: { t: { run: async () => {} } } })
    await app.inject({ method: 'GET', url: '/dags/route_dag' })
    await app.inject({ method: 'GET', url: '/dags/other_dag_name' })    // same route pattern, 404 from handler
    await app.inject({ method: 'GET', url: '/no/such/route/at/all.json' })
    const { text } = await scrape()
    expect(text).toContain('airflow_http_request_duration_seconds_count{method="GET",route="/dags/:dagId",status="200"} 1')
    expect(text).toContain('airflow_http_request_duration_seconds_count{method="GET",route="/dags/:dagId",status="404"} 1')
    const httpLines = text.split('\n').filter(l => l.startsWith('airflow_http_request_duration_seconds'))
    expect(httpLines.some(l => /route_dag|other_dag_name|no\/such/.test(l))).toBe(false)   // raw path never becomes a label
    expect(text).toContain('airflow_http_request_duration_seconds_count{method="GET",route="unmatched",status="404"} 1')
  })

  it('caches DB-derived gauges for METRICS_CACHE_SECONDS, but not live process metrics', async () => {
    process.env.METRICS_CACHE_SECONDS = '60'
    try {
      const dag = { id: 'cache_dag', schedule: null, tasks: { t: { run: async () => {} } } }
      register(dag)
      await scrape()                                                  // primes the cache
      await createRun(db, dag)
      const stale = await scrape()
      expect(value(stale.text, 'airflow_dag_runs{dag_id="cache_dag",state="queued"}')).toBe(0)

      recordSchedulerTick(1, true)                                    // in-process metric — never cached
      expect(value((await scrape()).text, 'airflow_scheduler_ticks_total')).toBe(1)

      clearMetricsCache()
      expect(value((await scrape()).text, 'airflow_dag_runs{dag_id="cache_dag",state="queued"}')).toBe(1)
    } finally {
      process.env.METRICS_CACHE_SECONDS = '0'
    }
  })
})

describe('GET /metrics — auth', () => {
  const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
  let client: MongoClient
  let app: FastifyInstance

  beforeAll(async () => {
    vi.resetModules()
    process.env.API_KEYS = 'scrape-key'
    process.env.METRICS_CACHE_SECONDS = '0'
    const { buildServer: build } = await import('../../api/server.js')
    client = new MongoClient(MONGO_URL)
    await client.connect()
    app = build(client.db('airflow_test_metrics_auth'), { rateLimitMax: 0 })
    await app.ready()
  })
  afterAll(async () => {
    await app.close(); await client.db('airflow_test_metrics_auth').dropDatabase(); await client.close()
    delete process.env.API_KEYS; delete process.env.METRICS_CACHE_SECONDS
  })

  it('requires a bearer key when auth is enabled', async () => {
    expect((await app.inject({ method: 'GET', url: '/metrics' })).statusCode).toBe(401)
    expect((await app.inject({ method: 'GET', url: '/metrics', headers: { authorization: 'Bearer nope' } })).statusCode).toBe(401)
  })

  it('serves metrics to a valid key and reports airflow_auth_enabled 1', async () => {
    const res = await app.inject({ method: 'GET', url: '/metrics', headers: { authorization: 'Bearer scrape-key' } })
    expect(res.statusCode).toBe(200)
    expect(res.body).toMatch(/^airflow_auth_enabled 1$/m)
  })
})
