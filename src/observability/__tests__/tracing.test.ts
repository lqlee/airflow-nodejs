/**
 * OpenTelemetry traces: a span per Dag run attempt and a child span per task try, emitted when they
 * finish — both from the pure emitters and end to end through the scheduler.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { InMemorySpanExporter, type ReadableSpan } from '@opentelemetry/sdk-trace'
import { initTracing, shutdownTracing, isTracingEnabled, emitRunSpan, emitTaskSpan, runSpanIds } from '../tracing.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun, cancelRun } from '../../scheduler/index.js'
import { setRunState } from '../../scheduler/actions.js'
import { drainPool } from '../../scheduler/pool.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME = 'airflow_test_tracing'
let client: MongoClient
let db: Db
const exporter = new InMemorySpanExporter()

beforeAll(async () => {
  process.env.DB_NAME = DB_NAME
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(DB_NAME)
  expect(await initTracing({ exporter })).toBe(true)
})
afterAll(async () => {
  await shutdownTracing()
  await drainPool({ timeoutMs: 15_000, pollMs: 25 })
  await db.dropDatabase()
  await client.close()
  delete process.env.DB_NAME
})
afterEach(async () => {
  exporter.reset()
  clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'event_logs']) await db.collection(c).deleteMany({})
})

const ms = (t: [number, number]) => t[0] * 1000 + Math.round(t[1] / 1e6)
const spans = () => exporter.getFinishedSpans()
const byName = (prefix: string) => spans().filter(s => s.name.startsWith(prefix))
const parentOf = (s: ReadableSpan) => s.parentSpanContext?.spanId

describe('emitters', () => {
  const runId = new ObjectId()
  const queuedAt = new Date('2026-03-02T06:00:00.000Z')
  const run = { _id: runId, dag_id: 'etl', state: 'failed', created_at: queuedAt, queued_at: queuedAt, ended_at: new Date('2026-03-02T06:05:00.000Z'), trigger_type: 'cron' }

  it('run span: deterministic ids, real start/end, state and error status', () => {
    emitRunSpan(run)
    const [span] = spans()
    expect(span.spanContext()).toMatchObject(runSpanIds(runId.toString(), queuedAt))
    expect(span.name).toBe('dag_run etl')
    expect(parentOf(span)).toBeUndefined()
    expect([ms(span.startTime), ms(span.endTime)]).toEqual([queuedAt.getTime(), run.ended_at.getTime()])
    expect(span.attributes).toMatchObject({ 'airflow.dag_id': 'etl', 'airflow.run_id': runId.toString(), 'airflow.run.state': 'failed', 'airflow.run.trigger_type': 'cron' })
    expect(span.status.code).toBe(2)   // ERROR
  })

  it('task span: child of the run attempt span, recorded before or after the run span', () => {
    const ti = { dag_run_id: runId.toString(), dag_id: 'etl', task_id: 'load', map_index: 3, try_number: 1, started_at: new Date('2026-03-02T06:01:00Z') }
    emitTaskSpan(ti, run, 'failed', new Date('2026-03-02T06:02:00Z'), 'connection refused')
    emitRunSpan(run)
    const [task, runSpan] = spans()
    expect(task.spanContext().traceId).toBe(runSpan.spanContext().traceId)
    expect(parentOf(task)).toBe(runSpan.spanContext().spanId)
    expect(task.name).toBe('task etl.load[3]')
    expect(task.attributes).toMatchObject({ 'airflow.task_id': 'load', 'airflow.task.try_number': 2, 'airflow.task.map_index': 3, 'airflow.task.state': 'failed' })
    expect(task.status).toEqual({ code: 2, message: 'connection refused' })
  })

  it('a cleared run gets a new run span in the same trace', () => {
    const first = runSpanIds('r', new Date('2026-03-02T06:00:00Z'))
    const second = runSpanIds('r', new Date('2026-03-03T06:00:00Z'))
    expect(second.traceId).toBe(first.traceId)
    expect(second.spanId).not.toBe(first.spanId)
    expect(first.traceId).toMatch(/^[0-9a-f]{32}$/)
    expect(first.spanId).toMatch(/^[0-9a-f]{16}$/)
  })
})

describe('through the scheduler', () => {
  function dag(id: string, tasks: DagDefinition['tasks']): DagDefinition {
    const d: DagDefinition = { id, schedule: null, tasks }
    register(d)
    return d
  }

  it('a finished run emits its span and one span per task try under it', async () => {
    const d = dag('traced', {
      extract: { run: async () => {} },
      load: { dependsOn: ['extract'], run: async () => { throw new Error('no space left') } },
    })
    const runId = await createRun(db, d)
    await advanceRun(db, runId)
    await drainPool({ timeoutMs: 15_000, pollMs: 25 })

    const [runSpan] = byName('dag_run traced')
    expect(runSpan.attributes['airflow.run.state']).toBe('failed')
    const tasks = byName('task traced.')
    expect(tasks.map(t => [t.name, t.attributes['airflow.task.state']]).sort()).toEqual([
      ['task traced.extract', 'success'], ['task traced.load', 'failed'],
    ])
    for (const t of tasks) expect(parentOf(t)).toBe(runSpan.spanContext().spanId)
    expect(tasks.find(t => t.name.endsWith('load'))!.status.message).toContain('no space left')
  })

  it('cancelling or marking an unfinished run ends its span; re-marking a finished run does not emit again', async () => {
    const d = dag('cancel_me', { a: { run: async () => {} } })
    const cancelled = await createRun(db, d)
    expect(await cancelRun(db, cancelled)).toBe(true)
    const marked = await createRun(db, d)
    expect((await setRunState(db, marked, 'failed')).ok).toBe(true)
    expect((await setRunState(db, marked, 'success')).ok).toBe(true)   // already finished: no new attempt

    expect(byName('dag_run cancel_me').map(s => s.attributes['airflow.run.state'])).toEqual(['cancelled', 'failed'])
  })

  it('emits nothing when tracing is off', async () => {
    await shutdownTracing()
    expect(isTracingEnabled()).toBe(false)
    emitRunSpan({ _id: new ObjectId(), dag_id: 'x', state: 'success', created_at: new Date() })
    expect(spans()).toEqual([])
    expect(await initTracing({ exporter })).toBe(true)
  })
})
