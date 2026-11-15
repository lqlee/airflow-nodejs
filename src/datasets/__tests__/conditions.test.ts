/** Dataset conditions (all / any / alias), runtime alias events, and dataset-or-time Dags. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'
import { evaluateConsumers, triggerDatasetConsumers, recordTaskDatasetEvents, aliasKey } from '../index.js'
import { allOf, anyOf, datasetAlias, describeCondition, validateCondition, normalizeCondition } from '../conditions.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { ensureIndexes } from '../../db/indexes.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB = 'airflow_test_dataset_conditions'
let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.DB_NAME = DB   // forked workers write their dataset events here
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(DB)
  await ensureIndexes(db)
})
afterAll(async () => { delete process.env.DB_NAME; await db.dropDatabase(); await client.close() })
afterEach(async () => {
  clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'dataset_events', 'dataset_watermarks', 'dataset_alias_links', 'event_logs']) {
    await db.collection(c).deleteMany({})
  }
})

const A = 's3://lake/a'
const B = 's3://lake/b'
const C = 's3://lake/c'
const consumer = (id: string, datasets: DagDefinition['datasets'], extra: Partial<DagDefinition> = {}): DagDefinition =>
  ({ id, schedule: null, datasets, tasks: { t: { run: async () => {} } }, ...extra })
const wm = (dagId: string, marks: Record<string, string>) =>
  Object.fromEntries(Object.entries(marks).map(([uri, id]) => [`${dagId}::${uri}`, id]))
const notPaused = async () => false
const trigger = (dags: DagDefinition[]) => triggerDatasetConsumers(db, dags, createRun, notPaused)
const emit = (uri: string) => db.collection('dataset_events').insertOne({ uri, produced_by: 'producer', run_id: 'r', emitted_at: new Date() })
const runCount = (dagId: string) => db.collection('dag_runs').countDocuments({ dag_id: dagId, trigger_type: 'dataset' })

describe('conditions — pure', () => {
  it('describe / normalize / validate', () => {
    expect(describeCondition(allOf(A, anyOf(B, datasetAlias('x'))))).toBe(`${A} & (${B} | alias:x)`)
    expect(normalizeCondition([A, B])).toEqual({ all: [A, B] })
    expect(normalizeCondition([])).toBeNull()
    expect(() => validateCondition({ any: [] })).toThrow(/non-empty/)
    expect(() => validateCondition({ all: [A], any: [B] })).toThrow(/exactly one/)
    expect(() => validateCondition({ or: [A] })).toThrow(/unknown key 'or'/)
    expect(() => validateCondition({ any: [A, { alias: '' }] })).toThrow(/datasets\.any\[1\]\.alias/)
  })

  it('any fires on one new event and consumes every ready dataset; all needs each one', () => {
    const anyDag = consumer('any_dag', anyOf(A, B, C))
    const allDag = consumer('all_dag', allOf(A, B))
    const latest = { [A]: 'e2', [B]: 'e5', [C]: 'e1' }
    const marks = { ...wm('any_dag', { [A]: 'e1', [B]: 'e4', [C]: 'e1' }), ...wm('all_dag', { [A]: 'e1', [B]: 'e5' }) }

    const decisions = evaluateConsumers([anyDag, allDag], latest, marks)
    expect(decisions).toHaveLength(1)
    expect(decisions[0]).toMatchObject({ dag: { id: 'any_dag' }, shouldFire: true, newWatermarks: { [A]: 'e2', [B]: 'e5' } })
    expect(decisions[0].consumed!.sort()).toEqual([A, B])
  })

  it('nested: a & (b | c)', () => {
    const d = consumer('nest', allOf(A, anyOf(B, C)))
    const base = wm('nest', { [A]: 'a0', [B]: 'b0', [C]: 'c0' })
    expect(evaluateConsumers([d], { [A]: 'a1', [B]: 'b0', [C]: 'c1' }, base)[0]?.shouldFire).toBe(true)
    expect(evaluateConsumers([d], { [A]: 'a0', [B]: 'b1', [C]: 'c1' }, base)).toHaveLength(0)
  })

  it('a dataset added to an existing consumer is seeded on its own and does not block the others', () => {
    const d = consumer('grown', anyOf(A, B))   // B was just added: no watermark yet
    const [dec] = evaluateConsumers([d], { [A]: 'a1', [B]: 'b9' }, wm('grown', { [A]: 'a0' }))
    expect(dec).toMatchObject({ shouldFire: true, consumed: [A], newWatermarks: { [A]: 'a1', [B]: 'b9' } })
  })

  it('an alias counts datasets attached after the consumer started watching it', () => {
    const d = consumer('al', datasetAlias('daily'))
    const t0 = new Date('2026-01-01T00:00:00Z')
    const links = { daily: [{ uri: A, linkedAt: new Date('2025-12-31T00:00:00Z') }, { uri: B, linkedAt: new Date('2026-01-02T00:00:00Z') }] }
    const latestAt = { [A]: new Date('2025-12-31T00:00:00Z'), [B]: new Date('2026-01-02T00:00:00Z') }

    // First sight of the alias: only seeds
    expect(evaluateConsumers([d], { [A]: 'a1', [B]: 'b1' }, {}, { links, latestAt, since: {} })[0]).toMatchObject({ shouldFire: false, aliasSeeds: ['daily'] })
    // Watching since t0: B's event came later → fire; A's older event is only seeded
    const [dec] = evaluateConsumers([d], { [A]: 'a1', [B]: 'b1' }, {}, { links, latestAt, since: { [aliasKey('al', 'daily')]: t0 } })
    expect(dec).toMatchObject({ shouldFire: true, consumed: [B], newWatermarks: { [A]: 'a1', [B]: 'b1' } })
  })
})

describe('triggerDatasetConsumers — conditions', () => {
  it('an any-consumer runs once per batch of events, then waits for the next one', async () => {
    const d = consumer('any_db', anyOf(A, B))
    await emit(A); await emit(B)
    expect(await trigger([d])).toBe(0)           // first sight: seed
    await emit(A); await emit(B)
    expect(await trigger([d])).toBe(1)           // both new → one run, both consumed
    expect(await trigger([d])).toBe(0)
    await emit(B)
    expect(await trigger([d])).toBe(1)
    expect(await runCount('any_db')).toBe(2)
  })

  it('a Dag with a cron schedule and datasets also runs on dataset events (dataset-or-time)', async () => {
    const d = consumer('or_time', [A], { schedule: '0 0 * * *' })
    await emit(A)
    await trigger([d])
    await emit(A)
    expect(await trigger([d])).toBe(1)
    expect(await db.collection('dag_runs').findOne({ dag_id: 'or_time' })).toMatchObject({ trigger_type: 'dataset', logical_date: null })
  })
})

describe('aliases — events emitted by tasks at runtime', () => {
  it('a task emitting through an alias runs the alias consumer; a failed task emits nothing', async () => {
    const reader = consumer('alias_reader', datasetAlias('partitions'))
    const writer: DagDefinition = {
      id: 'alias_writer', schedule: null,
      tasks: {
        ok: { run: new Function(`return async (ctx) => { ctx.emitDataset('s3://lake/p=1', { alias: 'partitions', extra: { rows: 3 } }) }`)() as () => Promise<void> },
        bad: { run: new Function(`return async (ctx) => { ctx.emitDataset('s3://lake/p=bad', { alias: 'partitions' }); throw new Error('x') }`)() as () => Promise<void> },
      },
    }
    register(reader); register(writer)
    expect(await trigger([reader, writer])).toBe(0)   // reader starts watching the alias

    const runId = await createRun(db, writer)
    for (let i = 0; i < 6; i++) await advanceRun(db, runId)

    const events = await db.collection('dataset_events').find().toArray()
    expect(events).toEqual([expect.objectContaining({ uri: 's3://lake/p=1', task_id: 'ok', source_alias: 'partitions', extra: { rows: 3 }, run_id: runId })])
    expect(await trigger([reader, writer])).toBe(1)
    expect(await trigger([reader, writer])).toBe(0)
  }, 30_000)

  it('recordTaskDatasetEvents attaches each dataset to its alias once', async () => {
    const src = { dagId: 'w', runId: 'r', taskId: 't' }
    await recordTaskDatasetEvents(db, src, [{ uri: A, alias: 'x' }, { uri: B, alias: 'x' }])
    await recordTaskDatasetEvents(db, src, [{ uri: A, alias: 'x' }, { uri: C }])
    const links = await db.collection('dataset_alias_links').find().sort({ uri: 1 }).toArray()
    expect(links.map(l => [l.alias, l.uri])).toEqual([['x', A], ['x', B]])
    expect(await db.collection('dataset_events').countDocuments()).toBe(4)
    await expect(recordTaskDatasetEvents(db, src, [{ uri: '' }])).rejects.toThrow(/non-empty/)
  })
})

describe('API', () => {
  let app: FastifyInstance
  beforeAll(async () => { app = buildServer(db); await app.ready() })
  afterAll(async () => { await app.close() })

  it('GET /dags shows the condition; GET /dataset-aliases lists attached datasets', async () => {
    register(consumer('api_cond', allOf(A, anyOf(B, datasetAlias('x')))))
    await recordTaskDatasetEvents(db, { dagId: 'w', runId: 'r', taskId: 't' }, [{ uri: C, alias: 'x' }])

    const dags = (await app.inject({ method: 'GET', url: '/dags' })).json() as Array<Record<string, unknown>>
    expect(dags.find(d => d.id === 'api_cond')).toMatchObject({
      datasets: [A, B, 'alias:x'],
      dataset_condition_text: `${A} & (${B} | alias:x)`,
    })
    const aliases = (await app.inject({ method: 'GET', url: '/dataset-aliases' })).json()
    expect(aliases).toEqual([{ alias: 'x', datasets: [{ uri: C, linked_at: expect.any(String) }] }])
  })
})
