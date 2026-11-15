/**
 * Built-in providers against real services (docker-compose.providers-test.yml):
 *
 *   docker compose -f docker-compose.providers-test.yml up -d
 *   PROVIDERS_IT=1 npx vitest run src/providers
 *
 * The first block runs operators through the scheduler (forked worker, connections from the database
 * over the execution API); the rest call the worker runtime directly to cover each operator's behaviour.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { randomBytes } from 'node:crypto'
import {
  SQLExecuteQueryOperator, SQLCheckOperator, EmailOperator, ProduceToTopicOperator, ConsumeFromTopicOperator,
} from '../operators.js'
import { runOperator, type OperatorTaskCtx } from '../runtime.js'
import type { ConnectionRuntime } from '../../../connections/index.js'
import { upsertConnection } from '../../../connections/index.js'
import { register, clearRegistry } from '../../../dag/registry.js'
import { createRun } from '../../../scheduler/runs.js'
import { advanceRun } from '../../../scheduler/index.js'
import { waitFor } from '../../../scheduler/__tests__/wait-for.js'
import { xcomPull } from '../../../xcom/index.js'

const IT = process.env.PROVIDERS_IT === '1'
const RUN = randomBytes(3).toString('hex')

const CONNS: Record<string, ConnectionRuntime> = {
  it_pg: { conn_id: 'it_pg', conn_type: 'postgres', host: 'localhost', port: 55432, schema: 'it', login: 'airflow', password: 'airflow', extra: null },
  it_mysql: { conn_id: 'it_mysql', conn_type: 'mysql', host: 'localhost', port: 53306, schema: 'it', login: 'airflow', password: 'airflow', extra: null },
  it_kafka: { conn_id: 'it_kafka', conn_type: 'kafka', host: 'localhost:59092', port: null, schema: null, login: null, password: null, extra: null },
  smtp_default: { conn_id: 'smtp_default', conn_type: 'smtp', host: 'localhost', port: 51025, schema: null, login: null, password: null,
    extra: { disable_ssl: true, disable_tls: true, from_email: 'airflow@it.local' } },
}
const MAILPIT = 'http://localhost:58025/api/v1'

function ctxFor(over: Partial<OperatorTaskCtx> = {}) {
  const pushed: Record<string, unknown> = {}
  const ctx: OperatorTaskCtx = {
    dagId: 'it', runId: 'r', taskId: 't', mapIndex: null, mapValue: null, conf: {},
    logicalDate: new Date('2026-03-02T00:00:00Z'), dataIntervalStart: null, dataIntervalEnd: null,
    xcom: { push: async (k, v) => { pushed[k] = v } },
    connections: { get: async id => CONNS[id] ?? null },
    log: { info: () => {}, warn: () => {} },
    ...over,
  }
  return { ctx, pushed, run: (impl: string, args: Record<string, unknown>) => runOperator(impl, args, ctx) }
}

async function mailpitMessages(): Promise<{ ID: string; Subject: string; To: { Address: string }[]; Attachments: number }[]> {
  const res = await fetch(`${MAILPIT}/messages`)
  return ((await res.json()) as { messages: never[] }).messages
}

describe.skipIf(!IT)('providers through the scheduler', () => {
  const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
  let client: MongoClient
  let db: Db
  const saved = { ...process.env }

  beforeAll(async () => {
    process.env.ENCRYPTION_KEYS ??= 'c'.repeat(64)
    client = new MongoClient(MONGO_URL)
    await client.connect()
    db = client.db('airflow_test_providers_it')
    await db.dropDatabase()
    for (const c of Object.values(CONNS)) {
      await upsertConnection(db, {
        conn_id: c.conn_id, conn_type: c.conn_type, host: c.host ?? undefined, port: c.port, schema: c.schema ?? undefined,
        login: c.login ?? undefined, password: c.password ?? undefined, extra: c.extra ? JSON.stringify(c.extra) : undefined,
      })
    }
  })
  afterAll(async () => {
    clearRegistry()
    await db.dropDatabase()
    await client.close()
    process.env = saved
  })

  it('runs SQL, check, Kafka and email operators in forked workers with connections from the database', async () => {
    const table = `it_sales_${RUN}`
    const topic = `it-orders-${RUN}`
    const { Kafka } = (await import('kafkajs')).default
    const admin = new Kafka({ brokers: ['localhost:59092'], logLevel: 0 }).admin()
    await admin.connect(); await admin.createTopics({ topics: [{ topic }] }); await admin.disconnect()

    const dag = {
      id: `it_providers_${RUN}`, schedule: null,
      tasks: {
        create: SQLExecuteQueryOperator({ connId: 'it_pg', sql: [
          `CREATE TABLE ${table} (ds date, region text, amount int)`,
          `INSERT INTO ${table} VALUES ('{{ ds }}', 'eu', 5), ('{{ ds }}', 'us', 7)`,
        ] }),
        select: SQLExecuteQueryOperator({ connId: 'it_pg', sql: `SELECT region, amount FROM ${table} WHERE amount > $1 ORDER BY region`, parameters: [1], dependsOn: ['create'] }),
        check: SQLCheckOperator({ connId: 'it_pg', sql: `SELECT COUNT(*) FROM ${table}`, dependsOn: ['create'] }),
        publish: ProduceToTopicOperator({ connId: 'it_kafka', topic, messages: [{ key: 'run', value: { dag: '{{ dag_id }}' } }], dependsOn: ['check'] }),
        consume: ConsumeFromTopicOperator({ connId: 'it_kafka', topics: [topic], groupId: `g-${RUN}`, fromBeginning: true, maxMessages: 1, dependsOn: ['publish'] }),
        mail: EmailOperator({ to: ['ops@it.local'], subject: `loaded ${RUN} {{ ds }}`, text: 'rows loaded', dependsOn: ['select', 'consume'] }),
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)
    const run = await waitFor(
      () => db.collection('dag_runs').findOne({ _id: new ObjectId(runId) }),
      r => r?.state === 'success' || r?.state === 'failed', 60_000, 200)
    const tis = await db.collection('task_instances').find({ dag_run_id: runId }).project({ task_id: 1, state: 1, error: 1 }).toArray()
    expect(run?.state, JSON.stringify(tis)).toBe('success')

    expect(await xcomPull(db, runId, 'select', 'return_value')).toEqual([{ region: 'eu', amount: 5 }, { region: 'us', amount: 7 }])
    expect(await xcomPull(db, runId, 'create', 'return_value')).toEqual({ rowCount: 2 })
    expect(await xcomPull(db, runId, 'consume', 'return_value')).toMatchObject([{ topic, key: 'run', value: JSON.stringify({ dag: dag.id }) }])
    const mail = (await mailpitMessages()).find(m => m.Subject.startsWith(`loaded ${RUN} `))
    expect(mail?.To.map(t => t.Address)).toEqual(['ops@it.local'])

    const { ctx } = ctxFor()
    await runOperator('sql.execute', { connId: 'it_pg', sql: `DROP TABLE ${table}`, doXcomPush: false }, ctx)
    clearRegistry()
  }, 90_000)
})

describe.skipIf(!IT)('common-sql', () => {
  it('postgres: binds parameters, rolls back a failed multi-statement run and caps rows', async () => {
    const t = `it_pg_${RUN}`
    const { run, pushed } = ctxFor()
    await run('sql.execute', { connId: 'it_pg', sql: `CREATE TABLE ${t} (id int primary key, name text); INSERT INTO ${t} VALUES (1, 'a'), (2, 'b'), (3, 'c')`, splitStatements: true })
    expect(await run('sql.execute', { connId: 'it_pg', sql: `SELECT name FROM ${t} WHERE id >= $1 ORDER BY id`, parameters: [2] }))
      .toEqual([{ name: 'b' }, { name: 'c' }])
    expect(pushed.return_value).toEqual([{ name: 'b' }, { name: 'c' }])

    await expect(run('sql.execute', { connId: 'it_pg', sql: [`DELETE FROM ${t}`, `INSERT INTO ${t} VALUES (1, 'dup'), (1, 'dup')`] }))
      .rejects.toThrow(/duplicate key/)
    expect(await run('sql.execute', { connId: 'it_pg', sql: `SELECT COUNT(*) AS n FROM ${t}` })).toEqual([{ n: '3' }])   // DELETE rolled back

    expect(await run('sql.execute', { connId: 'it_pg', sql: `SELECT id FROM ${t} ORDER BY id`, maxRows: 2 })).toEqual([{ id: 1 }, { id: 2 }])
    expect(await run('sql.execute', { connId: 'it_pg', sql: [`SELECT 1 AS a`, `SELECT 2 AS b`], returnLast: false })).toEqual([[{ a: 1 }], [{ b: 2 }]])

    expect(await run('sql.sensor', { connId: 'it_pg', sql: `SELECT COUNT(*) FROM ${t} WHERE name = 'c'` })).toBe(true)
    expect(await run('sql.sensor', { connId: 'it_pg', sql: `SELECT COUNT(*) FROM ${t} WHERE name = 'zz'` })).toBe(false)
    await expect(run('sql.check', { connId: 'it_pg', sql: `SELECT COUNT(*) AS n, MIN(id) AS lo FROM ${t} WHERE id > 99` }))
      .rejects.toThrow(/SQL check failed: falsy value\(s\) in n, lo/)
    await run('sql.execute', { connId: 'it_pg', sql: `DROP TABLE ${t}` })
  })

  it('mysql: positional and named parameters, affected rows, transactions', async () => {
    const t = `it_my_${RUN}`
    const { run } = ctxFor()
    await run('sql.execute', { connId: 'it_mysql', sql: `CREATE TABLE ${t} (id int primary key, name varchar(10))` })
    expect(await run('sql.execute', { connId: 'it_mysql', sql: `INSERT INTO ${t} VALUES (?, ?), (?, ?)`, parameters: [1, 'a', 2, 'b'] }))
      .toEqual({ rowCount: 2 })
    expect(await run('sql.execute', { connId: 'it_mysql', sql: `SELECT name FROM ${t} WHERE id = :id`, parameters: { id: 2 } })).toEqual([{ name: 'b' }])
    await expect(run('sql.execute', { connId: 'it_mysql', sql: [`DELETE FROM ${t}`, `INSERT INTO ${t} VALUES (1, 'x')`, `INSERT INTO ${t} VALUES (1, 'x')`] }))
      .rejects.toThrow(/Duplicate entry/)
    expect(await run('sql.execute', { connId: 'it_mysql', sql: `SELECT COUNT(*) AS n FROM ${t}` })).toEqual([{ n: '2' }])
    await run('sql.execute', { connId: 'it_mysql', sql: `DROP TABLE ${t}` })
  })
})

describe.skipIf(!IT)('apache-kafka', () => {
  it('consumers commit only what they returned; the sensor finds a matching message', async () => {
    const topic = `it-kafka-${RUN}`
    const { Kafka } = (await import('kafkajs')).default
    const admin = new Kafka({ brokers: ['localhost:59092'], logLevel: 0 }).admin()
    await admin.connect(); await admin.createTopics({ topics: [{ topic }] }); await admin.disconnect()
    const { run } = ctxFor()

    expect(await run('kafka.produce', { connId: 'it_kafka', topic, messages: ['one', { key: 'k', value: { n: 2 }, headers: { src: 'it' } }, 'three'] }))
      .toMatchObject({ topic, count: 3 })
    const group = `cg-${RUN}`
    const first = await run('kafka.consume', { connId: 'it_kafka', topics: topic, groupId: group, fromBeginning: true, maxMessages: 2 }) as { value: string; key: string | null; headers: object }[]
    expect(first.map(m => m.value)).toEqual(['one', '{"n":2}'])
    expect(first[1]).toMatchObject({ key: 'k', headers: { src: 'it' } })
    const rest = await run('kafka.consume', { connId: 'it_kafka', topics: topic, groupId: group, fromBeginning: true, maxMessages: 10, maxWaitMs: 3000 }) as { value: string }[]
    expect(rest.map(m => m.value)).toEqual(['three'])

    expect(await run('kafka.await', { connId: 'it_kafka', topics: [topic], groupId: `aw-${RUN}`, fromBeginning: true, match: 'thr' })).toBe(true)
    expect(await run('kafka.await', { connId: 'it_kafka', topics: [topic], groupId: `aw2-${RUN}`, fromBeginning: true, match: 'nope', maxWaitMs: 2000 })).toBe(false)
  }, 60_000)
})

describe.skipIf(!IT)('google', () => {
  const saved = { ...process.env }
  let dir: string
  beforeAll(async () => {
    process.env.STORAGE_EMULATOR_HOST = 'http://localhost:54443'
    process.env.BIGQUERY_EMULATOR_HOST = 'http://localhost:59050'
    process.env.GOOGLE_CLOUD_PROJECT = 'it-project'
    dir = await mkdtemp(join(tmpdir(), 'anode-gcs-'))
    await fetch('http://localhost:54443/storage/v1/b?project=it-project', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ name: `it-bucket-${RUN}` }),
    })
  })
  afterAll(async () => { process.env = saved; await rm(dir, { recursive: true, force: true }) })

  it('GCS: upload, list, exists, download, delete by prefix', async () => {
    const bucket = `it-bucket-${RUN}`
    const { run } = ctxFor()
    const src = join(dir, 'in.csv')
    await writeFile(src, 'a,b\n1,2\n')
    expect(await run('gcs.upload', { bucket, src, dst: 'raw/{{ ds }}/in.csv' })).toBe(`gs://${bucket}/raw/2026-03-02/in.csv`)
    await run('gcs.upload', { bucket, data: 'hello', dst: 'raw/note.txt' })
    await run('gcs.upload', { bucket, data: 'keep', dst: 'other/x.txt' })

    expect(await run('gcs.list', { bucket, prefix: 'raw/' })).toEqual(['raw/2026-03-02/in.csv', 'raw/note.txt'])
    expect(await run('gcs.exists', { bucket, object: 'raw/note.txt' })).toBe(true)
    expect(await run('gcs.download', { bucket, objectName: 'raw/note.txt' })).toBe('hello')
    await expect(run('gcs.download', { bucket, objectName: 'raw/note.txt', maxBytes: 2 })).rejects.toThrow(/over maxBytes 2/)
    const out = join(dir, 'out.csv')
    expect(await run('gcs.download', { bucket, objectName: 'raw/2026-03-02/in.csv', filename: out })).toBe(out)
    expect(await readFile(out, 'utf8')).toBe('a,b\n1,2\n')

    expect(await run('gcs.delete', { bucket, prefix: 'raw/' })).toEqual(['raw/2026-03-02/in.csv', 'raw/note.txt'])
    expect(await run('gcs.exists', { bucket, object: 'raw/note.txt' })).toBe(false)
    expect(await run('gcs.list', { bucket })).toEqual(['other/x.txt'])
    await expect(run('gcs.delete', { bucket })).rejects.toThrow(/exactly one of objects or a non-empty prefix/)
  })

  it('BigQuery: query rows typed by schema; check fails on a falsy value', async () => {
    const { run, pushed } = ctxFor()
    expect(await run('bigquery.job', { sql: "SELECT 1 AS n, 'a' AS s, TRUE AS ok" })).toEqual([{ n: 1, s: 'a', ok: true }])
    expect(pushed.return_value).toEqual([{ n: 1, s: 'a', ok: true }])
    await expect(run('bigquery.check', { sql: 'SELECT 1 AS a, 0 AS b' })).rejects.toThrow(/falsy value\(s\) in b/)
    await expect(run('bigquery.job', { sql: 'SELECT * FROM nope.missing' })).rejects.toThrow(/BigQuery/)
  }, 60_000)
})

describe.skipIf(!IT)('smtp', () => {
  it('sends text and html with attachments; recipients may be a comma-separated string', async () => {
    const dir = await mkdtemp(join(tmpdir(), 'anode-mail-'))
    const file = join(dir, 'report.txt')
    await writeFile(file, 'report body')
    const { run } = ctxFor()
    const r = await run('email.send', { to: 'a@it.local, b@it.local', cc: ['c@it.local'], subject: `report ${RUN} {{ ds }}`, html: '<b>done</b>', files: [file] }) as { accepted: string[] }
    expect(r.accepted.sort()).toEqual(['a@it.local', 'b@it.local', 'c@it.local'])
    const msg = (await mailpitMessages()).find(m => m.Subject === `report ${RUN} 2026-03-02`)
    expect(msg?.Attachments).toBe(1)
    await rm(dir, { recursive: true, force: true })
  })
})
