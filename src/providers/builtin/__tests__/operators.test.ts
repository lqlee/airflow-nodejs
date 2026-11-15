/**
 * Built-in operators without external services: factories (validation, serialization), the worker
 * runtime (templating, XCom, connections) and the pure helpers each provider relies on.
 */
import { describe, it, expect } from 'vitest'
import {
  SQLExecuteQueryOperator, SqlSensor, EmailOperator, GCSObjectExistenceSensor, ProduceToTopicOperator, operatorTask,
} from '../operators.js'
import { runOperator, renderArgsDeep, toJsonSafe, withOperatorRuntime, type OperatorTaskCtx } from '../runtime.js'
import { splitSql, sqlStatements } from '../args.js'
import { isTruthy } from '../sql.js'
import { bqRowsToObjects } from '../google.js'
import { kafkaConfig } from '../kafka.js'
import { smtpTransport } from '../email.js'
import { serializeDag, hydrateDag } from '../../../dag/serialize.js'
import { listAllProviders, getOperator } from '../../registry.js'
import type { ConnectionRuntime } from '../../../connections/index.js'
import type { DagDefinition } from '../../../dag/types.js'

function fakeCtx(conns: Record<string, Partial<ConnectionRuntime>> = {}, over: Partial<OperatorTaskCtx> = {}) {
  const pushed: Record<string, unknown> = {}
  const ctx: OperatorTaskCtx = {
    dagId: 'd', runId: 'r1', taskId: 't', mapIndex: null, mapValue: null,
    conf: { table: 'sales' }, logicalDate: new Date('2026-03-02T06:00:00Z'),
    dataIntervalStart: new Date('2026-03-01T06:00:00Z'), dataIntervalEnd: new Date('2026-03-02T06:00:00Z'),
    xcom: { push: async (k, v) => { pushed[k] = v } },
    connections: {
      get: async id => (conns[id] ? {
        conn_id: id, conn_type: 'generic', host: null, port: null, schema: null, login: null, password: null, extra: null, ...conns[id],
      } : null),
    },
    log: { info: () => {}, warn: () => {} },
    ...over,
  }
  return { ctx, pushed }
}

describe('operator factories', () => {
  it('build a self-contained run function; task options pass through, operator options are inlined', async () => {
    const onFailure = async () => {}
    const task = SQLExecuteQueryOperator({ connId: 'wh', sql: "SELECT '{{ ds }}'", retries: 2, dependsOn: ['a'], onFailure })
    expect(task).toMatchObject({ retries: 2, dependsOn: ['a'], onFailure })
    expect(String(task.run)).toBe(`async (ctx) => ctx.runOperator("sql.execute", {"connId":"wh","sql":"SELECT '{{ ds }}'"})`)

    // what a forked worker does with the serialized source
    const calls: unknown[] = []
    const fn = new Function(`return (${String(task.run)})`)() as (ctx: unknown) => Promise<unknown>
    await fn({ runOperator: async (...a: unknown[]) => { calls.push(a) } })
    expect(calls).toEqual([['sql.execute', { connId: 'wh', sql: "SELECT '{{ ds }}'" }]])
  })

  it('sensors get a poke function and keep their poke settings', () => {
    const s = SqlSensor({ connId: 'wh', sql: 'SELECT 1', pokeInterval: 5000 })
    expect(s.run).toBeUndefined()
    expect(typeof s.poke).toBe('function')
    expect(s.pokeInterval).toBe(5000)
  })

  it('reject unknown, missing and non-data options at Dag parse time', () => {
    expect(() => EmailOperator({ to: 'a@x.io', subject: 's', bodyy: 'x' })).toThrow(/EmailOperator: unknown option 'bodyy'/)
    expect(() => EmailOperator({ to: 'a@x.io' })).toThrow(/missing required option\(s\) subject/)
    expect(() => ProduceToTopicOperator({ connId: 'k', topic: 't', messages: [() => 1] })).toThrow(/messages\[0\] must be plain data/)
    expect(() => GCSObjectExistenceSensor({ bucket: 'b', object: new Date() as unknown as string })).toThrow(/must be a plain object/)
    expect(() => operatorTask('NopeOperator', {})).toThrow(/unknown operator 'NopeOperator'/)
  })

  it('survive Dag serialization: the hydrated task still calls the operator with its options', async () => {
    const dag: DagDefinition = { id: 'ser_ops', schedule: null, tasks: { q: SQLExecuteQueryOperator({ connId: 'wh', sql: 'SELECT 1' }) } }
    const hydrated = hydrateDag(serializeDag(dag))
    const fn = new Function(`return (${String(hydrated.tasks.q.run)})`)() as (ctx: unknown) => Promise<unknown>
    let got: unknown
    await fn({ runOperator: async (...a: unknown[]) => { got = a } })
    expect(got).toEqual(['sql.execute', { connId: 'wh', sql: 'SELECT 1' }])
  })

  it('are listed as built-in providers and reachable through getOperator', () => {
    const names = listAllProviders().filter(p => p.source === 'builtin').map(p => p.name)
    expect(names).toEqual(['common-sql', 'google', 'apache-kafka', 'smtp'])
    expect(listAllProviders().find(p => p.name === 'google')?.connection_types).toEqual(['google_cloud_platform'])
    const op = getOperator('smtp', 'EmailOperator')!
    expect(typeof op({ to: 'a@x.io', subject: 's' }).run).toBe('function')
    expect(getOperator('google', 'EmailOperator')).toBeUndefined()
  })
})

describe('operator runtime', () => {
  it('renders templates in every option string, including map_value for mapped instances', () => {
    const { ctx } = fakeCtx({}, { mapIndex: 1, mapValue: 'eu' })
    const out = renderArgsDeep({
      sql: "DELETE FROM {{ params.table }} WHERE ds = '{{ ds }}' AND region = '{{ map_value }}'",
      messages: [{ key: '{{ ds_nodash }}', value: { start: '{{ data_interval_start }}' } }],
      maxRows: 5,
    }, {
      dag_id: ctx.dagId, run_id: ctx.runId, task_id: ctx.taskId, logical_date: ctx.logicalDate, created_at: new Date(),
      conf: ctx.conf, data_interval_start: ctx.dataIntervalStart, data_interval_end: ctx.dataIntervalEnd,
      map_index: ctx.mapIndex, map_value: ctx.mapValue,
    })
    expect(out).toEqual({
      sql: "DELETE FROM sales WHERE ds = '2026-03-02' AND region = 'eu'",
      messages: [{ key: '20260302', value: { start: '2026-03-01T06:00:00.000Z' } }],
      maxRows: 5,
    })
  })

  it('fails clearly on a missing connection or one of the wrong type', async () => {
    const { ctx } = fakeCtx({ pg: { conn_type: 'postgres' } })
    await expect(runOperator('email.send', { to: 'a@x.io', subject: 's' }, ctx)).rejects.toThrow("connection 'smtp_default' not found")
    await expect(runOperator('email.send', { connId: 'pg', to: 'a@x.io', subject: 's' }, ctx))
      .rejects.toThrow("connection 'pg' is of type 'postgres' — expected smtp or email")
    await expect(runOperator('sql.nope', {}, ctx)).rejects.toThrow("unknown operator implementation 'sql.nope'")
  })

  it('withOperatorRuntime gives the ctx a runOperator bound to that ctx', async () => {
    const { ctx } = fakeCtx()
    const full = withOperatorRuntime({ ...ctx })
    await expect(full.runOperator('kafka.produce', { connId: 'none', topic: 't', messages: ['x'] })).rejects.toThrow("connection 'none' not found")
  })

  it('makes driver values JSON-safe', () => {
    expect(toJsonSafe({ d: new Date('2026-01-01T00:00:00Z'), b: Buffer.from('hi'), n: 12n, big: 2n ** 70n, a: [null, undefined] }))
      .toEqual({ d: '2026-01-01T00:00:00.000Z', b: 'aGk=', n: 12, big: '1180591620717411303424', a: [null, null] })
  })
})

describe('provider helpers', () => {
  it('splitSql splits on ; outside quotes, comments and dollar-quoted bodies', () => {
    expect(splitSql(`INSERT INTO t VALUES ('a;b'); -- c;d
      CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql; /* x;y */ SELECT "a;b" ;;`)).toEqual([
      "INSERT INTO t VALUES ('a;b')",
      '-- c;d\n      CREATE FUNCTION f() RETURNS int AS $$ SELECT 1; $$ LANGUAGE sql',
      '/* x;y */ SELECT "a;b"',
    ])
    expect(() => sqlStatements(['SELECT 1', ''], false)).toThrow(/non-empty/)
  })

  it('isTruthy reads numeric strings as numbers (postgres COUNT) and otherwise follows Python', () => {
    expect([0, '0', '0.0', '', null, false, [], {}].map(isTruthy)).toEqual(Array(8).fill(false))
    expect([1, '12', 'no', true, [0], { a: 1 }].map(isTruthy)).toEqual(Array(6).fill(true))
  })

  it('bqRowsToObjects types cells by the schema, including records and repeated fields', () => {
    const fields = [
      { name: 'n', type: 'INTEGER' }, { name: 'f', type: 'FLOAT' }, { name: 'ok', type: 'BOOLEAN' },
      { name: 'at', type: 'TIMESTAMP' }, { name: 'tags', type: 'STRING', mode: 'REPEATED' },
      { name: 'rec', type: 'RECORD', fields: [{ name: 'x', type: 'INTEGER' }] },
    ]
    const rows = [{ f: [{ v: '42' }, { v: '1.5' }, { v: 'true' }, { v: '1.7e9' }, { v: [{ v: 'a' }, { v: 'b' }] }, { v: { f: [{ v: '7' }] } }] }]
    expect(bqRowsToObjects(rows, fields)).toEqual([
      { n: 42, f: 1.5, ok: true, at: '2023-11-14T22:13:20.000Z', tags: ['a', 'b'], rec: { x: 7 } },
    ])
  })

  it('kafkaConfig reads Airflow-style extras or host/login', () => {
    const base = { conn_id: 'k', conn_type: 'kafka', schema: null, extra: null } as const
    expect(kafkaConfig({ ...base, host: 'b1,b2:9093', port: 9092, login: 'u', password: 'p' })).toEqual({
      clientId: 'airflow-nodejs', brokers: ['b1:9092', 'b2:9093'], ssl: false,
      sasl: { mechanism: 'plain', username: 'u', password: 'p' }, groupId: undefined,
    })
    expect(kafkaConfig({ ...base, host: null, port: null, login: null, password: null, extra: {
      'bootstrap.servers': 'k1:9094', 'security.protocol': 'SASL_SSL', 'sasl.mechanism': 'SCRAM-SHA-512',
      'sasl.username': 'su', 'sasl.password': 'sp', 'group.id': 'g1', 'client.id': 'c1',
    } })).toEqual({
      clientId: 'c1', brokers: ['k1:9094'], ssl: true, sasl: { mechanism: 'scram-sha-512', username: 'su', password: 'sp' }, groupId: 'g1',
    })
    expect(() => kafkaConfig({ ...base, host: null, port: null, login: null, password: null })).toThrow(/no bootstrap servers/)
  })

  it('smtpTransport follows the Airflow smtp extras: SSL unless disable_ssl, then STARTTLS unless disable_tls', () => {
    const conn = { conn_id: 's', conn_type: 'smtp', host: 'mx', port: null, schema: null, login: 'u', password: 'p', extra: null }
    expect(smtpTransport(conn)).toMatchObject({ port: 465, secure: true, requireTLS: false, ignoreTLS: false, auth: { user: 'u', pass: 'p' } })
    expect(smtpTransport({ ...conn, extra: { disable_ssl: true } })).toMatchObject({ port: 587, secure: false, requireTLS: true, ignoreTLS: false })
    expect(smtpTransport({ ...conn, port: 25, login: null, extra: { disable_ssl: 'true', disable_tls: true, timeout: 5 } }))
      .toMatchObject({ port: 25, secure: false, requireTLS: false, ignoreTLS: true, auth: undefined, connectionTimeout: 5000 })
  })
})
