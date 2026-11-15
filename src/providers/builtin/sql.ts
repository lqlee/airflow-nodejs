/**
 * common-sql provider: SQLExecuteQueryOperator, SQLCheckOperator, SqlSensor on postgres (pg) and
 * mysql / mariadb (mysql2) connections.
 *
 *   postgres  host, port (5432), schema = database, login, password;
 *             extra: sslmode (disable | require | verify-ca | verify-full), connect_timeout (s),
 *             application_name, statement_timeout (ms)
 *   mysql     host, port (3306), schema = database, login, password;
 *             extra: ssl (true | { ca, … }), charset, connect_timeout (s)
 *
 * Parameters are bound by the driver, never interpolated: postgres `$1`, mysql `?` (or `:name` with an
 * object). Several statements (an array, or splitStatements) run in one transaction unless autocommit.
 */
import type { ConnectionRuntime } from '../../connections/index.js'
import { loadDriver } from './drivers.js'
import type { OperatorImpl } from './runtime.js'
import { bool, num, sqlParams, sqlStatements, str } from './args.js'

const POSTGRES = ['postgres', 'postgresql']
const MYSQL = ['mysql', 'mariadb']
const SQL_TYPES = [...POSTGRES, ...MYSQL]

/** One statement's outcome: rows for a query, else the affected row count. */
type StatementResult = { rows: Record<string, unknown>[] } | { rowCount: number }

interface SqlSession {
  query(sql: string, params: unknown[] | Record<string, unknown> | undefined): Promise<StatementResult>
  close(): Promise<void>
}

const extraOf = (conn: ConnectionRuntime) => conn.extra ?? {}

async function openPostgres(conn: ConnectionRuntime): Promise<SqlSession> {
  const pg = await loadDriver('pg')
  const extra = extraOf(conn)
  const sslmode = String(extra.sslmode ?? 'prefer')
  const ssl = ['require', 'verify-ca', 'verify-full'].includes(sslmode) ? { rejectUnauthorized: sslmode !== 'require' } : undefined
  const client = new pg.Client({
    host: conn.host ?? 'localhost', port: conn.port ?? 5432, database: conn.schema ?? undefined,
    user: conn.login ?? undefined, password: conn.password ?? undefined, ssl,
    connectionTimeoutMillis: extra.connect_timeout !== undefined ? Number(extra.connect_timeout) * 1000 : 30_000,
    application_name: typeof extra.application_name === 'string' ? extra.application_name : 'airflow-nodejs',
    statement_timeout: extra.statement_timeout !== undefined ? Number(extra.statement_timeout) : undefined,
  })
  await client.connect()
  return {
    async query(sql, params) {
      if (params !== undefined && !Array.isArray(params)) throw new Error('postgres parameters must be an array (bound to $1, $2, …)')
      const raw = await client.query({ text: sql, values: params })
      const r = Array.isArray(raw) ? raw[raw.length - 1] : raw   // a multi-statement string returns one result each
      return r.fields?.length ? { rows: r.rows as Record<string, unknown>[] } : { rowCount: r.rowCount ?? 0 }
    },
    close: () => client.end(),
  }
}

async function openMysql(conn: ConnectionRuntime): Promise<SqlSession> {
  const mysql = await loadDriver('mysql2/promise')
  const extra = extraOf(conn)
  const ssl = extra.ssl === true || extra.ssl === 'true' ? {} : (extra.ssl && typeof extra.ssl === 'object' ? extra.ssl as Record<string, unknown> : undefined)
  const connection = await mysql.createConnection({
    host: conn.host ?? 'localhost', port: conn.port ?? 3306, database: conn.schema ?? undefined,
    user: conn.login ?? undefined, password: conn.password ?? undefined,
    ssl: ssl as never, charset: typeof extra.charset === 'string' ? extra.charset : undefined,
    connectTimeout: extra.connect_timeout !== undefined ? Number(extra.connect_timeout) * 1000 : 30_000,
    multipleStatements: false, supportBigNumbers: true, bigNumberStrings: true,
  })
  return {
    async query(sql, params) {
      const named = params !== undefined && !Array.isArray(params)
      const [res] = await connection.query({ sql, values: params as never, namedPlaceholders: named })
      return Array.isArray(res) ? { rows: res as Record<string, unknown>[] } : { rowCount: (res as { affectedRows?: number }).affectedRows ?? 0 }
    },
    close: () => connection.end(),
  }
}

async function openSession(conn: ConnectionRuntime): Promise<SqlSession> {
  return POSTGRES.includes(conn.conn_type) ? openPostgres(conn) : openMysql(conn)
}

/** Run statements on a fresh connection (in a transaction when several and not autocommit). */
async function runStatements(
  args: Record<string, unknown>, rt: Parameters<OperatorImpl>[1], statements: string[], autocommit: boolean,
): Promise<StatementResult[]> {
  const conn = await rt.connection(str(args.connId, 'connId'), SQL_TYPES)
  const params = sqlParams(args.parameters)
  const session = await openSession(conn)
  const transactional = !autocommit && statements.length > 1
  try {
    if (transactional) await session.query(POSTGRES.includes(conn.conn_type) ? 'BEGIN' : 'START TRANSACTION', undefined)
    const results: StatementResult[] = []
    for (const [i, sql] of statements.entries()) {
      const r = await session.query(sql, params)
      rt.log(`[sql] statement ${i + 1}/${statements.length}: ${'rows' in r ? `${r.rows.length} row(s)` : `${r.rowCount} row(s) affected`}`)
      results.push(r)
    }
    if (transactional) await session.query('COMMIT', undefined)
    return results
  } catch (err) {
    if (transactional) await session.query('ROLLBACK', undefined).catch(() => undefined)
    throw err
  } finally {
    await session.close().catch(() => undefined)
  }
}

/** Python-style truthiness, with numeric strings read as numbers (postgres returns COUNT(*) as a string). */
export function isTruthy(v: unknown): boolean {
  if (typeof v === 'string' && /^\s*-?\d+(\.\d+)?\s*$/.test(v)) return Number(v) !== 0
  if (Array.isArray(v)) return v.length > 0
  if (v && typeof v === 'object') return Object.keys(v).length > 0
  return Boolean(v)
}

const firstRow = (r: StatementResult | undefined) => (r && 'rows' in r ? r.rows[0] : undefined)

export const IMPLS: Record<string, OperatorImpl> = {
  'sql.execute': async (args, rt) => {
    const statements = sqlStatements(args.sql, bool(args.splitStatements, 'splitStatements') ?? false)
    const maxRows = num(args.maxRows, 'maxRows') ?? 1000
    const results = await runStatements(args, rt, statements, bool(args.autocommit, 'autocommit') ?? false)
    const shape = (r: StatementResult) => {
      if (!('rows' in r)) return { rowCount: r.rowCount }
      if (r.rows.length > maxRows) rt.log(`[sql] ${r.rows.length} rows — returning the first ${maxRows} (maxRows)`)
      return r.rows.slice(0, maxRows)
    }
    return (bool(args.returnLast, 'returnLast') ?? true) ? shape(results[results.length - 1]) : results.map(shape)
  },
  'sql.check': async (args, rt) => {
    const [result] = await runStatements(args, rt, [str(args.sql, 'sql')], true)
    const row = firstRow(result)
    if (!row) throw new Error('SQL check failed: the query returned no rows')
    const falsy = Object.entries(row).filter(([, v]) => !isTruthy(v)).map(([k]) => k)
    if (falsy.length) throw new Error(`SQL check failed: falsy value(s) in ${falsy.join(', ')} — ${JSON.stringify(row)}`)
    return null
  },
  'sql.sensor': async (args, rt) => {
    const [result] = await runStatements(args, rt, [str(args.sql, 'sql')], true)
    const row = firstRow(result)
    return row !== undefined && isTruthy(Object.values(row)[0])
  },
}
