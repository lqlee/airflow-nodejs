/**
 * Worker side of the built-in operators: `ctx.runOperator(impl, args)` (see operators.ts) renders the
 * options' templates, runs the implementation and pushes its result as XCom `return_value`.
 * Implementations get connections through the task's own ctx (the execution API — decrypted by the
 * control plane, masked in logs) and load their npm driver lazily (drivers.ts).
 */
import type { ConnectionRuntime } from '../../connections/index.js'
import { renderTemplate, type TemplateContext } from '../../scheduler/template.js'
import { OPERATORS, type OperatorKind } from './catalog.js'

/** The parts of a task ctx the runtime uses. */
export interface OperatorTaskCtx {
  dagId: string
  runId: string
  taskId: string
  mapIndex: number | null
  mapValue: unknown
  conf: Record<string, unknown>
  logicalDate: Date | null
  dataIntervalStart: Date | null
  dataIntervalEnd: Date | null
  xcom: { push: (key: string, value: unknown) => Promise<void> }
  connections: { get: (connId: string) => Promise<ConnectionRuntime | null> }
  log?: { info: (msg: unknown) => void; warn: (msg: unknown) => void }
}

export interface OperatorRuntime {
  /** The connection, which must exist and have one of `types`. */
  connection(connId: string, types: readonly string[]): Promise<ConnectionRuntime>
  /** The connection if `connId` is given (and exists with one of `types`), else null — for default credentials. */
  optionalConnection(connId: string | undefined, types: readonly string[]): Promise<ConnectionRuntime | null>
  log(msg: string): void
}

export type OperatorImpl = (args: Record<string, unknown>, rt: OperatorRuntime) => Promise<unknown>

const MODULES: Record<string, () => Promise<Record<string, OperatorImpl>>> = {
  sql: () => import('./sql.js').then(m => m.IMPLS),
  gcs: () => import('./google.js').then(m => m.IMPLS),
  bigquery: () => import('./google.js').then(m => m.IMPLS),
  kafka: () => import('./kafka.js').then(m => m.IMPLS),
  email: () => import('./email.js').then(m => m.IMPLS),
}

const KIND_BY_IMPL = new Map<string, OperatorKind>(Object.values(OPERATORS).map(s => [s.impl, s.kind]))

/** Render `{{ … }}` in every string of the options (keys are left alone). */
export function renderArgsDeep<T>(value: T, tctx: TemplateContext): T {
  if (typeof value === 'string') return renderTemplate(value, tctx) as T
  if (Array.isArray(value)) return value.map(v => renderArgsDeep(v, tctx)) as T
  if (value && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, renderArgsDeep(v, tctx)])) as T
  }
  return value
}

/** Make a driver result JSON-safe for XCom: Dates → ISO, bytes → base64, bigint → number/string. */
export function toJsonSafe(v: unknown): unknown {
  if (v === null || v === undefined) return v ?? null
  if (v instanceof Date) return Number.isNaN(v.getTime()) ? null : v.toISOString()
  if (typeof v === 'bigint') return Number.isSafeInteger(Number(v)) ? Number(v) : v.toString()
  if (v instanceof Uint8Array) return Buffer.from(v).toString('base64')
  if (Array.isArray(v)) return v.map(toJsonSafe)
  if (typeof v === 'object') return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, toJsonSafe(x)]))
  return v
}

export async function runOperator(impl: string, rawArgs: Record<string, unknown>, ctx: OperatorTaskCtx): Promise<unknown> {
  const kind = KIND_BY_IMPL.get(impl)
  const load = MODULES[impl.split('.')[0]]
  if (!kind || !load) throw new Error(`unknown operator implementation '${impl}'`)

  const tctx: TemplateContext = {
    dag_id: ctx.dagId, run_id: ctx.runId, task_id: ctx.taskId,
    logical_date: ctx.logicalDate, created_at: ctx.logicalDate ?? new Date(), conf: ctx.conf,
    data_interval_start: ctx.dataIntervalStart, data_interval_end: ctx.dataIntervalEnd,
    map_index: ctx.mapIndex, map_value: ctx.mapValue,
  }
  const args = renderArgsDeep(rawArgs, tctx)

  const fetchConn = async (connId: string, types: readonly string[]) => {
    const conn = await ctx.connections.get(connId)
    if (!conn) return null
    const type = conn.conn_type.toLowerCase().replace(/-/g, '_')
    if (!types.includes(type)) throw new Error(`connection '${connId}' is of type '${conn.conn_type}' — expected ${types.join(' or ')}`)
    return { ...conn, conn_type: type }
  }
  const rt: OperatorRuntime = {
    connection: async (connId, types) => {
      const conn = await fetchConn(connId, types)
      if (!conn) throw new Error(`connection '${connId}' not found`)
      return conn
    },
    optionalConnection: async (connId, types) => (connId ? fetchConn(connId, types) : null),
    log: msg => (ctx.log ? ctx.log.info(msg) : console.log(msg)),
  }

  const impls = await load()
  const result = toJsonSafe(await impls[impl](args, rt))
  if (kind === 'poke') return result === true
  if (args.doXcomPush !== false && result !== null && result !== undefined) await ctx.xcom.push('return_value', result)
  return result
}

/** Give a task ctx its `runOperator` (worker, Kubernetes worker-cli, `anode tasks test`). */
export function withOperatorRuntime<T extends object>(ctx: T): T & { runOperator: (impl: string, args: Record<string, unknown>) => Promise<unknown> } {
  const full = ctx as T & { runOperator: (impl: string, args: Record<string, unknown>) => Promise<unknown> }
  full.runOperator = (impl, args) => runOperator(impl, args, full as unknown as OperatorTaskCtx)
  return full
}
