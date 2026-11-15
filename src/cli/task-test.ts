/**
 * `anode tasks test <dag_id> <task_id>` — run one task's function locally, like `airflow tasks test`:
 * no dependency checks, no state written. Imports the Dag file from the Dag folder in this process, so
 * use it on a developer machine with the Dag code checked out, not on the control plane.
 *
 *   XCom        pushes stay in memory and are printed; pulls read earlier pushes of this test, else
 *               (with --run-id) the XComs of that real run through the REST API
 *   variables   the local secrets backend (SECRETS_BACKEND, e.g. env: AIRFLOW_VAR_<KEY>), else the REST
 *               API — which never returns secret values
 *   connections the local secrets backend (AIRFLOW_CONN_<ID>), else the REST API without password/extra
 *   datasets    emitted events are printed, not recorded
 *   defer()     the trigger is polled in this process until it fires or times out
 *
 * Runs run / poke (sensor) / branch / shortCircuit functions. Shell, Python, Java, container,
 * Kubernetes and cross-Dag tasks need the scheduler — trigger the Dag for those.
 */
import { readFile } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'
import { listDagFiles, parseDagFile } from '../dag/loader.js'
import { validateParams } from '../dag/params.js'
import type { DagDefinition, TaskDefinition } from '../dag/types.js'
import { isDynamicMapped, isLiteralMapped } from '../scheduler/mapping.js'
import { runInfoForLogicalDate } from '../scheduler/data-interval.js'
import { getSecretsBackend } from '../secrets/index.js'
import { registerSecret, registerSensitiveFields, redact } from '../secrets/masker.js'
import type { ConnectionRuntime } from '../connections/index.js'
import type { EmittedDataset } from '../datasets/index.js'
import { ApiError, type ApiClient } from './client.js'
import type { Io } from './output.js'

export interface TaskTestOptions {
  dagId: string
  taskId: string
  dagsDir: string
  /** Dag file to import instead of searching the Dag folder. */
  file?: string
  logicalDate?: Date
  conf?: Record<string, unknown>
  mapIndex?: number
  /** Map value for mapped tasks (required when the task maps over an upstream XCom). */
  mapValue?: unknown
  /** Pull upstream XComs from this real run. */
  runId?: string
  /** REST API for variables, connections and --run-id XComs; null = local secrets backend only. */
  api: ApiClient | null
  io: Io
}

export interface TaskTestResult {
  ok: boolean
  error?: string
  xcoms: Record<string, unknown>
  datasets: EmittedDataset[]
}

type TaskFn = (ctx: unknown) => Promise<unknown>
type Kind = 'run' | 'poke' | 'branch' | 'shortCircuit'

const UNSUPPORTED = ['shell', 'python', 'java', 'container', 'kubernetes', 'triggerDag', 'externalTask'] as const

class DeferSignal {
  constructor(readonly trigger: TaskFn, readonly interval: number, readonly timeout: number) {}
}

/** Import the Dag folder's files until one defines `dagId` — files mentioning the id first. */
export async function findDag(dagId: string, dagsDir: string, file?: string): Promise<DagDefinition> {
  if (file) {
    const { dag } = await parseDagFile(resolve(file))
    if (dag.id !== dagId) throw new Error(`${file} defines Dag '${dag.id}', not '${dagId}'`)
    return dag
  }
  const files = await listDagFiles(dagsDir)
  if (!files) throw new Error(`Dag folder not found: ${dagsDir} (set DAG_DIR or --dags-dir)`)
  const mentions = await Promise.all(files.map(async f => (await readFile(join(dagsDir, f), 'utf8')).includes(dagId)))
  const ordered = [...files.filter((_, i) => mentions[i]), ...files.filter((_, i) => !mentions[i])]
  const errors: string[] = []
  for (const f of ordered) {
    try {
      const { dag } = await parseDagFile(join(dagsDir, f))
      if (dag.id === dagId) return dag
    } catch (err) {
      errors.push(`\n  ${f}: ${(err as Error).message}`)
    }
  }
  throw new Error(`Dag '${dagId}' not found in ${dagsDir}${errors.length ? ` — files that failed to import:${errors.join('')}` : ''}`)
}

function taskFunction(task: TaskDefinition, taskId: string): { kind: Kind; fn: TaskFn } {
  const other = UNSUPPORTED.find(k => task[k] !== undefined)
  if (other) throw new Error(`'${taskId}' is a ${other} task — tasks test runs function tasks (run, poke, branch, shortCircuit); trigger the Dag to run it`)
  if (task.poke) return { kind: 'poke', fn: task.poke as TaskFn }
  if (task.branch) return { kind: 'branch', fn: task.branch as TaskFn }
  if (task.shortCircuit) return { kind: 'shortCircuit', fn: task.shortCircuit as TaskFn }
  if (task.run) return { kind: 'run', fn: task.run as TaskFn }
  throw new Error(`'${taskId}' has no function to run`)
}

function mapping(task: TaskDefinition, taskId: string, opts: TaskTestOptions): { mapIndex: number | null; mapValue: unknown } {
  const hasValue = opts.mapValue !== undefined
  if (isLiteralMapped(task.expand)) {
    if (hasValue) return { mapIndex: opts.mapIndex ?? 0, mapValue: opts.mapValue }
    const n = task.expand.length
    if (opts.mapIndex === undefined) throw new Error(`'${taskId}' is mapped over ${n} value(s) — pass --map-index 0..${n - 1}`)
    if (opts.mapIndex < 0 || opts.mapIndex >= n) throw new Error(`--map-index must be 0..${n - 1}`)
    return { mapIndex: opts.mapIndex, mapValue: task.expand[opts.mapIndex] }
  }
  if (isDynamicMapped(task.expand)) {
    if (!hasValue) throw new Error(`'${taskId}' maps over XCom '${task.expand.from}.${task.expand.key}' — pass --map-value <json>`)
    return { mapIndex: opts.mapIndex ?? 0, mapValue: opts.mapValue }
  }
  if (opts.mapIndex !== undefined || hasValue) throw new Error(`'${taskId}' is not a mapped task`)
  return { mapIndex: null, mapValue: null }
}

function runFields(dag: DagDefinition, logicalDate: Date | undefined) {
  if (!logicalDate) {
    return { logicalDate: null, dataIntervalStart: null, dataIntervalEnd: null, ds: new Date().toISOString().slice(0, 10) }
  }
  let interval = { start: logicalDate, end: logicalDate }
  try { interval = runInfoForLogicalDate(dag, logicalDate).dataInterval } catch { /* not a cron schedule */ }
  return {
    logicalDate, dataIntervalStart: interval.start, dataIntervalEnd: interval.end,
    ds: logicalDate.toISOString().slice(0, 10),
  }
}

const preview = (v: unknown) => { const s = JSON.stringify(v) ?? String(v); return s.length > 200 ? `${s.slice(0, 200)}…` : s }
const seg = encodeURIComponent

export async function testTask(opts: TaskTestOptions): Promise<TaskTestResult> {
  const { io, api, dagId, taskId } = opts
  const xcoms: Record<string, unknown> = {}
  const datasets: EmittedDataset[] = []
  const warned = new Set<string>()
  const warnOnce = (key: string, msg: string) => { if (!warned.has(key)) { warned.add(key); io.err(`[warn] ${msg}`) } }

  /** REST read that maps 404 to null and an unreachable API to null with a warning. */
  const apiGet = async <T>(path: string, query?: Record<string, string>): Promise<T | null> => {
    if (!api) return null
    try {
      return await api.get<T>(path, query)
    } catch (err) {
      if (err instanceof ApiError && err.status === 404) return null
      if (err instanceof ApiError && (err.status === 0 || err.status === 401 || err.status === 403)) {
        warnOnce(`api${err.status}`, `REST API unavailable for lookups: ${err.message}`)
        return null
      }
      throw err
    }
  }

  const started = performance.now()
  try {
    const dag = await findDag(dagId, opts.dagsDir, opts.file)
    const task = dag.tasks[taskId]
    if (!task) throw new Error(`Dag '${dagId}' has no task '${taskId}' (tasks: ${Object.keys(dag.tasks).join(', ')})`)
    const { kind, fn } = taskFunction(task, taskId)
    const { mapIndex, mapValue } = mapping(task, taskId, opts)
    const params = validateParams(dag, opts.conf ?? {})
    if (!params.ok) throw new Error(`param validation failed:${params.errors.map(e => `\n  ${e.param}: ${e.message}`).join('')}`)
    const conf = params.mergedConf
    const backend = getSecretsBackend()

    const log = (level: string, write: (s: string) => void) =>
      (msg: unknown) => write(`[${level}] ${typeof msg === 'string' ? msg : JSON.stringify(msg)}`)

    const baseCtx = {
      dagId, taskId, mapIndex, mapValue,
      runId: opts.runId ?? `test__${new Date().toISOString()}`,
      conf, params: conf,
      ...runFields(dag, opts.logicalDate),
      xcom: {
        push: async (key: string, value: unknown) => {
          xcoms[key] = value
          io.out(`[xcom] push ${key} = ${preview(value)}`)
        },
        pull: async (fromTaskId: string, key: string) => {
          if (fromTaskId === taskId && key in xcoms) return xcoms[key]
          if (!opts.runId) {
            warnOnce('xcom', `no --run-id: XCom pulls from other tasks return null`)
            return null
          }
          const rows = await apiGet<{ map_index: number | null; value: unknown }[]>(
            `/dag-runs/${seg(opts.runId)}/xcoms`, { task_id: fromTaskId, key })
          if (!rows?.length) return null
          // Mapped upstream: every instance's value by map_index (as the worker's pull does)
          return rows.some(r => r.map_index !== null) ? rows.map(r => r.value) : rows[rows.length - 1].value
        },
      },
      variables: {
        get: async (key: string): Promise<string | null> => {
          const local = await backend.getVariable(key)
          if (local !== null) { registerSecret(local); return local }
          const v = await apiGet<{ value: string | null; is_secret?: boolean }>(`/variables/${seg(key)}`)
          if (v?.is_secret && v.value === null) {
            warnOnce(`var:${key}`, `variable '${key}' is secret — the REST API does not return it; set AIRFLOW_VAR_${key.toUpperCase()} with SECRETS_BACKEND=env`)
          }
          return v?.value ?? null
        },
      },
      connections: {
        get: async (connId: string): Promise<ConnectionRuntime | null> => {
          const local = await backend.getConnection(connId)
          if (local) { registerSecret(local.password); registerSensitiveFields(local.extra); return local }
          const c = await apiGet<Omit<ConnectionRuntime, 'password' | 'extra'> & { has_password?: boolean; has_extra?: boolean }>(`/connections/${seg(connId)}`)
          if (!c) return null
          if (c.has_password || c.has_extra) {
            warnOnce(`conn:${connId}`, `connection '${connId}': the REST API does not return password/extra; set AIRFLOW_CONN_${connId.toUpperCase()} with SECRETS_BACKEND=env`)
          }
          return { conn_id: c.conn_id, conn_type: c.conn_type, host: c.host, port: c.port, schema: c.schema, login: c.login, password: null, extra: null }
        },
      },
      emitDataset: (uri: string, o: { alias?: string; extra?: Record<string, unknown> } = {}) => {
        datasets.push({ uri, ...o })
        io.out(`[dataset] ${uri}${o.alias ? ` (alias ${o.alias})` : ''}`)
      },
      log: { debug: log('DEBUG', io.out), info: log('INFO', io.out), warn: log('WARN', io.err), error: log('ERROR', io.err) },
    }
    const ctx = {
      ...baseCtx,
      defer: (trigger: TaskFn, o: { timeout?: number; interval?: number } = {}): Promise<never> => {
        throw new DeferSignal(trigger, Math.max(100, o.interval ?? 10_000), o.timeout ?? 0)
      },
    }

    io.out(`[test] ${dagId}.${taskId}${mapIndex !== null ? `[${mapIndex}]` : ''} (${kind}) — nothing is recorded`)
    const call = withTimeout(fn, task.timeout)

    if (kind === 'poke') {
      const interval = Math.max(1_000, task.pokeInterval ?? 30_000)
      const deadline = Date.now() + (task.sensorTimeout ?? 3_600_000)
      for (let n = 1; !(await call(ctx)); n++) {
        if (Date.now() + interval > deadline) throw new Error(`sensor timed out after ${n} poke(s)`)
        io.out(`[poke] #${n} not ready — next poke in ${interval}ms`)
        await sleep(interval)
      }
    } else {
      try {
        const result = await call(ctx)
        if (kind === 'branch') io.out(`[branch] → ${result === null || result === undefined ? '(none — all downstream skipped)' : [result].flat().join(', ')}`)
        if (kind === 'shortCircuit') io.out(`[short-circuit] ${result ? 'truthy — downstream runs' : 'falsy — downstream would be skipped'}`)
      } catch (err) {
        if (!(err instanceof DeferSignal)) throw err
        datasets.length = 0   // events emitted before defer() are discarded, as in a real run
        const deferredAt = Date.now()
        for (let n = 1; !(await err.trigger(baseCtx)); n++) {
          if (err.timeout > 0 && Date.now() - deferredAt + err.interval > err.timeout) throw new Error(`deferred task timed out after ${err.timeout}ms`)
          io.out(`[defer] trigger check #${n} not ready — next in ${err.interval}ms`)
          await sleep(err.interval)
        }
      }
    }
    io.out(`[test] ✓ ${dagId}.${taskId} succeeded in ${Math.round(performance.now() - started)}ms`)
    return { ok: true, xcoms, datasets }
  } catch (err) {
    const error = redact(err instanceof Error ? err.message : String(err))
    io.err(`[test] ✗ ${dagId}.${taskId} failed: ${error}`)
    return { ok: false, error, xcoms, datasets }
  }
}

/** Fail the call once the task's `timeout` (ms) passes, as the executor does. */
function withTimeout(fn: TaskFn, timeoutMs: number | undefined): TaskFn {
  if (!timeoutMs || timeoutMs <= 0) return fn
  return async ctx => {
    let timer: NodeJS.Timeout | undefined
    try {
      return await Promise.race([
        fn(ctx),
        new Promise<never>((_, reject) => { timer = setTimeout(() => reject(new Error(`task timed out after ${timeoutMs}ms`)), timeoutMs) }),
      ])
    } finally {
      clearTimeout(timer)
    }
  }
}
