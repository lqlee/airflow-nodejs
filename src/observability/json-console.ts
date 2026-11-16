/**
 * LOG_FORMAT=json: every console.log/info/warn/error/debug of a server process becomes one JSON object
 * per line on stdout — what Splunk (and the WCNP log forwarder) index without a parsing rule:
 *
 *   {"ts":"2026-03-02T06:00:00.123Z","level":"info","logger":"scheduler","msg":"run 65f… → success","service":"airflow-nodejs","pid":7}
 *
 * The `[component]` prefix the code already writes becomes `logger`; an Error argument becomes `err`
 * (name, message, stack). Known secret values are masked, as in task logs.
 *
 * Only the server processes install this (main, the standalone worker). Task output these processes
 * echo (echoTaskLine) becomes a record tagged with the task instance; what is stored as the task's log
 * (src/logs) is unchanged.
 */
import { format } from 'node:util'
import { redact } from '../secrets/masker.js'
import { parseLevelFromLine } from '../logs/index.js'

export type ConsoleLevel = 'debug' | 'info' | 'warn' | 'error'

export interface LogRecord {
  ts: string
  level: ConsoleLevel
  logger?: string
  msg: string
  service: string
  pid: number
  err?: { name: string; message: string; stack?: string }
}

const PREFIX = /^\s*\[([\w.:/-]+)\]\s*/

/** One console call → the JSON record written for it. */
export function formatLogRecord(level: ConsoleLevel, args: unknown[], now: Date = new Date()): LogRecord {
  const err = args.find((a): a is Error => a instanceof Error)
  let msg = format(...args.map(a => (err && a === err ? err.message : a)))
  let logger: string | undefined
  const m = PREFIX.exec(msg)
  if (m) { logger = m[1]; msg = msg.slice(m[0].length) }
  const record: LogRecord = {
    ts: now.toISOString(),
    level,
    ...(logger ? { logger } : {}),
    msg: redact(msg.trim()),
    service: process.env.OTEL_SERVICE_NAME || 'airflow-nodejs',
    pid: process.pid,
  }
  if (err) record.err = { name: err.name, message: redact(err.message), ...(err.stack ? { stack: redact(err.stack) } : {}) }
  return record
}

const METHODS: Array<[keyof Console & ('log' | 'info' | 'warn' | 'error' | 'debug'), ConsoleLevel]> = [
  ['log', 'info'], ['info', 'info'], ['warn', 'warn'], ['error', 'error'], ['debug', 'debug'],
]

let restore: (() => void) | null = null
let writeLine: ((line: string) => void) | null = null

/**
 * Route console output through formatLogRecord when LOG_FORMAT=json (or `force`). `write` defaults to
 * stdout. Returns whether it was installed; uninstallJsonConsole() puts the original methods back.
 */
export function installJsonConsole(opts: { force?: boolean; write?: (line: string) => void } = {}): boolean {
  if (restore) return true
  if (!opts.force && process.env.LOG_FORMAT?.trim().toLowerCase() !== 'json') return false
  const write = opts.write ?? ((line: string) => { process.stdout.write(line) })
  writeLine = write
  const originals = METHODS.map(([name]) => [name, console[name]] as const)
  for (const [name, level] of METHODS) {
    console[name] = (...args: unknown[]) => {
      let line: string
      try { line = JSON.stringify(formatLogRecord(level, args)) } catch { line = JSON.stringify({ ts: new Date().toISOString(), level, msg: String(args[0]) }) }
      write(`${line}\n`)
    }
  }
  restore = () => { for (const [name, fn] of originals) console[name] = fn }
  return true
}

export function uninstallJsonConsole(): void {
  restore?.()
  restore = null
  writeLine = null
}

/**
 * Echo a task's output line on this process's console. Plain mode writes `plain` (default: the line);
 * JSON mode writes a record tagged with the task instance, at the level the line declares ([WARN] …)
 * or its stream's default.
 */
export function echoTaskLine(
  ti: { dag_id: string; dag_run_id: string; task_id: string; map_index?: number | null },
  stream: 'stdout' | 'stderr',
  line: string,
  plain: string = line,
): void {
  if (!writeLine) {
    (stream === 'stderr' ? process.stderr : process.stdout).write(`${plain}\n`)
    return
  }
  writeLine(`${JSON.stringify({
    ts: new Date().toISOString(),
    level: parseLevelFromLine(line) ?? (stream === 'stderr' ? 'error' : 'info'),
    logger: 'task',
    msg: redact(line),
    service: process.env.OTEL_SERVICE_NAME || 'airflow-nodejs',
    pid: process.pid,
    dag_id: ti.dag_id,
    run_id: ti.dag_run_id,
    task_id: ti.task_id,
    ...(ti.map_index != null && ti.map_index >= 0 ? { map_index: ti.map_index } : {}),
  })}\n`)
}
