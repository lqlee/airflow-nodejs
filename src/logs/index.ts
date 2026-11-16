/**
 * Task Log Backend — pluggable log storage.
 *
 * Default: 'file' — writes one .log file per task under LOG_DIR.
 * Alternative: 'mongodb' — stores lines in task_logs collection (legacy).
 *
 * Select via LOG_BACKEND env var:
 *   LOG_BACKEND=file      (default) — logs/<dag_id>/<run_id>/<task_id>.log
 *   LOG_BACKEND=mongodb   — MongoDB task_logs collection
 *   LOG_BACKEND=gcs       — lines are written to the local file while the task runs; when its process
 *                           exits they are appended to gs://$LOG_GCS_BUCKET/$LOG_GCS_PREFIX/<same path>
 *                           and the local file is removed. Reads return the object plus any local lines
 *                           not uploaded yet, so a retry on another replica — or a restarted pod —
 *                           still shows every try. See ./gcs.ts.
 *
 * Log directory: LOG_DIR env var, default: './logs' (relative to cwd).
 *
 * File format (one JSON line per log entry):
 *   {"ts":"2024-01-15T06:03:01.456Z","stream":"stdout","level":"info","line":"[INFO] starting"}
 *
 * This keeps MongoDB lean: it stores run/task state metadata only.
 * Log files can be rotated, shipped to S3, or discarded independently.
 */

import type { Db } from 'mongodb'
import { appendFile, mkdir, readFile, unlink } from 'node:fs/promises'
import { resolve, dirname, relative, sep } from 'node:path'
import { existsSync } from 'node:fs'
import type { ChildProcess } from 'node:child_process'
import { redact } from '../secrets/masker.js'
import { trackWork } from '../scheduler/pool.js'
import { gcsLogLocation, downloadObject, uploadObject } from './gcs.js'

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface LogLine {
  dag_run_id: string
  dag_id:     string
  task_id:    string
  ts:         Date
  stream:     'stdout' | 'stderr'
  level:      LogLevel
  line:       string
}

const LEVEL_RANK: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 }

// ── Backend selection ─────────────────────────────────────────────────────────

type LogBackend = 'file' | 'mongodb' | 'gcs'

function getBackend(): LogBackend {
  const b = (process.env.LOG_BACKEND ?? 'file').toLowerCase().trim()
  return b === 'mongodb' || b === 'gcs' ? b : 'file'
}

function getLogDir(): string {
  return resolve(process.cwd(), process.env.LOG_DIR ?? 'logs')
}

// ── Helpers ───────────────────────────────────────────────────────────────────

/**
 * Detect log level from a line prefix written by ctx.log.*
 * Lines from ctx.log are prefixed: "[INFO] ...", "[WARN] ...", etc.
 */
export function parseLevelFromLine(line: string): LogLevel | null {
  const match = /^\[(DEBUG|INFO|WARN|ERROR)\] /.exec(line)
  if (!match) return null
  const map: Record<string, LogLevel> = { DEBUG: 'debug', INFO: 'info', WARN: 'warn', ERROR: 'error' }
  return map[match[1]] ?? null
}

/** Safe file path: replaces any char that isn't alphanumeric/hyphen/underscore with '_' */
function safeName(s: string): string {
  return s.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0, 64)
}

function taskLogPath(dagId: string, runId: string, taskId: string): string {
  return resolve(getLogDir(), safeName(dagId), safeName(runId), `${safeName(taskId)}.log`)
}

/**
 * Operations on one log file run one after another, in call order: appends keep the order the lines
 * were produced in, and an upload (gcs) sees every line appended before it was requested.
 */
const fileQueues = new Map<string, Promise<void>>()
function inFileOrder(path: string, op: () => Promise<void>): Promise<void> {
  const next = (fileQueues.get(path) ?? Promise.resolve()).catch(() => {}).then(op)
  fileQueues.set(path, next)
  next.finally(() => { if (fileQueues.get(path) === next) fileQueues.delete(path) }).catch(() => {})
  return next
}

// ── File backend ──────────────────────────────────────────────────────────────

async function appendLogFile(
  dagId: string, runId: string, taskId: string,
  stream: 'stdout' | 'stderr', line: string, level: LogLevel,
): Promise<void> {
  const filePath = taskLogPath(dagId, runId, taskId)
  const entry = JSON.stringify({ ts: new Date().toISOString(), stream, level, line }) + '\n'
  await inFileOrder(filePath, async () => {
    const dir = dirname(filePath)
    if (!existsSync(dir)) await mkdir(dir, { recursive: true })
    await appendFile(filePath, entry, 'utf8')
  })
}

async function readLogFile(
  dagId: string, runId: string, taskId: string,
  opts: { level?: LogLevel; stream?: 'stdout' | 'stderr' } = {},
): Promise<LogLine[]> {
  const filePath = taskLogPath(dagId, runId, taskId)
  if (!existsSync(filePath)) return []
  return parseLogFile(await readFile(filePath, 'utf8'), dagId, runId, taskId, opts)
}

/** The JSON-lines format shared by the file and gcs backends → LogLines, filtered. */
function parseLogFile(
  raw: string, dagId: string, runId: string, taskId: string,
  opts: { level?: LogLevel; stream?: 'stdout' | 'stderr' } = {},
): LogLine[] {
  const minRank = opts.level ? LEVEL_RANK[opts.level] : 0

  const lines: LogLine[] = []
  for (const row of raw.split('\n').filter(Boolean)) {
    try {
      const parsed = JSON.parse(row) as { ts: string; stream: string; level: string; line: string }
      const level = parsed.level as LogLevel
      if (LEVEL_RANK[level] < minRank) continue
      if (opts.stream && parsed.stream !== opts.stream) continue
      lines.push({
        dag_run_id: runId,
        dag_id:     dagId,
        task_id:    taskId,
        ts:         new Date(parsed.ts),
        stream:     parsed.stream as 'stdout' | 'stderr',
        level,
        line:       parsed.line,
      })
    } catch {
      // malformed line — skip
    }
  }
  return lines  // already ordered by append time (file is append-only)
}

// ── GCS backend (local staging file + one object per task instance) ──────────

function gcsObjectName(dagId: string, runId: string, taskId: string): { bucket: string; name: string } {
  const { bucket, prefix } = gcsLogLocation()
  const rel = relative(getLogDir(), taskLogPath(dagId, runId, taskId)).split(sep).join('/')
  return { bucket, name: prefix ? `${prefix}/${rel}` : rel }
}

async function readLogGcs(
  dagId: string, runId: string, taskId: string,
  opts: { level?: LogLevel; stream?: 'stdout' | 'stderr' } = {},
): Promise<LogLine[]> {
  const { bucket, name } = gcsObjectName(dagId, runId, taskId)
  const filePath = taskLogPath(dagId, runId, taskId)
  const [remote, local] = await Promise.all([
    downloadObject(bucket, name),
    existsSync(filePath) ? readFile(filePath, 'utf8').catch(() => '') : Promise.resolve(''),
  ])
  return parseLogFile(`${remote ?? ''}${local}`, dagId, runId, taskId, opts)
}

/**
 * LOG_BACKEND=gcs: append the task's staged lines to its object and remove the local file. No-op for
 * the other backends, or when nothing is staged.
 */
export async function uploadTaskLog(dagId: string, runId: string, taskId: string): Promise<void> {
  if (getBackend() !== 'gcs') return
  const filePath = taskLogPath(dagId, runId, taskId)
  await inFileOrder(filePath, async () => {
    if (!existsSync(filePath)) return
    const local = await readFile(filePath, 'utf8')
    if (local) {
      const { bucket, name } = gcsObjectName(dagId, runId, taskId)
      const remote = await downloadObject(bucket, name)   // earlier tries, possibly from another replica
      await uploadObject(bucket, name, `${remote ?? ''}${local}`)
    }
    await unlink(filePath)
  })
}

/**
 * Upload the task's log once `child` (the process whose output is being logged) has exited and its
 * output streams are drained, so the last lines are included. Tracked, so graceful shutdown waits.
 */
export function uploadTaskLogOnExit(child: ChildProcess, ti: { dag_id: string; dag_run_id: string; task_id: string }): void {
  if (getBackend() !== 'gcs') return
  child.once('close', () => {
    void trackWork(uploadTaskLog(ti.dag_id, ti.dag_run_id, ti.task_id).catch((err: unknown) => {
      console.error(`[logs] uploading the log of ${ti.dag_id}.${ti.task_id} (run ${ti.dag_run_id}) failed — kept locally: ${(err as Error).message}`)
    }))
  })
}

// ── MongoDB backend ───────────────────────────────────────────────────────────

async function appendLogMongo(
  db: Db, dagId: string, runId: string, taskId: string,
  stream: 'stdout' | 'stderr', line: string, level: LogLevel,
): Promise<void> {
  await db.collection<LogLine>('task_logs').insertOne({
    dag_run_id: runId,
    dag_id:     dagId,
    task_id:    taskId,
    ts:         new Date(),
    stream,
    level,
    line,
  })
}

async function readLogMongo(
  db: Db, dagId: string, runId: string, taskId: string,
  opts: { level?: LogLevel; stream?: 'stdout' | 'stderr' } = {},
): Promise<LogLine[]> {
  const minRank = opts.level ? LEVEL_RANK[opts.level] : 0
  const levelsToInclude = (Object.keys(LEVEL_RANK) as LogLevel[]).filter(l => LEVEL_RANK[l] >= minRank)
  const query: Record<string, unknown> = { dag_run_id: runId, task_id: taskId, level: { $in: levelsToInclude } }
  if (opts.stream) query.stream = opts.stream
  return db.collection<LogLine>('task_logs').find(query).sort({ ts: 1 }).toArray()
}

// ── Public API ────────────────────────────────────────────────────────────────

/**
 * Append a log line for a task instance.
 * Routes to file or MongoDB backend based on LOG_BACKEND env var.
 * level defaults: stdout → 'info', stderr → 'error'
 */
export async function appendLog(
  db: Db,
  dagRunId: string,
  dagId: string,
  taskId: string,
  stream: 'stdout' | 'stderr',
  line: string,
  level?: LogLevel,
): Promise<void> {
  const resolvedLevel: LogLevel = level ?? (stream === 'stderr' ? 'error' : 'info')
  const backend = getBackend()
  // Second line of defence: secrets this process knows are masked even if the worker missed them
  const masked = redact(line)

  if (backend === 'file' || backend === 'gcs') {
    await appendLogFile(dagId, dagRunId, taskId, stream, masked, resolvedLevel)
  } else {
    await appendLogMongo(db, dagId, dagRunId, taskId, stream, masked, resolvedLevel)
  }
}

/**
 * Fetch log lines for a task instance.
 * Routes to the same backend used for writing.
 * Filters: level (minimum severity), stream (stdout|stderr).
 */
export async function getTaskLogs(
  db: Db,
  dagRunId: string,
  taskId: string,
  opts: { level?: LogLevel; stream?: 'stdout' | 'stderr' } = {},
  dagId?: string,  // required for file backend; optional for mongodb (read from collection)
): Promise<LogLine[]> {
  const backend = getBackend()

  if (backend === 'file') {
    // For file backend we need dag_id to build the path.
    // If not supplied, fall back to listing from the run directory.
    const resolvedDagId = dagId ?? await resolveDagIdFromRun(dagRunId)
    if (!resolvedDagId) return []
    return readLogFile(resolvedDagId, dagRunId, taskId, opts)
  } else if (backend === 'gcs') {
    if (!dagId) return []
    return readLogGcs(dagId, dagRunId, taskId, opts)
  } else {
    return readLogMongo(db, dagId ?? '', dagRunId, taskId, opts)
  }
}

/**
 * Resolve dag_id from dag_run_id by querying MongoDB metadata.
 * Used by file backend when dag_id is not passed directly.
 */
async function resolveDagIdFromRun(_runId: string): Promise<string | null> {
  // This is called from the API which has access to the DB via app.mongo.
  // Callers that have dag_id available should pass it directly.
  // This fallback reads from file system path listing (not implemented here —
  // callers are expected to pass dag_id when using file backend).
  return null
}

/** Returns the log file path for a task (useful for debugging / external tools). */
export function getLogFilePath(dagId: string, runId: string, taskId: string): string {
  return taskLogPath(dagId, runId, taskId)
}

/** Returns the current log backend name ('file', 'mongodb' or 'gcs'). */
export function getLogBackendName(): LogBackend {
  return getBackend()
}
