/**
 * Task Log Backend — pluggable log storage.
 *
 * Default: 'file' — writes one .log file per task under LOG_DIR.
 * Alternative: 'mongodb' — stores lines in task_logs collection (legacy).
 *
 * Select via LOG_BACKEND env var:
 *   LOG_BACKEND=file      (default) — logs/<dag_id>/<run_id>/<task_id>.log
 *   LOG_BACKEND=mongodb   — MongoDB task_logs collection
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
import { appendFile, mkdir, readFile } from 'node:fs/promises'
import { resolve, dirname } from 'node:path'
import { existsSync } from 'node:fs'

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

type LogBackend = 'file' | 'mongodb'

function getBackend(): LogBackend {
  const b = (process.env.LOG_BACKEND ?? 'file').toLowerCase().trim()
  return (b === 'mongodb') ? 'mongodb' : 'file'
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

// ── File backend ──────────────────────────────────────────────────────────────

async function appendLogFile(
  dagId: string, runId: string, taskId: string,
  stream: 'stdout' | 'stderr', line: string, level: LogLevel,
): Promise<void> {
  const filePath = taskLogPath(dagId, runId, taskId)
  const dir = dirname(filePath)
  if (!existsSync(dir)) {
    await mkdir(dir, { recursive: true })
  }
  const entry = JSON.stringify({ ts: new Date().toISOString(), stream, level, line }) + '\n'
  await appendFile(filePath, entry, 'utf8')
}

async function readLogFile(
  dagId: string, runId: string, taskId: string,
  opts: { level?: LogLevel; stream?: 'stdout' | 'stderr' } = {},
): Promise<LogLine[]> {
  const filePath = taskLogPath(dagId, runId, taskId)
  if (!existsSync(filePath)) return []

  const raw = await readFile(filePath, 'utf8')
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

  if (backend === 'file') {
    await appendLogFile(dagId, dagRunId, taskId, stream, line, resolvedLevel)
  } else {
    await appendLogMongo(db, dagId, dagRunId, taskId, stream, line, resolvedLevel)
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

/** Returns the current log backend name ('file' or 'mongodb'). */
export function getLogBackendName(): LogBackend {
  return getBackend()
}
