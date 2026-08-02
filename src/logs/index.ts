import type { Db } from 'mongodb'

/**
 * Detect log level from a line prefix written by ctx.log.*
 * Lines from ctx.log are prefixed: "[INFO] ...", "[WARN] ...", etc.
 * Returns the detected level, or null if no prefix found.
 */
export function parseLevelFromLine(line: string): LogLevel | null {
  const match = /^\[(DEBUG|INFO|WARN|ERROR)\] /.exec(line)
  if (!match) return null
  const map: Record<string, LogLevel> = { DEBUG: 'debug', INFO: 'info', WARN: 'warn', ERROR: 'error' }
  return map[match[1]] ?? null
}

export type LogLevel = 'debug' | 'info' | 'warn' | 'error'

export interface LogLine {
  dag_run_id: string
  dag_id: string
  task_id: string
  ts: Date
  stream: 'stdout' | 'stderr'
  /** Severity level. Defaults to 'info' for stdout, 'error' for stderr. */
  level: LogLevel
  line: string
}

/**
 * Append a log line for a task instance.
 * level defaults based on stream: stdout → 'info', stderr → 'error'
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
  await db.collection<LogLine>('task_logs').insertOne({
    dag_run_id: dagRunId,
    dag_id:     dagId,
    task_id:    taskId,
    ts:         new Date(),
    stream,
    level:      resolvedLevel,
    line,
  })
}

/**
 * Fetch log lines for a task instance.
 * Optional filters: level (minimum severity) and stream.
 *
 * Level ordering: debug < info < warn < error
 * Passing level='warn' returns warn + error lines only.
 */
export async function getTaskLogs(
  db: Db,
  dagRunId: string,
  taskId: string,
  opts: {
    level?: LogLevel      // minimum severity to include
    stream?: 'stdout' | 'stderr'
  } = {},
): Promise<LogLine[]> {
  const LEVEL_RANK: Record<LogLevel, number> = { debug: 0, info: 1, warn: 2, error: 3 }
  const minRank = opts.level ? LEVEL_RANK[opts.level] : 0

  // Build query — include levels >= minRank
  const levelsToInclude = (Object.keys(LEVEL_RANK) as LogLevel[])
    .filter(l => LEVEL_RANK[l] >= minRank)

  const query: Record<string, unknown> = {
    dag_run_id: dagRunId,
    task_id: taskId,
    level: { $in: levelsToInclude },
  }
  if (opts.stream) query.stream = opts.stream

  return db
    .collection<LogLine>('task_logs')
    .find(query)
    .sort({ ts: 1 })
    .toArray()
}
