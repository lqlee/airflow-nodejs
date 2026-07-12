import type { Db } from 'mongodb'

export interface LogLine {
  dag_run_id: string
  dag_id: string
  task_id: string
  ts: Date
  stream: 'stdout' | 'stderr'
  line: string
}

/**
 * Append a log line for a task instance.
 */
export async function appendLog(
  db: Db,
  dagRunId: string,
  dagId: string,
  taskId: string,
  stream: 'stdout' | 'stderr',
  line: string
): Promise<void> {
  await db.collection<LogLine>('task_logs').insertOne({
    dag_run_id: dagRunId,
    dag_id: dagId,
    task_id: taskId,
    ts: new Date(),
    stream,
    line,
  })
}

/**
 * Fetch all log lines for a task instance, sorted by timestamp.
 */
export async function getTaskLogs(
  db: Db,
  dagRunId: string,
  taskId: string
): Promise<LogLine[]> {
  return db
    .collection<LogLine>('task_logs')
    .find({ dag_run_id: dagRunId, task_id: taskId })
    .sort({ ts: 1 })
    .toArray()
}
