import type { Db, WithId } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'

export interface DagRun {
  dag_id: string
  dag_version: string | null   // sha256[:12] of the dag source file at run creation time
  logical_date: Date | null    // scheduled execution date; null for ad-hoc/manual runs
  state: 'queued' | 'running' | 'success' | 'failed' | 'cancelled'
  created_at: Date
}

export interface TaskInstance {
  dag_run_id: string        // stringified ObjectId of parent dag_run
  dag_id: string
  task_id: string
  group_id: string | null   // TaskGroup membership label; null for ungrouped tasks
  state: 'queued' | 'running' | 'success' | 'failed' | 'cancelled'
  depends_on: string[]
  try_number: number
  max_retries: number       // max allowed retries (0 = no retries)
  retry_delay: number       // ms to wait before requeue
  timeout_ms: number        // 0 = no timeout; >0 = kill worker after this many ms
  started_at: Date | null
  ended_at: Date | null
  error: string | null
  created_at: Date
}

export interface CreateRunOptions {
  /** Logical execution date (backfill). Defaults to now (ad-hoc / manual trigger). */
  logicalDate?: Date
}

/**
 * Create a dag_run + one task_instance per task for the given Dag.
 * Returns the run id (string).
 */
export async function createRun(db: Db, dag: DagDefinition, opts: CreateRunOptions = {}): Promise<string> {
  const now = new Date()

  // Insert dag_run — stamp the dag version so every run records which code ran it
  const runResult = await db.collection<DagRun>('dag_runs').insertOne({
    dag_id: dag.id,
    dag_version: dag.version ?? null,
    logical_date: opts.logicalDate ?? null,
    state: 'queued',
    created_at: now,
  })
  const runId = runResult.insertedId.toString()

  // Insert one task_instance per task
  const taskDocs: TaskInstance[] = Object.entries(dag.tasks).map(([taskId, task]) => ({
    dag_run_id: runId,
    dag_id: dag.id,
    task_id: taskId,
    group_id: task.group ?? null,
    state: 'queued',
    depends_on: task.dependsOn ?? [],
    try_number: 0,
    max_retries: task.retries ?? 0,
    retry_delay: task.retryDelay ?? 0,
    timeout_ms: task.timeout ?? 0,
    started_at: null,
    ended_at: null,
    error: null,
    created_at: now,
  }))

  await db.collection<TaskInstance>('task_instances').insertMany(taskDocs)

  console.log(`[scheduler] created run ${runId} for dag '${dag.id}' with ${taskDocs.length} tasks`)
  return runId
}
