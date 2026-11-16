/**
 * Task Instance Try History.
 *
 * Each time a task instance completes a try (success, fail, or retry) a record
 * is appended to `task_instance_tries`. The current `try_number` on the task
 * instance is used as the try index.
 *
 * This is append-only — no updates. The current task_instance row is the
 * authoritative live state; tries are the historical record.
 */

import { ObjectId, type Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { trackWork } from './pool.js'
import { isTracingEnabled, emitTaskSpan } from '../observability/tracing.js'

export interface TaskTry {
  dag_run_id: string
  dag_id: string
  task_id: string
  map_index: number | null
  try_number: number          // 0-based; matches task_instance.try_number at time of this try
  state: 'success' | 'failed' // the outcome of THIS try (not a retry-queued state)
  started_at: Date | null
  ended_at: Date
  error: string | null        // null for success
}

/**
 * Append a try record and emit the try's trace span. Fire-and-forget safe — errors are swallowed so a
 * failed write never breaks the task outcome — and tracked, so graceful shutdown waits for it.
 */
export function recordTry(
  db: Db,
  ti: TaskInstance,
  outcome: 'success' | 'failed',
  endedAt: Date,
  error: string | null = null,
): Promise<void> {
  return trackWork(Promise.all([
    insertTry(db, ti, outcome, endedAt, error),
    traceTry(db, ti, outcome, endedAt, error),
  ]).then(() => {}))
}

/** The span's parent is the run attempt the try belonged to: it needs the run's queue time. */
async function traceTry(db: Db, ti: TaskInstance, outcome: 'success' | 'failed', endedAt: Date, error: string | null): Promise<void> {
  if (!isTracingEnabled()) return
  try {
    const run = await db.collection('dag_runs').findOne(
      { _id: new ObjectId(ti.dag_run_id) }, { projection: { queued_at: 1, created_at: 1 } },
    )
    if (run) emitTaskSpan(ti, { queued_at: run.queued_at as Date | undefined, created_at: run.created_at as Date }, outcome, endedAt, error)
  } catch (err) {
    console.error(`[tracing] span for ${ti.dag_id}.${ti.task_id} not recorded: ${(err as Error).message}`)
  }
}

async function insertTry(db: Db, ti: TaskInstance, outcome: 'success' | 'failed', endedAt: Date, error: string | null): Promise<void> {
  try {
    await db.collection<TaskTry>('task_instance_tries').insertOne({
      dag_run_id: ti.dag_run_id,
      dag_id: ti.dag_id,
      task_id: ti.task_id,
      map_index: ti.map_index ?? null,
      try_number: ti.try_number,
      state: outcome,
      started_at: ti.started_at ? new Date(ti.started_at) : null,   // a string when the instance came through a BullMQ job
      ended_at: endedAt,
      error: error ?? null,
    })
  } catch {
    // Swallow — try history is observability; must not affect task outcome
  }
}
