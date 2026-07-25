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

import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'

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
 * Append a try record. Fire-and-forget safe — errors are swallowed so a
 * failed write never breaks the task outcome.
 */
export async function recordTry(
  db: Db,
  ti: TaskInstance,
  outcome: 'success' | 'failed',
  endedAt: Date,
  error: string | null = null,
): Promise<void> {
  try {
    await db.collection<TaskTry>('task_instance_tries').insertOne({
      dag_run_id: ti.dag_run_id,
      dag_id: ti.dag_id,
      task_id: ti.task_id,
      map_index: ti.map_index ?? null,
      try_number: ti.try_number,
      state: outcome,
      started_at: ti.started_at ?? null,
      ended_at: endedAt,
      error: error ?? null,
    })
  } catch {
    // Swallow — try history is observability; must not affect task outcome
  }
}
