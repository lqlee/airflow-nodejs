/**
 * Result writes shared by every executor (local fork/spawn, Kubernetes, BullMQ worker).
 *
 * A result only lands while the instance is still running (or deferred, for trigger results). If the
 * state changed underneath the executor — the run was cancelled or timed out, a user marked the task,
 * the zombie reaper handed it to a retry — the late result is dropped instead of overwriting that change.
 */
import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { fireRetryCallback } from './callbacks.js'

type TiKey = Pick<TaskInstance, 'dag_run_id' | 'task_id' | 'map_index'>

/** Unique filter for a single task instance — includes map_index for mapped tasks. */
export function tiFilter(ti: TiKey): { dag_run_id: string; task_id: string; map_index: number | null } {
  return { dag_run_id: ti.dag_run_id, task_id: ti.task_id, map_index: ti.map_index ?? null }
}

const IN_FLIGHT = { $in: ['running', 'deferred'] }

export async function markSuccess(db: Db, ti: TiKey): Promise<void> {
  await db.collection('task_instances').updateOne(
    { ...tiFilter(ti), state: IN_FLIGHT },
    { $set: { state: 'success', ended_at: new Date() } },
  )
}

export async function markFailed(db: Db, ti: TiKey, error: string): Promise<void> {
  await db.collection('task_instances').updateOne(
    { ...tiFilter(ti), state: IN_FLIGHT },
    { $set: { state: 'failed', ended_at: new Date(), error } },
  )
}

/**
 * Delay in ms before the next retry. try_number is the 0-based attempt that just failed,
 * so the first retry waits retry_delay, then retry_delay×m, retry_delay×m², … capped at
 * max_retry_delay. A multiplier <= 1 (or absent) keeps the delay fixed.
 */
export function computeRetryDelay(
  ti: Pick<TaskInstance, 'retry_delay' | 'retry_backoff' | 'max_retry_delay' | 'try_number'>,
): number {
  const base = ti.retry_delay ?? 0
  const mult = ti.retry_backoff ?? 0
  let delay = mult > 1 ? base * Math.pow(mult, ti.try_number) : base
  if (ti.max_retry_delay && ti.max_retry_delay > 0) delay = Math.min(delay, ti.max_retry_delay)
  return Math.round(delay)
}

/**
 * Update moving a failed try to up_for_retry. The delay is a timestamp in the DB, not a timer in
 * this process, so the retry survives restarts and any replica (or BullMQ worker) can pick it up.
 */
export function retryUpdate(ti: Parameters<typeof computeRetryDelay>[0], error: string, now: Date = new Date()) {
  return {
    $set: {
      state: 'up_for_retry',
      next_retry_at: new Date(now.getTime() + computeRetryDelay(ti)),
      ended_at: now,
      error,
      claimed_by: null,
    },
    $inc: { try_number: 1 },
  }
}

/** Failed try with retries left → up_for_retry (only while still running) and fire onRetry. */
export async function scheduleRetry(db: Db, ti: TaskInstance, error: string, now: Date = new Date()): Promise<void> {
  void fireRetryCallback(ti, error)
  await db.collection('task_instances').updateOne({ ...tiFilter(ti), state: 'running' }, retryUpdate(ti, error, now))
}
