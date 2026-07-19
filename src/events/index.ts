/**
 * Audit / Event Log.
 *
 * Events are written by core functions (createRun, pauseDag, cancelRun, etc.)
 * and read via GET /event-logs. The write side is fire-and-forget — a failed
 * write must never take down the action it is recording.
 *
 * Event types:
 *   run_triggered   — a dag_run was created (manual, cron, backfill, dataset)
 *   run_success     — a dag_run reached 'success'
 *   run_failed      — a dag_run reached 'failed'
 *   dag_paused      — dag was paused
 *   dag_resumed     — dag was unpaused
 *   run_cancelled   — dag_run was cancelled via API
 *   task_cleared    — a task instance was cleared back to queued
 */

import type { Db } from 'mongodb'

export type EventType =
  | 'run_triggered'
  | 'run_success'
  | 'run_failed'
  | 'dag_paused'
  | 'dag_resumed'
  | 'run_cancelled'
  | 'task_cleared'

export interface EventLogRecord {
  event_type: EventType
  dag_id: string | null
  dag_run_id: string | null
  task_id: string | null
  map_index: number | null
  metadata: Record<string, unknown>
  created_at: Date
}

export interface EventLogQuery {
  dag_id?: string
  dag_run_id?: string
  task_id?: string
  event_type?: EventType
}

/** Build a MongoDB filter from query parameters — pure, no DB needed. */
export function buildEventFilter(q: EventLogQuery): Record<string, unknown> {
  const filter: Record<string, unknown> = {}
  if (q.dag_id) filter['dag_id'] = q.dag_id
  if (q.dag_run_id) filter['dag_run_id'] = q.dag_run_id
  if (q.task_id) filter['task_id'] = q.task_id
  if (q.event_type) filter['event_type'] = q.event_type
  return filter
}

/**
 * Record an audit event. Fire-and-forget: logs on failure but never throws.
 * Call sites must not await this in the hot path — use `void recordEvent(...)`.
 */
export async function recordEvent(
  db: Db,
  event_type: EventType,
  opts: {
    dag_id?: string | null
    dag_run_id?: string | null
    task_id?: string | null
    map_index?: number | null
    metadata?: Record<string, unknown>
  } = {},
): Promise<void> {
  try {
    await db.collection<EventLogRecord>('event_logs').insertOne({
      event_type,
      dag_id: opts.dag_id ?? null,
      dag_run_id: opts.dag_run_id ?? null,
      task_id: opts.task_id ?? null,
      map_index: opts.map_index ?? null,
      metadata: opts.metadata ?? {},
      created_at: new Date(),
    })
  } catch (err) {
    // Never propagate — the audited action already completed
    console.error(`[events] failed to record '${event_type}':`, err)
  }
}

const VALID_EVENT_TYPES = new Set<string>([
  'run_triggered', 'run_success', 'run_failed',
  'dag_paused', 'dag_resumed', 'run_cancelled', 'task_cleared',
])

export function isEventType(v: unknown): v is EventType {
  return typeof v === 'string' && VALID_EVENT_TYPES.has(v)
}
