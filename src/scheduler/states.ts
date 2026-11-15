/**
 * Task instance states — Airflow's model, minus `scheduled`/`restarting`.
 *
 *   queued            waiting for upstreams / a free slot (Airflow: none + scheduled + queued)
 *   running           claimed by a scheduler or worker (claimed_by)
 *   deferred          waiting on a trigger, holds no slot
 *   up_for_retry      failed a try, claimable again once next_retry_at has passed
 *   up_for_reschedule sensor in reschedule mode, claimable again once next_poke_at has passed
 *   success / failed / skipped / cancelled
 *   upstream_failed   could never run because an upstream failed
 *   removed           the task is no longer in the Dag
 */
export const TI_STATES = [
  'queued', 'running', 'deferred', 'up_for_retry', 'up_for_reschedule',
  'success', 'failed', 'upstream_failed', 'skipped', 'removed', 'cancelled',
] as const
export type TiState = typeof TI_STATES[number]

/** Finished: the instance needs no more work. */
export const TI_FINISHED: ReadonlySet<string> = new Set(['success', 'failed', 'upstream_failed', 'skipped', 'removed', 'cancelled'])

/** Not finished — what cancel / run timeout must stop. */
export const TI_UNFINISHED = ['queued', 'running', 'deferred', 'up_for_retry', 'up_for_reschedule'] as const

/** Waiting to be (re)claimed — the claim loop looks at these. */
export const TI_CLAIMABLE = ['queued', 'up_for_retry', 'up_for_reschedule'] as const

/** Finished states a clear may reset (removed stays removed until the task is back in the Dag). */
export const TI_CLEARABLE = ['success', 'failed', 'upstream_failed', 'skipped', 'cancelled'] as const

/** States a user may set on a task instance. */
export const TI_SETTABLE = ['success', 'failed', 'skipped'] as const
export type TiSettableState = typeof TI_SETTABLE[number]

export const RUN_STATES = ['queued', 'running', 'success', 'failed', 'cancelled'] as const

/** One task_id's state as its downstream tasks see it, summed over its (mapped) instances. */
export type AggState = 'success' | 'failed' | 'upstream_failed' | 'skipped' | 'pending' | 'removed'

/**
 * Aggregate the instance states of one task_id.
 * removed instances are ignored (a task removed entirely reads 'removed'); any failed instance makes
 * the task failed, otherwise any upstream_failed one makes it upstream_failed; a finished mix with
 * neither success nor failure (e.g. cancelled) reads 'skipped'.
 */
export function aggregateState(states: string[]): AggState {
  const live = states.filter(s => s !== 'removed')
  if (live.length === 0) return states.length > 0 ? 'removed' : 'pending'
  if (live.every(s => s === 'success')) return 'success'
  if (live.every(s => s === 'skipped')) return 'skipped'
  if (live.some(s => s === 'failed')) return 'failed'
  if (live.some(s => s === 'upstream_failed')) return 'upstream_failed'
  if (live.every(s => TI_FINISHED.has(s))) return 'skipped'
  return 'pending'
}

/** aggregateState per task_id for a run's instances. */
export function aggregateByTask(instances: Array<{ task_id: string; state: string }>): Map<string, AggState> {
  const byTask = new Map<string, string[]>()
  for (const i of instances) {
    const arr = byTask.get(i.task_id) ?? []
    arr.push(i.state)
    byTask.set(i.task_id, arr)
  }
  const out = new Map<string, AggState>()
  for (const [taskId, states] of byTask) out.set(taskId, aggregateState(states))
  return out
}
