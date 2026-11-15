/**
 * Zombie task detection — a task instance left `running` by a process that is gone.
 *
 * Each claim stamps `claimed_by` with the owner's INSTANCE_ID; owners heartbeat (instance.ts).
 * A running instance whose owner has not heartbeated within ZOMBIE_THRESHOLD_MS is handled like a
 * task failure (Airflow's zombie handling): up_for_retry when it has retries left, failed otherwise,
 * with a failed try recorded.
 *
 * Rows from before owner tracking (no `claimed_by`) keep the old startup-recovery behaviour:
 * they are requeued without spending a retry, because there is no way to tell whether they crashed.
 * A graceful shutdown hands its own running tasks back the same way (releaseOwnedTasks).
 */
import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { recordTry } from './tries.js'
import { retryUpdate, failedUpdate } from './ti-writes.js'
import { INSTANCE_ID, ZOMBIE_THRESHOLD_MS, listLiveInstanceIds, pruneDeadInstances } from './instance.js'

type RunningTi = TaskInstance & { _id: unknown }

export interface ReapResult {
  retried: number
  failed: number
  requeued: number   // legacy rows without an owner
}

export async function reapZombies(
  db: Db,
  opts: { now?: Date; thresholdMs?: number } = {},
): Promise<ReapResult> {
  const now = opts.now ?? new Date()
  const cutoff = new Date(now.getTime() - (opts.thresholdMs ?? ZOMBIE_THRESHOLD_MS))
  // This process is alive by definition — never reap its own tasks, even if its heartbeat write lagged.
  const live = [...new Set([...(await listLiveInstanceIds(db, cutoff)), INSTANCE_ID])]

  const candidates = await db.collection<RunningTi>('task_instances').find({
    state: 'running',
    claimed_by: { $nin: live },
    // A just-claimed task gets one full threshold before it can be judged (covers rows written by an
    // older version without heartbeats during a rolling deploy)
    $or: [{ started_at: null }, { started_at: { $lt: cutoff } }],
  }).toArray()

  const result: ReapResult = { retried: 0, failed: 0, requeued: 0 }
  for (const ti of candidates) {
    const owner = ti.claimed_by ?? null
    // CAS on owner + try_number: if the task moved on (finished, re-claimed) since we read it, leave it
    const filter = { _id: ti._id, state: 'running', claimed_by: owner, try_number: ti.try_number } as Record<string, unknown>

    if (owner === null) {
      const res = await db.collection('task_instances').updateOne(filter, {
        $set: { state: 'queued', started_at: null, claimed_by: null },
        $inc: { try_number: 1 },
      })
      result.requeued += res.modifiedCount
      continue
    }

    const error = `Task heartbeat lost: owner '${owner}' stopped responding (zombie)`
    const retry = ti.try_number < (ti.max_retries ?? 0)
    const res = await db.collection('task_instances').updateOne(filter, retry
      ? retryUpdate(ti, error, now)   // up_for_retry after the task's retry delay, like any failed try
      : failedUpdate(error, now))
    if (res.modifiedCount === 0) continue
    await recordTry(db, ti, 'failed', now, error)
    if (retry) result.retried++
    else result.failed++
    console.warn(`[zombie] ${ti.dag_id}.${ti.task_id}${ti.map_index !== null && ti.map_index !== undefined ? `[${ti.map_index}]` : ''} (run ${ti.dag_run_id}) — ${error} → ${retry ? 'retry' : 'failed'}`)
  }
  if (result.requeued > 0) console.log(`[zombie] requeued ${result.requeued} running task(s) with no owner (left by a previous version)`)

  // Keep the registry small: anything silent for 10× the threshold is long gone
  await pruneDeadInstances(db, new Date(now.getTime() - 10 * (opts.thresholdMs ?? ZOMBIE_THRESHOLD_MS)))
  return result
}

/**
 * Graceful shutdown: hand this process's still-running tasks back to the queue for another replica
 * (or the next start) — an orderly hand-back, so no retry is spent. Same as the old startup recovery.
 */
export async function releaseOwnedTasks(db: Db): Promise<number> {
  const res = await db.collection('task_instances').updateMany(
    { state: 'running', claimed_by: INSTANCE_ID },
    { $set: { state: 'queued', started_at: null, claimed_by: null }, $inc: { try_number: 1 } },
  )
  if (res.modifiedCount > 0) console.log(`[shutdown] handed ${res.modifiedCount} running task(s) back to the queue`)
  return res.modifiedCount
}
