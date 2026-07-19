import { ObjectId, type Db } from 'mongodb'
import { loadDags } from '../dag/loader.js'
import { getDag, listDags } from '../dag/registry.js'
import { claimReadyTasks } from './claim.js'
import { executeTask } from './executor.js'
import { syncCronJobs, stopAllCronJobs } from './cron.js'
import { checkSlaBreaches } from '../sla/index.js'
import { emitOutlets, triggerDatasetConsumers } from '../datasets/index.js'
import { createRun } from './runs.js'
import { isDagPaused } from '../dag/pause.js'
import { fireWebhook, type DeliverOptions } from '../webhooks/index.js'

const POLL_INTERVAL_MS = 5_000

let timer: ReturnType<typeof setInterval> | null = null

/**
 * Set to true during graceful shutdown so advanceRun's claim loop bails
 * rather than forking new child processes after SIGTERM is received.
 * Rows left as queued/running are recovered by recoverOrphanedRuns() on next boot.
 */
let _shuttingDown = false

/** Exposed for graceful shutdown and testing. */
export function setShuttingDown(value: boolean): void {
  _shuttingDown = value
}

export function isShuttingDown(): boolean {
  return _shuttingDown
}

export function startScheduler(db: Db): void {
  _shuttingDown = false
  console.log(`[scheduler] starting — polling every ${POLL_INTERVAL_MS / 1000}s`)

  void tick(db)
  timer = setInterval(() => void tick(db), POLL_INTERVAL_MS)
}

export function stopScheduler(): void {
  _shuttingDown = true
  if (timer) {
    clearInterval(timer)
    timer = null
  }
  stopAllCronJobs()
  console.log('[scheduler] stopped')
}

async function tick(db: Db): Promise<void> {
  try {
    await loadDags()
    const dags = listDags()

    // Sync cron jobs whenever dags reload (picks up schedule changes)
    syncCronJobs(db, dags)

    // Check SLA breaches for all active runs
    await checkSlaBreaches(db, dags)

    // Advance any active runs — cancelled/success/failed are excluded
    const activeRuns = await db
      .collection('dag_runs')
      .find({ state: { $in: ['queued', 'running'] } })
      .toArray()

    for (const run of activeRuns) {
      await advanceRun(db, run._id.toString())
    }

    // Trigger dataset-aware consumers whose datasets have new events
    await triggerDatasetConsumers(db, dags, createRun, isDagPaused)
  } catch (err) {
    console.error('[scheduler] tick error:', err)
  }
}

/**
 * Drive a single dag_run forward.
 * No-ops if the run is already in a terminal state (success/failed/cancelled).
 * webhookOptions is injected in tests to capture outbound calls without hitting the network.
 */
export async function advanceRun(db: Db, dagRunId: string, webhookOptions?: DeliverOptions): Promise<void> {
  // Guard: skip if already terminal
  const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(dagRunId) })
  if (!run || run.state === 'cancelled' || run.state === 'success' || run.state === 'failed') return

  // Mark run as running if still queued
  await db.collection('dag_runs').updateOne(
    { _id: new ObjectId(dagRunId), state: 'queued' },
    { $set: { state: 'running' } }
  )

  // Claim all currently-ready tasks and execute in parallel.
  // Re-check cancellation and shutdown flag before each wave.
  // On shutdown: bail without forking new children — recoverOrphanedRuns()
  // will re-claim any queued/running rows on next boot.
  let claimed = await claimReadyTasks(db, dagRunId)
  while (claimed.length > 0) {
    if (_shuttingDown) return
    const current = await db.collection('dag_runs').findOne({ _id: new ObjectId(dagRunId) })
    if (current?.state === 'cancelled') return

    await Promise.all(claimed.map(ti => executeTask(db, ti)))
    claimed = await claimReadyTasks(db, dagRunId)
  }

  // Check overall run completion
  const tasks = await db.collection('task_instances').find({ dag_run_id: dagRunId }).toArray()
  const allDone = tasks.every(t => t.state === 'success' || t.state === 'failed' || t.state === 'cancelled')
  const anyFailed = tasks.some(t => t.state === 'failed')

  if (allDone) {
    const finalState = anyFailed ? 'failed' : 'success'
    // CAS: only transition if run is still in a non-terminal state (guards concurrent ticks)
    const transitioned = await db.collection('dag_runs').findOneAndUpdate(
      { _id: new ObjectId(dagRunId), state: { $in: ['queued', 'running'] } },
      { $set: { state: finalState, ended_at: new Date() } },
      { returnDocument: 'after' },
    )
    if (!transitioned) return  // another tick already finalized this run
    console.log(`[scheduler] run ${dagRunId} → ${finalState}`)

    const dag = getDag(transitioned.dag_id)

    // Emit dataset outlets only on success — exactly-once via CAS guard above
    if (finalState === 'success' && dag) {
      await emitOutlets(db, dag, dagRunId)
    }

    // Fire webhook callback if configured — fire-and-forget, never blocks the tick loop
    const callbackUrl = finalState === 'success' ? dag?.onSuccess : dag?.onFailure
    if (callbackUrl) {
      fireWebhook(callbackUrl, {
        dag_id: transitioned.dag_id,
        run_id: dagRunId,
        state: finalState,
        logical_date: transitioned.logical_date ?? null,
        conf: (transitioned.conf as Record<string, unknown>) ?? {},
        tags: (transitioned.tags as string[]) ?? [],
        ended_at: transitioned.ended_at as Date,
      }, webhookOptions)
    }
  }
}

export type ClearResult =
  | { cleared: true; clearedCount: number }
  | { cleared: false; reason: 'run_not_found' | 'task_not_found' | 'task_not_terminal' }

/**
 * Clear one or all instances of a task back to queued, then un-terminal the run.
 *
 * Semantics:
 *   - Only clears instances whose state ∈ {success, failed, cancelled}.
 *     Clearing a running instance would cause two concurrent workers on the same row.
 *   - try_number is reset to 0 (fresh retry budget).
 *   - If mapIndex is given: clears only that specific instance (full tiFilter identity).
 *     If mapIndex is null/undefined: clears ALL instances of taskId.
 *   - After clearing, the parent run is reset to 'queued' so the scheduler tick
 *     re-picks it. Without this, tick() skips terminal runs forever.
 *   - Downstream tasks are NOT cleared (YAGNI — re-run of just this task may be enough).
 */
export async function clearTaskInstance(
  db: Db,
  dagRunId: string,
  taskId: string,
  mapIndex?: number | null,
): Promise<ClearResult> {
  const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(dagRunId) })
  if (!run) return { cleared: false, reason: 'run_not_found' }

  // Build the filter: full identity when mapIndex given; all instances otherwise
  const taskFilter: Record<string, unknown> = {
    dag_run_id: dagRunId,
    task_id: taskId,
    // Only clear terminal instances — never touch a running one
    state: { $in: ['success', 'failed', 'cancelled'] },
  }
  if (mapIndex !== undefined && mapIndex !== null) {
    taskFilter['map_index'] = mapIndex
  }

  const result = await db.collection('task_instances').updateMany(
    taskFilter,
    {
      $set: {
        state: 'queued',
        started_at: null,
        ended_at: null,
        error: null,
        next_poke_at: null,
        first_poked_at: null,
        poke_count: 0,
        try_number: 0,   // reset retry budget
      },
    },
  )

  if (result.matchedCount === 0) {
    // Task not found at all OR it's in a non-terminal state (running/queued)
    const exists = await db.collection('task_instances').findOne({
      dag_run_id: dagRunId, task_id: taskId,
    })
    return { cleared: false, reason: exists ? 'task_not_terminal' : 'task_not_found' }
  }

  // Un-terminal the run so tick() re-picks it. Use updateOne (not CAS) because
  // we must reset even if the run is already terminal — that's the whole point.
  await db.collection('dag_runs').updateOne(
    { _id: new ObjectId(dagRunId) },
    { $set: { state: 'queued', ended_at: null } },
  )

  console.log(`[scheduler] cleared ${result.modifiedCount} instance(s) of task '${taskId}' in run ${dagRunId}`)
  return { cleared: true, clearedCount: result.modifiedCount }
}

/**
 * Cancel a dag_run atomically:
 * - Marks run state → cancelled
 * - Marks all queued/running tasks → cancelled
 * Returns false if the run was already in a terminal state.
 */
export async function cancelRun(db: Db, dagRunId: string): Promise<boolean> {
  const result = await db.collection('dag_runs').findOneAndUpdate(
    { _id: new ObjectId(dagRunId), state: { $in: ['queued', 'running'] } },
    { $set: { state: 'cancelled', ended_at: new Date() } },
    { returnDocument: 'after' }
  )

  if (!result) return false  // already terminal or not found

  // Cancel all non-terminal tasks in one shot
  await db.collection('task_instances').updateMany(
    { dag_run_id: dagRunId, state: { $in: ['queued', 'running'] } },
    { $set: { state: 'cancelled', ended_at: new Date(), error: 'Cancelled by user' } }
  )

  console.log(`[scheduler] run ${dagRunId} → cancelled`)
  return true
}
