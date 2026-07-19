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

export function startScheduler(db: Db): void {
  console.log(`[scheduler] starting — polling every ${POLL_INTERVAL_MS / 1000}s`)

  void tick(db)
  timer = setInterval(() => void tick(db), POLL_INTERVAL_MS)
}

export function stopScheduler(): void {
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
  // Re-check cancellation before each wave so a cancel mid-run takes effect quickly.
  let claimed = await claimReadyTasks(db, dagRunId)
  while (claimed.length > 0) {
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
