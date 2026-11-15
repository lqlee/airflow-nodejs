import { ObjectId, type Db } from 'mongodb'
import { syncDagsFromDb } from '../dag/sync.js'
import { isProcessorEmbedded, maybeRunDagProcessor, stopDagProcessor } from '../dag/processor.js'
import { stopSandbox } from '../sandbox/client.js'
import { getDag, listDags } from '../dag/registry.js'
import { claimReadyTasks, skipUnsatisfiableTasks, applyBranchDecisions, applyShortCircuitDecisions, expandDynamicMapped } from './claim.js'
import { pollDeferredTasks, executeTask, claimCapacity } from './executor.js'
import { verifyRunIntegrity } from './integrity.js'
import { clearTaskInstances } from './actions.js'
import { TI_FINISHED, TI_UNFINISHED } from './states.js'
import { syncCronJobs, stopAllCronJobs, tickTimetables, tickCatchup, resetAllTimetables } from './cron.js'
import { isLeader } from './lease.js'
import type { DagDefinition } from '../dag/types.js'
import { startLiveness, stopLiveness } from './instance.js'
import { reapZombies } from './zombies.js'
import { maybePurge } from '../db/cleanup.js'
import { checkDeadlines, pruneDeadlines } from '../deadlines/index.js'
import { emitOutlets, triggerDatasetConsumers } from '../datasets/index.js'
import { createRun } from './runs.js'
import { isDagPaused } from '../dag/pause.js'
import { fireWebhook, type DeliverOptions } from '../webhooks/index.js'
import { recordEvent } from '../events/index.js'
import { flushTaskCallbacks, fireExecuteCallbacks, notify } from './callbacks.js'
import { recordSchedulerTick } from '../metrics/registry.js'
import { getPausedBackfillIds, buildActiveRunFilter } from './backfill-filter.js'

const POLL_INTERVAL_MS = 5_000

let timer: ReturnType<typeof setInterval> | null = null

/**
 * Set to true during graceful shutdown so advanceRun's claim loop bails
 * rather than forking new child processes after SIGTERM is received.
 * Rows left running are handed back by releaseOwnedTasks() (graceful exit) or reaped as zombies.
 */
let _shuttingDown = false

/** Leadership seen by the previous tick — losing it resets leader-only in-memory state. */
let _wasLeader = false

/** Exposed for graceful shutdown and testing. */
export function setShuttingDown(value: boolean): void {
  _shuttingDown = value
}

export function isShuttingDown(): boolean {
  return _shuttingDown
}

export function startScheduler(db: Db): void {
  _shuttingDown = false
  _wasLeader = false
  console.log(`[scheduler] starting — polling every ${POLL_INTERVAL_MS / 1000}s`)

  // Register + heartbeat + leader election first, so the first tick already knows whether it leads
  timer = setInterval(() => void tick(db), POLL_INTERVAL_MS)
  void startLiveness(db, { lease: true }).then(() => { if (timer) void tick(db) })
}

export function stopScheduler(): void {
  _shuttingDown = true
  if (timer) {
    clearInterval(timer)
    timer = null
  }
  stopLiveness()
  stopAllCronJobs()
  stopDagProcessor()
  stopSandbox()
  console.log('[scheduler] stopped')
}

async function tick(db: Db): Promise<void> {
  const startedAt = performance.now()
  let ok = true
  try {
    // The leader re-parses changed Dag files in the background (a child process — user code never runs
    // here); every replica picks up the serialized result. DAG_PROCESSOR=external leaves parsing to
    // a separate `npm run dag-processor` process.
    if (isLeader() && isProcessorEmbedded()) {
      void maybeRunDagProcessor(db).catch(err => console.error('[dag-processor] run failed:', err))
    }
    await syncDagsFromDb(db)
    await runTickWork(db, listDags())
  } catch (err) {
    ok = false
    console.error('[scheduler] tick error:', err)
  } finally {
    recordSchedulerTick((performance.now() - startedAt) / 1000, ok)
  }
}

/**
 * One scheduler pass over the loaded Dags (everything a tick does after reloading Dag files).
 * Exported for tests — the tick itself first syncs the registry from the serialized Dags.
 */
export async function runTickWork(db: Db, dags: DagDefinition[]): Promise<void> {
  // Work that CREATES runs or changes task state on a timer runs on one replica only (the leader).
  // Every replica advances runs below — claims are atomic per task instance.
  const leader = isLeader()
  if (!leader && _wasLeader) {
    stopAllCronJobs()
    resetAllTimetables()
  }
  _wasLeader = leader

  if (leader) {
    // Sync cron jobs whenever dags reload (picks up schedule changes)
    syncCronJobs(db, dags)

    // Create runs for every missed occurrence of catchup dags
    await tickCatchup(db, dags)

    // Tick timetable-scheduled dags (custom schedule functions)
    await tickTimetables(db, dags)

    // Poll deferred tasks — check trigger conditions, resume or reschedule
    await pollDeferredTasks(db)

    // Deadline alerts (and the sla alias) whose time has passed → missed + callbacks
    await checkDeadlines(db, dags)

    // Running tasks whose owner process stopped heartbeating → retry / fail
    await reapZombies(db)

    // Retention purge (RETENTION_DAYS) — throttled internally, never throws
    await maybePurge(db)
  }

  // Advance any active runs — cancelled/success/failed are excluded.
  // Runs belonging to paused backfills are also excluded (resume re-enables them).
  const pausedBackfillIds = [...(await getPausedBackfillIds(db))]
  const activeRuns = await db
    .collection('dag_runs')
    .find(buildActiveRunFilter(pausedBackfillIds))
    .sort({ ordering_date: 1, created_at: 1 })   // timeline order — with maxActiveRuns + depends_on_past an
    // earlier-dated run must get the slot before a later-dated one that waits on it
    .toArray()

  for (const run of activeRuns) {
    await advanceRun(db, run._id.toString())
  }

  // Callbacks flagged on runs that were not advanced above (e.g. finished in another replica's pass)
  await flushTaskCallbacks(db)

  // Trigger dataset-aware consumers whose datasets have new events
  if (leader) await triggerDatasetConsumers(db, dags, createRun, isDagPaused)
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

  const dagDef = getDag(run.dag_id as string)

  // Mark run as running if still queued — unless the Dag is at its maxActiveRuns limit,
  // in which case the run stays queued and a later tick retries.
  if (run.state === 'queued') {
    if (dagDef?.maxActiveRuns && dagDef.maxActiveRuns > 0) {
      const running = await db.collection('dag_runs').countDocuments({ dag_id: run.dag_id, state: 'running' })
      if (running >= dagDef.maxActiveRuns) return
    }
    await db.collection('dag_runs').updateOne(
      { _id: new ObjectId(dagRunId), state: 'queued' },
      { $set: { state: 'running', started_at: new Date() } }
    )
    run.started_at = new Date()
  }

  // runTimeout: fail the run (and its unfinished tasks) once it has been running too long.
  // Checked here and between waves; the normal completion path below finalizes + fires hooks.
  let timedOut = await failIfRunTimedOut(db, dagRunId, dagDef?.runTimeout, run.started_at ?? run.created_at)

  // Claim all currently-ready tasks and execute in parallel.
  // After each wave, skip any tasks whose trigger rule can never be satisfied.
  // Re-check cancellation and shutdown flag before each wave.
  // On shutdown: bail without forking new children — the claimed rows are handed
  // back to the queue by releaseOwnedTasks() before the process exits.
  // Instances of tasks deleted from the Dag → removed (and back to queued if the task returns)
  if (dagDef) await verifyRunIntegrity(db, dagRunId, dagDef)

  // Fire callbacks of results written since the last pass — before a re-claim can overwrite the flag
  await flushTaskCallbacks(db, { runId: dagRunId })

  let claimed = timedOut ? [] : await claimReadyTasks(db, dagRunId, { limit: claimCapacity() })
  while (claimed.length > 0) {
    if (_shuttingDown) return
    const current = await db.collection('dag_runs').findOne({ _id: new ObjectId(dagRunId) })
    if (current?.state === 'cancelled') return

    fireExecuteCallbacks(claimed)
    await Promise.all(claimed.map(ti => executeTask(db, ti)))
    await flushTaskCallbacks(db, { runId: dagRunId })

    timedOut = await failIfRunTimedOut(db, dagRunId, dagDef?.runTimeout, run.started_at ?? run.created_at)
    if (timedOut) break

    // Apply branch decisions: skip non-selected direct dependents of branch tasks.
    // Must run before expandDynamicMapped so the cascade picks up branch-skips.
    await applyBranchDecisions(db, dagRunId)
    await applyShortCircuitDecisions(db, dagRunId)

    // Expand dynamic-mapped tasks: replace placeholders with real instances once
    // the source XCom is available. Must run before skipUnsatisfiableTasks so
    // the cascade can process newly-created instances.
    await expandDynamicMapped(db, dagRunId)

    // After tasks complete, skip any queued tasks whose rule is now unsatisfiable.
    // Cascades until stable (e.g. skipping A may make B unsatisfiable too).
    await skipUnsatisfiableTasks(db, dagRunId)

    claimed = await claimReadyTasks(db, dagRunId, { limit: claimCapacity() })
  }

  // Final passes: handle tasks that were unsatisfiable from the start or
  // dynamic tasks whose source just succeeded in the last wave.
  await applyShortCircuitDecisions(db, dagRunId)
  await expandDynamicMapped(db, dagRunId)
  await skipUnsatisfiableTasks(db, dagRunId)

  // Check overall run completion — skipped / upstream_failed / removed are finished too.
  // deferred / up_for_retry / up_for_reschedule are not: the run waits for them.
  const tasks = await db.collection('task_instances').find({ dag_run_id: dagRunId }).toArray()
  const allDone = tasks.every(t => TI_FINISHED.has(t.state))
  // A failed teardown does not fail the run unless it opted in (Airflow on_failure_fail_dagrun)
  const anyFailed = tasks.some(t => t.state === 'failed' && (!t.is_teardown || t.on_failure_fail_dagrun))

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

    // Audit — after CAS win so only the winning tick records the event
    void recordEvent(db, finalState === 'success' ? 'run_success' : 'run_failed', {
      dag_id: transitioned.dag_id,
      dag_run_id: dagRunId,
    })

    const dag = getDag(transitioned.dag_id)

    // Emit dataset outlets only on success — exactly-once via CAS guard above
    if (finalState === 'success' && dag) {
      await emitOutlets(db, dag, dagRunId)
    }
    // It succeeded in time: its pending deadlines are moot (a failed run keeps them, as in Airflow)
    if (finalState === 'success') await pruneDeadlines(db, dagRunId)

    const webhookPayload = {
      dag_id: transitioned.dag_id,
      run_id: dagRunId,
      state: finalState as 'success' | 'failed',
      logical_date: transitioned.logical_date ?? null,
      conf: (transitioned.conf as Record<string, unknown>) ?? {},
      tags: (transitioned.tags as string[]) ?? [],
      ended_at: transitioned.ended_at as Date,
    }

    // Dag callbacks — exactly once (after the CAS win), fire-and-forget. URLs get the webhook payload.
    const callbacks = finalState === 'success' ? dag?.onSuccess : dag?.onFailure
    for (const target of callbacks === undefined ? [] : Array.isArray(callbacks) ? callbacks : [callbacks]) {
      if (typeof target === 'string') { fireWebhook(target, webhookPayload, webhookOptions); continue }
      void notify(target, {
        dagId: webhookPayload.dag_id, runId: dagRunId, state: webhookPayload.state, logicalDate: webhookPayload.logical_date,
        conf: webhookPayload.conf, tags: webhookPayload.tags, endedAt: webhookPayload.ended_at,
      }, `${finalState === 'success' ? 'onSuccess' : 'onFailure'} ${webhookPayload.dag_id}`)
    }

    // Fire global webhooks (GLOBAL_SUCCESS_WEBHOOK / GLOBAL_FAILURE_WEBHOOK)
    // These receive the same payload as per-DAG webhooks.
    const globalUrl = finalState === 'success'
      ? process.env.GLOBAL_SUCCESS_WEBHOOK
      : process.env.GLOBAL_FAILURE_WEBHOOK
    if (globalUrl) {
      fireWebhook(globalUrl, webhookPayload, webhookOptions)
    }

    // Fire global Slack webhook with a formatted message
    // GLOBAL_SLACK_WEBHOOK fires for ALL runs (success + failure)
    // GLOBAL_SLACK_SUCCESS_WEBHOOK fires on success only
    // GLOBAL_SLACK_FAILURE_WEBHOOK fires on failure only
    const slackUrl = process.env.GLOBAL_SLACK_WEBHOOK
      || (finalState === 'success' ? process.env.GLOBAL_SLACK_SUCCESS_WEBHOOK : null)
      || (finalState === 'failed'  ? process.env.GLOBAL_SLACK_FAILURE_WEBHOOK : null)
    if (slackUrl) {
      const icon = finalState === 'success' ? '✅' : '❌'
      const runShort = dagRunId.slice(-8)
      const ts = (transitioned.ended_at as Date)?.toISOString().slice(0,16).replace('T',' ') + 'Z'
      const slackBody = {
        text: `${icon} *${transitioned.dag_id}* — ${finalState}`,
        attachments: [{
          color: finalState === 'success' ? '#48bb78' : '#fc8181',
          fields: [
            { title: 'DAG',    value: transitioned.dag_id, short: true },
            { title: 'State',  value: finalState,           short: true },
            { title: 'Run ID', value: runShort,              short: true },
            { title: 'Ended',  value: ts,                   short: true },
          ],
        }],
      }
      // Fire as raw JSON body (Slack incoming webhook format)
      fireWebhook(slackUrl, slackBody as any, webhookOptions)
    }
  }
}

export type ClearResult =
  | { cleared: true; clearedCount: number }
  | { cleared: false; reason: 'run_not_found' | 'task_not_found' | 'task_not_terminal' }

/**
 * Clear one or all instances of a single task back to queued (finished instances only) and put the run
 * back to queued. See clearTaskInstances (actions.ts) for downstream / upstream / failed-only clears.
 */
export async function clearTaskInstance(
  db: Db,
  dagRunId: string,
  taskId: string,
  mapIndex?: number | null,
): Promise<ClearResult> {
  const res = await clearTaskInstances(db, dagRunId, { taskIds: [taskId], mapIndex })
  if (!res.ok) return { cleared: false, reason: res.reason }
  if (res.changed.length === 0) return { cleared: false, reason: 'task_not_terminal' }
  return { cleared: true, clearedCount: res.changed.length }
}

/**
 * If the run has exceeded `runTimeout` ms since it started running, mark every unfinished
 * task failed so the caller's completion check finalizes the run as failed.
 * Returns true when the run is (now) timed out.
 */
async function failIfRunTimedOut(
  db: Db,
  dagRunId: string,
  runTimeout: number | undefined,
  startedAt: Date | null | undefined,
): Promise<boolean> {
  if (!runTimeout || runTimeout <= 0 || !startedAt) return false
  if (Date.now() - new Date(startedAt).getTime() < runTimeout) return false

  const res = await db.collection('task_instances').updateMany(
    { dag_run_id: dagRunId, state: { $in: [...TI_UNFINISHED] } },
    { $set: { state: 'failed', ended_at: new Date(), error: `Dag run exceeded runTimeout (${runTimeout}ms)` } },
  )
  if (res.modifiedCount > 0) {
    console.warn(`[scheduler] run ${dagRunId} exceeded runTimeout ${runTimeout}ms — failed ${res.modifiedCount} task(s)`)
  }
  return true
}

/**
 * Cancel a dag_run atomically:
 * - Marks run state → cancelled
 * - Marks all unfinished tasks (queued/running/deferred/up_for_*) → cancelled
 * Returns false if the run was already in a terminal state.
 */
export async function cancelRun(db: Db, dagRunId: string): Promise<boolean> {
  const result = await db.collection('dag_runs').findOneAndUpdate(
    { _id: new ObjectId(dagRunId), state: { $in: ['queued', 'running'] } },
    { $set: { state: 'cancelled', ended_at: new Date() } },
    { returnDocument: 'after' }
  )

  if (!result) return false  // already terminal or not found
  await pruneDeadlines(db, dagRunId)

  // Cancel all non-terminal tasks in one shot
  await db.collection('task_instances').updateMany(
    { dag_run_id: dagRunId, state: { $in: [...TI_UNFINISHED] } },
    { $set: { state: 'cancelled', ended_at: new Date(), error: 'Cancelled by user' } }
  )

  console.log(`[scheduler] run ${dagRunId} → cancelled`)
  void recordEvent(db, 'run_cancelled', {
    dag_id: result.dag_id as string,
    dag_run_id: dagRunId,
  })
  return true
}
