import cron from 'node-cron'
import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
import { getDag } from '../dag/registry.js'
import { createRun } from './runs.js'
import { advanceRun } from './index.js'
import { isDagPaused } from '../dag/pause.js'

// Store both the task and the expression so we can detect schedule changes.
// Previously only the task was stored — this prevented syncCronJobs from
// detecting when a dag's cron expression was updated in the file.
interface CronEntry {
  task: cron.ScheduledTask
  expression: string
}

const cronJobs = new Map<string, CronEntry>()

/**
 * Register a cron job for a Dag.
 * If a job already exists for this dagId (any expression), it is stopped and replaced.
 */
export function scheduleDag(db: Db, dag: DagDefinition): void {
  if (!dag.schedule) return

  if (!cron.validate(dag.schedule)) {
    console.warn(`[cron] invalid cron expression for dag '${dag.id}': '${dag.schedule}' — skipping`)
    return
  }

  // Stop and remove existing job (handles both "new" and "schedule changed" cases)
  unscheduleDag(dag.id)

  const task = cron.schedule(dag.schedule, async () => {
    // Skip if paused — check DB each time so pause/resume takes effect immediately
    const paused = await isDagPaused(db, dag.id)
    if (paused) {
      console.log(`[cron] ⏸  dag '${dag.id}' is paused — skipping scheduled run`)
      return
    }

    console.log(`[cron] ⏰ dag '${dag.id}' triggered by schedule '${dag.schedule}'`)
    try {
      const runId = await createRun(db, dag, { triggerType: 'cron' })
      await advanceRun(db, runId)
    } catch (err) {
      console.error(`[cron] error running dag '${dag.id}':`, err)
    }
  })

  cronJobs.set(dag.id, { task, expression: dag.schedule })
  console.log(`[cron] scheduled dag '${dag.id}' → '${dag.schedule}'`)
}

/**
 * Remove the cron job for a dag (e.g. when it's removed or schedule changes).
 */
export function unscheduleDag(dagId: string): void {
  const existing = cronJobs.get(dagId)
  if (existing) {
    existing.task.stop()
    cronJobs.delete(dagId)
  }
}

/**
 * Stop all active cron jobs.
 */
export function stopAllCronJobs(): void {
  for (const [, entry] of cronJobs) {
    entry.task.stop()
  }
  cronJobs.clear()
  console.log('[cron] all jobs stopped')
}

/**
 * Sync cron jobs to the current registry:
 * - Add jobs for newly scheduled dags
 * - Replace jobs whose schedule expression changed (bug fix: was silently ignored before)
 * - Remove jobs for dags no longer present or no longer scheduled
 */
export function syncCronJobs(db: Db, dags: DagDefinition[]): void {
  const activeDagIds = new Set(dags.filter(d => d.schedule).map(d => d.id))

  // Remove jobs for dags no longer active or no longer scheduled
  for (const dagId of cronJobs.keys()) {
    if (!activeDagIds.has(dagId)) {
      unscheduleDag(dagId)
      console.log(`[cron] removed job for dag '${dagId}' (no longer scheduled)`)
    }
  }

  // Add or replace jobs for scheduled dags
  for (const dag of dags) {
    if (!dag.schedule) continue
    const existing = cronJobs.get(dag.id)
    if (!existing) {
      // First time we see this dag with a schedule
      scheduleDag(db, dag)
    } else if (existing.expression !== dag.schedule) {
      // Schedule changed — replace old job with new expression
      console.log(`[cron] schedule changed for dag '${dag.id}': '${existing.expression}' → '${dag.schedule}'`)
      scheduleDag(db, dag)
    }
    // expression unchanged — leave existing job running
  }
}

/** Return the expression currently registered for a dag, or undefined if not scheduled. */
export function getScheduledExpression(dagId: string): string | undefined {
  return cronJobs.get(dagId)?.expression
}

/** Number of active cron jobs (for testing). */
export function activeCronJobCount(): number {
  return cronJobs.size
}

// ── Timetable scheduling ───────────────────────────────────────────────────────

interface TimetableEntry {
  /** Time the last run was created (null = no runs yet). */
  lastRunAt: Date | null
  /** Total runs created so far by this timetable. */
  runCount: number
  /** Pre-computed next fire time (null = permanently stopped). */
  nextFireAt: Date | null
  /** True once nextFireAt returned null — no further calls made. */
  stopped: boolean
}

const timetableEntries = new Map<string, TimetableEntry>()

/**
 * Called on every scheduler tick for DAGs with a `timetable` function.
 * Fires a new run if nextFireAt <= now, then computes the next fire time.
 * Idempotent — safe to call repeatedly even if the DAG hasn't changed.
 */
export async function tickTimetables(db: Db, dags: DagDefinition[]): Promise<void> {
  const now = new Date()

  // Remove entries for dags that no longer have a timetable
  const activeTimetableIds = new Set(dags.filter(d => d.timetable && !d.schedule).map(d => d.id))
  for (const dagId of timetableEntries.keys()) {
    if (!activeTimetableIds.has(dagId)) {
      timetableEntries.delete(dagId)
      console.log(`[timetable] removed entry for dag '${dagId}'`)
    }
  }

  for (const dag of dags) {
    if (!dag.timetable || dag.schedule) continue  // timetable requires schedule: null

    const paused = await isDagPaused(db, dag.id)
    if (paused) continue

    let entry = timetableEntries.get(dag.id)

    // First tick for this dag — compute initial nextFireAt
    if (!entry) {
      // Re-fetch lastRunAt from DB to survive server restarts.
      // Count only timetable-triggered runs so the runCount limit logic works correctly.
      // lastRunAt uses any run (cron/manual/timetable) to avoid re-firing on restart.
      const lastRun = await db.collection('dag_runs')
        .findOne({ dag_id: dag.id }, { sort: { created_at: -1 } })
      const runCount = await db.collection('dag_runs')
        .countDocuments({ dag_id: dag.id, trigger_type: 'timetable' })

      const lastRunAt = lastRun ? new Date(lastRun.created_at as Date) : null
      let nextFireAt: Date | null = null
      try {
        nextFireAt = dag.timetable(lastRunAt, runCount)
      } catch (err) {
        console.error(`[timetable] dag '${dag.id}' timetable() threw on init:`, err)
      }

      entry = { lastRunAt, runCount, nextFireAt, stopped: nextFireAt === null }
      timetableEntries.set(dag.id, entry)

      console.log(`[timetable] registered dag '${dag.id}' → next fire: ${nextFireAt?.toISOString() ?? 'never'}`)
    }

    if (entry.stopped || entry.nextFireAt === null) continue
    if (entry.nextFireAt > now) continue

    // Fire time reached — create run
    console.log(`[timetable] ⏰ dag '${dag.id}' triggered by timetable (run #${entry.runCount + 1})`)
    const firedAt = new Date()
    try {
      const runId = await createRun(db, dag, { triggerType: 'timetable' })
      void advanceRun(db, runId)

      entry.lastRunAt = firedAt
      entry.runCount += 1

      // Compute next fire time using the live dag function (hot-reload aware)
      const liveDag = getDag(dag.id)
      const liveFn = liveDag?.timetable ?? dag.timetable
      let nextFireAt: Date | null = null
      try {
        nextFireAt = liveFn(firedAt, entry.runCount)
      } catch (err) {
        console.error(`[timetable] dag '${dag.id}' timetable() threw after fire:`, err)
      }

      entry.nextFireAt = nextFireAt
      entry.stopped = nextFireAt === null

      if (nextFireAt) {
        console.log(`[timetable] dag '${dag.id}' next fire: ${nextFireAt.toISOString()}`)
      } else {
        console.log(`[timetable] dag '${dag.id}' timetable returned null — no more runs`)
      }
    } catch (err) {
      console.error(`[timetable] error creating run for dag '${dag.id}':`, err)
    }
  }
}

/**
 * Reset a timetable entry (e.g. after dag file reload with changed timetable fn).
 * Called by syncCronJobs when a dag's timetable reference changes.
 */
export function resetTimetable(dagId: string): void {
  timetableEntries.delete(dagId)
}

/** Number of active timetable entries (for testing). */
export function activeTimetableCount(): number {
  return timetableEntries.size
}

/** Get timetable state for a dag (for testing/API). */
export function getTimetableEntry(dagId: string): TimetableEntry | undefined {
  return timetableEntries.get(dagId)
}
