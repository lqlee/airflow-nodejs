import cron from 'node-cron'
import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
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
