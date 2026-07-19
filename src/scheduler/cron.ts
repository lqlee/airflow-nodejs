import cron from 'node-cron'
import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
import { createRun } from './runs.js'
import { advanceRun } from './index.js'
import { isDagPaused } from '../dag/pause.js'

// Map of dagId → active cron task
const cronJobs = new Map<string, cron.ScheduledTask>()

/**
 * Register a cron job for a Dag.
 * If a job already exists for this dagId, it is replaced.
 */
export function scheduleDag(db: Db, dag: DagDefinition): void {
  if (!dag.schedule) return

  if (!cron.validate(dag.schedule)) {
    console.warn(`[cron] invalid cron expression for dag '${dag.id}': '${dag.schedule}' — skipping`)
    return
  }

  // Remove existing job if any
  unscheduleDag(dag.id)

  const job = cron.schedule(dag.schedule, async () => {
    // Skip if paused — check DB each time so pause/resume takes effect immediately
    const paused = await isDagPaused(db, dag.id)
    if (paused) {
      console.log(`[cron] ⏸  dag '${dag.id}' is paused — skipping scheduled run`)
      return
    }

    console.log(`[cron] ⏰ dag '${dag.id}' triggered by schedule '${dag.schedule}'`)
    try {
      const runId = await createRun(db, dag)
      await advanceRun(db, runId)
    } catch (err) {
      console.error(`[cron] error running dag '${dag.id}':`, err)
    }
  })

  cronJobs.set(dag.id, job)
  console.log(`[cron] scheduled dag '${dag.id}' → '${dag.schedule}'`)
}

/**
 * Remove the cron job for a dag (e.g. when it's removed or schedule changes).
 */
export function unscheduleDag(dagId: string): void {
  const existing = cronJobs.get(dagId)
  if (existing) {
    existing.stop()
    cronJobs.delete(dagId)
  }
}

/**
 * Stop all active cron jobs.
 */
export function stopAllCronJobs(): void {
  for (const [dagId, job] of cronJobs) {
    job.stop()
    cronJobs.delete(dagId)
  }
  console.log('[cron] all jobs stopped')
}

/**
 * Sync cron jobs to the current registry:
 * - Add jobs for new scheduled dags
 * - Remove jobs for dags no longer present or unscheduled
 */
export function syncCronJobs(db: Db, dags: DagDefinition[]): void {
  const activeDagIds = new Set(dags.filter(d => d.schedule).map(d => d.id))

  // Remove jobs for dags no longer active
  for (const dagId of cronJobs.keys()) {
    if (!activeDagIds.has(dagId)) {
      unscheduleDag(dagId)
      console.log(`[cron] removed job for dag '${dagId}' (no longer scheduled)`)
    }
  }

  // Add/update jobs for scheduled dags
  for (const dag of dags) {
    if (!dag.schedule) continue
    const existing = cronJobs.get(dag.id)
    // Re-register if new or schedule changed
    if (!existing) {
      scheduleDag(db, dag)
    }
  }
}
