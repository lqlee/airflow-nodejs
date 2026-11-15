import cron from 'node-cron'
import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
import { getDag } from '../dag/registry.js'
import { createRun, DuplicateRunError } from './runs.js'
import { advanceRun } from './index.js'
import { isDagPaused } from '../dag/pause.js'
import { enumerateDates } from './backfill.js'
import { isLeader } from './lease.js'
import { runInfoForFire } from './data-interval.js'

/**
 * Logical date of a cron firing: the scheduled instant, truncated to the expression's resolution
 * (minute for 5-field expressions, second for 6-field ones) so every replica derives the same value.
 * Matches Airflow 3's CronTriggerTimetable, where the logical date is the trigger time.
 */
export function cronLogicalDate(expression: string, firedAt: Date): Date {
  const unit = expression.trim().split(/\s+/).length >= 6 ? 1_000 : 60_000
  return new Date(Math.floor(firedAt.getTime() / unit) * unit)
}

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
  // catchup Dags are driven by tickCatchup() so every missed occurrence gets a run
  if (dag.catchup) {
    unscheduleDag(dag.id)
    return
  }

  if (!cron.validate(dag.schedule)) {
    console.warn(`[cron] invalid cron expression for dag '${dag.id}': '${dag.schedule}' — skipping`)
    return
  }

  // Stop and remove existing job (handles both "new" and "schedule changed" cases)
  unscheduleDag(dag.id)

  const expression = dag.schedule
  const task = cron.schedule(expression, async (firedAt) => {
    await fireCronRun(db, dag, expression, firedAt instanceof Date ? firedAt : new Date())
  })

  cronJobs.set(dag.id, { task, expression: dag.schedule })
  console.log(`[cron] scheduled dag '${dag.id}' → '${dag.schedule}'`)
}

/**
 * What a cron job does when it fires: create (and start) the run for this occurrence.
 * Returns the run id, or null when skipped — not the leader, Dag paused, or the occurrence
 * already has a run (another replica fired it first). Never throws.
 */
export async function fireCronRun(
  db: Db,
  dag: DagDefinition,
  expression: string,
  firedAt: Date,
  opts: { advance?: boolean } = {},
): Promise<string | null> {
  // Leadership can move between ticks; only the current leader creates scheduled runs
  if (!isLeader()) return null
  try {
    // Skip if paused — check DB each time so pause/resume takes effect immediately
    if (await isDagPaused(db, dag.id)) {
      console.log(`[cron] ⏸  dag '${dag.id}' is paused — skipping scheduled run`)
      return null
    }
    console.log(`[cron] ⏰ dag '${dag.id}' triggered by schedule '${expression}'`)
    // node-cron fires on the local clock — the interval's other end is computed on the same clock
    const info = runInfoForFire({ ...dag, schedule: expression }, cronLogicalDate(expression, firedAt))
    const runId = await createRun(db, dag, { triggerType: 'cron', logicalDate: info.logicalDate, dataInterval: info.dataInterval })
    if (opts.advance !== false) await advanceRun(db, runId)
    return runId
  } catch (err) {
    if (err instanceof DuplicateRunError) {
      console.log(`[cron] dag '${dag.id}' already has a run for ${err.logicalDate.toISOString()} — skipping`)
    } else {
      console.error(`[cron] error running dag '${dag.id}':`, err)
    }
    return null
  }
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
  const activeDagIds = new Set(dags.filter(d => d.schedule && !d.catchup).map(d => d.id))

  // Remove jobs for dags no longer active or no longer scheduled
  for (const dagId of cronJobs.keys()) {
    if (!activeDagIds.has(dagId)) {
      unscheduleDag(dagId)
      console.log(`[cron] removed job for dag '${dagId}' (no longer scheduled)`)
    }
  }

  // Add or replace jobs for scheduled dags
  for (const dag of dags) {
    if (!dag.schedule || dag.catchup) continue
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

// ── Catch-up scheduling ────────────────────────────────────────────────────────

/** Runs created per Dag per tick — a long gap is replayed over several ticks. */
export const CATCHUP_MAX_RUNS_PER_TICK = 100

/** Process-local baseline for Dags with neither startDate nor any prior catch-up run. */
const catchupFirstSeen = new Map<string, Date>()

/**
 * Where to start enumerating fire times: end of the latest catch-up run's data interval (its logical
 * date on older runs) > startDate > first time we saw the Dag. Runs whose logical date falls before the
 * baseline are never created, so in interval mode the first run covers [startDate, next tick].
 */
async function resolveCatchupBaseline(db: Db, dag: DagDefinition, now: Date): Promise<Date | null> {
  const last = await db.collection('dag_runs').findOne(
    { dag_id: dag.id, trigger_type: 'catchup', logical_date: { $ne: null } },
    { sort: { logical_date: -1 } },
  )
  if (last) return new Date((last.data_interval_end ?? last.logical_date) as Date)

  if (dag.startDate !== undefined) {
    const start = new Date(dag.startDate)
    if (Number.isNaN(start.getTime())) {
      console.warn(`[catchup] dag '${dag.id}' has an invalid startDate '${String(dag.startDate)}' — skipping`)
      return null
    }
    return start
  }

  if (!catchupFirstSeen.has(dag.id)) catchupFirstSeen.set(dag.id, now)
  return catchupFirstSeen.get(dag.id)!
}

/**
 * Create a queued run for every scheduled occurrence (UTC) of each `catchup: true` Dag that
 * has come due and has no run yet. Idempotent — existing (dag_id, logical_date) pairs are
 * skipped, so it is safe on every tick and across restarts. Oldest dates first.
 * `now` and `maxPerTick` are injectable for tests.
 */
export async function tickCatchup(db: Db, dags: DagDefinition[], now: Date = new Date(), maxPerTick: number = CATCHUP_MAX_RUNS_PER_TICK): Promise<number> {
  // setInterval doesn't wait for the previous tick — overlapping calls would both see the
  // same missing dates and create duplicate runs. Skip if one is already in flight.
  if (catchupInFlight) return 0
  catchupInFlight = true
  try {
    return await runCatchup(db, dags, now, maxPerTick)
  } finally {
    catchupInFlight = false
  }
}

let catchupInFlight = false

async function runCatchup(db: Db, dags: DagDefinition[], now: Date, maxPerTick: number): Promise<number> {
  let created = 0
  for (const dag of dags) {
    if (!dag.catchup || !dag.schedule) continue
    if (!cron.validate(dag.schedule)) continue
    if (await isDagPaused(db, dag.id)) continue

    const from = await resolveCatchupBaseline(db, dag, now)
    if (!from || from > now) continue

    const infos = enumerateDates(dag.schedule, from, now)
      .map(fire => runInfoForFire(dag, fire, 'UTC'))
      .filter(i => i.logicalDate >= from)
    if (infos.length === 0) continue

    const existing = await db.collection('dag_runs')
      .find({ dag_id: dag.id, logical_date: { $gte: from, $lte: now } }, { projection: { logical_date: 1 } })
      .toArray()
    const have = new Set(existing.map(r => new Date(r.logical_date as Date).getTime()))

    const missing = infos.filter(i => !have.has(i.logicalDate.getTime())).slice(0, maxPerTick)
    let createdForDag = 0
    for (const info of missing) {
      try {
        await createRun(db, dag, { logicalDate: info.logicalDate, dataInterval: info.dataInterval, triggerType: 'catchup' })
        createdForDag++
      } catch (err) {
        if (!(err instanceof DuplicateRunError)) throw err   // raced another scheduler or a backfill
      }
    }
    created += createdForDag
    if (createdForDag > 0) {
      console.log(`[catchup] dag '${dag.id}' → created ${createdForDag} run(s) up to ${missing[missing.length - 1].logicalDate.toISOString()}`)
    }
  }
  return created
}

/** Test helper — forget in-memory catch-up baselines. */
export function resetCatchupState(): void {
  catchupFirstSeen.clear()
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

/**
 * Forget every in-memory timetable entry. Called when this process loses leadership: the next leader
 * fires the runs meanwhile, so entries must be rebuilt from the DB if leadership comes back.
 */
export function resetAllTimetables(): void {
  timetableEntries.clear()
}

/** Number of active timetable entries (for testing). */
export function activeTimetableCount(): number {
  return timetableEntries.size
}

/** Get timetable state for a dag (for testing/API). */
export function getTimetableEntry(dagId: string): TimetableEntry | undefined {
  return timetableEntries.get(dagId)
}
