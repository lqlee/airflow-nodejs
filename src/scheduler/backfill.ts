import cronParser from 'cron-parser'
import { ObjectId, type Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
import { createRun } from './runs.js'

export const BACKFILL_MAX_RUNS = 500

export type BackfillState = 'active' | 'paused' | 'cancelled'

export interface BackfillDoc {
  dag_id: string
  start: Date
  end: Date
  state: BackfillState
  run_ids: string[]
  created_count: number
  skipped_count: number
  total_dates: number
  created_at: Date
  updated_at: Date
}

export interface BackfillRequest {
  start: Date   // inclusive
  end: Date     // inclusive
}

export interface BackfillResult {
  backfill_id: string  // ID of the backfill entity
  created: string[]    // run ids created
  skipped: number      // (dag_id, logical_date) pairs already had a run
  dates: Date[]        // all scheduled dates in range (created + skipped)
}

/**
 * Enumerate all cron occurrences of a dag's schedule in [start, end] (inclusive).
 * Returns dates in ascending order.
 */
export function enumerateDates(schedule: string, start: Date, end: Date): Date[] {
  const dates: Date[] = []

  // currentDate is exclusive in cron-parser (it returns dates *after* currentDate)
  // so we subtract 1ms to make start inclusive.
  // tz: 'UTC' ensures cron expressions fire at UTC clock times, not local wall time,
  // giving deterministic behaviour across machines and CI environments.
  const iter = cronParser.parseExpression(schedule, {
    currentDate: new Date(start.getTime() - 1),
    endDate: end,
    iterator: true,
    tz: 'UTC',
  })

  while (true) {
    try {
      const { value, done } = iter.next() as { value: { toDate(): Date }; done: boolean }
      const d = value.toDate()
      // Guard: only include dates within range (done=true is the last valid item)
      if (d <= end) dates.push(d)
      if (done) break
    } catch {
      break
    }
  }

  return dates
}

// getPausedBackfillIds and buildActiveRunFilter live in backfill-filter.ts
// to avoid a circular dependency with scheduler/index.ts.
export { getPausedBackfillIds, buildActiveRunFilter } from './backfill-filter.js'

/**
 * Run backfill for a dag: persist a backfill entity, create one queued run
 * per scheduled date in [start, end] (skipping existing), stamp each run
 * with backfill_id so the scheduler can pause/cancel the whole group.
 *
 * Throws if:
 * - dag has no schedule (null)
 * - start > end
 * - date count exceeds BACKFILL_MAX_RUNS
 */
export async function backfill(
  db: Db,
  dag: DagDefinition,
  req: BackfillRequest,
): Promise<BackfillResult> {
  if (!dag.schedule) {
    throw new RangeError(`Dag '${dag.id}' has no schedule — backfill requires a cron schedule`)
  }
  if (req.start > req.end) {
    throw new RangeError('start must be before or equal to end')
  }

  const dates = enumerateDates(dag.schedule, req.start, req.end)

  if (dates.length > BACKFILL_MAX_RUNS) {
    throw new RangeError(
      `Backfill would create ${dates.length} runs — exceeds limit of ${BACKFILL_MAX_RUNS}. Narrow the date range.`,
    )
  }

  // Find already-existing logical_date values for this dag in this range
  const existing = await db
    .collection('dag_runs')
    .find(
      { dag_id: dag.id, logical_date: { $gte: req.start, $lte: req.end } },
      { projection: { logical_date: 1 } },
    )
    .toArray()

  const existingMs = new Set(existing.map(r => new Date(r.logical_date as Date).getTime()))

  // Insert the backfill entity first — state is set before runs are created
  const now = new Date()
  const bfResult = await db.collection<BackfillDoc>('backfills').insertOne({
    dag_id: dag.id,
    start: req.start,
    end: req.end,
    state: 'active',
    run_ids: [],        // filled below
    created_count: 0,   // updated below
    skipped_count: 0,
    total_dates: dates.length,
    created_at: now,
    updated_at: now,
  })
  const backfillId = bfResult.insertedId.toString()

  const created: string[] = []
  let skipped = 0

  for (const date of dates) {
    if (existingMs.has(date.getTime())) {
      skipped++
      continue
    }
    const runId = await createRun(db, dag, {
      logicalDate: date,
      tags: ['backfill'],
      triggerType: 'backfill',
      backfillId,
    })
    created.push(runId)
  }

  // Update the backfill doc with final counts and run list
  await db.collection('backfills').updateOne(
    { _id: bfResult.insertedId },
    { $set: { run_ids: created, created_count: created.length, skipped_count: skipped, updated_at: new Date() } },
  )

  console.log(
    `[backfill] dag '${dag.id}' ${req.start.toISOString()} → ${req.end.toISOString()}: ` +
      `${created.length} created, ${skipped} skipped (backfill ${backfillId})`,
  )

  return { backfill_id: backfillId, created, skipped, dates }
}

/**
 * Pause a backfill — its queued/running runs are excluded from tick() advancement
 * until resumed. In-flight 'running' runs finish their current wave but won't
 * start new tasks.
 * Returns false if backfill not found or already terminal.
 */
export async function pauseBackfill(db: Db, backfillId: string): Promise<boolean> {
  if (!ObjectId.isValid(backfillId)) return false
  const result = await db.collection('backfills').updateOne(
    { _id: new ObjectId(backfillId), state: 'active' },
    { $set: { state: 'paused', updated_at: new Date() } },
  )
  return result.matchedCount > 0
}

/**
 * Resume a paused backfill — its runs re-enter the tick() advancement pool.
 * Returns false if backfill not found or not currently paused.
 */
export async function resumeBackfill(db: Db, backfillId: string): Promise<boolean> {
  if (!ObjectId.isValid(backfillId)) return false
  const result = await db.collection('backfills').updateOne(
    { _id: new ObjectId(backfillId), state: 'paused' },
    { $set: { state: 'active', updated_at: new Date() } },
  )
  return result.matchedCount > 0
}

/**
 * Cancel a backfill — marks it cancelled, then cancels all non-terminal runs.
 * Returns false if not found or already cancelled.
 */
export async function cancelBackfill(db: Db, backfillId: string): Promise<{ cancelled: boolean; runsCancelled: number }> {
  if (!ObjectId.isValid(backfillId)) return { cancelled: false, runsCancelled: 0 }

  // Mark the entity first so the tick immediately stops picking up its runs
  const result = await db.collection<BackfillDoc>('backfills').findOneAndUpdate(
    { _id: new ObjectId(backfillId), state: { $in: ['active', 'paused'] } },
    { $set: { state: 'cancelled', updated_at: new Date() } },
    { returnDocument: 'after' },
  )
  if (!result) return { cancelled: false, runsCancelled: 0 }

  // Cancel each non-terminal run inline (avoids circular import with index.ts)
  let runsCancelled = 0
  for (const runId of (result as BackfillDoc & { run_ids: string[] }).run_ids ?? []) {
    if (!ObjectId.isValid(runId)) continue
    const runResult = await db.collection('dag_runs').findOneAndUpdate(
      { _id: new ObjectId(runId), state: { $in: ['queued', 'running'] } },
      { $set: { state: 'cancelled', ended_at: new Date() } },
      { returnDocument: 'after' },
    )
    if (runResult) {
      await db.collection('task_instances').updateMany(
        { dag_run_id: runId, state: { $in: ['queued', 'running'] } },
        { $set: { state: 'cancelled', ended_at: new Date(), error: 'Backfill cancelled' } },
      )
      runsCancelled++
    }
  }

  console.log(`[backfill] ${backfillId} cancelled — ${runsCancelled} run(s) cancelled`)
  return { cancelled: true, runsCancelled }
}

export interface BackfillSummary {
  backfill_id: string
  dag_id: string
  start: Date
  end: Date
  state: BackfillState
  /** Derived: all runs terminal and backfill not paused/cancelled */
  completed: boolean
  created_count: number
  skipped_count: number
  total_dates: number
  run_ids: string[]
  created_at: Date
  updated_at: Date
}

/** Format a raw BackfillDoc for API responses. */
export function formatBackfill(doc: BackfillDoc & { _id: ObjectId }, runStates?: string[]): BackfillSummary {
  const completed =
    doc.state === 'active' &&
    (runStates !== undefined
      ? runStates.every(s => s === 'success' || s === 'failed' || s === 'cancelled')
      : false)

  return {
    backfill_id: doc._id.toString(),
    dag_id: doc.dag_id,
    start: doc.start,
    end: doc.end,
    state: doc.state,
    completed,
    created_count: doc.created_count,
    skipped_count: doc.skipped_count,
    total_dates: doc.total_dates,
    run_ids: doc.run_ids,
    created_at: doc.created_at,
    updated_at: doc.updated_at,
  }
}
