import cronParser from 'cron-parser'
import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
import { createRun } from './runs.js'

export const BACKFILL_MAX_RUNS = 500

export interface BackfillRequest {
  start: Date   // inclusive
  end: Date     // inclusive
}

export interface BackfillResult {
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

/**
 * Run backfill for a dag: create one queued run per scheduled date in [start, end]
 * that does not already have a run. Runs are left queued — the scheduler tick drives them.
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

  const created: string[] = []
  let skipped = 0

  for (const date of dates) {
    if (existingMs.has(date.getTime())) {
      skipped++
      continue
    }
    const runId = await createRun(db, dag, { logicalDate: date, tags: ['backfill'], triggerType: 'backfill' })
    created.push(runId)
  }

  console.log(
    `[backfill] dag '${dag.id}' ${req.start.toISOString()} → ${req.end.toISOString()}: ` +
      `${created.length} created, ${skipped} skipped`,
  )

  return { created, skipped, dates }
}
