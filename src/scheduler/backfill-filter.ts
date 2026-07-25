/**
 * Backfill tick-gate helpers — in a separate module to avoid circular imports.
 * backfill.ts → runs.ts (OK)
 * index.ts → backfill-filter.ts (OK, no dependency on index.ts)
 */

import type { Db } from 'mongodb'

/**
 * Get the set of backfill_ids currently in 'paused' state.
 * Used by tick() to exclude their runs from advancement.
 */
export async function getPausedBackfillIds(db: Db): Promise<Set<string>> {
  const docs = await db.collection('backfills')
    .find({ state: 'paused' })
    .project({ _id: 1 })
    .toArray()
  return new Set(docs.map(d => (d as { _id: { toString(): string } })._id.toString()))
}

/**
 * Build the MongoDB filter for active runs, excluding paused backfills.
 * Pure function — testable without a real DB.
 *
 * Normal (non-backfill) runs have backfill_id:null and are unaffected by $nin.
 */
export function buildActiveRunFilter(pausedBackfillIds: string[]): Record<string, unknown> {
  const filter: Record<string, unknown> = { state: { $in: ['queued', 'running'] } }
  if (pausedBackfillIds.length > 0) {
    // Exclude runs that belong to a paused backfill; null/absent backfill_id passes through
    filter['$or'] = [
      { backfill_id: null },
      { backfill_id: { $exists: false } },
      { backfill_id: { $nin: pausedBackfillIds } },
    ]
  }
  return filter
}
