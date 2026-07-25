import { createHash } from 'node:crypto'
import type { Db } from 'mongodb'

/**
 * Compute a short sha256 hex digest of dag source bytes.
 * Returns the first 12 hex chars — enough to identify a version uniquely
 * while staying readable in logs and UI.
 */
export function hashDagSource(source: string): string {
  return createHash('sha256').update(source, 'utf8').digest('hex').slice(0, 12)
}

// ── Dag version persistence ────────────────────────────────────────────────

export interface DagVersionDoc {
  dag_id: string
  version: string      // sha256[:12]
  source: string       // full source text of the dag file
  first_seen: Date     // when this (dag_id, version) was first recorded
  task_ids: string[]   // snapshot of task ids at time of first load
}

export interface DagVersionSummary {
  dag_id: string
  version: string
  first_seen: Date
  task_ids: string[]
  run_count: number   // derived: dag_runs with matching dag_id + dag_version
}

/**
 * Record a dag version the first time it is seen (idempotent via $setOnInsert).
 * Dedup is done by (dag_id, version) filter — NOT relying on a unique index
 * because ensureIndexes isn't called in test setups.
 *
 * Re-loading the same unchanged file → exactly ONE row; source change → second row.
 */
export async function recordDagVersion(
  db: Db,
  dagId: string,
  version: string,
  source: string,
  taskIds: string[],
): Promise<void> {
  try {
    await db.collection<DagVersionDoc>('dag_versions').updateOne(
      { dag_id: dagId, version },
      {
        $setOnInsert: {
          dag_id: dagId,
          version,
          source,
          first_seen: new Date(),
          task_ids: taskIds,
        },
      },
      { upsert: true },
    )
  } catch {
    // Swallow — stub DB in tests or transient write failures must not crash loadDags
  }
}

/** List all recorded versions for a dag, newest first. */
export async function listDagVersions(db: Db, dagId: string): Promise<DagVersionSummary[]> {
  const docs = await db.collection<DagVersionDoc>('dag_versions')
    .find({ dag_id: dagId })
    .sort({ first_seen: -1 })
    .toArray()

  // Derive run_count for each version
  return Promise.all(docs.map(async d => ({
    dag_id: d.dag_id,
    version: d.version,
    first_seen: d.first_seen,
    task_ids: d.task_ids,
    run_count: await db.collection('dag_runs').countDocuments({ dag_id: dagId, dag_version: d.version }),
  })))
}

/** Get the source for a specific version (or the latest if version omitted). */
export async function getDagSource(
  db: Db,
  dagId: string,
  version?: string,
): Promise<DagVersionDoc | null> {
  if (version) {
    return db.collection<DagVersionDoc>('dag_versions').findOne({ dag_id: dagId, version })
  }
  // Default to most recently first-seen version
  return db.collection<DagVersionDoc>('dag_versions')
    .findOne({ dag_id: dagId }, { sort: { first_seen: -1 } })
}
