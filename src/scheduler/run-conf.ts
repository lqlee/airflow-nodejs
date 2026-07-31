import type { Db } from 'mongodb'
import { ObjectId } from 'mongodb'

/**
 * Fetch the `conf` object for a given run from the DB.
 * Called in the worker process so conf is read fresh for each task execution.
 * Returns {} if the run is not found or has no conf.
 */
export async function getRunConf(db: Db, runId: string): Promise<Record<string, unknown>> {
  const run = await db.collection('dag_runs').findOne(
    { _id: new ObjectId(runId) },
    { projection: { conf: 1 } },
  )
  return (run?.conf as Record<string, unknown>) ?? {}
}

export interface RunMeta {
  conf: Record<string, unknown>
  created_at: Date
  logical_date: Date | null
}

/**
 * Fetch conf + date fields needed for template rendering.
 * Called in the executor before spawning shell/python/java/container tasks.
 */
export async function getRunMeta(db: Db, runId: string): Promise<RunMeta> {
  const run = await db.collection('dag_runs').findOne(
    { _id: new ObjectId(runId) },
    { projection: { conf: 1, created_at: 1, logical_date: 1 } },
  )
  return {
    conf:         (run?.conf as Record<string, unknown>) ?? {},
    created_at:   run?.created_at ? new Date(run.created_at as Date) : new Date(),
    logical_date: run?.logical_date ? new Date(run.logical_date as Date) : null,
  }
}
