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
