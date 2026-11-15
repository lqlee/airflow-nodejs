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
  data_interval_start: Date | null
  data_interval_end: Date | null
}

const asDate = (v: unknown): Date | null => (v ? new Date(v as Date) : null)

/**
 * Fetch conf + date fields needed for template rendering.
 * Called in the executor before spawning shell/python/java/container tasks.
 */
export async function getRunMeta(db: Db, runId: string): Promise<RunMeta> {
  const run = await db.collection('dag_runs').findOne(
    { _id: new ObjectId(runId) },
    { projection: { conf: 1, created_at: 1, logical_date: 1, data_interval_start: 1, data_interval_end: 1 } },
  )
  return {
    conf:                (run?.conf as Record<string, unknown>) ?? {},
    created_at:          asDate(run?.created_at) ?? new Date(),
    logical_date:        asDate(run?.logical_date),
    data_interval_start: asDate(run?.data_interval_start),
    data_interval_end:   asDate(run?.data_interval_end),
  }
}

/** Run fields every task function sees on its ctx (worker, Kubernetes worker-cli, deferred triggers). */
export interface RunContextFields {
  conf: Record<string, unknown>
  /** Same object as conf — Airflow `params` (param defaults are merged into conf at trigger time). */
  params: Record<string, unknown>
  logicalDate: Date | null
  dataIntervalStart: Date | null
  dataIntervalEnd: Date | null
  /** Logical date (or creation time when there is none) as YYYY-MM-DD. */
  ds: string
}

export async function getRunContextFields(db: Db, runId: string): Promise<RunContextFields> {
  const meta = await getRunMeta(db, runId)
  return {
    conf: meta.conf,
    params: meta.conf,
    logicalDate: meta.logical_date,
    dataIntervalStart: meta.data_interval_start,
    dataIntervalEnd: meta.data_interval_end,
    ds: (meta.logical_date ?? meta.created_at).toISOString().slice(0, 10),
  }
}
