/**
 * Retention purge — delete finished Dag runs older than a cutoff, with everything hanging off them
 * (task instances, tries, XComs, DB logs, SLA alerts, audit events, log files), plus old audit events.
 *
 * Deletes go in bounded batches (find a page of ids → delete by id) so no single statement or
 * Spanner transaction grows with the table — same idea as Airflow's `airflow db clean`.
 *
 * Runs automatically on the scheduler leader when RETENTION_DAYS > 0 (at most once per
 * RETENTION_INTERVAL_MS, default 1h), or on demand: `npm run db:cleanup -- --days 30 [--dry-run]`.
 */
import { rm } from 'node:fs/promises'
import { dirname } from 'node:path'
import type { Db, Filter, Document } from 'mongodb'
import { getLogBackendName, getLogFilePath } from '../logs/index.js'

const TERMINAL_RUN_STATES = ['success', 'failed', 'cancelled']
/** Collections whose rows belong to a run through `dag_run_id`. */
const RUN_CHILDREN = ['task_instances', 'task_instance_tries', 'xcoms', 'task_logs', 'sla_alerts', 'deadlines', 'event_logs'] as const

export interface PurgeResult {
  dryRun: boolean
  olderThan: Date
  runs: number
  /** Rows deleted (or that would be) per collection, run children and old event_logs included. */
  rows: Record<string, number>
  logDirs: number
}

export interface PurgeOptions {
  olderThan: Date
  /** Runs per batch (default 100). Child rows are deleted in pages of 1000. */
  batchSize?: number
  dryRun?: boolean
}

export async function purgeOldRuns(db: Db, opts: PurgeOptions): Promise<PurgeResult> {
  const batchSize = Math.max(1, Math.floor(opts.batchSize ?? 100))
  const dryRun = opts.dryRun === true
  const result: PurgeResult = { dryRun, olderThan: opts.olderThan, runs: 0, rows: {}, logDirs: 0 }
  const add = (coll: string, n: number) => { result.rows[coll] = (result.rows[coll] ?? 0) + n }
  const runFilter = { state: { $in: TERMINAL_RUN_STATES }, created_at: { $lt: opts.olderThan } }
  // gcs: only leftover staged files here — the objects age out by a bucket lifecycle rule
  const fileLogs = getLogBackendName() === 'file' || getLogBackendName() === 'gcs'

  for (let offset = 0; ; offset += dryRun ? batchSize : 0) {
    // Deleting shrinks the result set, so a real purge always reads the first page; a dry run pages through
    const runs = await db.collection('dag_runs').find(runFilter, { projection: { _id: 1, dag_id: 1 } })
      .sort({ created_at: 1 }).skip(offset).limit(batchSize).toArray()
    if (runs.length === 0) break
    const runIds = runs.map(r => r._id.toString())

    for (const coll of RUN_CHILDREN) {
      add(coll, await deleteInPages(db, coll, { dag_run_id: { $in: runIds } }, dryRun))
    }
    add('dag_runs', await deleteInPages(db, 'dag_runs', { _id: { $in: runs.map(r => r._id) } }, dryRun))
    result.runs += runs.length

    if (fileLogs) {
      for (const r of runs) {
        const dir = dirname(getLogFilePath(String(r.dag_id), r._id.toString(), '_'))
        if (!dryRun) await rm(dir, { recursive: true, force: true })
        result.logDirs++
      }
    }
    if (runs.length < batchSize) break
  }

  // Audit events not tied to a run (pause/resume, ...) age out on the same cutoff; run events
  // live exactly as long as their run
  add('event_logs', await deleteInPages(db, 'event_logs', { dag_run_id: null, created_at: { $lt: opts.olderThan } }, dryRun))
  return result
}

/** Delete (or count) matching rows a page of ids at a time. */
async function deleteInPages(db: Db, coll: string, filter: Filter<Document>, dryRun: boolean, pageSize = 1000): Promise<number> {
  if (dryRun) return db.collection(coll).countDocuments(filter)
  let total = 0
  for (;;) {
    const page = await db.collection(coll).find(filter, { projection: { _id: 1 } }).limit(pageSize).toArray()
    if (page.length === 0) return total
    const res = await db.collection(coll).deleteMany({ _id: { $in: page.map(d => d._id) } })
    total += res.deletedCount
    if (page.length < pageSize) return total
  }
}

// ── scheduled purge ──────────────────────────────────────────────────────────

const DAY_MS = 86_400_000
let lastPurgeAt = 0

/** RETENTION_DAYS (0/unset = keep forever). */
export function retentionDays(env: NodeJS.ProcessEnv = process.env): number {
  const n = Number(env.RETENTION_DAYS)
  return Number.isFinite(n) && n > 0 ? n : 0
}

/** Called from the leader's tick; purges at most once per RETENTION_INTERVAL_MS. Never throws. */
export async function maybePurge(db: Db, now: Date = new Date()): Promise<PurgeResult | null> {
  const days = retentionDays()
  if (days === 0) return null
  const intervalMs = Number(process.env.RETENTION_INTERVAL_MS) > 0 ? Number(process.env.RETENTION_INTERVAL_MS) : 3_600_000
  if (now.getTime() - lastPurgeAt < intervalMs) return null
  lastPurgeAt = now.getTime()
  try {
    const res = await purgeOldRuns(db, { olderThan: new Date(now.getTime() - days * DAY_MS) })
    if (res.runs > 0 || (res.rows.event_logs ?? 0) > 0) {
      console.log(`[retention] purged ${res.runs} run(s) older than ${days}d — rows: ${JSON.stringify(res.rows)}`)
    }
    return res
  } catch (err) {
    console.error('[retention] purge failed:', err)
    return null
  }
}

/** Test helper. */
export function resetPurgeState(): void {
  lastPurgeAt = 0
}
