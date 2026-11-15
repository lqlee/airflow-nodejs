/**
 * Deadline alerts (Airflow 3 `DeadlineAlert`): "this run should have succeeded by <reference> + <interval>".
 *
 * A deadline is computed once, when the run is created (queued_at deadlines again when it is re-queued by
 * a clear), and stored in `deadlines`. Each scheduler tick marks the ones whose time has passed as missed
 * — a compare-and-set, so one replica handles each miss — records a `deadline_missed` event and fires the
 * alert's callbacks. A run that succeeds drops its pending deadlines; a failed run keeps them (it never
 * succeeded in time), as in Airflow. A cancelled run drops them too.
 *
 * The Dag-level `sla` (ms) is an alias for `{ name: 'sla', reference: 'queued_at', interval: sla }`; its
 * misses are also written to `sla_alerts`, which the SLA API, UI banner and metric read.
 */
import { ObjectId, type Db } from 'mongodb'
import type { Callbacks, DagDefinition } from '../dag/types.js'
import { notifyAll } from '../scheduler/callbacks.js'
import { recordEvent } from '../events/index.js'
import type { SlaAlert } from '../sla/index.js'

export type DeadlineReference =
  /** The run's logical date. Runs without one (manual, dataset) get no deadline. */
  | 'logical_date'
  /** When the run was queued (created, or re-queued by a clear). */
  | 'queued_at'
  /** A fixed point in time. */
  | { fixed: string | Date }
  /** Now + the average duration of the Dag's last `maxRuns` (default 10) finished runs; none until `minRuns` exist. */
  | { averageRuntime: { maxRuns?: number; minRuns?: number } }

export interface DeadlineMissInfo {
  dagId: string
  runId: string
  /** The alert's name (default `deadline_<index>`; `sla` for the sla alias). */
  name: string
  reference: string
  deadlineTime: Date
  /** The run's state when the miss was detected. */
  runState: string
  logicalDate: Date | null
}

export interface DeadlineAlert {
  name?: string
  reference: DeadlineReference
  /** ms added to the reference time (may be negative for a fixed time). */
  interval: number
  /** Function, `{ notify }` object, webhook URL, or a list of them. */
  callback?: Callbacks<DeadlineMissInfo>
}

export interface DeadlineDoc {
  dag_id: string
  dag_run_id: string
  name: string
  reference: string
  deadline_time: Date
  missed: boolean
  missed_at: Date | null
  created_at: Date
}

const COLLECTION = 'deadlines'
export const SLA_ALERT_NAME = 'sla'

type RunRef = { _id: ObjectId; dag_id: string; logical_date?: Date | null; queued_at?: Date | null; created_at: Date }

/** The Dag's alerts, the `sla` alias included, each with its effective name. */
export function deadlineAlertsOf(dag: DagDefinition): Array<DeadlineAlert & { name: string }> {
  const list = dag.deadline === undefined ? [] : Array.isArray(dag.deadline) ? dag.deadline : [dag.deadline]
  const alerts = list.map((a, i) => ({ ...a, name: a.name ?? `deadline_${i}` }))
  if (dag.sla && dag.sla > 0) alerts.push({ name: SLA_ALERT_NAME, reference: 'queued_at', interval: dag.sla })
  return alerts
}

export function referenceLabel(ref: DeadlineReference): string {
  if (typeof ref === 'string') return ref
  return 'fixed' in ref ? 'fixed' : 'average_runtime'
}

/** Mean duration (ms) of the Dag's last `maxRuns` finished runs, or null with fewer than `minRuns`. */
async function averageRuntime(db: Db, dagId: string, maxRuns = 10, minRuns = maxRuns): Promise<number | null> {
  const runs = await db.collection('dag_runs')
    .find({ dag_id: dagId, state: { $in: ['success', 'failed'] }, started_at: { $ne: null }, ended_at: { $ne: null } })
    .sort({ ordering_date: -1 })
    .limit(Math.max(1, maxRuns))
    .toArray()
  const durations = runs
    .map(r => new Date(r.ended_at).getTime() - new Date(r.started_at).getTime())
    .filter(d => Number.isFinite(d) && d >= 0)
  if (durations.length === 0 || durations.length < Math.max(1, minRuns)) return null
  return durations.reduce((a, b) => a + b, 0) / durations.length
}

/** Deadline time for one alert, or null when the reference has no value for this run. */
export async function computeDeadline(db: Db, alert: DeadlineAlert, run: RunRef, now: Date): Promise<Date | null> {
  const ref = alert.reference
  let base: Date | null
  if (ref === 'logical_date') base = run.logical_date ?? null
  else if (ref === 'queued_at') base = run.queued_at ?? run.created_at
  else if ('fixed' in ref) base = new Date(ref.fixed)
  else {
    const avg = await averageRuntime(db, run.dag_id, ref.averageRuntime.maxRuns, ref.averageRuntime.minRuns)
    base = avg === null ? null : new Date(now.getTime() + avg)
  }
  if (!base || Number.isNaN(base.getTime())) return null
  return new Date(base.getTime() + (alert.interval ?? 0))
}

/**
 * Store the run's deadlines (optionally only those of one reference — a re-queue recomputes queued_at).
 * Best effort per alert, like Airflow: one bad alert does not stop the others or the run.
 */
export async function createDeadlines(db: Db, dag: DagDefinition, run: RunRef, opts: { only?: 'queued_at'; now?: Date } = {}): Promise<number> {
  const now = opts.now ?? new Date()
  const docs: DeadlineDoc[] = []
  for (const alert of deadlineAlertsOf(dag)) {
    if (opts.only && referenceLabel(alert.reference) !== opts.only) continue
    try {
      const time = await computeDeadline(db, alert, run, now)
      if (!time) continue
      docs.push({
        dag_id: dag.id, dag_run_id: run._id.toString(), name: alert.name, reference: referenceLabel(alert.reference),
        deadline_time: time, missed: false, missed_at: null, created_at: now,
      })
    } catch (err) {
      console.warn(`[deadline] ${dag.id} '${alert.name}': ${err instanceof Error ? err.message : String(err)}`)
    }
  }
  if (docs.length > 0) await db.collection<DeadlineDoc>(COLLECTION).insertMany(docs)
  return docs.length
}

/** The run was re-queued (cleared): its queued_at deadlines start over. Missed ones stay as history. */
export async function requeueDeadlines(db: Db, dag: DagDefinition | undefined, runId: string, now: Date = new Date()): Promise<void> {
  await db.collection(COLLECTION).deleteMany({ dag_run_id: runId, missed: false, reference: 'queued_at' })
  if (!dag) return
  const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
  if (run) await createDeadlines(db, dag, run as unknown as RunRef, { only: 'queued_at', now })
}

/** Drop a run's pending deadlines (it succeeded or was cancelled). */
export async function pruneDeadlines(db: Db, runId: string): Promise<void> {
  await db.collection(COLLECTION).deleteMany({ dag_run_id: runId, missed: false })
}

/**
 * Active runs created before their Dag had deadlines (or before this version) get them now.
 * Runs created by createRun are stamped `deadlines_set`, so this is normally an empty query.
 */
async function backfillDeadlines(db: Db, dags: DagDefinition[], now: Date): Promise<void> {
  const withAlerts = new Map(dags.filter(d => deadlineAlertsOf(d).length > 0).map(d => [d.id, d]))
  if (withAlerts.size === 0) return
  const runs = await db.collection('dag_runs').find({
    state: { $in: ['queued', 'running'] }, dag_id: { $in: [...withAlerts.keys()] }, deadlines_set: { $ne: true },
  }).limit(500).toArray()
  for (const run of runs) {
    const claimed = await db.collection('dag_runs').updateOne({ _id: run._id, deadlines_set: { $ne: true } }, { $set: { deadlines_set: true } })
    if (claimed.modifiedCount === 0) continue
    await createDeadlines(db, withAlerts.get(run.dag_id)!, run as unknown as RunRef, { now })
  }
}

/**
 * Mark deadlines whose time has passed as missed and alert. Returns the in-flight callbacks (for tests).
 * `dags` supplies the callbacks; a miss of a Dag no longer loaded is still recorded.
 */
export async function checkDeadlines(db: Db, dags: DagDefinition[], now: Date = new Date()): Promise<Promise<void>[]> {
  await backfillDeadlines(db, dags, now)

  const due = await db.collection<DeadlineDoc>(COLLECTION)
    .find({ missed: false, deadline_time: { $lte: now } })
    .sort({ deadline_time: 1 })
    .limit(200)
    .toArray()
  const byId = new Map(dags.map(d => [d.id, d]))
  const fired: Promise<void>[] = []

  for (const d of due) {
    const won = await db.collection(COLLECTION).updateOne({ _id: d._id, missed: false }, { $set: { missed: true, missed_at: now } })
    if (won.modifiedCount === 0) continue   // another replica handled it

    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(d.dag_run_id) })
    if (!run) {   // the run was deleted (retention, tests): nothing to alert about
      await db.collection(COLLECTION).deleteOne({ _id: d._id })
      continue
    }
    const info: DeadlineMissInfo = {
      dagId: d.dag_id, runId: d.dag_run_id, name: d.name, reference: d.reference, deadlineTime: d.deadline_time,
      runState: run.state as string, logicalDate: (run.logical_date as Date) ?? null,
    }
    console.warn(`[deadline] ⚠️  ${d.dag_id} run ${d.dag_run_id} missed deadline '${d.name}' (${d.deadline_time.toISOString()}, run ${info.runState})`)
    void recordEvent(db, 'deadline_missed', {
      dag_id: d.dag_id, dag_run_id: d.dag_run_id,
      metadata: { name: d.name, reference: d.reference, deadline_time: d.deadline_time.toISOString(), run_state: info.runState },
    })

    const dag = byId.get(d.dag_id)
    if (d.name === SLA_ALERT_NAME && dag?.sla) await recordSlaAlert(db, d, run, dag.sla, now)
    const alert = dag ? deadlineAlertsOf(dag).find(a => a.name === d.name) : undefined
    if (alert?.callback) {
      fired.push(notifyAll(alert.callback, info, `deadline '${d.name}' ${d.dag_id}`, {
        event: 'deadline_missed', dag_id: info.dagId, run_id: info.runId, name: info.name, reference: info.reference,
        deadline_time: info.deadlineTime, run_state: info.runState, logical_date: info.logicalDate,
      }))
    }
  }
  return fired
}

async function recordSlaAlert(db: Db, d: DeadlineDoc & { _id: unknown }, run: Record<string, unknown> | null, slaMs: number, now: Date): Promise<void> {
  const since = (run?.queued_at ?? run?.created_at) as Date | undefined
  const alert: SlaAlert = {
    dag_id: d.dag_id,
    dag_run_id: d.dag_run_id,
    sla_ms: slaMs,
    elapsed_ms: since ? now.getTime() - new Date(since).getTime() : slaMs,
    fired_at: now,
    acked: false,
    acked_at: null,
  }
  // Unique per run: a second miss (re-queued run) keeps the first alert
  await db.collection<SlaAlert>('sla_alerts').insertOne(alert).catch((err: { code?: number }) => {
    if (err.code !== 11000) throw err
  })
}

export async function listDeadlines(
  db: Db, opts: { dagId?: string; runId?: string; missed?: boolean; limit?: number } = {},
): Promise<Array<DeadlineDoc & { _id: unknown }>> {
  const filter: Record<string, unknown> = {}
  if (opts.dagId) filter.dag_id = opts.dagId
  if (opts.runId) filter.dag_run_id = opts.runId
  if (opts.missed !== undefined) filter.missed = opts.missed
  return db.collection<DeadlineDoc>(COLLECTION).find(filter).sort({ deadline_time: -1 }).limit(Math.min(opts.limit ?? 100, 500)).toArray()
}
