/**
 * SLA alerting — fires when a dag_run exceeds its Dag's sla (ms) deadline.
 *
 * Design:
 *  - Each scheduler tick calls checkSlaBreaches(db, dags)
 *  - Runs that have been queued/running longer than sla_ms get a record in `sla_alerts`
 *  - Each (dag_run_id) fires at most once — deduplicated by upsert
 *  - Alerts can be acknowledged via POST /sla-alerts/:id/ack
 */

import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'

export interface SlaAlert {
  dag_id: string
  dag_run_id: string
  sla_ms: number
  elapsed_ms: number
  fired_at: Date
  acked: boolean
  acked_at: Date | null
}

/**
 * Scan all active runs against their Dag's SLA. Insert one alert per breach,
 * deduplicated — subsequent ticks will not insert duplicate alerts.
 */
export async function checkSlaBreaches(db: Db, dags: DagDefinition[]): Promise<void> {
  // Build map of dagId → sla_ms for dags that have an SLA configured
  const slaMap = new Map<string, number>()
  for (const dag of dags) {
    if (dag.sla && dag.sla > 0) slaMap.set(dag.id, dag.sla)
  }
  if (slaMap.size === 0) return

  const now = new Date()

  // Find all non-terminal runs for dags that have an SLA
  const activeRuns = await db
    .collection('dag_runs')
    .find({
      state: { $in: ['queued', 'running'] },
      dag_id: { $in: [...slaMap.keys()] },
    })
    .toArray()

  for (const run of activeRuns) {
    const sla = slaMap.get(run.dag_id)
    if (!sla) continue

    const elapsed = now.getTime() - new Date(run.created_at).getTime()
    if (elapsed < sla) continue  // within deadline

    const runId = run._id.toString()

    // Upsert — only insert if no alert already exists for this run
    const existing = await db.collection('sla_alerts').findOne({ dag_run_id: runId })
    if (existing) continue

    const alert: SlaAlert = {
      dag_id: run.dag_id,
      dag_run_id: runId,
      sla_ms: sla,
      elapsed_ms: elapsed,
      fired_at: now,
      acked: false,
      acked_at: null,
    }
    await db.collection<SlaAlert>('sla_alerts').insertOne(alert)
    console.warn(`[sla] ⚠️  dag '${run.dag_id}' run ${runId} breached SLA of ${sla}ms (elapsed: ${elapsed}ms)`)
  }
}

/** Return all alerts, most recent first. */
export async function listAlerts(db: Db, opts: { unackedOnly?: boolean } = {}): Promise<SlaAlert[]> {
  const filter = opts.unackedOnly ? { acked: false } : {}
  return db
    .collection<SlaAlert>('sla_alerts')
    .find(filter)
    .sort({ fired_at: -1 })
    .limit(100)
    .toArray()
}

/** Acknowledge an alert by its string id. Returns false if not found. */
export async function ackAlert(db: Db, alertId: string): Promise<boolean> {
  const { ObjectId } = await import('mongodb')
  if (!ObjectId.isValid(alertId)) return false
  const result = await db.collection('sla_alerts').updateOne(
    { _id: new ObjectId(alertId) },
    { $set: { acked: true, acked_at: new Date() } }
  )
  return result.matchedCount > 0
}
