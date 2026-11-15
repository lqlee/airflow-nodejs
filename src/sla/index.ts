/**
 * SLA alerts — a Dag's `sla` (ms) is a deadline alert alias (src/deadlines): a run that has not
 * succeeded `sla` ms after it was queued gets one record in `sla_alerts` (unique per run).
 * Alerts can be acknowledged via POST /sla-alerts/:id/ack.
 */

import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
import { checkDeadlines } from '../deadlines/index.js'

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
 * The SLA is a deadline alias (src/deadlines): checking deadlines records SLA breaches in `sla_alerts`,
 * one per run. Kept for callers of the old API.
 */
export async function checkSlaBreaches(db: Db, dags: DagDefinition[]): Promise<void> {
  await checkDeadlines(db, dags)
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
