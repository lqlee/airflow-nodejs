/**
 * Pause/resume state for Dags — persisted in MongoDB `dag_paused` collection.
 *
 * A paused Dag will not fire new cron runs. Manual triggers are still allowed
 * so operators can test individual runs while the schedule is frozen.
 */

import type { Db } from 'mongodb'
import { recordEvent } from '../events/index.js'

const COLLECTION = 'dag_paused'

export async function pauseDag(db: Db, dagId: string): Promise<void> {
  await db.collection(COLLECTION).updateOne(
    { dag_id: dagId },
    { $set: { dag_id: dagId, paused: true, updated_at: new Date() } },
    { upsert: true }
  )
  void recordEvent(db, 'dag_paused', { dag_id: dagId })
}

export async function resumeDag(db: Db, dagId: string): Promise<void> {
  await db.collection(COLLECTION).updateOne(
    { dag_id: dagId },
    { $set: { dag_id: dagId, paused: false, updated_at: new Date() } },
    { upsert: true }
  )
  void recordEvent(db, 'dag_resumed', { dag_id: dagId })
}

export async function isDagPaused(db: Db, dagId: string): Promise<boolean> {
  const doc = await db.collection(COLLECTION).findOne({ dag_id: dagId })
  return doc?.paused === true
}

/** Returns a Set of all currently paused dag IDs. */
export async function getPausedDagIds(db: Db): Promise<Set<string>> {
  const docs = await db.collection(COLLECTION).find({ paused: true }).toArray()
  return new Set(docs.map(d => d.dag_id as string))
}
