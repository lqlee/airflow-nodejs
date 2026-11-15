/**
 * Short-lived claim locks — the counterpart of Airflow's pool row lock in the scheduler's critical section.
 *
 * Limits that span runs (pool slots, a task's maxActiveTis) are checked by counting running instances and
 * then claiming. Two replicas doing that at once could both see the last free slot. A claimer therefore
 * takes the lock for the limit first (`pool:<name>`, `task:<dag_id>:<task_id>`), counts, claims, and
 * releases it. Locks are try-only: a busy lock means "skip this instance for now", so claimers never wait
 * on each other and cannot deadlock. A crashed holder's lock simply expires.
 *
 * One document per key in `claim_locks`: { _id: key, holder, expires_at }.
 */
import type { Db } from 'mongodb'
import { INSTANCE_ID } from './instance.js'

const COLLECTION = 'claim_locks'
export const CLAIM_LOCK_TTL_MS = 15_000

type LockDoc = { _id: string; holder: string; expires_at: Date }

let seq = 0

/** Take the lock if it is free or expired. Returns a release token, or null when someone else holds it. */
export async function tryLock(db: Db, key: string, ttlMs = CLAIM_LOCK_TTL_MS, now: Date = new Date()): Promise<string | null> {
  // A token per acquisition (not per process): two overlapping ticks in one process must exclude each other too
  const token = `${INSTANCE_ID}#${++seq}`
  try {
    const doc = await db.collection<LockDoc>(COLLECTION).findOneAndUpdate(
      { _id: key, expires_at: { $lt: now } },
      { $set: { holder: token, expires_at: new Date(now.getTime() + ttlMs) } },
      { upsert: true, returnDocument: 'after' },
    )
    return doc?.holder === token ? token : null
  } catch (err) {
    if ((err as { code?: number }).code !== 11000) throw err
    // A live lock exists and the upsert collided with it — unless that row is our own write, committed
    // by an attempt the driver then retried (Spanner can report a commit it did apply as failed)
    const doc = await db.collection<LockDoc>(COLLECTION).findOne({ _id: key })
    return doc?.holder === token ? token : null
  }
}

export async function unlock(db: Db, key: string, token: string): Promise<void> {
  await db.collection<LockDoc>(COLLECTION).deleteOne({ _id: key, holder: token })
}
