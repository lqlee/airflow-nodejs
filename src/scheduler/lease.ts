/**
 * Scheduler leader lease — with several replicas, exactly one runs the "create work" loops
 * (cron, catch-up, timetables, deferred polling, SLA, datasets, zombie reaping, retention).
 * Every replica still claims and executes tasks: claims are atomic per task instance.
 *
 * One document in `scheduler_leases`: { _id: 'scheduler', holder, expires_at }. The holder renews it
 * before it expires; anyone may take it over once expired. Renewal runs on the liveness loop
 * (instance.ts), never on the scheduler tick — a tick can last as long as the tasks it waits on.
 *
 * Until election starts (startLiveness with lease) the process behaves as a standalone scheduler:
 * isLeader() is true. That keeps library/test use of the tick helpers unchanged.
 */
import type { Db } from 'mongodb'

const LEASE_ID = 'scheduler'
const COLLECTION = 'scheduler_leases'

let electionActive = false
/** performance.now() deadline until which this process may act as leader. */
let leaderUntil = 0

export function isLeader(): boolean {
  return !electionActive || performance.now() < leaderUntil
}

export function setElectionActive(active: boolean): void {
  electionActive = active
  if (!active) leaderUntil = 0
}

/**
 * Take or renew the lease. Returns true when `holder` is the leader afterwards.
 * The local deadline is measured from BEFORE the write and shortened by a safety margin, so clock
 * skew between replicas (expires_at is compared on another replica's clock) cannot let two leaders overlap.
 */
export async function renewLease(db: Db, holder: string, ttlMs: number, now: Date = new Date()): Promise<boolean> {
  const startedAt = performance.now()
  let won = false
  try {
    const doc = await db.collection<{ _id: string; holder: string; expires_at: Date }>(COLLECTION).findOneAndUpdate(
      { _id: LEASE_ID, $or: [{ holder }, { expires_at: { $lt: now } }] },
      { $set: { holder, expires_at: new Date(now.getTime() + ttlMs), renewed_at: now } },
      { upsert: true, returnDocument: 'after' },
    )
    won = doc?.holder === holder
  } catch (err) {
    // Someone else holds a live lease: our upsert collided with their document
    if ((err as { code?: number }).code !== 11000) { leaderUntil = 0; throw err }
  }
  const wasLeader = performance.now() < leaderUntil
  leaderUntil = won ? startedAt + ttlMs - safetyMargin(ttlMs) : 0
  if (won && !wasLeader) console.log(`[lease] ${holder} is now the scheduler leader`)
  if (!won && wasLeader) console.warn(`[lease] ${holder} lost scheduler leadership`)
  return won
}

/** Give the lease up (graceful shutdown) so another replica takes over without waiting for expiry. */
export async function releaseLease(db: Db, holder: string): Promise<void> {
  leaderUntil = 0
  await db.collection<{ _id: string }>(COLLECTION).deleteOne({ _id: LEASE_ID, holder })
}

export async function getLeaseHolder(db: Db, now: Date = new Date()): Promise<string | null> {
  const doc = await db.collection<{ _id: string; holder: string; expires_at: Date }>(COLLECTION).findOne({ _id: LEASE_ID })
  return doc && new Date(doc.expires_at) > now ? doc.holder : null
}

function safetyMargin(ttlMs: number): number {
  return Math.min(5_000, Math.floor(ttlMs / 3))
}
