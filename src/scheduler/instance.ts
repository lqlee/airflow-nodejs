/**
 * Process liveness — every scheduler/worker process registers itself in `scheduler_instances` and
 * refreshes `heartbeat_at` on a timer. Task instances record the process that claimed them
 * (`claimed_by`), so a task whose owner stopped heartbeating is a zombie (see zombies.ts).
 *
 * The same loop renews the scheduler leader lease (lease.ts) when asked to.
 *
 * Env:
 *   HEARTBEAT_INTERVAL_MS   how often to heartbeat / renew the lease   (default 10s)
 *   ZOMBIE_THRESHOLD_MS     heartbeat age after which an owner is dead (default 60s, ≥ 3× interval)
 *   SCHEDULER_LEASE_TTL_MS  leader lease lifetime                      (default 30s, ≥ 2× interval)
 */
import { hostname } from 'node:os'
import { randomBytes } from 'node:crypto'
import type { Db } from 'mongodb'
import { renewLease, releaseLease, setElectionActive } from './lease.js'

/** Unique per process start — never reused, so a restarted pod is a new owner. */
export const INSTANCE_ID = `${hostname()}-${process.pid}-${randomBytes(3).toString('hex')}`

const COLLECTION = 'scheduler_instances'

function envMs(name: string, fallback: number): number {
  const n = Number(process.env[name])
  return Number.isFinite(n) && n > 0 ? n : fallback
}

export const HEARTBEAT_INTERVAL_MS = envMs('HEARTBEAT_INTERVAL_MS', 10_000)
export const ZOMBIE_THRESHOLD_MS = Math.max(envMs('ZOMBIE_THRESHOLD_MS', 60_000), 3 * HEARTBEAT_INTERVAL_MS)
export const LEASE_TTL_MS = Math.max(envMs('SCHEDULER_LEASE_TTL_MS', 30_000), 2 * HEARTBEAT_INTERVAL_MS)

export interface InstanceDoc {
  _id: string
  hostname: string
  pid: number
  role: 'scheduler' | 'worker'
  started_at: Date
  heartbeat_at: Date
}

export async function heartbeat(db: Db, role: InstanceDoc['role'], now: Date = new Date()): Promise<void> {
  await db.collection<InstanceDoc>(COLLECTION).updateOne(
    { _id: INSTANCE_ID },
    { $set: { heartbeat_at: now }, $setOnInsert: { hostname: hostname(), pid: process.pid, role, started_at: now } },
    { upsert: true },
  )
}

/** Ids of processes that heartbeated at or after `since`. */
export async function listLiveInstanceIds(db: Db, since: Date): Promise<string[]> {
  const docs = await db.collection<InstanceDoc>(COLLECTION).find({ heartbeat_at: { $gte: since } }, { projection: { _id: 1 } }).toArray()
  return docs.map(d => d._id)
}

export async function listInstances(db: Db): Promise<InstanceDoc[]> {
  return db.collection<InstanceDoc>(COLLECTION).find({}).sort({ heartbeat_at: -1 }).toArray()
}

/** Forget processes that have been silent far longer than any zombie check needs. */
export async function pruneDeadInstances(db: Db, olderThan: Date): Promise<number> {
  const res = await db.collection<InstanceDoc>(COLLECTION).deleteMany({ heartbeat_at: { $lt: olderThan } })
  return res.deletedCount
}

export async function deregisterInstance(db: Db): Promise<void> {
  await db.collection<InstanceDoc>(COLLECTION).deleteOne({ _id: INSTANCE_ID })
}

// ── liveness loop ─────────────────────────────────────────────────────────────

let timer: ReturnType<typeof setInterval> | null = null
let leaseEnabled = false

/**
 * Heartbeat now, then every HEARTBEAT_INTERVAL_MS. With `lease: true` the loop also takes/renews the
 * leader lease. Resolves after the first beat so the caller knows whether it leads before its first tick.
 * Errors are logged, never thrown — a DB blip must not crash the process (a lost lease just stops leader work).
 */
export async function startLiveness(db: Db, opts: { lease: boolean; role?: InstanceDoc['role'] }): Promise<void> {
  stopLiveness()
  leaseEnabled = opts.lease
  if (opts.lease) setElectionActive(true)
  const role = opts.role ?? (opts.lease ? 'scheduler' : 'worker')
  const beat = async () => {
    try { await heartbeat(db, role) } catch (err) { console.error('[liveness] heartbeat failed:', (err as Error).message) }
    if (!leaseEnabled) return
    try { await renewLease(db, INSTANCE_ID, LEASE_TTL_MS) } catch (err) { console.error('[liveness] lease renewal failed:', (err as Error).message) }
  }
  timer = setInterval(() => void beat(), HEARTBEAT_INTERVAL_MS)
  timer.unref()
  await beat()
}

/** Stop the timer (synchronous — safe in stopScheduler). Leadership lapses at the next isLeader() check. */
export function stopLiveness(): void {
  if (timer) clearInterval(timer)
  timer = null
  if (leaseEnabled) setElectionActive(false)
  leaseEnabled = false
}

/** Graceful exit: give up the lease (fast failover) and remove this process from the registry. */
export async function shutdownLiveness(db: Db): Promise<void> {
  stopLiveness()
  await releaseLease(db, INSTANCE_ID).catch(err => console.warn('[liveness] lease release failed:', (err as Error).message))
  await deregisterInstance(db).catch(err => console.warn('[liveness] deregister failed:', (err as Error).message))
}
