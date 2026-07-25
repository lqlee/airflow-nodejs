/**
 * Resource Pools — named slot limits for task concurrency.
 *
 * A pool has N slots. Tasks that declare `pool: 'pool_name'` acquire one slot
 * before forking and release it when done — limiting how many tasks in that
 * pool run concurrently across the scheduler.
 *
 * Enforcement sits in executor.ts (local-fork mode only; BullMQ mode skips it,
 * same as the global MAX_WORKERS semaphore). Each acquire is per-task-instance
 * — per-task slot cost is fixed at 1 (MVP; Airflow supports >1).
 *
 * Missing pool → fall through (global-only gating), warning logged once.
 */

import type { Db } from 'mongodb'

export interface Pool {
  name: string        // unique identifier, referenced by task.pool
  slots: number       // max concurrent tasks (>= 1)
  description: string
  created_at: Date
  updated_at: Date
}

export interface PoolSummary {
  name: string
  slots: number
  description: string
  open_slots: number    // slots - active_task_count (derived)
  occupied_slots: number
  created_at: Date
  updated_at: Date
}

// ── In-memory per-pool semaphore ───────────────────────────────────────────

/** Active (acquired) count per pool name. */
const poolActive = new Map<string, number>()
/** Waiters per pool name. */
const poolQueue = new Map<string, Array<() => void>>()

/**
 * Acquire one slot in the named pool.
 * Reads `slots` from the DB — always fresh, honours runtime PATCH.
 * Returns immediately if the pool is not in the DB (fall-through).
 */
export async function acquirePool(db: Db, poolName: string): Promise<void> {
  const doc = await db.collection<Pool>('pools').findOne({ name: poolName })
  if (!doc) {
    console.warn(`[pools] task references unknown pool '${poolName}' — running without pool limit`)
    return
  }

  const slots = doc.slots
  const active = poolActive.get(poolName) ?? 0

  if (active < slots) {
    poolActive.set(poolName, active + 1)
    return
  }

  // Pool full — wait
  return new Promise<void>((resolve) => {
    if (!poolQueue.has(poolName)) poolQueue.set(poolName, [])
    poolQueue.get(poolName)!.push(resolve)
  })
}

/**
 * Release one slot in the named pool, unblocking the next waiter if any.
 * No-op for unknown pools (consistent with fall-through on acquire).
 */
export function releasePool(poolName: string): void {
  const waiters = poolQueue.get(poolName) ?? []
  const next = waiters.shift()
  if (next) {
    // Hand slot directly to next waiter; active count stays the same
    next()
  } else {
    const active = poolActive.get(poolName) ?? 0
    if (active > 0) poolActive.set(poolName, active - 1)
  }
}

/** Active slot count for a pool (used in PoolSummary and tests). */
export function poolActiveCount(poolName: string): number {
  return poolActive.get(poolName) ?? 0
}

/** Queue depth for a pool (waiting tasks). */
export function poolQueueDepth(poolName: string): number {
  return poolQueue.get(poolName)?.length ?? 0
}

/** Reset all per-pool state — test helper only. */
export function resetAllPools(): void {
  poolActive.clear()
  poolQueue.clear()
}

// ── DB CRUD ────────────────────────────────────────────────────────────────

export async function listPools(db: Db): Promise<PoolSummary[]> {
  const docs = await db.collection<Pool>('pools').find({}).sort({ name: 1 }).toArray()
  return docs.map(p => ({
    name: p.name,
    slots: p.slots,
    description: p.description,
    open_slots: Math.max(0, p.slots - (poolActive.get(p.name) ?? 0)),
    occupied_slots: poolActive.get(p.name) ?? 0,
    created_at: p.created_at,
    updated_at: p.updated_at,
  }))
}

export async function getPool(db: Db, name: string): Promise<PoolSummary | null> {
  const p = await db.collection<Pool>('pools').findOne({ name })
  if (!p) return null
  return {
    name: p.name,
    slots: p.slots,
    description: p.description,
    open_slots: Math.max(0, p.slots - (poolActive.get(p.name) ?? 0)),
    occupied_slots: poolActive.get(p.name) ?? 0,
    created_at: p.created_at,
    updated_at: p.updated_at,
  }
}

export async function createPool(
  db: Db,
  name: string,
  slots: number,
  description = '',
): Promise<PoolSummary> {
  const now = new Date()
  await db.collection<Pool>('pools').insertOne({ name, slots, description, created_at: now, updated_at: now })
  return { name, slots, description, open_slots: slots, occupied_slots: 0, created_at: now, updated_at: now }
}

export async function updatePool(
  db: Db,
  name: string,
  patch: { slots?: number; description?: string },
): Promise<PoolSummary | null> {
  const update: Record<string, unknown> = { updated_at: new Date() }
  if (patch.slots !== undefined) update['slots'] = patch.slots
  if (patch.description !== undefined) update['description'] = patch.description

  const result = await db.collection<Pool>('pools').findOneAndUpdate(
    { name },
    { $set: update },
    { returnDocument: 'after' },
  )
  if (!result) return null
  return getPool(db, name)
}

export async function deletePool(db: Db, name: string): Promise<boolean> {
  const result = await db.collection('pools').deleteOne({ name })
  return result.deletedCount > 0
}
