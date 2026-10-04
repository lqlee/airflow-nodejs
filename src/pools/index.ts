/**
 * Resource Pools — named slot limits for task concurrency.
 *
 * A pool has N slots. Tasks that declare `pool: 'pool_name'` acquire one slot
 * before forking and release it when done — limiting how many tasks in that
 * pool run concurrently across the scheduler.
 *
 * Enforcement sits in executor.ts (local-fork mode only; BullMQ mode skips it,
 * same as the global MAX_WORKERS semaphore). Each acquire is per-task-instance;
 * a task may occupy several slots via `poolSlots` (default 1).
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

/** Slots currently held per pool name (sum of held task slot costs). */
const poolActive = new Map<string, number>()
/** Last-seen pool size per pool name — used when draining waiters on release. */
const poolCapacity = new Map<string, number>()
/** FIFO waiters per pool name, each wanting `slots` slots. */
const poolQueue = new Map<string, Array<{ slots: number; resolve: () => void }>>()

/**
 * Acquire `slots` slots (default 1) in the named pool.
 * Reads pool size from the DB — always fresh, honours runtime PATCH.
 * A request larger than the pool is clamped to the pool size (it could never be granted).
 * Returns the number of slots actually held — pass it to releasePool. Returns 0 for an
 * unknown pool (fall-through, nothing held).
 */
export async function acquirePool(db: Db, poolName: string, slots = 1): Promise<number> {
  const doc = await db.collection<Pool>('pools').findOne({ name: poolName })
  if (!doc) {
    console.warn(`[pools] task references unknown pool '${poolName}' — running without pool limit`)
    return 0
  }

  poolCapacity.set(poolName, doc.slots)
  const want = Math.min(Math.max(1, Math.floor(slots)), doc.slots)
  const active = poolActive.get(poolName) ?? 0
  const queue = poolQueue.get(poolName)

  // Respect FIFO: don't jump ahead of tasks already waiting
  if (!queue?.length && active + want <= doc.slots) {
    poolActive.set(poolName, active + want)
    return want
  }

  // Pool full — wait. drainPool() grants the slots (and counts them) before resolving.
  await new Promise<void>((resolve) => {
    if (!poolQueue.has(poolName)) poolQueue.set(poolName, [])
    poolQueue.get(poolName)!.push({ slots: want, resolve })
  })
  return want
}

/**
 * Release `slots` slots (default 1) in the named pool, then grant waiting tasks in
 * strict FIFO order while they fit. No-op for unknown pools / zero slots.
 */
export function releasePool(poolName: string, slots = 1): void {
  const held = poolActive.get(poolName) ?? 0
  if (held > 0) poolActive.set(poolName, Math.max(0, held - Math.max(0, slots)))
  drainPool(poolName)
}

/** Grant queued waiters in order until the head no longer fits (no starvation of big tasks). */
function drainPool(poolName: string): void {
  const queue = poolQueue.get(poolName)
  const capacity = poolCapacity.get(poolName)
  if (!queue || capacity === undefined) return
  while (queue.length > 0) {
    const active = poolActive.get(poolName) ?? 0
    const head = queue[0]
    if (active + head.slots > capacity) break
    queue.shift()
    poolActive.set(poolName, active + head.slots)
    head.resolve()
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
  poolCapacity.clear()
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
