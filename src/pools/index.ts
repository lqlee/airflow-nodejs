/**
 * Resource Pools — named slot limits for task concurrency.
 *
 * A pool has N slots. Tasks that declare `pool: 'pool_name'` take `poolSlots` slots (default 1)
 * while running, limiting how many tasks in that pool run at once.
 *
 * Enforced when a task is claimed (scheduler/claim-limits.ts): occupancy is the sum of the slots of
 * the pool's running task instances in the DB, so the limit holds across scheduler replicas and in
 * BullMQ / Kubernetes-executor mode. Deferred and up_for_retry instances hold no slots.
 *
 * Missing pool → fall through (no pool limit), warning logged once.
 */

import type { Db } from 'mongodb'
import { slotsFor } from '../scheduler/claim-limits.js'

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
  open_slots: number      // slots - occupied_slots
  occupied_slots: number  // slots held by running task instances
  queued_slots: number    // slots wanted by instances waiting to run (queued / up_for_retry / up_for_reschedule)
  created_at: Date
  updated_at: Date
}

const WAITING = ['queued', 'up_for_retry', 'up_for_reschedule']

/** Occupied (running) and queued slots per pool name, from the task instances in the DB. */
export async function getPoolUsage(db: Db, pools: Pool[]): Promise<Map<string, { occupied: number; queued: number }>> {
  const usage = new Map(pools.map(p => [p.name, { occupied: 0, queued: 0 }]))
  if (pools.length === 0) return usage
  const capacity = new Map(pools.map(p => [p.name, Math.max(1, p.slots)]))
  const rows = await db.collection('task_instances').find(
    { pool: { $in: pools.map(p => p.name) }, state: { $in: ['running', ...WAITING] } },
    { projection: { pool: 1, pool_slots: 1, state: 1 } },
  ).toArray()
  for (const r of rows) {
    const u = usage.get(r.pool as string)
    if (!u) continue
    const n = slotsFor(r as { pool_slots?: number }, capacity.get(r.pool as string)!)
    if (r.state === 'running') u.occupied += n
    else u.queued += n
  }
  return usage
}

function summarize(p: Pool, u: { occupied: number; queued: number } | undefined): PoolSummary {
  const occupied = u?.occupied ?? 0
  return {
    name: p.name,
    slots: p.slots,
    description: p.description,
    open_slots: Math.max(0, p.slots - occupied),
    occupied_slots: occupied,
    queued_slots: u?.queued ?? 0,
    created_at: p.created_at,
    updated_at: p.updated_at,
  }
}

// ── DB CRUD ────────────────────────────────────────────────────────────────

export async function listPools(db: Db): Promise<PoolSummary[]> {
  const docs = await db.collection<Pool>('pools').find({}).sort({ name: 1 }).toArray()
  const usage = await getPoolUsage(db, docs)
  return docs.map(p => summarize(p, usage.get(p.name)))
}

export async function getPool(db: Db, name: string): Promise<PoolSummary | null> {
  const p = await db.collection<Pool>('pools').findOne({ name })
  if (!p) return null
  return summarize(p, (await getPoolUsage(db, [p])).get(p.name))
}

export async function createPool(
  db: Db,
  name: string,
  slots: number,
  description = '',
): Promise<PoolSummary> {
  const now = new Date()
  await db.collection<Pool>('pools').insertOne({ name, slots, description, created_at: now, updated_at: now })
  return getPool(db, name) as Promise<PoolSummary>
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
