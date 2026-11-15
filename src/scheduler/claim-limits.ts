/**
 * Admission control for one claim pass: pool slots, maxActiveTis and maxActiveTisPerRun.
 *
 * Limits are counted from the DB (running task instances), not from per-process memory, so they hold
 * across scheduler replicas and in BullMQ mode. The first time a pass needs a limit it takes the
 * limit's claim lock and loads the current count; later admissions in the same pass reuse it and
 * commit() bumps it. release() drops the locks once the pass has claimed what it can.
 *
 * Deferred and up_for_retry instances hold no slots (Airflow's default for pools).
 */
import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { tryLock, unlock } from './claim-locks.js'

interface PoolCount { capacity: number; used: number }
interface TaskCount { inDag: number; byRun: Map<string, number> }

type Candidate = Pick<TaskInstance, 'dag_id' | 'dag_run_id' | 'task_id' | 'pool' | 'pool_slots' | 'max_active_tis' | 'max_active_tis_per_run'>

const warnedPools = new Set<string>()

const poolKey = (name: string) => `pool:${name}`
const taskKey = (ti: Candidate) => `task:${ti.dag_id}:${ti.task_id}`
const hasTaskCap = (ti: Candidate) => (ti.max_active_tis ?? 0) > 0 || (ti.max_active_tis_per_run ?? 0) > 0

/** Slots an instance takes in a pool: poolSlots, clamped to the pool size (a bigger request could never run). */
export function slotsFor(ti: Pick<TaskInstance, 'pool_slots'>, capacity: number): number {
  return Math.min(Math.max(1, Math.floor(ti.pool_slots ?? 1)), capacity)
}

export class ClaimLimits {
  private readonly held = new Map<string, string>()   // lock key → token
  private readonly busy = new Set<string>()           // held by another claimer during this pass
  private readonly pools = new Map<string, PoolCount | null>()   // null: unknown pool, no limit
  private readonly tasks = new Map<string, TaskCount>()

  constructor(private readonly db: Db) {}

  /** True when `ti` fits every limit it is subject to right now. */
  async admit(ti: Candidate): Promise<boolean> {
    if (hasTaskCap(ti)) {
      const t = await this.taskCount(ti)
      if (!t) return false
      const perDag = ti.max_active_tis ?? 0
      if (perDag > 0 && t.inDag >= perDag) return false
      const perRun = ti.max_active_tis_per_run ?? 0
      if (perRun > 0 && (t.byRun.get(ti.dag_run_id) ?? 0) >= perRun) return false
    }
    if (ti.pool) {
      const p = await this.poolCount(ti.pool)
      if (p === undefined) return false
      if (p && (p.capacity <= 0 || p.used + slotsFor(ti, p.capacity) > p.capacity)) return false
    }
    return true
  }

  /** Count an instance this pass has just claimed. */
  commit(ti: Candidate): void {
    const t = this.tasks.get(taskKey(ti))
    if (t) {
      t.inDag++
      t.byRun.set(ti.dag_run_id, (t.byRun.get(ti.dag_run_id) ?? 0) + 1)
    }
    const p = ti.pool ? this.pools.get(ti.pool) : null
    if (p) p.used += slotsFor(ti, p.capacity)
  }

  async release(): Promise<void> {
    const held = [...this.held]
    this.held.clear()
    await Promise.all(held.map(([key, token]) => unlock(this.db, key, token).catch((err: unknown) =>
      console.warn(`[claim] could not release lock ${key} (it expires on its own):`, (err as Error).message))))
  }

  private async lock(key: string): Promise<boolean> {
    if (this.held.has(key)) return true
    if (this.busy.has(key)) return false
    const token = await tryLock(this.db, key)
    if (!token) { this.busy.add(key); return false }
    this.held.set(key, token)
    return true
  }

  /** undefined: lock busy (skip for now); null: unknown pool (no limit). */
  private async poolCount(name: string): Promise<PoolCount | null | undefined> {
    if (this.pools.has(name)) return this.pools.get(name)
    if (!(await this.lock(poolKey(name)))) return undefined
    const pool = await this.db.collection('pools').findOne({ name })
    if (!pool) {
      if (!warnedPools.has(name)) {
        warnedPools.add(name)
        console.warn(`[pools] task references unknown pool '${name}' — running without pool limit`)
      }
      this.pools.set(name, null)
      return null
    }
    const capacity = Number(pool.slots) || 0
    const running = await this.db.collection('task_instances')
      .find({ state: 'running', pool: name }, { projection: { pool_slots: 1 } }).toArray()
    const used = running.reduce((n, r) => n + slotsFor(r as Pick<TaskInstance, 'pool_slots'>, Math.max(1, capacity)), 0)
    const count = { capacity, used }
    this.pools.set(name, count)
    return count
  }

  private async taskCount(ti: Candidate): Promise<TaskCount | null> {
    const key = taskKey(ti)
    const known = this.tasks.get(key)
    if (known) return known
    if (!(await this.lock(key))) return null
    const running = await this.db.collection('task_instances')
      .find({ dag_id: ti.dag_id, task_id: ti.task_id, state: 'running' }, { projection: { dag_run_id: 1 } }).toArray()
    const byRun = new Map<string, number>()
    for (const r of running) byRun.set(r.dag_run_id as string, (byRun.get(r.dag_run_id as string) ?? 0) + 1)
    const count = { inDag: running.length, byRun }
    this.tasks.set(key, count)
    return count
  }
}
