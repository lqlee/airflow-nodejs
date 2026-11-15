/**
 * User actions on task instances and runs (Airflow's Clear / Mark success / Mark failed).
 *
 *   clearTaskInstances  — reset finished instances to queued, optionally with their downstream and/or
 *                         upstream tasks, or only the failed ones; the run goes back to queued.
 *   setTaskInstanceState — mark instances success / failed / skipped, optionally with downstream/upstream.
 *   setRunState         — mark a whole run success or failed.
 *
 * Running and deferred instances are never cleared (that would put two executors on one row). They can be
 * marked: the executor's late result is then dropped (ti-writes.ts) — the process itself is not killed.
 * Every action records an audit event. A dry run returns the instances that would change without writing.
 */
import { ObjectId, type Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { getDag } from '../dag/registry.js'
import { recordEvent } from '../events/index.js'
import { pruneDeadlines, requeueDeadlines } from '../deadlines/index.js'
import { TI_CLEARABLE, TI_FINISHED, type TiSettableState } from './states.js'

export interface TiRef { task_id: string; map_index: number | null; state: string }

export interface SelectOptions {
  /** Tasks to act on; omit for every task in the run. */
  taskIds?: string[]
  /** Restrict the named tasks to one mapped instance (downstream/upstream tasks are taken whole). */
  mapIndex?: number | null
  includeDownstream?: boolean
  includeUpstream?: boolean
  dryRun?: boolean
}

export interface ClearOptions extends SelectOptions {
  /** Only clear failed / upstream_failed instances. */
  onlyFailed?: boolean
}

export type ActionResult =
  | { ok: true; changed: TiRef[] }
  | { ok: false; reason: 'run_not_found' | 'task_not_found'; taskId?: string }

type Ti = TaskInstance & { _id: ObjectId }

const RUN_FINISHED: ReadonlySet<string> = new Set(['success', 'failed', 'cancelled'])

const ref = (t: Ti): TiRef => ({ task_id: t.task_id, map_index: t.map_index ?? null, state: t.state })

/** Transitive closure over depends_on, in one direction. */
function closure(all: Ti[], start: Set<string>, direction: 'downstream' | 'upstream'): Set<string> {
  const deps = new Map<string, string[]>()
  for (const t of all) if (!deps.has(t.task_id)) deps.set(t.task_id, t.depends_on ?? [])
  const out = new Set<string>()
  const frontier = [...start]
  while (frontier.length > 0) {
    const cur = frontier.pop()!
    const next = direction === 'upstream'
      ? deps.get(cur) ?? []
      : [...deps].filter(([, d]) => d.includes(cur)).map(([id]) => id)
    for (const id of next) if (!out.has(id) && !start.has(id)) { out.add(id); frontier.push(id) }
  }
  return out
}

/** Load a run's instances and pick the ones an action applies to. */
async function select(db: Db, runId: string, opts: SelectOptions): Promise<{ ok: true; run: Record<string, unknown>; selected: Ti[] } | Extract<ActionResult, { ok: false }>> {
  if (!ObjectId.isValid(runId)) return { ok: false, reason: 'run_not_found' }
  const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
  if (!run) return { ok: false, reason: 'run_not_found' }
  const all = await db.collection<Ti>('task_instances').find({ dag_run_id: runId }).toArray()

  const known = new Set(all.map(t => t.task_id))
  const roots = new Set(opts.taskIds ?? known)
  for (const id of roots) if (!known.has(id)) return { ok: false, reason: 'task_not_found', taskId: id }

  const extra = new Set<string>()
  if (opts.includeDownstream) for (const id of closure(all, roots, 'downstream')) extra.add(id)
  if (opts.includeUpstream) for (const id of closure(all, roots, 'upstream')) extra.add(id)

  const mapIndex = opts.mapIndex
  const selected = all.filter(t =>
    extra.has(t.task_id) ||
    (roots.has(t.task_id) && (mapIndex === undefined || mapIndex === null || t.map_index === mapIndex)))
  return { ok: true, run, selected }
}

/** Put a finished run back in the scheduler's hands. */
async function reopenRun(db: Db, runId: string, state: 'queued' | 'running'): Promise<void> {
  const now = new Date()
  await db.collection('dag_runs').updateOne({ _id: new ObjectId(runId) },
    { $set: { state, ended_at: null, ...(state === 'queued' ? { queued_at: now } : {}) } })
  // Re-queued: queued_at deadlines count from now (Airflow recalculates them the same way)
  if (state === 'queued') {
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) }, { projection: { dag_id: 1 } })
    if (run) await requeueDeadlines(db, getDag(run.dag_id as string), runId, now)
  }
}

/**
 * Reset finished instances to queued. try_number keeps counting (the try history stays unique) and the
 * instance gets its full retry budget again, as in Airflow. HITL tasks need a fresh approval.
 */
export async function clearTaskInstances(db: Db, runId: string, opts: ClearOptions = {}): Promise<ActionResult> {
  const sel = await select(db, runId, opts)
  if (!sel.ok) return sel

  const states: readonly string[] = opts.onlyFailed ? ['failed', 'upstream_failed'] : TI_CLEARABLE
  const targets = sel.selected.filter(t => states.includes(t.state))
  if (opts.dryRun || targets.length === 0) return { ok: true, changed: targets.map(ref) }

  const dag = getDag(sel.run.dag_id as string)
  const changed: TiRef[] = []
  for (const t of targets) {
    const tryNumber = (t.try_number ?? 0) + (t.started_at ? 1 : 0)   // a try ran → the next one is new
    const retries = dag?.tasks[t.task_id]?.retries ?? t.max_retries ?? 0
    const res = await db.collection('task_instances').updateOne({ _id: t._id, state: t.state }, {
      $set: {
        state: 'queued', started_at: null, ended_at: null, error: null, claimed_by: null,
        next_poke_at: null, first_poked_at: null, poke_count: 0, next_retry_at: null,
        deferred_trigger_fn: null, deferred_at: null, triggered_run_id: null,
        try_number: tryNumber, max_retries: tryNumber + retries,
        ...(t.is_hitl ? { hitl_state: 'pending', hitl_note: null, hitl_responded_at: null } : {}),
      },
    })
    if (res.modifiedCount > 0) changed.push(ref(t))
  }

  if (changed.length > 0) {
    // Un-terminal the run so the tick re-picks it (also from success/failed/cancelled — that's the point)
    await reopenRun(db, runId, 'queued')
    const wholeRun = opts.taskIds === undefined
    void recordEvent(db, wholeRun ? 'run_cleared' : 'task_cleared', {
      dag_id: sel.run.dag_id as string,
      dag_run_id: runId,
      task_id: !wholeRun && opts.taskIds!.length === 1 ? opts.taskIds![0] : null,
      map_index: opts.mapIndex ?? null,
      metadata: {
        cleared_count: changed.length,
        task_ids: [...new Set(changed.map(c => c.task_id))],
        include_downstream: opts.includeDownstream === true,
        include_upstream: opts.includeUpstream === true,
        only_failed: opts.onlyFailed === true,
      },
    })
    console.log(`[scheduler] cleared ${changed.length} instance(s) in run ${runId}`)
  }
  return { ok: true, changed }
}

/** Mark instances success / failed / skipped. Removed instances and ones already in that state are left alone. */
export async function setTaskInstanceState(
  db: Db, runId: string, state: TiSettableState, opts: SelectOptions = {},
): Promise<ActionResult> {
  const sel = await select(db, runId, opts)
  if (!sel.ok) return sel

  const targets = sel.selected.filter(t => t.state !== state && t.state !== 'removed')
  if (opts.dryRun || targets.length === 0) return { ok: true, changed: targets.map(ref) }

  const now = new Date()
  const changed: TiRef[] = []
  for (const t of targets) {
    const res = await db.collection('task_instances').updateOne({ _id: t._id, state: t.state }, {
      $set: {
        state, ended_at: now, claimed_by: null, next_retry_at: null,
        error: state === 'failed' ? 'Marked failed by user' : null,
      },
    })
    if (res.modifiedCount > 0) changed.push(ref(t))
  }

  if (changed.length > 0) {
    // A finished run must be re-evaluated: marking can change its outcome or unblock downstream tasks
    if (RUN_FINISHED.has(sel.run.state as string)) {
      await reopenRun(db, runId, 'running')
    }
    void recordEvent(db, 'task_state_set', {
      dag_id: sel.run.dag_id as string,
      dag_run_id: runId,
      task_id: opts.taskIds?.length === 1 ? opts.taskIds[0] : null,
      map_index: opts.mapIndex ?? null,
      metadata: {
        state,
        changed_count: changed.length,
        task_ids: [...new Set(changed.map(c => c.task_id))],
        include_downstream: opts.includeDownstream === true,
        include_upstream: opts.includeUpstream === true,
      },
    })
  }
  return { ok: true, changed }
}

/**
 * Mark a run success or failed, like Airflow's "Mark run as …":
 *   success → every instance not yet success/skipped/removed becomes success
 *   failed  → running/deferred instances fail, waiting ones are skipped; finished ones keep their state
 * Dag-level success/failure callbacks are not fired for a manual change.
 */
export async function setRunState(
  db: Db, runId: string, state: 'success' | 'failed', opts: { dryRun?: boolean } = {},
): Promise<ActionResult> {
  const sel = await select(db, runId, {})
  if (!sel.ok) return sel

  const plan = sel.selected.flatMap(t => {
    if (state === 'success') return ['success', 'skipped', 'removed'].includes(t.state) ? [] : [{ t, to: 'success' }]
    if (TI_FINISHED.has(t.state)) return []
    return [{ t, to: t.state === 'running' || t.state === 'deferred' ? 'failed' : 'skipped' }]
  })
  if (opts.dryRun) return { ok: true, changed: plan.map(p => ref(p.t)) }

  const now = new Date()
  const changed: TiRef[] = []
  for (const { t, to } of plan) {
    const res = await db.collection('task_instances').updateOne({ _id: t._id, state: t.state }, {
      $set: { state: to, ended_at: now, claimed_by: null, next_retry_at: null, ...(to === 'failed' ? { error: 'Run marked failed by user' } : {}) },
    })
    if (res.modifiedCount > 0) changed.push(ref(t))
  }
  await db.collection('dag_runs').updateOne(
    { _id: new ObjectId(runId) },
    { $set: { state, ended_at: now, ...(sel.run.started_at ? {} : { started_at: now }) } },
  )
  if (state === 'success') await pruneDeadlines(db, runId)
  void recordEvent(db, 'run_state_set', {
    dag_id: sel.run.dag_id as string,
    dag_run_id: runId,
    metadata: { state, previous_state: sel.run.state, changed_count: changed.length },
  })
  return { ok: true, changed }
}
