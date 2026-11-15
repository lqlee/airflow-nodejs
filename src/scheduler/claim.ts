import { ObjectId, type Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { INSTANCE_ID } from './instance.js'
import { aggregateByTask, TI_CLAIMABLE, type AggState } from './states.js'
import { ClaimLimits } from './claim-limits.js'

type UpstreamState = Exclude<AggState, 'removed'>

/** failed or upstream_failed — what the failure-based rules count (Airflow: failed + upstream_failed). */
const isFail = (s: UpstreamState) => s === 'failed' || s === 'upstream_failed'

/**
 * Returns true if this task's trigger_rule is satisfied by the upstream aggregate states.
 * "satisfied" means → claim and run.
 */
export function isSatisfied(
  triggerRule: string,
  upstreamStates: UpstreamState[],
  /** Parallel to upstreamStates: true where that upstream is a setup task. Only `all_done_setup_success` reads it. */
  setupFlags: boolean[] = [],
): boolean {
  // 'always' ignores upstream state entirely — runs even while upstreams are pending
  if (triggerRule === 'always') return true

  // If any upstream is still pending, rule cannot be evaluated yet — not satisfied
  if (upstreamStates.some(s => s === 'pending')) return false

  switch (triggerRule) {
    case 'all_success':
      return upstreamStates.every(s => s === 'success')
    case 'all_failed':
      return upstreamStates.every(s => isFail(s) || s === 'skipped')
    case 'all_done':
      return true  // all upstreams are terminal (pending check above)
    case 'one_success':
      return upstreamStates.some(s => s === 'success')
    case 'one_failed':
      return upstreamStates.some(isFail)
    case 'none_failed':
      return upstreamStates.every(s => s === 'success' || s === 'skipped')
    case 'one_done':
      // upstream_failed is not "done" here: that task never ran (Airflow counts success + failed only)
      return upstreamStates.some(s => s === 'success' || s === 'failed')
    case 'none_skipped':
      return upstreamStates.every(s => s !== 'skipped')
    case 'all_skipped':
      return upstreamStates.every(s => s === 'skipped')
    case 'none_failed_min_one_success':
      return !upstreamStates.some(isFail) && upstreamStates.some(s => s === 'success')
    case 'all_done_min_one_success':
      // Skipped upstreams are not "done" for this rule
      return upstreamStates.every(s => s !== 'skipped') && upstreamStates.some(s => s === 'success')
    case 'all_done_setup_success': {
      // All upstreams done (pending check above); if any are setups, at least one must have succeeded
      const setupStates = upstreamStates.filter((_, i) => setupFlags[i])
      return setupStates.length === 0 || setupStates.some(s => s === 'success')
    }
    default:
      return upstreamStates.every(s => s === 'success')  // unknown rule → all_success
  }
}

/**
 * Returns true if this task's trigger_rule can NEVER be satisfied given current upstream states.
 * A rule is unsatisfiable when all upstreams are terminal AND satisfied() returned false.
 */
export function isUnsatisfiable(
  triggerRule: string,
  upstreamStates: UpstreamState[],
  setupFlags: boolean[] = [],
): boolean {
  // Still waiting on upstreams — not yet unsatisfiable
  if (upstreamStates.some(s => s === 'pending')) return false
  // All upstreams terminal: if not satisfied now, can never be
  return !isSatisfied(triggerRule, upstreamStates, setupFlags)
}

/**
 * The state a task gets when its rule can never be satisfied (null while it still can be), following
 * Airflow's TriggerRuleDep: upstream_failed when the rule failed because of an upstream failure,
 * skipped when it failed because of skips (or because a failure-triggered rule saw no failure).
 */
export function unsatisfiedState(
  triggerRule: string,
  upstreamStates: UpstreamState[],
  setupFlags: boolean[] = [],
): 'skipped' | 'upstream_failed' | null {
  if (!isUnsatisfiable(triggerRule, upstreamStates, setupFlags)) return null
  const failed = upstreamStates.some(isFail)
  const allSkipped = upstreamStates.length > 0 && upstreamStates.every(s => s === 'skipped')
  const anySkipped = upstreamStates.some(s => s === 'skipped')
  switch (triggerRule) {
    case 'all_failed': case 'one_failed': case 'one_done': case 'none_skipped': case 'all_skipped':
      return 'skipped'
    case 'one_success': case 'none_failed_min_one_success':
      return failed || !allSkipped ? 'upstream_failed' : 'skipped'
    case 'all_done_min_one_success':
      return anySkipped ? 'skipped' : 'upstream_failed'
    case 'all_done_setup_success': {
      const setups = upstreamStates.filter((_, i) => setupFlags[i])
      return setups.every(s => s === 'skipped') ? 'skipped' : 'upstream_failed'
    }
    default:   // all_success, none_failed, unknown rules
      return failed ? 'upstream_failed' : 'skipped'
  }
}

/**
 * Upstream aggregate states for a task, parallel to its setup flags. Removed upstreams are dropped:
 * like Airflow, a task deleted from the Dag neither blocks nor fails its downstream tasks.
 */
function upstreamOf(
  inst: TaskInstance, agg: Map<string, AggState>, setupIds: Set<string>,
): { states: UpstreamState[]; setupFlags: boolean[] } {
  const states: UpstreamState[] = []
  const setupFlags: boolean[] = []
  for (const dep of inst.depends_on) {
    const s = agg.get(dep) ?? 'pending'
    if (s === 'removed') continue
    states.push(s)
    setupFlags.push(setupIds.has(dep))
  }
  return { states, setupFlags }
}

function collectSetupIds(instances: TaskInstance[]): Set<string> {
  return new Set(instances.filter(i => i.is_setup).map(i => i.task_id))
}

/** depends_on_past passes when the task is absent from the previous run or every instance succeeded/was skipped. */
export function isPastSatisfied(previousStates: string[] | undefined): boolean {
  if (!previousStates || previousStates.length === 0) return true
  return previousStates.every(s => s === 'success' || s === 'skipped' || s === 'removed')
}

/**
 * Task states (per task_id) of the previous run of the same Dag — the latest earlier,
 * non-cancelled run by ordering_date. Empty map when this is the first run.
 */
async function getPreviousRunTaskStates(db: Db, dagRunId: string): Promise<Map<string, string[]>> {
  const result = new Map<string, string[]>()
  const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(dagRunId) })
  const orderingDate = (run?.ordering_date ?? run?.logical_date ?? run?.created_at) as Date | undefined
  if (!run || !orderingDate) return result

  const prev = await db.collection('dag_runs').findOne(
    {
      dag_id: run.dag_id,
      _id: { $ne: run._id },
      state: { $ne: 'cancelled' },
      // Equal dates tie-break on _id so two runs never wait on each other
      $or: [
        { ordering_date: { $lt: orderingDate } },
        { ordering_date: orderingDate, _id: { $lt: run._id } },
      ],
    },
    { sort: { ordering_date: -1, _id: -1 } },
  )
  if (!prev) return result

  const instances = await db.collection('task_instances')
    .find({ dag_run_id: prev._id.toString() }, { projection: { task_id: 1, state: 1 } })
    .toArray()
  for (const t of instances) {
    const arr = result.get(t.task_id as string) ?? []
    arr.push(t.state as string)
    result.set(t.task_id as string, arr)
  }
  return result
}

const CLAIMABLE: ReadonlySet<string> = new Set(TI_CLAIMABLE)

/** Time gates: a retry waits for next_retry_at, a sensor (reschedule mode) for next_poke_at. */
function isDue(inst: TaskInstance, now: Date): boolean {
  if (inst.state === 'up_for_retry' && inst.next_retry_at && new Date(inst.next_retry_at) > now) return false
  if (inst.next_poke_at !== null && inst.next_poke_at !== undefined && new Date(inst.next_poke_at) > now) return false
  return true
}

export interface ClaimOptions {
  /** Claim at most this many instances (free local worker slots). Default: no limit. */
  limit?: number
  now?: Date
}

/**
 * Atomically claim the currently-ready instances of a run: queued ones whose trigger rule is met,
 * plus up_for_retry / up_for_reschedule ones whose wait is over.
 * Readiness is evaluated in JS; each claim is a compare-and-set on the instance's state.
 * Pools and per-task concurrency are admitted by ClaimLimits (DB counts under claim locks).
 */
export async function claimReadyTasks(
  db: Db,
  dagRunId: string,
  opts: ClaimOptions = {},
): Promise<TaskInstance[]> {
  const limit = opts.limit ?? Infinity
  if (limit <= 0) return []
  const now = opts.now ?? new Date()

  // Fetch ALL instances for this run to compute upstream states
  const allInstances = await db
    .collection<TaskInstance>('task_instances')
    .find({ dag_run_id: dagRunId })
    .toArray()

  const aggState = aggregateByTask(allInstances)

  // Sort: higher priority first, then FIFO (created_at ascending) as tiebreaker.
  // This ensures high-priority tasks are claimed before low-priority ones when
  // multiple tasks are ready simultaneously under a constrained pool.
  allInstances.sort((a, b) => {
    const pa = a.priority ?? 0
    const pb = b.priority ?? 0
    if (pb !== pa) return pb - pa  // higher priority first
    // Tiebreaker: FIFO by creation time
    const ta = a.created_at ? new Date(a.created_at).getTime() : 0
    const tb = b.created_at ? new Date(b.created_at).getTime() : 0
    return ta - tb
  })

  const claimed: TaskInstance[] = []
  const setupIds = collectSetupIds(allInstances)

  // depends_on_past gate — resolve the previous run's task states once per claim
  const pastStates = allInstances.some(i => i.depends_on_past && CLAIMABLE.has(i.state))
    ? await getPreviousRunTaskStates(db, dagRunId)
    : null

  const limits = new ClaimLimits(db)
  try {
    for (const inst of allInstances) {
      if (claimed.length >= limit) break
      if (!CLAIMABLE.has(inst.state)) continue

      // Dynamic placeholder gate — never execute; only expandDynamicMapped handles these
      if (inst.is_dynamic_placeholder) continue

      // Retry delay / sensor poke gate
      if (!isDue(inst, now)) continue

      // HITL gate
      if (inst.is_hitl && inst.hitl_state !== 'approved') continue

      // depends_on_past gate
      if (inst.depends_on_past && !isPastSatisfied(pastStates?.get(inst.task_id))) continue

      // Evaluate trigger rule against upstream aggregate states
      const up = upstreamOf(inst, aggState, setupIds)
      if (!isSatisfied(inst.trigger_rule ?? 'all_success', up.states, up.setupFlags)) continue

      // Pool slots + maxActiveTis / maxActiveTisPerRun
      if (!(await limits.admit(inst))) continue

      // Atomically claim this specific instance (CAS on the state it was read in)
      const claimedInst = await db.collection<TaskInstance>('task_instances').findOneAndUpdate(
        { dag_run_id: dagRunId, task_id: inst.task_id, map_index: inst.map_index ?? null, state: inst.state },
        // claimed_by: the owner whose heartbeat keeps this task from being reaped as a zombie
        { $set: { state: 'running', started_at: new Date(), ended_at: null, claimed_by: INSTANCE_ID } },
        { returnDocument: 'after' },
      )
      if (claimedInst) {
        claimed.push(claimedInst)
        limits.commit(inst)
      }
    }
  } finally {
    await limits.release()
  }

  return claimed
}

/**
 * Finish every queued task whose trigger rule can never be satisfied: upstream_failed when an upstream
 * failure is the reason, skipped otherwise (see unsatisfiedState).
 * Called after each execution wave so runs can terminate cleanly.
 *
 * Returns the number of tasks finished this way.
 */
export async function skipUnsatisfiableTasks(db: Db, dagRunId: string): Promise<number> {
  let total = 0

  // Cascade: finishing a task changes aggregate states → may make more tasks unsatisfiable
  // Iterate until stable (usually 1-2 passes for simple DAGs)
  for (let pass = 0; pass < 20; pass++) {
    const allInstances = await db
      .collection<TaskInstance>('task_instances')
      .find({ dag_run_id: dagRunId })
      .toArray()

    const aggState = aggregateByTask(allInstances)
    const setupIds = collectSetupIds(allInstances)
    const toFinish: Array<{ inst: TaskInstance; state: 'skipped' | 'upstream_failed' }> = []
    for (const inst of allInstances) {
      if (inst.state !== 'queued') continue
      const up = upstreamOf(inst, aggState, setupIds)
      const state = unsatisfiedState(inst.trigger_rule ?? 'all_success', up.states, up.setupFlags)
      if (state) toFinish.push({ inst, state })
    }

    if (toFinish.length === 0) break

    for (const { inst, state } of toFinish) {
      await db.collection<TaskInstance>('task_instances').updateOne(
        { dag_run_id: dagRunId, task_id: inst.task_id, map_index: inst.map_index ?? null, state: 'queued' },
        { $set: { state, ended_at: new Date() } },
      )
    }
    total += toFinish.length
  }

  return total
}

/**
 * After a branch task succeeds, read its XCom '_branch_decision' and skip
 * all direct dependents that were NOT selected.
 *
 * Called from advanceRun after each execution wave, before skipUnsatisfiableTasks.
 * Returns the number of tasks skipped by branch decisions.
 */
export async function applyBranchDecisions(db: Db, dagRunId: string): Promise<number> {
  // Find all branch tasks that just succeeded in this run
  const branchInstances = await db.collection<TaskInstance>('task_instances').find({
    dag_run_id: dagRunId,
    is_branch: true,
    state: 'success',
  }).toArray()

  if (branchInstances.length === 0) return 0

  // Fetch all task instances for this run once (to find direct dependents)
  const allInstances = await db.collection<TaskInstance>('task_instances')
    .find({ dag_run_id: dagRunId })
    .toArray()

  let totalSkipped = 0

  for (const branchTi of branchInstances) {
    // Read branch decision from XCom
    const xcomDoc = await db.collection('xcoms').findOne({
      dag_run_id: dagRunId,
      task_id: branchTi.task_id,
      key: '_branch_decision',
    })

    // selected: the task_ids this branch wants to activate
    const rawDecision = xcomDoc?.value
    const selected = new Set<string>(
      Array.isArray(rawDecision) ? rawDecision.filter((x: unknown) => typeof x === 'string') : []
    )

    // Validate: all selected ids must be direct dependents of this branch task
    const directDependents = allInstances
      .filter(ti => ti.depends_on.includes(branchTi.task_id))
      .map(ti => ti.task_id)
    const directDependentSet = new Set(directDependents)

    for (const selectedId of selected) {
      if (!directDependentSet.has(selectedId)) {
        console.warn(`[branch] '${branchTi.task_id}' returned unknown task_id '${selectedId}' — ignoring`)
        selected.delete(selectedId)
      }
    }

    // Skip all direct dependents NOT in the selected set (and still queued)
    for (const ti of allInstances) {
      if (!ti.depends_on.includes(branchTi.task_id)) continue
      if (ti.state !== 'queued') continue
      if (selected.has(ti.task_id)) continue
      if (ti.is_teardown) continue  // teardowns must still run to release what their setup acquired

      // This dependent was not selected — skip it
      await db.collection<TaskInstance>('task_instances').updateOne(
        { dag_run_id: dagRunId, task_id: ti.task_id, map_index: ti.map_index ?? null, state: 'queued' },
        { $set: { state: 'skipped', ended_at: new Date() } },
      )
      totalSkipped++
    }

    if (totalSkipped > 0 || directDependents.length > 0) {
      console.log(`[branch] '${branchTi.task_id}' selected [${[...selected].join(', ')}], skipped ${totalSkipped} task(s)`)
    }
  }

  return totalSkipped
}

/**
 * After a ShortCircuit task succeeds, read its XCom `_short_circuit`. When false, skip every
 * downstream task (direct AND transitive) that is still queued — regardless of trigger rule.
 * Returns the number of tasks skipped.
 */
export async function applyShortCircuitDecisions(db: Db, dagRunId: string): Promise<number> {
  const all = await db.collection<TaskInstance>('task_instances').find({ dag_run_id: dagRunId }).toArray()
  const shorted = all.filter(t => t.is_short_circuit && t.state === 'success')
  if (shorted.length === 0) return 0

  const doomed = new Set<string>()
  for (const sc of shorted) {
    const doc = await db.collection('xcoms').findOne({ dag_run_id: dagRunId, task_id: sc.task_id, key: '_short_circuit' })
    if (doc?.value !== false) continue

    // BFS over the depends_on graph for everything downstream of this task
    const frontier = [sc.task_id]
    while (frontier.length > 0) {
      const current = frontier.pop()!
      for (const t of all) {
        // Teardowns are exempt (they must still release what their setup acquired) and
        // the walk does not continue through them.
        if (t.depends_on.includes(current) && !doomed.has(t.task_id) && !t.is_teardown) {
          doomed.add(t.task_id)
          frontier.push(t.task_id)
        }
      }
    }
  }
  if (doomed.size === 0) return 0

  const res = await db.collection<TaskInstance>('task_instances').updateMany(
    { dag_run_id: dagRunId, task_id: { $in: [...doomed] }, state: 'queued' },
    { $set: { state: 'skipped', ended_at: new Date() } },
  )
  if (res.modifiedCount > 0) console.log(`[short-circuit] skipped ${res.modifiedCount} downstream task instance(s)`)
  return res.modifiedCount
}

/**
 * Expand dynamic-mapped tasks once their source XCom is available.
 *
 * For each placeholder instance (is_dynamic_placeholder=true) whose source
 * task has succeeded:
 *   - Read the XCom array from the source task
 *   - If array is non-empty: delete placeholder + insert real instances
 *   - If array is empty or not an array: mark placeholder as 'skipped'
 *   - If source task failed: the skipUnsatisfiableTasks cascade handles the
 *     placeholder (all_success dep on source → skipped automatically)
 *
 * Called from advanceRun after applyBranchDecisions, before skipUnsatisfiableTasks.
 * Returns the number of new instances created (0 for empty/skip cases).
 */
export async function expandDynamicMapped(db: Db, dagRunId: string): Promise<number> {
  // Find all placeholder instances for this run
  const placeholders = await db.collection<TaskInstance>('task_instances').find({
    dag_run_id: dagRunId,
    is_dynamic_placeholder: true,
    state: 'queued',
  }).toArray()

  if (placeholders.length === 0) return 0

  // Aggregate state of the source tasks
  const allInstances = await db.collection<TaskInstance>('task_instances')
    .find({ dag_run_id: dagRunId })
    .toArray()
  const aggState = aggregateByTask(allInstances)

  let totalCreated = 0

  for (const placeholder of placeholders) {
    const src = placeholder.dynamic_expand_source
    if (!src) continue

    // Check if source task succeeded
    const sourceState = aggState.get(src.from)
    if (sourceState !== 'success') continue  // still waiting or not yet terminal

    // Read XCom from source task
    const xcomDoc = await db.collection('xcoms').findOne({
      dag_run_id: dagRunId,
      task_id: src.from,
      key: src.key,
    })

    const rawValue = xcomDoc?.value
    const isValidArray = Array.isArray(rawValue)

    if (!isValidArray || rawValue.length === 0) {
      // Empty or non-array → skip the mapped task
      await db.collection<TaskInstance>('task_instances').updateOne(
        { dag_run_id: dagRunId, task_id: placeholder.task_id, map_index: null, is_dynamic_placeholder: true, state: 'queued' },
        { $set: { state: 'skipped', ended_at: new Date() } },
      )
      console.log(`[dynamic-map] '${placeholder.task_id}': source '${src.from}.${src.key}' is ${isValidArray ? 'empty' : 'not an array'} — skipped`)
      continue
    }

    // Non-empty array — replace placeholder with real instances
    // First delete the placeholder atomically
    const deleted = await db.collection<TaskInstance>('task_instances').deleteOne({
      dag_run_id: dagRunId,
      task_id: placeholder.task_id,
      map_index: null,
      is_dynamic_placeholder: true,
      state: 'queued',
    })

    if (!deleted.deletedCount) continue  // race: another tick already handled it

    // Insert N real instances
    const now = new Date()
    const realInstances: TaskInstance[] = rawValue.map((value: unknown, index: number) => {
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      const { _id, ...rest } = placeholder as TaskInstance & { _id?: unknown }
      return {
        ...rest,                  // inherit all fields EXCEPT _id (let MongoDB generate new ones)
        map_index: index,
        map_value: value,
        state: 'queued' as const,
        is_dynamic_placeholder: false,
        dynamic_expand_source: null,
        started_at: null,
        ended_at: null,
        error: null,
        created_at: now,
      } as TaskInstance
    })

    await db.collection<TaskInstance>('task_instances').insertMany(realInstances)
    totalCreated += realInstances.length
    console.log(`[dynamic-map] '${placeholder.task_id}': expanded to ${realInstances.length} instances from '${src.from}.${src.key}'`)
  }

  return totalCreated
}

// Keep old single-claim export for backward compatibility with tests
export async function claimNextTask(db: Db, dagRunId: string): Promise<TaskInstance | null> {
  const results = await claimReadyTasks(db, dagRunId)
  return results[0] ?? null
}
