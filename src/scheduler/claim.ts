import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'

// Terminal states — a task is done once it reaches one of these
const TERMINAL = new Set(['success', 'failed', 'cancelled', 'skipped'])

/**
 * Per-task-id aggregate state, computed from all mapped instances.
 * Non-mapped tasks have exactly 1 instance.
 * Mapped tasks: success iff all instances succeeded; failed iff any terminal-failed.
 */
function taskAggState(states: string[]): 'success' | 'failed' | 'skipped' | 'pending' {
  if (states.length === 0) return 'pending'
  if (states.every(s => s === 'success')) return 'success'
  if (states.every(s => s === 'skipped')) return 'skipped'
  if (states.some(s => s === 'failed')) return 'failed'
  if (states.every(s => TERMINAL.has(s))) return 'skipped'  // all done but not success/failed
  return 'pending'  // at least one not yet terminal
}

/**
 * Returns true if this task's trigger_rule is satisfied by the upstream aggregate states.
 * "satisfied" means → claim and run.
 */
export function isSatisfied(
  triggerRule: string,
  upstreamStates: Array<'success' | 'failed' | 'skipped' | 'pending'>,
): boolean {
  // If any upstream is still pending, rule cannot be evaluated yet — not satisfied
  if (upstreamStates.some(s => s === 'pending')) return false

  switch (triggerRule) {
    case 'all_success':
      return upstreamStates.every(s => s === 'success')
    case 'all_failed':
      return upstreamStates.every(s => s === 'failed' || s === 'skipped')
    case 'all_done':
      return true  // all upstreams are terminal (pending check above)
    case 'one_success':
      return upstreamStates.some(s => s === 'success')
    case 'one_failed':
      return upstreamStates.some(s => s === 'failed')
    case 'none_failed':
      return upstreamStates.every(s => s === 'success' || s === 'skipped')
    default:
      return upstreamStates.every(s => s === 'success')  // unknown rule → all_success
  }
}

/**
 * Returns true if this task's trigger_rule can NEVER be satisfied given current upstream states.
 * When unsatisfiable, the task should be marked 'skipped' so the run can terminate.
 *
 * A rule is unsatisfiable when all upstreams are terminal AND satisfied() returned false.
 */
export function isUnsatisfiable(
  triggerRule: string,
  upstreamStates: Array<'success' | 'failed' | 'skipped' | 'pending'>,
): boolean {
  // Still waiting on upstreams — not yet unsatisfiable
  if (upstreamStates.some(s => s === 'pending')) return false
  // All upstreams terminal: if not satisfied now, can never be
  return !isSatisfied(triggerRule, upstreamStates)
}

/**
 * Atomically claim ALL currently-ready queued tasks for a run.
 * Readiness is evaluated in JS against trigger rules, then claimed atomically.
 *
 * Returns claimed tasks. Also returns a list of task_ids that are now unsatisfiable
 * (should be skipped by the caller).
 */
export async function claimReadyTasks(
  db: Db,
  dagRunId: string,
): Promise<TaskInstance[]> {
  // Fetch ALL instances for this run to compute upstream states
  const allInstances = await db
    .collection<TaskInstance>('task_instances')
    .find({ dag_run_id: dagRunId })
    .toArray()

  // Group by task_id — collect all instance states
  const byTaskId = new Map<string, string[]>()
  for (const inst of allInstances) {
    const arr = byTaskId.get(inst.task_id) ?? []
    arr.push(inst.state)
    byTaskId.set(inst.task_id, arr)
  }

  // Compute aggregate state per task_id
  const aggState = new Map<string, 'success' | 'failed' | 'skipped' | 'pending'>()
  for (const [taskId, states] of byTaskId) {
    aggState.set(taskId, taskAggState(states))
  }

  const now = new Date()
  const claimed: TaskInstance[] = []

  for (const inst of allInstances) {
    if (inst.state !== 'queued') continue

    // Dynamic placeholder gate — never execute; only expandDynamicMapped handles these
    if (inst.is_dynamic_placeholder) continue

    // Sensor poke gate
    if (inst.next_poke_at !== null && inst.next_poke_at > now) continue

    // HITL gate
    if (inst.is_hitl && inst.hitl_state !== 'approved') continue

    // Evaluate trigger rule against upstream aggregate states
    const upstreamStates = inst.depends_on.map(dep => aggState.get(dep) ?? 'pending')
    const rule = inst.trigger_rule ?? 'all_success'

    if (!isSatisfied(rule, upstreamStates)) continue

    // Atomically claim this specific instance
    const claimed_inst = await db.collection<TaskInstance>('task_instances').findOneAndUpdate(
      { dag_run_id: dagRunId, task_id: inst.task_id, map_index: inst.map_index ?? null, state: 'queued' },
      { $set: { state: 'running', started_at: new Date() } },
      { returnDocument: 'after' },
    )
    if (claimed_inst) claimed.push(claimed_inst)
  }

  return claimed
}

/**
 * Mark all queued tasks whose trigger rule is permanently unsatisfiable as 'skipped'.
 * Called after each execution wave so runs can terminate cleanly.
 *
 * Returns the number of tasks skipped.
 * Cascades: if a skipped task was an upstream for another task, caller should re-evaluate.
 */
export async function skipUnsatisfiableTasks(db: Db, dagRunId: string): Promise<number> {
  let totalSkipped = 0

  // Cascade: skipping a task changes aggregate states → may make more tasks unsatisfiable
  // Iterate until stable (usually 1-2 passes for simple DAGs)
  for (let pass = 0; pass < 20; pass++) {
    const allInstances = await db
      .collection<TaskInstance>('task_instances')
      .find({ dag_run_id: dagRunId })
      .toArray()

    const byTaskId = new Map<string, string[]>()
    for (const inst of allInstances) {
      const arr = byTaskId.get(inst.task_id) ?? []
      arr.push(inst.state)
      byTaskId.set(inst.task_id, arr)
    }

    const aggState = new Map<string, 'success' | 'failed' | 'skipped' | 'pending'>()
    for (const [taskId, states] of byTaskId) {
      aggState.set(taskId, taskAggState(states))
    }

    // Find all queued instances that are now unsatisfiable
    const toSkip: TaskInstance[] = []
    for (const inst of allInstances) {
      if (inst.state !== 'queued') continue
      const upstreamStates = inst.depends_on.map(dep => aggState.get(dep) ?? 'pending')
      const rule = inst.trigger_rule ?? 'all_success'
      if (isUnsatisfiable(rule, upstreamStates)) {
        toSkip.push(inst)
      }
    }

    if (toSkip.length === 0) break

    // Mark them skipped
    for (const inst of toSkip) {
      await db.collection<TaskInstance>('task_instances').updateOne(
        { dag_run_id: dagRunId, task_id: inst.task_id, map_index: inst.map_index ?? null, state: 'queued' },
        { $set: { state: 'skipped', ended_at: new Date() } },
      )
    }
    totalSkipped += toSkip.length
  }

  return totalSkipped
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

  // Build aggregate state map for dependency checking
  const allInstances = await db.collection<TaskInstance>('task_instances')
    .find({ dag_run_id: dagRunId })
    .toArray()

  const byTaskId = new Map<string, string[]>()
  for (const inst of allInstances) {
    const arr = byTaskId.get(inst.task_id) ?? []
    arr.push(inst.state)
    byTaskId.set(inst.task_id, arr)
  }

  const aggState = new Map<string, 'success' | 'failed' | 'skipped' | 'pending'>()
  for (const [taskId, states] of byTaskId) {
    // Simplified: success = all success; otherwise check terminals
    if (states.every(s => s === 'success')) aggState.set(taskId, 'success')
    else if (states.some(s => s === 'failed')) aggState.set(taskId, 'failed')
    else if (states.every(s => TERMINAL.has(s))) aggState.set(taskId, 'skipped')
    else aggState.set(taskId, 'pending')
  }

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
