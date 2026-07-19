import type { DagDefinition, TaskDefinition } from './types.js'

/**
 * Given a DagDefinition that may include `groups` with group-level `dependsOn`,
 * expand group→group dependencies into task-level `depends_on` edges and return
 * a new DagDefinition with flattened tasks.
 *
 * Rules:
 * - If group B `dependsOn` group A, every "root" task of B (tasks in B with no
 *   depends_on referencing another task IN B) gains edges to every "leaf" task of
 *   A (tasks in A not depended on by any other task IN A).
 * - If a group has no tasks, it is ignored (no edges added).
 * - dangling group references (a task's `group` or a group's `dependsOn` pointing
 *   at a non-existent group) throw a descriptive error at load time.
 * - Existing task-level `depends_on` are preserved and merged.
 */
export function expandGroups(dag: DagDefinition): DagDefinition {
  const { groups, tasks } = dag
  if (!groups) return dag

  // ── Validate group refs ────────────────────────────────────────────────────

  // Tasks must reference a declared group
  for (const [taskId, task] of Object.entries(tasks)) {
    if (task.group !== undefined && !(task.group in groups)) {
      throw new Error(
        `Dag '${dag.id}': task '${taskId}' references undeclared group '${task.group}'`,
      )
    }
  }

  // Groups can only depend on declared groups
  for (const [groupId, group] of Object.entries(groups)) {
    for (const dep of group.dependsOn ?? []) {
      if (!(dep in groups)) {
        throw new Error(
          `Dag '${dag.id}': group '${groupId}' depends on undeclared group '${dep}'`,
        )
      }
    }
  }

  // ── Build per-group task lists ─────────────────────────────────────────────

  const groupTasks: Record<string, string[]> = {}
  for (const groupId of Object.keys(groups)) groupTasks[groupId] = []
  for (const [taskId, task] of Object.entries(tasks)) {
    if (task.group) groupTasks[task.group].push(taskId)
  }

  // ── Leaf/root helpers ──────────────────────────────────────────────────────

  /**
   * "Leaf" tasks of a group = tasks in the group that are NOT listed in the
   * `depends_on` of any other task IN the same group.
   * These are the "exit points" of the group.
   */
  function leavesOf(groupId: string): string[] {
    const members = groupTasks[groupId]
    if (members.length === 0) return []
    const internalDeps = new Set<string>()
    for (const taskId of members) {
      for (const dep of tasks[taskId].dependsOn ?? []) {
        if (members.includes(dep)) internalDeps.add(dep)
      }
    }
    return members.filter(id => !internalDeps.has(id))
  }

  /**
   * "Root" tasks of a group = tasks in the group that have NO `depends_on`
   * entries pointing to another task IN the same group.
   * These are the "entry points" of the group.
   */
  function rootsOf(groupId: string): string[] {
    const members = groupTasks[groupId]
    if (members.length === 0) return []
    return members.filter(id => {
      const deps = tasks[id].dependsOn ?? []
      return !deps.some(dep => members.includes(dep))
    })
  }

  // ── Expand group→group edges into task→task edges ──────────────────────────

  // Build a map of extra edges: taskId → additional depends_on task ids
  const extraDeps: Record<string, Set<string>> = {}
  const ensureSet = (id: string) => {
    if (!extraDeps[id]) extraDeps[id] = new Set()
    return extraDeps[id]
  }

  for (const [groupId, group] of Object.entries(groups)) {
    for (const upstreamGroupId of group.dependsOn ?? []) {
      const upstreamLeaves = leavesOf(upstreamGroupId)
      const downstreamRoots = rootsOf(groupId)
      for (const rootId of downstreamRoots) {
        for (const leafId of upstreamLeaves) {
          ensureSet(rootId).add(leafId)
        }
      }
    }
  }

  // ── Build new tasks record with merged depends_on ──────────────────────────

  const expandedTasks: Record<string, TaskDefinition> = {}
  for (const [taskId, task] of Object.entries(tasks)) {
    const existing = new Set(task.dependsOn ?? [])
    const extra = extraDeps[taskId] ?? new Set()
    const merged = [...new Set([...existing, ...extra])]
    expandedTasks[taskId] = { ...task, dependsOn: merged }
  }

  return { ...dag, tasks: expandedTasks }
}
