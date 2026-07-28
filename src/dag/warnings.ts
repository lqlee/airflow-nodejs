/**
 * Dag Warning analysis — soft validation on successfully-loaded dags.
 *
 * Warnings do NOT prevent a dag from loading or running. They surface authoring
 * issues that could cause unexpected behaviour: missing run logic, unknown
 * dependency references, orphaned group members, etc.
 *
 * analyzeWarnings() is a pure function — testable without DB or filesystem.
 */

import type { DagDefinition } from './types.js'

export type WarningType =
  | 'no_run_logic'          // task has no run, poke, or expand
  | 'unknown_dependency'    // depends_on references a task_id not in this dag
  | 'unknown_group'         // task.group references a group_id not in this dag
  | 'no_tasks'              // dag has zero tasks
  | 'sensor_no_timeout'     // sensor task has no sensorTimeout (could run forever)
  | 'circular_dependency'   // cycle in depends_on graph

export interface DagWarning {
  dag_id: string
  warning_type: WarningType
  /** Human-readable explanation */
  message: string
  /** task_id(s) involved, if applicable */
  task_ids: string[]
  detected_at: Date
}

/**
 * Analyse a dag definition for soft issues.
 * Pure — no side effects, no DB access.
 */
export function analyzeWarnings(dag: DagDefinition, now = new Date()): DagWarning[] {
  const warnings: DagWarning[] = []
  const taskIds = new Set(Object.keys(dag.tasks))
  const groupIds = new Set(Object.keys(dag.groups ?? {}))

  const warn = (type: WarningType, message: string, task_ids: string[] = []) => {
    warnings.push({ dag_id: dag.id, warning_type: type, message, task_ids, detected_at: now })
  }

  // No tasks at all
  if (taskIds.size === 0) {
    warn('no_tasks', `Dag '${dag.id}' has no tasks defined`)
    return warnings  // further checks are moot
  }

  for (const [taskId, task] of Object.entries(dag.tasks)) {
    // Task has no executable logic (shell and python tasks count as run logic)
    if (!task.run && !task.poke && !task.shell && !task.python && !Array.isArray(task.expand)) {
      warn('no_run_logic',
        `Task '${taskId}' has no run, poke, or expand — it will succeed immediately`,
        [taskId])
    }

    // Unknown dependency references
    for (const dep of task.dependsOn ?? []) {
      if (!taskIds.has(dep)) {
        warn('unknown_dependency',
          `Task '${taskId}' depends_on '${dep}' which is not defined in this dag`,
          [taskId])
      }
    }

    // Unknown group reference
    if (task.group && groupIds.size > 0 && !groupIds.has(task.group)) {
      warn('unknown_group',
        `Task '${taskId}' references group '${task.group}' which is not defined in this dag`,
        [taskId])
    }

    // Sensor with no timeout (could run until OOM)
    if (task.poke && !task.sensorTimeout) {
      warn('sensor_no_timeout',
        `Sensor task '${taskId}' has no sensorTimeout — it will run until it succeeds or the process restarts`,
        [taskId])
    }
  }

  // Cycle detection (DFS)
  const visited = new Set<string>()
  const inStack = new Set<string>()
  const cycleTaskIds: string[] = []

  function dfs(id: string): boolean {
    if (inStack.has(id)) return true
    if (visited.has(id)) return false
    visited.add(id)
    inStack.add(id)
    for (const dep of dag.tasks[id]?.dependsOn ?? []) {
      if (taskIds.has(dep) && dfs(dep)) {
        if (!cycleTaskIds.includes(id)) cycleTaskIds.push(id)
        return true
      }
    }
    inStack.delete(id)
    return false
  }

  for (const id of taskIds) {
    if (!visited.has(id)) dfs(id)
  }

  if (cycleTaskIds.length > 0) {
    warn('circular_dependency',
      `Dag '${dag.id}' has a circular dependency involving tasks: ${cycleTaskIds.join(', ')}`,
      cycleTaskIds)
  }

  return warnings
}
