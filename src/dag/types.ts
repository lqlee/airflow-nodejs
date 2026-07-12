export interface XComHelper {
  /** Push a value under key — available to downstream tasks via pull() */
  push: (key: string, value: unknown) => Promise<void>
  /** Pull a value pushed by an upstream task */
  pull: (fromTaskId: string, key: string) => Promise<unknown>
}

export interface TaskContext {
  dagId: string
  runId: string
  taskId: string
  xcom: XComHelper
}

export interface TaskDefinition {
  dependsOn?: string[]
  run: (ctx: TaskContext) => Promise<unknown>
}

export interface DagDefinition {
  id: string
  schedule: string | null  // cron expression, or null for manual-only
  tasks: Record<string, TaskDefinition>
}

/** Helper to define a Dag with full TypeScript inference */
export function dag(def: DagDefinition): DagDefinition {
  return def
}
