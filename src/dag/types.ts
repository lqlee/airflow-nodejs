export interface TaskContext {
  dagId: string
  runId: string
  taskId: string
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
