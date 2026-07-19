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
  group?: string           // optional TaskGroup membership — label only, no scheduler impact
  retries?: number        // max retry attempts (default: 0 = no retries)
  retryDelay?: number     // ms to wait before requeuing (default: 0)
  timeout?: number        // ms before worker is killed and task marked failed (default: no timeout)
  run?: (ctx: TaskContext) => Promise<unknown>

  /**
   * Sensor mode: if present, this task polls a condition instead of running once.
   * Return true → task succeeds; return false → task requeues after pokeInterval.
   * `run` should be omitted for sensor tasks.
   */
  poke?: (ctx: TaskContext) => Promise<boolean>
  /** ms between poke attempts when poke() returns false. Default: 30 000 (30s). Min: 1 000. */
  pokeInterval?: number
  /** ms total deadline for sensor; exceeding it marks the task failed. Default: 3 600 000 (1h). */
  sensorTimeout?: number
}

/**
 * A named group of tasks. Tasks declare membership via `group: 'groupId'`.
 * Groups can declare dependencies on other groups — the loader expands these
 * into task-level `depends_on` edges before registration.
 */
export interface TaskGroupDefinition {
  /** Human-readable label shown in the UI */
  label?: string
  /** This group's tasks wait until all tasks in each listed group complete */
  dependsOn?: string[]
}

export interface DagDefinition {
  id: string
  schedule: string | null  // cron expression, or null for manual-only
  sla?: number             // ms — if a run hasn't completed within this window, an SLA alert is fired
  version?: string         // sha256[:12] of the dag source file — stamped by the loader
  tasks: Record<string, TaskDefinition>
  /** Optional TaskGroup definitions. Tasks opt-in via task.group = 'groupId'. */
  groups?: Record<string, TaskGroupDefinition>
  /**
   * Dataset URIs this dag PRODUCES when it completes successfully.
   * e.g. ['s3://bucket/users/', 'pg://mydb/orders']
   */
  outlets?: string[]
  /**
   * Dataset URIs this dag CONSUMES. The dag runs when ALL listed datasets have
   * received a new event since the last trigger (AND-semantics).
   * A dag with `datasets` keeps `schedule: null` — cron scheduling is ignored.
   */
  datasets?: string[]
}

/** Helper to define a Dag with full TypeScript inference */
export function dag(def: DagDefinition): DagDefinition {
  return def
}
