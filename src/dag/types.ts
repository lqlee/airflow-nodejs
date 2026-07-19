export interface XComHelper {
  /** Push a value under key — available to downstream tasks via pull() */
  push: (key: string, value: unknown) => Promise<void>
  /**
   * Pull a value from an upstream task.
   * - Non-mapped task: returns the single pushed value.
   * - Mapped task: returns an array of all instances' values ordered by map_index.
   */
  pull: (fromTaskId: string, key: string) => Promise<unknown>
}

export interface ConnectionHelper {
  /** Retrieve a connection by conn_id. Returns null if not found. Decrypts in worker. */
  get: (connId: string) => Promise<{
    conn_id: string
    conn_type: string
    host: string | null
    port: number | null
    schema: string | null
    login: string | null
    password: string | null
    extra: Record<string, unknown> | null
  } | null>
}

export interface VariableHelper {
  /** Retrieve a variable value by key. Returns null if not found. Decrypts secrets in worker. */
  get: (key: string) => Promise<string | null>
}

export interface TaskContext {
  dagId: string
  runId: string
  taskId: string
  /** For mapped task instances: the 0-based index of this instance. Null for non-mapped tasks. */
  mapIndex: number | null
  /** For mapped task instances: the input value for this instance. Null for non-mapped tasks. */
  mapValue: unknown
  /**
   * Trigger-time configuration passed by the caller via POST /dags/:id/trigger.
   * Empty object for scheduled/backfill runs (no caller-supplied conf).
   * Read-only — tasks should not mutate this object.
   */
  conf: Record<string, unknown>
  xcom: XComHelper
  connections: ConnectionHelper
  variables: VariableHelper
}

export interface TaskDefinition {
  dependsOn?: string[]
  group?: string           // optional TaskGroup membership — label only, no scheduler impact
  retries?: number        // max retry attempts (default: 0 = no retries)
  retryDelay?: number     // ms to wait before requeuing (default: 0)
  timeout?: number        // ms before worker is killed and task marked failed (default: no timeout)
  run?: (ctx: TaskContext) => Promise<unknown>
  /**
   * Literal expand (Branch A): fan out this task over a static array of values.
   * One task_instance is created per value at run-creation time.
   * ctx.mapIndex (0-based) and ctx.mapValue are injected into each instance.
   * Downstream tasks that depend_on a mapped task wait for ALL instances to succeed.
   *
   * Branch B (XCom-driven dynamic expand) is a planned future extension.
   */
  expand?: unknown[]

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

  /**
   * URL to POST to when a run completes successfully.
   * Payload: { dag_id, run_id, state: 'success', logical_date, conf, tags, ended_at }
   * Delivery is fire-and-forget with a 5s timeout — failures are logged, not retried.
   * Only trusted authors can set this; never accept caller-supplied URLs at trigger time.
   */
  onSuccess?: string

  /**
   * URL to POST to when a run fails (any task failed).
   * Same payload shape as onSuccess, with state: 'failed'.
   */
  onFailure?: string
}

/** Helper to define a Dag with full TypeScript inference */
export function dag(def: DagDefinition): DagDefinition {
  return def
}
