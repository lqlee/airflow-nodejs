import type { Db, WithId } from 'mongodb'
import type { DagDefinition, TriggerRule } from '../dag/types.js'
import { planExpansion, isLiteralMapped, isDynamicMapped, isMappedTask } from './mapping.js'
import { computeEffectiveWeights } from './weights.js'
import { recordEvent } from '../events/index.js'
import { createDeadlines, deadlineAlertsOf } from '../deadlines/index.js'
import { runInfoForLogicalDate, type DataInterval } from './data-interval.js'

function defaultDataInterval(dag: DagDefinition, logicalDate: Date): DataInterval {
  try {
    return runInfoForLogicalDate(dag, logicalDate).dataInterval
  } catch {
    return { start: logicalDate, end: logicalDate }   // unparsable schedule — a point interval
  }
}
import type { TiState } from './states.js'

export interface DagRun {
  dag_id: string
  dag_version: string | null   // sha256[:12] of the dag source file at run creation time
  logical_date: Date | null    // scheduled execution date; null for ad-hoc/manual runs
  /** The period of data the run covers (Airflow data_interval) — null when logical_date is null. See data-interval.ts. */
  data_interval_start?: Date | null
  data_interval_end?: Date | null
  /**
   * Trigger-time configuration (arbitrary JSON object from the caller).
   * Empty object for cron/dataset/backfill runs.
   */
  conf: Record<string, unknown>
  /** User-supplied tags for filtering/organisation. Default: []. */
  tags: string[]
  /** Free-text note added post-hoc. Null until updated via POST /dag-runs/:id/note. */
  note: string | null
  state: 'queued' | 'running' | 'success' | 'failed' | 'cancelled'
  created_at: Date
  /**
   * Position of this run in the Dag's timeline: logical_date, or created_at when absent.
   * Used by depends_on_past to find the previous run. Absent on legacy runs.
   */
  ordering_date?: Date
  /** Parent run/task when started by a TriggerDagRun task; null/absent otherwise. */
  triggered_by?: { dag_id: string; dag_run_id: string; task_id: string } | null
  /** When the run first moved queued → running (basis for runTimeout). Absent on legacy runs. */
  started_at?: Date | null
  /** When the run was (last) queued: creation, or the latest clear. Basis for queued_at deadlines. */
  queued_at?: Date
  /** Its deadlines were computed at creation (src/deadlines) — older runs get them on the next tick. */
  deadlines_set?: boolean
  /** How the run was triggered. */
  trigger_type: 'manual' | 'cron' | 'backfill' | 'dataset' | 'timetable' | 'catchup' | 'triggered'
  /** ID of the parent backfill entity, or null for non-backfill runs. */
  backfill_id: string | null
}

export interface TaskInstance {
  dag_run_id: string        // stringified ObjectId of parent dag_run
  dag_id: string
  task_id: string
  group_id: string | null   // TaskGroup membership label; null for ungrouped tasks
  /** Resource pool name; null means no pool constraint (global semaphore only). */
  pool: string | null
  /** 0-based fan-out index for mapped tasks; null for non-mapped tasks */
  map_index: number | null
  /** Per-instance input value for mapped tasks; null for non-mapped tasks */
  map_value: unknown
  state: TiState
  depends_on: string[]
  /** Trigger rule — controls when this task runs. Default: 'all_success'. */
  trigger_rule: TriggerRule
  /** Priority weight — higher values claimed first. Default: 0. */
  priority: number
  try_number: number
  max_retries: number       // max allowed retries (0 = no retries)
  retry_delay: number       // ms to wait before requeue
  /** up_for_retry: earliest time the next try may be claimed. */
  next_retry_at?: Date | null
  /** true = wait for this task to have succeeded/skipped in the previous run. */
  depends_on_past?: boolean
  /** Backoff multiplier (<= 1 or absent = fixed delay). */
  retry_backoff?: number
  /** Cap in ms for backed-off delay (0/absent = uncapped). */
  max_retry_delay?: number
  /** Pool slots held while running; absent on legacy docs = 1. */
  pool_slots?: number
  /** Max concurrently running instances of this task across the Dag's runs (0/absent = unlimited). */
  max_active_tis?: number
  /** Max concurrently running instances of this task within this run (0/absent = unlimited). */
  max_active_tis_per_run?: number
  timeout_ms: number        // 0 = no timeout; >0 = kill worker after this many ms
  started_at: Date | null
  ended_at: Date | null
  error: string | null
  created_at: Date
  // Sensor fields (null for regular tasks)
  is_sensor: boolean        // true if this task has a poke function
  poke_interval_ms: number  // ms between poke attempts (0 for non-sensors)
  sensor_timeout_ms: number // total deadline in ms (0 = no timeout for non-sensors)
  first_poked_at: Date | null  // when poke was first invoked — NOT started_at (claim overwrites it)
  next_poke_at: Date | null    // earliest time for the next poke; null = ready immediately
  poke_count: number        // number of poke() calls made so far
  /**
   * Deferral fields — set when a run: task calls ctx.defer().
   * The trigger fn (serialized) is polled on each scheduler tick.
   * While deferred the worker slot is freed; deferred is non-terminal.
   */
  deferred_trigger_fn: string | null    // serialized trigger function
  deferred_at: Date | null              // when defer() was called
  defer_timeout_ms: number              // 0 = no deadline; matches task-level timeout
  /** true if this task is a branch task (has a branch: fn) */
  is_branch: boolean
  /** true if this task is a ShortCircuit task (has a shortCircuit: fn) */
  is_short_circuit?: boolean
  /** Setup task — a teardown's `all_done_setup_success` rule looks at these upstreams. */
  is_setup?: boolean
  /** Teardown task — always runs once upstreams finish; its failure doesn't fail the run unless on_failure_fail_dagrun. */
  is_teardown?: boolean
  on_failure_fail_dagrun?: boolean
  /** Run started by a TriggerDagRun task, polled while the task is deferred. */
  triggered_run_id?: string | null
  /**
   * true if this instance is a placeholder for a dynamic-expand task.
   * Placeholder blocks run completion until the source XCom is available;
   * replaced by real instances when the source task succeeds.
   */
  is_dynamic_placeholder: boolean
  /**
   * For dynamic-expand tasks: the source XCom reference { from, key }.
   * Null for literal-expand and non-mapped tasks.
   */
  dynamic_expand_source: { from: string; key: string } | null
  // HITL fields
  is_hitl: boolean                              // true = task requires human approval before executing
  hitl_state: 'pending' | 'approved' | 'rejected' | null  // null for non-HITL tasks
  hitl_prompt: string | null                    // optional prompt text for the approver
  hitl_note: string | null                      // human-supplied note when responding
  hitl_responded_at: Date | null                // when the response was recorded
  /** INSTANCE_ID of the process that claimed the running try (zombie detection). Absent on legacy rows. */
  claimed_by?: string | null
  /** A result whose task callbacks the scheduler has yet to fire (callbacks.ts). */
  callback_pending?: 'success' | 'failed' | 'retry' | null
  /** For a pending retry callback: the attempt that failed (1-based). */
  callback_try?: number
}

export interface CreateRunOptions {
  /** Logical execution date (backfill). Defaults to now (ad-hoc / manual trigger). */
  logicalDate?: Date
  /** Data interval; default derived from logicalDate and the Dag's scheduleMode (data-interval.ts). */
  dataInterval?: DataInterval
  /** Trigger-time configuration (arbitrary JSON object). Defaults to {}. */
  conf?: Record<string, unknown>
  /** Tags for filtering. Defaults to []. */
  tags?: string[]
  /**
   * Source that triggered this run — used for audit metadata only.
   * 'manual' = POST /trigger; 'cron' = scheduled; 'backfill' = backfill;
   * 'dataset' = data-aware scheduling. Defaults to 'manual'.
   */
  triggerType?: 'manual' | 'cron' | 'backfill' | 'dataset' | 'timetable' | 'catchup' | 'triggered'
  /** Set by a TriggerDagRun task — the parent run/task that started this run. */
  triggeredBy?: { dag_id: string; dag_run_id: string; task_id: string }
  /** Links this run to a backfill entity for lifecycle management. */
  backfillId?: string
}

/** Positive integer or 0 (= unlimited). */
function positiveInt(n: number | undefined): number {
  return n !== undefined && Number.isFinite(n) && n >= 1 ? Math.floor(n) : 0
}

/** A run for this (dag_id, logical_date) already exists — another scheduler or a backfill made it. */
export class DuplicateRunError extends Error {
  constructor(readonly dagId: string, readonly logicalDate: Date) {
    super(`Dag '${dagId}' already has a run for ${logicalDate.toISOString()}`)
    this.name = 'DuplicateRunError'
  }
}

/**
 * Create a dag_run + one task_instance per task for the given Dag.
 * Returns the run id (string).
 * Throws DuplicateRunError when `logicalDate` is set and the Dag already has a run for it.
 */
export async function createRun(db: Db, dag: DagDefinition, opts: CreateRunOptions = {}): Promise<string> {
  const now = new Date()
  const dataInterval = opts.dataInterval ?? (opts.logicalDate ? defaultDataInterval(dag, opts.logicalDate) : null)

  // Insert dag_run — stamp the dag version so every run records which code ran it
  const runResult = await db.collection<DagRun>('dag_runs').insertOne({
    dag_id: dag.id,
    dag_version: dag.version ?? null,
    logical_date: opts.logicalDate ?? null,
    data_interval_start: dataInterval?.start ?? null,
    data_interval_end: dataInterval?.end ?? null,
    conf: opts.conf ?? {},
    tags: opts.tags ?? [],
    note: null,
    state: 'queued',
    trigger_type: opts.triggerType ?? 'manual',
    created_at: now,
    queued_at: now,
    deadlines_set: true,
    ordering_date: opts.logicalDate ?? now,
    triggered_by: opts.triggeredBy ?? null,
    backfill_id: opts.backfillId ?? null,
  }).catch((err: unknown) => {
    // Unique (dag_id, logical_date) index — see ensureIndexes()
    if (opts.logicalDate && (err as { code?: number }).code === 11000) throw new DuplicateRunError(dag.id, opts.logicalDate)
    throw err
  })
  const runId = runResult.insertedId.toString()

  const MIN_POKE_INTERVAL_MS = 1_000
  const DEFAULT_POKE_INTERVAL_MS = 30_000
  const DEFAULT_SENSOR_TIMEOUT_MS = 3_600_000  // 1 hour

  // Insert one task_instance per task (or N instances for mapped tasks)
  const taskDocs: TaskInstance[] = []
  const weights = computeEffectiveWeights(dag.tasks)

  for (const [taskId, task] of Object.entries(dag.tasks)) {
    const isSensor = typeof task.poke === 'function'
    const isBranch = typeof task.branch === 'function'
    const isDynamic = isDynamicMapped(task.expand)

    // For dynamic-expand tasks: insert one placeholder instance to block run completion.
    // The placeholder is replaced by real instances when the source XCom is available.
    // For literal expand: insert N instances immediately.
    // For non-mapped tasks: insert one instance.
    const instances = isDynamic
      ? [{ map_index: null as number | null, map_value: null as unknown }]  // placeholder
      : isLiteralMapped(task.expand)
        ? planExpansion(task.expand)
        : [{ map_index: null as number | null, map_value: null as unknown }]

    // dynamic_expand_source: only set for the placeholder instance
    const dynamicExpandSource = isDynamic
      ? { from: (task.expand as { from: string; key: string }).from, key: (task.expand as { from: string; key: string }).key }
      : null

    for (const { map_index, map_value } of instances) {
      taskDocs.push({
        dag_run_id: runId,
        dag_id: dag.id,
        task_id: taskId,
        group_id: task.group ?? null,
        pool: task.pool ?? null,
        map_index,
        map_value,
        state: 'queued',
        depends_on: task.dependsOn ?? [],
        trigger_rule: task.triggerRule ?? (task.teardown === true ? 'all_done_setup_success' : 'all_success'),
        priority: weights.get(taskId) ?? 0,
        try_number: 0,
        max_retries: task.retries ?? 0,
        retry_delay: task.retryDelay ?? 0,
        depends_on_past: task.dependsOnPast === true,
        retry_backoff: task.retryExponentialBackoff ?? 0,
        max_retry_delay: task.maxRetryDelay ?? 0,
        pool_slots: Math.max(1, Math.floor(task.poolSlots ?? 1)),
        max_active_tis: positiveInt(task.maxActiveTis),
        max_active_tis_per_run: positiveInt(task.maxActiveTisPerRun),
        timeout_ms: task.timeout ?? 0,
        started_at: null,
        ended_at: null,
        error: null,
        created_at: now,
        is_sensor: isSensor,
        poke_interval_ms: isSensor
          ? Math.max(MIN_POKE_INTERVAL_MS, task.pokeInterval ?? DEFAULT_POKE_INTERVAL_MS)
          : 0,
        sensor_timeout_ms: isSensor ? (task.sensorTimeout ?? DEFAULT_SENSOR_TIMEOUT_MS) : 0,
        first_poked_at: null,
        next_poke_at: null,
        poke_count: 0,
        deferred_trigger_fn: null,
        deferred_at: null,
        defer_timeout_ms: task.timeout ?? 0,
        is_branch: isBranch,
        is_short_circuit: typeof task.shortCircuit === 'function',
        is_setup: task.setup === true,
        is_teardown: task.teardown === true,
        on_failure_fail_dagrun: task.teardown === true && task.onFailureFailDagrun === true,
        is_dynamic_placeholder: isDynamic,
        dynamic_expand_source: dynamicExpandSource,
        is_hitl: task.requiresApproval === true,
        hitl_state: task.requiresApproval === true ? 'pending' : null,
        hitl_prompt: task.hitlPrompt ?? null,
        hitl_note: null,
        hitl_responded_at: null,
      })
    }
  }

  await db.collection<TaskInstance>('task_instances').insertMany(taskDocs)

  if (deadlineAlertsOf(dag).length > 0) {
    await createDeadlines(db, dag, { _id: runResult.insertedId, dag_id: dag.id, logical_date: opts.logicalDate ?? null, queued_at: now, created_at: now }, { now })
  }

  console.log(`[scheduler] created run ${runId} for dag '${dag.id}' with ${taskDocs.length} tasks`)

  // Audit — fire-and-forget; never blocks or throws on the caller
  void recordEvent(db, 'run_triggered', {
    dag_id: dag.id,
    dag_run_id: runId,
    metadata: {
      trigger_type: opts.triggerType ?? 'manual',
      tags: opts.tags ?? [],
      logical_date: opts.logicalDate ?? null,
    },
  })

  return runId
}

export const RUN_STATES = ['queued', 'running', 'success', 'failed', 'cancelled'] as const satisfies readonly DagRun['state'][]

/** Values accepted by GET /dags?last_run_state= — a run state, or `none` for Dags that never ran. */
export const LAST_RUN_STATES = [...RUN_STATES, 'none'] as const

/** The newest run of a Dag, as shown on its card and used by the Dag list's last-run filter. */
export interface LastRun {
  run_id: string
  state: DagRun['state']
  created_at: Date
  logical_date: Date | null
}

/**
 * Newest run per Dag. One indexed lookup per Dag (dag_id, created_at) rather than an aggregation:
 * the Dag list is polled, and a $sort/$group over dag_runs would read every run on Spanner.
 */
export async function getLatestRuns(db: Db, dagIds: string[]): Promise<Map<string, LastRun>> {
  const runs = await Promise.all(dagIds.map(dagId => db.collection<DagRun>('dag_runs').findOne(
    { dag_id: dagId },
    { sort: { created_at: -1, _id: -1 }, projection: { state: 1, created_at: 1, logical_date: 1 } },
  )))
  const latest = new Map<string, LastRun>()
  runs.forEach((r, i) => {
    if (r) latest.set(dagIds[i], { run_id: r._id.toString(), state: r.state, created_at: r.created_at, logical_date: r.logical_date ?? null })
  })
  return latest
}
