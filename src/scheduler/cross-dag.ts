/**
 * Cross-Dag tasks: TriggerDagRun and ExternalTaskSensor.
 *
 * These run in the scheduler process (they only read/write run metadata), so this module
 * is DB-only logic returning outcomes; executor.ts applies the task-state transitions.
 */
import { ObjectId, type Db } from 'mongodb'
import { getDag } from '../dag/registry.js'
import { createRun, type TaskInstance } from './runs.js'
import type { TaskDefinition } from '../dag/types.js'

export type WaitResult =
  | { status: 'done' }
  | { status: 'pending' }
  | { status: 'failed'; error: string }

type TriggerCfg = NonNullable<TaskDefinition['triggerDag']>
type ExternalCfg = NonNullable<TaskDefinition['externalTask']>

/** Create the triggered run. Returns its id, or an error message when the trigger is invalid. */
export async function triggerDagRun(
  db: Db,
  ti: TaskInstance,
  cfg: TriggerCfg,
): Promise<{ runId: string } | { error: string }> {
  if (cfg.dagId === ti.dag_id) return { error: `Dag '${ti.dag_id}' cannot trigger itself` }
  const target = getDag(cfg.dagId)
  if (!target) return { error: `Cannot trigger unknown Dag '${cfg.dagId}'` }

  const runId = await createRun(db, target, {
    conf: cfg.conf ?? {},
    triggerType: 'triggered',
    triggeredBy: { dag_id: ti.dag_id, dag_run_id: ti.dag_run_id, task_id: ti.task_id },
  })
  return { runId }
}

/** TriggerDagRun with waitForCompletion: terminal state of the triggered run decides the outcome. */
export async function checkTriggeredRun(db: Db, runId: string | null | undefined): Promise<WaitResult> {
  if (!runId || !ObjectId.isValid(runId)) return { status: 'failed', error: 'Triggered run id is missing' }
  const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
  if (!run) return { status: 'failed', error: `Triggered run ${runId} no longer exists` }
  if (run.state === 'success') return { status: 'done' }
  if (run.state === 'failed' || run.state === 'cancelled') {
    return { status: 'failed', error: `Triggered run ${runId} ${run.state}` }
  }
  return { status: 'pending' }
}

/** Validate an ExternalTaskSensor target up front so a typo fails fast instead of timing out. */
export function validateExternalTask(cfg: ExternalCfg): string | null {
  const dag = getDag(cfg.dagId)
  if (!dag) return `ExternalTaskSensor: Dag '${cfg.dagId}' is not registered`
  if (cfg.taskId && !dag.tasks[cfg.taskId]) {
    return `ExternalTaskSensor: task '${cfg.taskId}' not found in Dag '${cfg.dagId}'`
  }
  return null
}

/** One poke of an ExternalTaskSensor. */
export async function checkExternalTask(db: Db, ti: TaskInstance, cfg: ExternalCfg): Promise<WaitResult> {
  const invalid = validateExternalTask(cfg)
  if (invalid) return { status: 'failed', error: invalid }

  const allowed = new Set(cfg.allowedStates ?? ['success'])
  const failed = new Set(cfg.failedStates ?? [])

  const filter: Record<string, unknown> = { dag_id: cfg.dagId }
  if (cfg.match === 'logical_date') {
    const own = await db.collection('dag_runs').findOne({ _id: new ObjectId(ti.dag_run_id) })
    if (!own?.logical_date) {
      return { status: 'failed', error: "ExternalTaskSensor match 'logical_date' needs this run to have a logical_date" }
    }
    filter['logical_date'] = own.logical_date
  }

  const run = await db.collection('dag_runs').findOne(filter, { sort: { created_at: -1, _id: -1 } })
  if (!run) return { status: 'pending' }

  const states: string[] = cfg.taskId
    ? (await db.collection('task_instances')
        .find({ dag_run_id: run._id.toString(), task_id: cfg.taskId }, { projection: { state: 1 } })
        .toArray()).map(t => t.state as string)
    : [run.state as string]
  if (states.length === 0) return { status: 'pending' }

  const bad = states.find(s => failed.has(s))
  if (bad) {
    const what = cfg.taskId ? `${cfg.dagId}.${cfg.taskId}` : cfg.dagId
    return { status: 'failed', error: `External ${what} is '${bad}'` }
  }
  return states.every(s => allowed.has(s)) ? { status: 'done' } : { status: 'pending' }
}
