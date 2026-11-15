/**
 * Keep an active run's task instances in line with the current Dag definition (Airflow's
 * DagRun.verify_integrity, removal half): instances of a task that is no longer in the Dag become
 * `removed` instead of running (or failing with "task not found"), and come back as `queued` if the
 * task reappears while the run is still active.
 *
 * Only waiting instances are touched — running and deferred ones finish what they are doing, and
 * finished ones keep their history. Tasks added to the Dag after the run was created are not added.
 */
import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'

const WAITING = ['queued', 'up_for_retry', 'up_for_reschedule']

export async function verifyRunIntegrity(db: Db, dagRunId: string, dag: DagDefinition): Promise<{ removed: number; restored: number }> {
  const taskIds = Object.keys(dag.tasks)
  const now = new Date()
  const removed = await db.collection('task_instances').updateMany(
    { dag_run_id: dagRunId, task_id: { $nin: taskIds }, state: { $in: WAITING } },
    { $set: { state: 'removed', ended_at: now, claimed_by: null } },
  )
  const restored = await db.collection('task_instances').updateMany(
    { dag_run_id: dagRunId, task_id: { $in: taskIds }, state: 'removed' },
    { $set: { state: 'queued', ended_at: null } },
  )
  if (removed.modifiedCount > 0) {
    console.log(`[scheduler] run ${dagRunId}: ${removed.modifiedCount} instance(s) of tasks no longer in Dag '${dag.id}' → removed`)
  }
  return { removed: removed.modifiedCount, restored: restored.modifiedCount }
}
