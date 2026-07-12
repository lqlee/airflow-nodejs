import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'

/**
 * Atomically claim the next queued task whose upstream deps are all done.
 * Uses findOneAndUpdate — MongoDB's atomic document-level equivalent of
 * PostgreSQL's SELECT ... FOR UPDATE SKIP LOCKED.
 *
 * Returns the claimed task, or null if nothing is available.
 */
export async function claimNextTask(db: Db, dagRunId: string): Promise<TaskInstance | null> {
  // Find task_ids that are already successful in this run
  const doneTasks = await db
    .collection<TaskInstance>('task_instances')
    .find({ dag_run_id: dagRunId, state: 'success' })
    .project({ task_id: 1 })
    .toArray()
  const doneIds = doneTasks.map(t => t.task_id)

  // Atomically claim a queued task whose every dep is in doneIds
  const claimed = await db.collection<TaskInstance>('task_instances').findOneAndUpdate(
    {
      dag_run_id: dagRunId,
      state: 'queued',
      // All upstream deps must be in the done set (or no deps at all)
      $or: [
        { depends_on: { $size: 0 } },
        { depends_on: { $not: { $elemMatch: { $nin: doneIds } } } },
      ],
    },
    { $set: { state: 'running', started_at: new Date() } },
    { sort: { created_at: 1 }, returnDocument: 'after' }
  )

  return claimed ?? null
}
