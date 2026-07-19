import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'

/**
 * Atomically claim ALL currently-ready queued tasks for a run.
 * Each findOneAndUpdate is atomic — safe for concurrent schedulers.
 * Returns an array of claimed tasks (may be empty).
 */
export async function claimReadyTasks(db: Db, dagRunId: string): Promise<TaskInstance[]> {
  const doneTasks = await db
    .collection<TaskInstance>('task_instances')
    .find({ dag_run_id: dagRunId, state: 'success' })
    .project({ task_id: 1 })
    .toArray()
  const doneIds = doneTasks.map(t => t.task_id)

  const now = new Date()
  const filter = {
    dag_run_id: dagRunId,
    state: 'queued',
    // Sensor poke gate: next_poke_at must be null (never poked) or <= now.
    // Use $and so the dependency $or is preserved as a sibling condition.
    $and: [
      {
        $or: [
          { next_poke_at: null },
          { next_poke_at: { $lte: now } },
        ],
      },
      {
        $or: [
          { depends_on: { $size: 0 } },
          { depends_on: { $not: { $elemMatch: { $nin: doneIds } } } },
        ],
      },
    ],
  }

  // Drain all claimable tasks — each claim is atomic
  const claimed: TaskInstance[] = []
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const task = await db.collection<TaskInstance>('task_instances').findOneAndUpdate(
      filter,
      { $set: { state: 'running', started_at: new Date() } },
      { sort: { created_at: 1 }, returnDocument: 'after' }
    )
    if (!task) break
    claimed.push(task)
  }

  return claimed
}

// Keep old single-claim export for backward compatibility with tests
export async function claimNextTask(db: Db, dagRunId: string): Promise<TaskInstance | null> {
  const results = await claimReadyTasks(db, dagRunId)
  return results[0] ?? null
}
