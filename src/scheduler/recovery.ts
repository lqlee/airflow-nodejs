import type { Db } from 'mongodb'

/**
 * On startup: reset any tasks/runs left in 'running' state from a previous
 * crashed/restarted process back to 'queued' so they get re-executed.
 */
export async function recoverOrphanedRuns(db: Db): Promise<void> {
  // Reset orphaned task_instances: running → queued
  const tiResult = await db.collection('task_instances').updateMany(
    { state: 'running' },
    {
      $set: { state: 'queued', started_at: null },
      $inc: { try_number: 1 },
    }
  )

  // Reset orphaned dag_runs: running → queued
  const drResult = await db.collection('dag_runs').updateMany(
    { state: 'running' },
    { $set: { state: 'queued' } }
  )

  if (tiResult.modifiedCount > 0 || drResult.modifiedCount > 0) {
    console.log(
      `[recovery] reset ${tiResult.modifiedCount} orphaned task(s) and ` +
      `${drResult.modifiedCount} orphaned run(s) → queued`
    )
  }
}
