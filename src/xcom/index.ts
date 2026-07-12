import { MongoClient, type Db } from 'mongodb'

export interface XComRecord {
  dag_run_id: string
  dag_id: string
  task_id: string       // task that pushed the value
  key: string
  value: unknown
  pushed_at: Date
}

/**
 * Push a value into XCom for a given task + key.
 * Upserts — pushing the same key twice overwrites the previous value.
 */
export async function xcomPush(
  db: Db,
  dagRunId: string,
  dagId: string,
  taskId: string,
  key: string,
  value: unknown
): Promise<void> {
  await db.collection<XComRecord>('xcoms').updateOne(
    { dag_run_id: dagRunId, task_id: taskId, key },
    {
      $set: {
        dag_run_id: dagRunId,
        dag_id: dagId,
        task_id: taskId,
        key,
        value,
        pushed_at: new Date(),
      },
    },
    { upsert: true }
  )
}

/**
 * Pull a value from XCom pushed by a specific upstream task.
 * Returns undefined if not found.
 */
export async function xcomPull(
  db: Db,
  dagRunId: string,
  fromTaskId: string,
  key: string
): Promise<unknown> {
  const doc = await db.collection<XComRecord>('xcoms').findOne({
    dag_run_id: dagRunId,
    task_id: fromTaskId,
    key,
  })
  return doc?.value
}
