import { type Db } from 'mongodb'

export interface XComRecord {
  dag_run_id: string
  dag_id: string
  task_id: string       // task that pushed the value
  /** null for non-mapped tasks; 0-based index for mapped task instances */
  map_index: number | null
  key: string
  value: unknown
  pushed_at: Date
}

/**
 * Push a value into XCom for a given task instance + key.
 * Upserts — pushing the same key from the same (task_id, map_index) overwrites.
 * map_index disambiguates instances of mapped tasks (null for non-mapped tasks).
 */
export async function xcomPush(
  db: Db,
  dagRunId: string,
  dagId: string,
  taskId: string,
  mapIndex: number | null,
  key: string,
  value: unknown,
): Promise<void> {
  await db.collection<XComRecord>('xcoms').updateOne(
    { dag_run_id: dagRunId, task_id: taskId, map_index: mapIndex ?? null, key },
    {
      $set: {
        dag_run_id: dagRunId,
        dag_id: dagId,
        task_id: taskId,
        map_index: mapIndex ?? null,
        key,
        value,
        pushed_at: new Date(),
      },
    },
    { upsert: true },
  )
}

/**
 * Pull a value from XCom pushed by an upstream task.
 *
 * - Non-mapped upstream task (map_index null): returns the single value.
 * - Mapped upstream task (multiple map_index values): returns an array of
 *   all instances' values ordered by map_index ascending (auto-collect semantic).
 *   Returns null if no matching entries exist.
 */
export async function xcomPull(
  db: Db,
  dagRunId: string,
  fromTaskId: string,
  key: string,
): Promise<unknown> {
  const docs = await db
    .collection<XComRecord>('xcoms')
    .find({ dag_run_id: dagRunId, task_id: fromTaskId, key })
    .sort({ map_index: 1 })
    .toArray()

  if (docs.length === 0) return undefined
  // Non-mapped: single doc with null map_index → return value directly
  if (docs.length === 1 && docs[0].map_index === null) return docs[0].value
  // Mapped: multiple instances → return ordered list of values
  return docs.map(d => d.value)
}
