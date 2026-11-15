import { Queue } from 'bullmq'
import { createRedisConnection, QUEUE_NAME } from './connection.js'
import type { TaskInstance } from '../scheduler/runs.js'

export interface TaskJobData {
  ti: TaskInstance & { _id?: unknown }  // task instance fields
  fn: string                            // serialized task function
}

let queue: Queue<TaskJobData> | null = null

export function getQueue(): Queue<TaskJobData> {
  if (!queue) {
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    queue = new Queue<TaskJobData>(QUEUE_NAME, {
      connection: createRedisConnection() as any,
      defaultJobOptions: {
        removeOnComplete: 100,   // keep last 100 completed jobs
        removeOnFail: 200,       // keep last 200 failed jobs
      },
    })
  }
  return queue
}

/** One job per try of an instance (BullMQ drops a duplicate id) — map_index keeps mapped instances apart. */
export function jobIdFor(ti: Pick<TaskInstance, 'dag_run_id' | 'task_id' | 'map_index' | 'try_number'>): string {
  return `${ti.dag_run_id}:${ti.task_id}:${ti.map_index ?? -1}:${ti.try_number}`
}

export async function enqueueTask(ti: TaskInstance, fn: string): Promise<void> {
  await getQueue().add(`${ti.dag_id}.${ti.task_id}`, { ti, fn }, { jobId: jobIdFor(ti) })
}

export async function closeQueue(): Promise<void> {
  if (queue) {
    await queue.close()
    queue = null
  }
}
