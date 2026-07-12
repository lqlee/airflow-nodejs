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
    queue = new Queue<TaskJobData>(QUEUE_NAME, {
      connection: createRedisConnection(),
      defaultJobOptions: {
        removeOnComplete: 100,   // keep last 100 completed jobs
        removeOnFail: 200,       // keep last 200 failed jobs
      },
    })
  }
  return queue
}

export async function enqueueTask(ti: TaskInstance, fn: string): Promise<void> {
  await getQueue().add(`${ti.dag_id}.${ti.task_id}`, { ti, fn }, {
    jobId: `${ti.dag_run_id}:${ti.task_id}:${ti.try_number}`,  // deduplicate
  })
}

export async function closeQueue(): Promise<void> {
  if (queue) {
    await queue.close()
    queue = null
  }
}
