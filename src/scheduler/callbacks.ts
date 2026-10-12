import { getDag } from '../dag/registry.js'
import type { TaskInstance } from './runs.js'

const CALLBACK_TIMEOUT_MS = 10_000

/**
 * Fire the task's `onRetry` callback for the attempt that just failed.
 * Fire-and-forget: never throws, never blocks the retry; a hung callback is abandoned after 10s.
 * Returns the in-flight promise so tests can await it.
 */
export function fireRetryCallback(ti: TaskInstance, error: string): Promise<void> {
  const cb = getDag(ti.dag_id)?.tasks[ti.task_id]?.onRetry
  if (typeof cb !== 'function') return Promise.resolve()

  const info = {
    dagId: ti.dag_id,
    runId: ti.dag_run_id,
    taskId: ti.task_id,
    mapIndex: ti.map_index ?? null,
    tryNumber: ti.try_number + 1,
    maxRetries: ti.max_retries,
    error,
  }
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out after ${CALLBACK_TIMEOUT_MS}ms`)), CALLBACK_TIMEOUT_MS)
  })
  return Promise.race([Promise.resolve().then(() => cb(info)), timeout])
    .catch(err => console.warn(`[callback] onRetry ${ti.dag_id}.${ti.task_id} failed: ${err instanceof Error ? err.message : String(err)}`))
    .finally(() => clearTimeout(timer))
}
