/**
 * Task and Dag callbacks (Airflow on_success / on_failure / on_retry / on_execute callbacks, notifiers).
 *
 * A callback is a notifier: a function, an object with `notify(info)`, or a webhook URL (POSTed as JSON).
 * Callbacks are fire-and-forget with a 10s timeout — errors are logged and never change a task or run.
 *
 * Task results are written by many processes (local executor, BullMQ workers, Kubernetes, the zombie
 * reaper), most of which do not hold the Dag's functions. So the write only flags the instance
 * (`callback_pending: 'success' | 'failed' | 'retry'`, see ti-writes.ts) and the scheduler fires the
 * callback: flushTaskCallbacks clears each flag with a compare-and-set first, so exactly one replica fires it.
 */
import type { Db } from 'mongodb'
import { getDag } from '../dag/registry.js'
import type { Notifier, TaskCallbackInfo, TaskDefinition } from '../dag/types.js'
import { deliverWebhook } from '../webhooks/index.js'
import type { TaskInstance } from './runs.js'

export const CALLBACK_TIMEOUT_MS = 10_000

export type CallbackKind = 'success' | 'failed' | 'retry'
export const CALLBACK_KINDS: readonly CallbackKind[] = ['success', 'failed', 'retry']

const TASK_HOOK = { success: 'onSuccess', failed: 'onFailure', retry: 'onRetry', running: 'onExecute' } as const
type TaskHook = keyof typeof TASK_HOOK

function toList<T>(v: T | T[] | undefined): T[] {
  return v === undefined ? [] : Array.isArray(v) ? v : [v]
}

const message = (err: unknown) => err instanceof Error ? err.message : String(err)

/** Run one notifier with the timeout. Never throws. `webhookBody` is what a URL notifier receives. */
export function notify<I extends object>(target: Notifier<I>, info: I, label: string, webhookBody: object = info): Promise<void> {
  let timer: NodeJS.Timeout | undefined
  const timeout = new Promise<never>((_, reject) => {
    timer = setTimeout(() => reject(new Error(`timed out after ${CALLBACK_TIMEOUT_MS}ms`)), CALLBACK_TIMEOUT_MS)
  })
  const call = Promise.resolve().then(async () => {
    if (typeof target === 'function') return target(info)
    if (typeof target === 'string') {
      const status = await deliverWebhook(target, webhookBody as never)
      if (status < 200 || status >= 300) throw new Error(`${target} → HTTP ${status}`)
      return
    }
    if (target && typeof target.notify === 'function') return target.notify(info)
    throw new Error('not a function, URL or { notify } object')
  })
  return Promise.race([call, timeout])
    .then(() => undefined, err => console.warn(`[callback] ${label} failed: ${message(err)}`))
    .finally(() => clearTimeout(timer))
}

/** Run every notifier of a callback setting in parallel. Never throws. */
export async function notifyAll<I extends object>(
  setting: Notifier<I> | Notifier<I>[] | undefined, info: I, label: string, webhookBody?: object,
): Promise<void> {
  await Promise.all(toList(setting).map(t => notify(t, info, label, webhookBody)))
}

function taskInfo(ti: TaskInstance, hook: TaskHook): TaskCallbackInfo {
  return {
    dagId: ti.dag_id,
    runId: ti.dag_run_id,
    taskId: ti.task_id,
    mapIndex: ti.map_index ?? null,
    // retry: try_number already moved past the failed attempt, which the write recorded in callback_try
    tryNumber: hook === 'retry' ? (ti.callback_try ?? ti.try_number) : ti.try_number + 1,
    maxRetries: ti.max_retries,
    state: hook === 'retry' ? 'up_for_retry' : hook,
    error: hook === 'failed' || hook === 'retry' ? (ti.error ?? '') : null,
  }
}

const webhookBodyFor = (info: TaskCallbackInfo) => ({
  event: `task_${info.state}`,
  dag_id: info.dagId, run_id: info.runId, task_id: info.taskId, map_index: info.mapIndex,
  try_number: info.tryNumber, max_retries: info.maxRetries, state: info.state, error: info.error,
})

function fireTask(task: TaskDefinition | undefined, hook: TaskHook, ti: TaskInstance): Promise<void> {
  const prop = TASK_HOOK[hook]
  const setting = task?.[prop] as Notifier<TaskCallbackInfo> | Notifier<TaskCallbackInfo>[] | undefined
  if (!setting) return Promise.resolve()
  const info = taskInfo(ti, hook)
  return notifyAll(setting, info, `${prop} ${ti.dag_id}.${ti.task_id}`, webhookBodyFor(info))
}

/** Fire the task's `onRetry` callbacks for the attempt that just failed (`ti` as it was while running). */
export function fireRetryCallback(ti: TaskInstance, error: string): Promise<void> {
  return fireTask(getDag(ti.dag_id)?.tasks[ti.task_id], 'retry', { ...ti, error, callback_try: ti.try_number + 1 })
}

/** onExecute for instances the scheduler just claimed. Returns the in-flight callbacks. */
export function fireExecuteCallbacks(claimed: TaskInstance[]): Promise<void>[] {
  return claimed
    .map(ti => ({ ti, task: getDag(ti.dag_id)?.tasks[ti.task_id] }))
    .filter(({ task }) => task?.onExecute)
    .map(({ ti, task }) => fireTask(task, 'running', ti))
}

function hasCallback(ti: TaskInstance): boolean {
  return !!getDag(ti.dag_id)?.tasks[ti.task_id]?.[TASK_HOOK[ti.callback_pending as CallbackKind]]
}

/**
 * Fire the callbacks flagged on task instances (of one run, or all runs). Each flag is cleared with a
 * compare-and-set before its callback fires, so concurrent ticks and replicas fire it once.
 * Returns the in-flight callbacks so tests can await them.
 */
export async function flushTaskCallbacks(db: Db, opts: { runId?: string; limit?: number } = {}): Promise<Promise<void>[]> {
  const coll = db.collection<TaskInstance>('task_instances')
  const filter = { callback_pending: { $in: [...CALLBACK_KINDS] }, ...(opts.runId ? { dag_run_id: opts.runId } : {}) }
  const pending = await coll.find(filter as never).limit(opts.limit ?? 200).toArray()
  if (pending.length === 0) return []

  // Flags with no callback to fire (the common case) are dropped in one write
  const silent = pending.filter(ti => !hasCallback(ti))
  if (silent.length > 0) {
    await coll.updateMany({ _id: { $in: silent.map(ti => ti._id) } } as never, { $set: { callback_pending: null } })
  }

  const fired: Promise<void>[] = []
  for (const ti of pending.filter(hasCallback)) {
    const kind = ti.callback_pending as CallbackKind
    const won = await coll.findOneAndUpdate({ _id: ti._id, callback_pending: kind } as never, { $set: { callback_pending: null } })
    if (!won) continue   // another tick or replica fired it
    fired.push(fireTask(getDag(ti.dag_id)?.tasks[ti.task_id], kind, ti))
  }
  return fired
}
