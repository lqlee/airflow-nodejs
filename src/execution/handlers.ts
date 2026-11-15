/**
 * Execution API operations — everything a running task may ask the control plane for, always within
 * the scope of its own task instance. Workers hold no database credentials; they reach these through
 *   - IPC, when forked by the local executor or a BullMQ consumer (execution/ipc.ts), or
 *   - HTTP with a task token (/execution/v1/*, api/routes/execution.ts) — Kubernetes pods.
 *
 *   context          [withMapValue?]  run conf/params/dates (+ this instance's mapValue unless false)
 *   xcom.push        [key, value]          writes as this task instance only
 *   xcom.pull        [fromTaskId, key]     reads within this run only
 *   connection.get   [connId]              decrypted connection (DB, then the secrets backend)
 *   variable.get     [key]                 { value, secret } — secret values must be masked by the worker
 *   datasets.record  [events]              dataset events emitted by the task (after it succeeded)
 *   logs.append      [lines]               task log lines (workers whose output is not captured by a parent)
 *   task.source      []                    the task's Dag file name and run-function source
 */
import { basename } from 'node:path'
import type { Db } from 'mongodb'
import { xcomPush, xcomPull } from '../xcom/index.js'
import { getConnectionRuntime } from '../connections/index.js'
import { readVariableForTask } from '../variables/index.js'
import { recordTaskDatasetEvents, type EmittedDataset } from '../datasets/index.js'
import { appendLog, parseLevelFromLine, type LogLevel } from '../logs/index.js'
import { getRunContextFields } from '../scheduler/run-conf.js'
import { getDag } from '../dag/registry.js'
import { userCodeRef } from '../dag/serialize.js'
import { registerSecret } from '../secrets/masker.js'
import type { TaskScope } from './token.js'

export type ExecutionOp =
  | 'context' | 'xcom.push' | 'xcom.pull' | 'connection.get' | 'variable.get'
  | 'datasets.record' | 'logs.append' | 'task.source'

export const EXECUTION_OPS: readonly ExecutionOp[] = [
  'context', 'xcom.push', 'xcom.pull', 'connection.get', 'variable.get', 'datasets.record', 'logs.append', 'task.source',
]

export class ExecutionApiError extends Error {
  constructor(public readonly status: number, message: string) { super(message) }
}

export interface LogLineInput { stream?: 'stdout' | 'stderr'; line: string; level?: LogLevel }

const MAX_KEY = 512
const MAX_LOG_LINES = 1000
const MAX_LOG_LINE = 64 * 1024

function str(v: unknown, name: string, max = MAX_KEY): string {
  if (typeof v !== 'string' || !v || v.length > max) throw new ExecutionApiError(400, `${name} must be a non-empty string of at most ${max} characters`)
  return v
}

/** The task instance a scope refers to, filtered to the try the scope was issued for while it runs. */
export function liveTaskFilter(scope: TaskScope): Record<string, unknown> {
  return {
    dag_run_id: scope.runId, task_id: scope.taskId, map_index: scope.mapIndex,
    state: 'running', try_number: scope.tryNumber,
  }
}

/** Throws 409 unless the scope's try is still running — a token is only good for its own attempt. */
export async function assertTaskLive(db: Db, scope: TaskScope): Promise<void> {
  const ti = await db.collection('task_instances').findOne(liveTaskFilter(scope), { projection: { _id: 1 } })
  if (!ti) throw new ExecutionApiError(409, 'task instance is not running this try — the token is no longer valid')
}

export async function handleExecutionOp(db: Db, scope: TaskScope, op: ExecutionOp, args: unknown[] = []): Promise<unknown> {
  switch (op) {
    case 'context': {
      // args[0] === false: the caller already has its mapValue (forked workers get it from the parent)
      const runFields = await getRunContextFields(db, scope.runId)
      if (args[0] === false) return runFields
      const ti = await db.collection('task_instances').findOne(
        { dag_run_id: scope.runId, task_id: scope.taskId, map_index: scope.mapIndex }, { projection: { map_value: 1 } })
      return { ...runFields, mapValue: ti?.map_value ?? null }
    }
    case 'xcom.push': {
      const key = str(args[0], 'key')
      await xcomPush(db, scope.runId, scope.dagId, scope.taskId, scope.mapIndex, key, args[1] ?? null)
      return null
    }
    case 'xcom.pull':
      return xcomPull(db, scope.runId, str(args[0], 'task_id'), str(args[1], 'key'))
    case 'connection.get':
      // Registering here too masks the secret in log lines this process writes for the task
      return getConnectionRuntime(db, str(args[0], 'conn_id'))
    case 'variable.get': {
      const v = await readVariableForTask(db, str(args[0], 'key'))
      if (v.value !== null && v.secret) registerSecret(v.value)
      return v
    }
    case 'datasets.record': {
      const events = args[0]
      if (!Array.isArray(events)) throw new ExecutionApiError(400, 'events must be an array')
      try {
        await recordTaskDatasetEvents(db, { dagId: scope.dagId, runId: scope.runId, taskId: scope.taskId }, events as EmittedDataset[])
      } catch (err) {
        throw new ExecutionApiError(400, (err as Error).message)
      }
      return null
    }
    case 'logs.append': {
      const lines = args[0]
      if (!Array.isArray(lines) || lines.length > MAX_LOG_LINES) throw new ExecutionApiError(400, `lines must be an array of at most ${MAX_LOG_LINES}`)
      for (const l of lines as LogLineInput[]) {
        const line = String(l?.line ?? '').slice(0, MAX_LOG_LINE)
        const stream = l?.stream === 'stderr' ? 'stderr' : 'stdout'
        const level = l?.level ?? parseLevelFromLine(line) ?? (stream === 'stderr' ? 'error' : 'info')
        await appendLog(db, scope.runId, scope.dagId, scope.taskId, stream, line, level)
      }
      return null
    }
    case 'task.source': {
      const task = getDag(scope.dagId)?.tasks[scope.taskId]
      if (!task?.run) throw new ExecutionApiError(404, `task '${scope.dagId}.${scope.taskId}' has no run function`)
      const fileloc = userCodeRef(task.run)?.fileloc
      return { file: fileloc ? basename(fileloc) : null, version: getDag(scope.dagId)?.version ?? null, run: String(task.run) }
    }
    default:
      throw new ExecutionApiError(404, `unknown operation '${String(op)}'`)
  }
}
