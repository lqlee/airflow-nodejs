/**
 * The parts of a task's `ctx` that talk to the control plane, built on the execution API
 * (execution/client.ts) — shared by the forked worker (scheduler/worker.ts) and the Kubernetes pod
 * entry point (scheduler/worker-cli.ts). Secrets the task reads are registered with this process's
 * masker, so its own output shows `***`.
 */
import type { ExecutionClient } from './client.js'
import type { EmittedDataset } from '../datasets/index.js'
import { registerSecret, registerSensitiveFields } from '../secrets/masker.js'
import type { ConnectionHelper } from '../dag/types.js'

export interface TaskIdentity {
  dagId: string
  runId: string
  taskId: string
  mapIndex: number | null
}

type Connection = Awaited<ReturnType<ConnectionHelper['get']>>

const DATE_FIELDS = ['logicalDate', 'dataIntervalStart', 'dataIntervalEnd'] as const

/** Run fields come over JSON (HTTP) as ISO strings — make them Dates again. */
function reviveDates(fields: Record<string, unknown>): Record<string, unknown> {
  const out = { ...fields }
  for (const k of DATE_FIELDS) if (typeof out[k] === 'string') out[k] = new Date(out[k] as string)
  return out
}

/** `id.mapValue`, when given, is used as-is (saves the API a lookup); otherwise the API supplies it. */
export async function buildApiTaskContext(api: ExecutionClient, id: TaskIdentity & { mapValue?: unknown }) {
  const known = id.mapValue !== undefined
  const fields = reviveDates(await api.call('context', known ? [false] : []) as Record<string, unknown>)
  const { mapValue, ...runFields } = known ? { ...fields, mapValue: id.mapValue } : fields
  const emitted: EmittedDataset[] = []

  const ctx = {
    dagId: id.dagId, runId: id.runId, taskId: id.taskId, mapIndex: id.mapIndex,
    mapValue: mapValue ?? null,
    ...runFields,
    xcom: {
      push: async (key: string, value: unknown) => { await api.call('xcom.push', [key, value]) },
      pull: (fromTaskId: string, key: string) => api.call('xcom.pull', [fromTaskId, key]),
    },
    connections: {
      get: async (connId: string) => {
        const conn = await api.call('connection.get', [connId]) as Connection
        if (conn) { registerSecret(conn.password); registerSensitiveFields(conn.extra) }
        return conn
      },
    },
    variables: {
      get: async (key: string) => {
        const v = await api.call('variable.get', [key]) as { value: string | null; secret: boolean }
        if (v.value !== null && v.secret) registerSecret(v.value)
        return v.value
      },
    },
    emitDataset: (uri: string, opts: { alias?: string; extra?: Record<string, unknown> } = {}) => {
      emitted.push({ uri, ...opts })
    },
  }

  /** Record the dataset events the task emitted — call once the task function succeeded. */
  const recordDatasets = async () => { if (emitted.length) await api.call('datasets.record', [emitted]) }
  return { ctx, recordDatasets }
}
