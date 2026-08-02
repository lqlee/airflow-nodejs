/**
 * Worker CLI — entry point for the Kubernetes executor pod.
 *
 * Run by the pod spawned by the Kubernetes executor:
 *   node dist/scheduler/worker-cli.js
 *
 * Reads task identity from environment variables:
 *   K8S_EXEC_DAG_ID       — DAG id
 *   K8S_EXEC_RUN_ID       — run id
 *   K8S_EXEC_TASK_ID      — task id
 *   K8S_EXEC_MAP_INDEX    — map index (empty string = null)
 *
 * Connects to MongoDB (MONGO_URL / DB_NAME), loads the dag from registry,
 * executes the task function, then exits.
 *
 * Exit codes:
 *   0 — task succeeded
 *   1 — task failed (error logged to stderr)
 *   2 — configuration error (missing env var, dag not found, etc.)
 */

import { MongoClient } from 'mongodb'
import { loadDags } from '../dag/loader.js'
import { getDag } from '../dag/registry.js'
import { getRunConf } from './run-conf.js'
import { xcomPush, xcomPull } from '../xcom/index.js'
import { getConnectionRuntime } from '../connections/index.js'
import { getVariableRuntime } from '../variables/index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME   = process.env.DB_NAME   ?? 'airflow'

const dagId    = process.env.K8S_EXEC_DAG_ID   ?? ''
const runId    = process.env.K8S_EXEC_RUN_ID   ?? ''
const taskId   = process.env.K8S_EXEC_TASK_ID  ?? ''
const mapIdxRaw = process.env.K8S_EXEC_MAP_INDEX ?? ''
const mapIndex = mapIdxRaw !== '' ? parseInt(mapIdxRaw, 10) : null

if (!dagId || !runId || !taskId) {
  console.error('[worker-cli] Missing required env: K8S_EXEC_DAG_ID, K8S_EXEC_RUN_ID, K8S_EXEC_TASK_ID')
  process.exit(2)
}

const client = new MongoClient(MONGO_URL)

try {
  await client.connect()
  const db = client.db(DB_NAME)

  // Load dags so the registry is populated
  await loadDags(db)

  const dag = getDag(dagId)
  if (!dag) {
    console.error(`[worker-cli] Dag '${dagId}' not found in registry`)
    process.exit(2)
  }

  const taskDef = dag.tasks[taskId]
  if (!taskDef) {
    console.error(`[worker-cli] Task '${taskId}' not found in dag '${dagId}'`)
    process.exit(2)
  }

  if (!taskDef.run) {
    console.error(`[worker-cli] Task '${taskId}' has no run: function — only run: tasks are supported by the Kubernetes executor`)
    process.exit(2)
  }

  const conf = await getRunConf(db, runId)

  const xcom = {
    push: (key: string, value: unknown) => xcomPush(db, runId, dagId, taskId, mapIndex, key, value),
    pull: (fromTaskId: string, key: string) => xcomPull(db, runId, fromTaskId, key),
  }
  const connections = { get: (connId: string) => getConnectionRuntime(db, connId) }
  const variables   = { get: (key: string)   => getVariableRuntime(db, key) }

  const ctx = {
    dagId, runId, taskId,
    mapIndex,
    mapValue: null,  // map_value not passed via env (large values); use XCom
    conf,
    xcom, connections, variables,
    defer: () => { throw new Error('ctx.defer() is not supported in Kubernetes executor mode') },
  }

  await taskDef.run(ctx as any)
  console.log(`[worker-cli] ✓ ${dagId}.${taskId}`)
  process.exit(0)

} catch (err: unknown) {
  const msg = err instanceof Error ? err.message : String(err)
  console.error(`[worker-cli] ✗ ${dagId}.${taskId}: ${msg}`)
  process.exit(1)
} finally {
  await client.close()
}
