/**
 * Worker CLI — entry point for the Kubernetes executor pod.
 *
 * Run by the pod spawned by the Kubernetes executor:
 *   node dist/scheduler/worker-cli.js
 *
 * Reads from environment variables:
 *   K8S_EXEC_DAG_ID       — DAG id
 *   K8S_EXEC_RUN_ID       — run id
 *   K8S_EXEC_TASK_ID      — task id
 *   K8S_EXEC_MAP_INDEX    — map index (empty string = null)
 *   EXECUTION_API_URL     — the deployment's API, reachable from the pod
 *   EXECUTION_API_TOKEN   — task token scoped to this try (execution/token.ts)
 *
 * Holds no database credentials: run context, XCom, connections, variables and dataset events go
 * through the execution API. The task function comes from the Dag file in the image (so closures over
 * the file's imports work); if the image does not have it, from the run-function source the API sends.
 * Output goes to the pod log, which the executor streams into the task log.
 *
 * Exit codes:
 *   0 — task succeeded
 *   1 — task failed (error logged to stderr)
 *   2 — configuration error (missing env var, task not found, etc.)
 */
import { existsSync } from 'node:fs'
import { resolve } from 'node:path'
import { getDagsDir, parseDagFile } from '../dag/loader.js'
import { httpExecutionClient } from '../execution/client.js'
import { buildApiTaskContext } from '../execution/task-context.js'
import { installStreamMasking, redact } from '../secrets/masker.js'

// Secrets the task reads are masked in everything the pod prints (its stdout is the task log)
installStreamMasking()

const dagId    = process.env.K8S_EXEC_DAG_ID   ?? ''
const runId    = process.env.K8S_EXEC_RUN_ID   ?? ''
const taskId   = process.env.K8S_EXEC_TASK_ID  ?? ''
const mapIdxRaw = process.env.K8S_EXEC_MAP_INDEX ?? ''
const mapIndex = mapIdxRaw !== '' ? parseInt(mapIdxRaw, 10) : null
const apiUrl   = process.env.EXECUTION_API_URL ?? ''
const apiToken = process.env.EXECUTION_API_TOKEN ?? ''

if (!dagId || !runId || !taskId) {
  console.error('[worker-cli] Missing required env: K8S_EXEC_DAG_ID, K8S_EXEC_RUN_ID, K8S_EXEC_TASK_ID')
  process.exit(2)
}
if (!apiUrl || !apiToken) {
  console.error('[worker-cli] Missing required env: EXECUTION_API_URL, EXECUTION_API_TOKEN (set EXECUTION_API_URL on the scheduler)')
  process.exit(2)
}

type RunFn = (ctx: unknown) => Promise<unknown>

/** The task's run function: from the Dag file in the image when it is there, else from the source. */
async function resolveRunFn(source: { file: string | null; run: string }): Promise<RunFn> {
  const filePath = source.file ? resolve(getDagsDir(), source.file) : null
  if (filePath && existsSync(filePath)) {
    try {
      const { dag } = await parseDagFile(filePath)
      const run = dag.id === dagId ? dag.tasks[taskId]?.run : undefined
      if (run) return run as RunFn
    } catch (err) {
      console.warn(`[worker-cli] cannot import ${source.file}: ${(err as Error).message} — using the parsed source`)
    }
  }
  return new Function(`return (${source.run})`)() as RunFn   // eslint-disable-line no-new-func
}

const api = httpExecutionClient(apiUrl, apiToken)
let exitCode = 0
try {
  const source = await api.call('task.source') as { file: string | null; run: string }
  const run = await resolveRunFn(source)
  const { ctx, recordDatasets } = await buildApiTaskContext(api, { dagId, runId, taskId, mapIndex })
  await run({
    ...ctx,
    defer: () => { throw new Error('ctx.defer() is not supported in Kubernetes executor mode') },
  })
  await recordDatasets()
  console.log(`[worker-cli] ✓ ${dagId}.${taskId}`)
} catch (err: unknown) {
  const status = (err as { status?: number }).status
  const msg = redact(err instanceof Error ? err.message : String(err))
  console.error(`[worker-cli] ✗ ${dagId}.${taskId}: ${msg}`)
  exitCode = status === 404 ? 2 : 1
} finally {
  api.close()
}
process.exit(exitCode)
