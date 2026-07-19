/**
 * Worker process — runs a single task function in isolation.
 * Connects directly to MongoDB for XCom, Connections, and Variables.
 * Secrets are decrypted HERE in the worker — never passed as plaintext over IPC.
 *
 * IPC protocol:
 *   parent → worker (regular task):  { type: 'run',  fn, ctx }
 *   parent → worker (sensor task):   { type: 'poke', fn, ctx }
 *   worker → parent:                 { type: 'done', outcome: 'success'|'reschedule'|'fail', error? }
 *
 * ctx includes: dagId, runId, taskId, mapIndex (null for non-mapped), mapValue (null for non-mapped)
 */
import { MongoClient } from 'mongodb'
import { xcomPush, xcomPull } from '../xcom/index.js'
import { getConnectionRuntime } from '../connections/index.js'
import { getVariableRuntime } from '../variables/index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME = process.env.DB_NAME ?? 'airflow'

type WorkerCtx = {
  dagId: string
  runId: string
  taskId: string
  mapIndex: number | null
  mapValue: unknown
}

type RunMsg  = { type: 'run';  fn: string; ctx: WorkerCtx }
type PokeMsg = { type: 'poke'; fn: string; ctx: WorkerCtx }
type WorkerMsg = RunMsg | PokeMsg

process.on('message', async (msg: WorkerMsg) => {
  if (msg.type !== 'run' && msg.type !== 'poke') return

  const { fn, ctx } = msg
  const client = new MongoClient(MONGO_URL)

  try {
    await client.connect()
    const db = client.db(DB_NAME)

    // XCom helpers — run-scoped; mapIndex threads through push for mapped instances
    const xcom = {
      push: (key: string, value: unknown) =>
        xcomPush(db, ctx.runId, ctx.dagId, ctx.taskId, ctx.mapIndex, key, value),
      pull: (fromTaskId: string, key: string) =>
        xcomPull(db, ctx.runId, fromTaskId, key),
    }

    // Connection helper — global (not run-scoped); decrypts in worker
    const connections = {
      get: (connId: string) => getConnectionRuntime(db, connId),
    }

    // Variable helper — global (not run-scoped); decrypts secrets in worker
    const variables = {
      get: (key: string) => getVariableRuntime(db, key),
    }

    // eslint-disable-next-line no-new-func
    const fn_ = new Function(`return (${fn})`)() as (
      ctx: WorkerCtx & { xcom: typeof xcom; connections: typeof connections; variables: typeof variables }
    ) => Promise<unknown>

    const fullCtx = { ...ctx, xcom, connections, variables }

    if (msg.type === 'poke') {
      const ready = await fn_(fullCtx) as boolean
      process.send!({ type: 'done', outcome: ready ? 'success' : 'reschedule' })
    } else {
      await fn_(fullCtx)
      process.send!({ type: 'done', outcome: 'success' })
    }
  } catch (err: unknown) {
    const error = err instanceof Error ? err.message : String(err)
    process.send!({ type: 'done', outcome: 'fail', error })
  } finally {
    await client.close()
    process.exit(0)
  }
})
