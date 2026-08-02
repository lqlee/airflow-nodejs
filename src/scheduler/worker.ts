/**
 * Worker process — runs a single task function in isolation.
 * Connects directly to MongoDB for XCom, Connections, Variables, and run conf.
 * Secrets are decrypted HERE in the worker — never passed as plaintext over IPC.
 *
 * IPC protocol:
 *   parent → worker (regular task):   { type: 'run',  fn, ctx }
 *   parent → worker (sensor task):    { type: 'poke', fn, ctx }
 *   worker → parent:                  { type: 'done', outcome: 'success'|'reschedule'|'fail'|'deferred', error?, triggerFn?, deferInterval? }
 *
 * ctx includes: dagId, runId, taskId, mapIndex, mapValue
 * Injected in worker: conf (from DB), xcom, connections, variables, defer
 */
import { MongoClient } from 'mongodb'
import { xcomPush, xcomPull } from '../xcom/index.js'
import { getConnectionRuntime } from '../connections/index.js'
import { getVariableRuntime } from '../variables/index.js'
import { getRunConf } from './run-conf.js'

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

/**
 * Thrown by ctx.defer() inside a run: function to signal deferral.
 * The worker catches it and sends the deferred outcome to the parent.
 */
class DeferSignal {
  constructor(
    public readonly triggerFn: string,
    public readonly interval: number,
    public readonly timeout: number,
  ) {}
}

process.on('message', async (msg: WorkerMsg) => {
  if (msg.type !== 'run' && msg.type !== 'poke') return

  const { fn, ctx } = msg
  const client = new MongoClient(MONGO_URL)

  try {
    await client.connect()
    const db = client.db(DB_NAME)

    // Trigger-time conf — read from DB, not IPC (conf could be large)
    const conf = await getRunConf(db, ctx.runId)

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

    // defer() — suspends the task and frees the worker slot.
    // Throws DeferSignal which is caught below and sent as 'deferred' outcome.
    const defer = (
      trigger: (tctx: unknown) => Promise<boolean>,
      opts: { timeout?: number; interval?: number } = {}
    ): Promise<never> => {
      const interval = Math.max(100, opts.interval ?? 10_000)  // minimum 100ms
      throw new DeferSignal(trigger.toString(), interval, opts.timeout ?? 0)
    }

    // eslint-disable-next-line no-new-func
    const fn_ = new Function(`return (${fn})`)() as (
      ctx: WorkerCtx & {
        conf: Record<string, unknown>
        xcom: typeof xcom
        connections: typeof connections
        variables: typeof variables
        defer: typeof defer
      }
    ) => Promise<unknown>

    const fullCtx = { ...ctx, conf, xcom, connections, variables, defer }

    if (msg.type === 'poke') {
      const ready = await fn_(fullCtx) as boolean
      process.send!({ type: 'done', outcome: ready ? 'success' : 'reschedule' })
    } else {
      await fn_(fullCtx)
      process.send!({ type: 'done', outcome: 'success' })
    }
  } catch (err: unknown) {
    if (err instanceof DeferSignal) {
      // Task chose to defer — send triggerFn and interval back to parent
      process.send!({ type: 'done', outcome: 'deferred', triggerFn: err.triggerFn, deferInterval: err.interval, deferTimeout: err.timeout })
    } else {
      const error = err instanceof Error ? err.message : String(err)
      process.send!({ type: 'done', outcome: 'fail', error })
    }
  } finally {
    await client.close()
    process.exit(0)
  }
})
