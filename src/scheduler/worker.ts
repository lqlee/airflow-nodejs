/**
 * Worker process — runs a single task function in isolation.
 * Connects directly to MongoDB for XCom — no IPC round-trips for data access.
 *
 * IPC protocol:
 *   parent → worker (regular task):  { type: 'run',  fn,   ctx }
 *   parent → worker (sensor task):   { type: 'poke', fn,   ctx }
 *   worker → parent:                 { type: 'done', outcome: 'success'|'reschedule'|'fail', error? }
 */
import { MongoClient } from 'mongodb'
import { xcomPush, xcomPull } from '../xcom/index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME = process.env.DB_NAME ?? 'airflow'

type RunMsg  = { type: 'run';  fn: string; ctx: { dagId: string; runId: string; taskId: string } }
type PokeMsg = { type: 'poke'; fn: string; ctx: { dagId: string; runId: string; taskId: string } }
type WorkerMsg = RunMsg | PokeMsg

process.on('message', async (msg: WorkerMsg) => {
  if (msg.type !== 'run' && msg.type !== 'poke') return

  const { fn, ctx } = msg
  const client = new MongoClient(MONGO_URL)

  try {
    await client.connect()
    const db = client.db(DB_NAME)

    // Build XCom helpers backed directly by MongoDB
    const xcom = {
      push: (key: string, value: unknown) =>
        xcomPush(db, ctx.runId, ctx.dagId, ctx.taskId, key, value),
      pull: (fromTaskId: string, key: string) =>
        xcomPull(db, ctx.runId, fromTaskId, key),
    }

    // eslint-disable-next-line no-new-func
    const fn_ = new Function(`return (${fn})`)() as (ctx: typeof ctx & { xcom: typeof xcom }) => Promise<unknown>

    if (msg.type === 'poke') {
      // Sensor: fn is the poke() function; returns boolean
      const ready = await fn_({ ...ctx, xcom }) as boolean
      process.send!({ type: 'done', outcome: ready ? 'success' : 'reschedule' })
    } else {
      // Regular task: fn is the run() function
      await fn_({ ...ctx, xcom })
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
