/**
 * Worker process — runs a single task function in isolation.
 * Connects directly to MongoDB for XCom — no IPC round-trips for data access.
 *
 * IPC protocol (simplified):
 *   parent → worker:  { type: 'run', fn, ctx }
 *   worker → parent:  { type: 'done', success, error? }
 */
import { MongoClient } from 'mongodb'
import { xcomPush, xcomPull } from '../xcom/index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB_NAME = process.env.DB_NAME ?? 'airflow'

type RunMsg = { type: 'run'; fn: string; ctx: { dagId: string; runId: string; taskId: string } }

process.on('message', async (msg: RunMsg) => {
  if (msg.type !== 'run') return

  const { fn, ctx } = msg
  const client = new MongoClient(MONGO_URL)

  try {
    await client.connect()
    const db = client.db(DB_NAME)

    // Build XCom helpers backed directly by MongoDB — zero IPC round-trips
    const xcom = {
      push: (key: string, value: unknown) =>
        xcomPush(db, ctx.runId, ctx.dagId, ctx.taskId, key, value),

      pull: (fromTaskId: string, key: string) =>
        xcomPull(db, ctx.runId, fromTaskId, key),
    }

    // eslint-disable-next-line no-new-func
    const taskFn = new Function(`return (${fn})`)() as (ctx: typeof ctx & { xcom: typeof xcom }) => Promise<unknown>
    await taskFn({ ...ctx, xcom })

    process.send!({ type: 'done', success: true })
  } catch (err: unknown) {
    const error = err instanceof Error ? err.message : String(err)
    process.send!({ type: 'done', success: false, error })
  } finally {
    await client.close()
    process.exit(0)
  }
})
