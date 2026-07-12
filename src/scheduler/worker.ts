/**
 * Worker process — runs a single task function in isolation.
 *
 * IPC protocol with parent:
 *   parent → worker:  { type: 'run', fn: string, ctx: { dagId, runId, taskId } }
 *   worker → parent:  { type: 'xcom:push', key, value }     (XCom write)
 *   parent → worker:  { type: 'xcom:pull:result', value }   (XCom read response)
 *   worker → parent:  { type: 'xcom:pull', fromTaskId, key } (XCom read request)
 *   worker → parent:  { type: 'done', success, error? }     (task finished)
 */

type RunMsg = { type: 'run'; fn: string; ctx: { dagId: string; runId: string; taskId: string } }
type XComPullResult = { type: 'xcom:pull:result'; value: unknown }
type IncomingMsg = RunMsg | XComPullResult

// Pending pull resolvers — keyed by a simple counter
const pendingPulls = new Map<number, (value: unknown) => void>()
let pullCounter = 0

process.on('message', async (msg: IncomingMsg) => {
  if (msg.type === 'xcom:pull:result') {
    // Parent responded to our pull request — resolve the waiting promise
    const resolve = pendingPulls.get(pullCounter)
    if (resolve) resolve(msg.value)
    return
  }

  if (msg.type === 'run') {
    const { fn, ctx } = msg

    // Build XCom helpers that communicate back to parent via IPC
    const xcom = {
      push: async (key: string, value: unknown): Promise<void> => {
        process.send!({ type: 'xcom:push', key, value })
      },
      pull: (fromTaskId: string, key: string): Promise<unknown> => {
        return new Promise((resolve) => {
          const id = ++pullCounter
          pendingPulls.set(id, resolve)
          process.send!({ type: 'xcom:pull', fromTaskId, key, id })
        })
      },
    }

    try {
      // eslint-disable-next-line no-new-func
      const taskFn = new Function(`return (${fn})`)() as (ctx: typeof ctx & { xcom: typeof xcom }) => Promise<unknown>
      await taskFn({ ...ctx, xcom })
      process.send!({ type: 'done', success: true })
    } catch (err: unknown) {
      const error = err instanceof Error ? err.message : String(err)
      process.send!({ type: 'done', success: false, error })
    } finally {
      process.exit(0)
    }
  }
})
