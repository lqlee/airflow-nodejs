/**
 * Worker process — runs a single task function in isolation.
 * Holds no database credentials: XCom, connections, variables, run conf and dataset events go through
 * the execution API (execution/client.ts) — over IPC to the parent that forked it, or over HTTP with a
 * task token when EXECUTION_API_URL / EXECUTION_API_TOKEN are set.
 * Secrets are decrypted by the control plane and masked in everything this process prints.
 *
 * IPC protocol:
 *   parent → worker (regular task):   { type: 'run',  fn, ctx }
 *   parent → worker (sensor task):    { type: 'poke', fn, ctx }
 *   worker → parent:                  { type: 'done', outcome: 'success'|'reschedule'|'fail'|'deferred', error?, triggerFn?, deferInterval? }
 *   worker ⇄ parent:                  { type: 'exec', … } / { type: 'exec-result', … }   (execution/ipc.ts)
 *
 * ctx includes: dagId, runId, taskId, mapIndex, mapValue
 * Injected in worker: conf, xcom, connections, variables, defer, log, emitDataset
 */
import type { LogLevel } from '../logs/index.js'
import { installStreamMasking, redact } from '../secrets/masker.js'
import { executionClientFromEnv } from '../execution/client.js'
import { buildApiTaskContext } from '../execution/task-context.js'

// Secrets the task reads (connections, secret variables) are masked in everything it prints
installStreamMasking()

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

// ctx.log — structured logging with severity levels. Writes ONLY to stdout/stderr with a [LEVEL]
// prefix; the parent captures the lines and stores them with the right level (parseLevelFromLine).
const makeLogFn = (level: LogLevel, stream: 'stdout' | 'stderr') =>
  (msg: unknown) => {
    const line = typeof msg === 'string' ? msg : JSON.stringify(msg)
    const prefixed = `[${level.toUpperCase()}] ${line}`
    if (stream === 'stdout') process.stdout.write(prefixed + '\n')
    else process.stderr.write(prefixed + '\n')
  }

const log = {
  debug: makeLogFn('debug', 'stdout'),
  info:  makeLogFn('info',  'stdout'),
  warn:  makeLogFn('warn',  'stderr'),
  error: makeLogFn('error', 'stderr'),
}

process.on('message', async (msg: WorkerMsg) => {
  if (msg?.type !== 'run' && msg?.type !== 'poke') return

  const { fn, ctx } = msg
  const api = executionClientFromEnv()
  let result: Record<string, unknown> = { type: 'done', outcome: 'fail', error: 'worker did not finish' }

  try {
    const { ctx: apiCtx, recordDatasets } = await buildApiTaskContext(api, ctx)

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
    const fn_ = new Function(`return (${fn})`)() as (ctx: unknown) => Promise<unknown>

    const fullCtx = { ...apiCtx, ...ctx, defer, log }

    if (msg.type === 'poke') {
      const ready = await fn_(fullCtx) as boolean
      result = { type: 'done', outcome: ready ? 'success' : 'reschedule' }
    } else {
      await fn_(fullCtx)
      await recordDatasets()
      result = { type: 'done', outcome: 'success' }
    }
  } catch (err: unknown) {
    if (err instanceof DeferSignal) {
      // Task chose to defer — send triggerFn and interval back to parent
      result = { type: 'done', outcome: 'deferred', triggerFn: err.triggerFn, deferInterval: err.interval, deferTimeout: err.timeout }
    } else {
      result = { type: 'done', outcome: 'fail', error: redact(err instanceof Error ? err.message : String(err)) }
    }
  } finally {
    api.close()
    // Exit once the result is on its way to the parent
    process.send!(result, () => process.exit(0))
  }
})
