/**
 * Scheduler side of the sandbox (sandbox/runner.ts): one long-lived child process that runs Dag-author
 * functions on the scheduler's behalf. Started on first use, restarted when it exits or stops
 * responding. Importing this module routes every serialized-Dag function stub (dag/serialize.ts) here.
 *
 *   SANDBOX_CALL_TIMEOUT_MS   per-call limit (default 30s). A call that times out is failed; if the
 *                             process also misses a ping (a synchronous infinite loop) it is replaced.
 */
import type { ChildProcess } from 'node:child_process'
import { setUserCodeInvoker, type UserCodeRef } from '../dag/serialize.js'
import { forkUserCodeProcess } from './fork.js'
import type { SandboxReply, SandboxRequest } from './runner.js'

export interface TriggerHandlers {
  xcomPush: (key: string, value: unknown) => Promise<void>
  xcomPull: (fromTaskId: string, key: string) => Promise<unknown>
}

interface Pending {
  resolve: (v: unknown) => void
  reject: (e: Error) => void
  timer: NodeJS.Timeout | null
  handlers?: TriggerHandlers
}

const callTimeoutMs = () => Number(process.env.SANDBOX_CALL_TIMEOUT_MS ?? 30_000)
const PING_TIMEOUT_MS = 2_000

let child: ChildProcess | null = null
let nextId = 1
const pending = new Map<number, Pending>()

function settle(id: number, fn: (p: Pending) => void): void {
  const p = pending.get(id)
  if (!p) return
  pending.delete(id)
  if (p.timer) clearTimeout(p.timer)
  fn(p)
  if (pending.size === 0) child?.channel?.unref()
}

function onMessage(proc: ChildProcess, msg: SandboxReply): void {
  if (msg.type === 'result') {
    settle(msg.id, p => msg.ok ? p.resolve(msg.value) : p.reject(new Error(msg.error ?? 'failed')))
  } else if (msg.type === 'pong') {
    settle(msg.id, p => p.resolve(true))
  } else if (msg.type === 'rpc') {
    const owner = pending.get(msg.reqId)
    const run = async () => {
      if (!owner?.handlers) throw new Error('no trigger in flight')
      const [a, b] = msg.args as [string, unknown]
      return msg.op === 'xcom.push' ? owner.handlers.xcomPush(a, b) : owner.handlers.xcomPull(a, b as string)
    }
    run().then(
      value => send(proc, { type: 'rpc-result', rpcId: msg.rpcId, ok: true, value }),
      err => send(proc, { type: 'rpc-result', rpcId: msg.rpcId, ok: false, error: (err as Error).message }),
    )
  }
}

function send(proc: ChildProcess, msg: SandboxRequest): void {
  if (proc.connected) proc.send(msg)
}

function ensureChild(): ChildProcess {
  if (child && child.connected) return child
  const proc = forkUserCodeProcess('sandbox/runner')
  proc.on('message', (msg: SandboxReply) => onMessage(proc, msg))
  proc.on('exit', (code, signal) => {
    if (child !== proc) return
    child = null
    for (const id of [...pending.keys()]) {
      settle(id, p => p.reject(new Error(`sandbox process exited (${signal ?? `code ${code}`})`)))
    }
  })
  proc.on('error', err => console.error('[sandbox] process error:', err.message))
  proc.unref()
  child = proc
  return proc
}

function request(build: (id: number) => SandboxRequest, timeoutMs: number, handlers?: TriggerHandlers): Promise<unknown> {
  const proc = ensureChild()
  const id = nextId++
  proc.channel?.ref()
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      settle(id, p => p.reject(new Error(`timed out after ${timeoutMs}ms`)))
      void checkAlive(proc)
    }, timeoutMs)
    pending.set(id, { resolve, reject, timer, handlers })
    send(proc, build(id))
  })
}

/** After a timeout: replace the process if it no longer answers (blocked event loop). */
async function checkAlive(proc: ChildProcess): Promise<void> {
  if (child !== proc) return
  const id = nextId++
  const alive = await new Promise<boolean>((resolve) => {
    const timer = setTimeout(() => settle(id, p => p.resolve(false)), PING_TIMEOUT_MS)
    pending.set(id, { resolve: v => resolve(Boolean(v)), reject: () => resolve(false), timer })
    send(proc, { type: 'ping', id })
  })
  if (!alive && child === proc) {
    console.warn('[sandbox] process is not responding — restarting it')
    proc.kill('SIGKILL')
  }
}

/** Call a Dag function (by its serialized reference) in the sandbox. */
export function invokeInSandbox(ref: UserCodeRef, args: unknown[], timeoutMs = callTimeoutMs()): Promise<unknown> {
  return request(id => ({ type: 'call', id, ref, args }), timeoutMs)
}

/** Evaluate a deferred trigger (function source) against `ctx`; ctx.xcom calls come back to `handlers`. */
export async function runTriggerInSandbox(
  source: string, ctx: Record<string, unknown>, handlers: TriggerHandlers, timeoutMs = callTimeoutMs(),
): Promise<boolean> {
  return Boolean(await request(id => ({ type: 'trigger', id, source, ctx }), timeoutMs, handlers))
}

/** Stop the sandbox process (shutdown, tests). In-flight calls fail. */
export function stopSandbox(): void {
  const proc = child
  child = null
  for (const id of [...pending.keys()]) settle(id, p => p.reject(new Error('sandbox stopped')))
  proc?.kill('SIGKILL')
}

setUserCodeInvoker((ref, args) => invokeInSandbox(ref, args))

export type { UserCodeRef }
