/**
 * Sandbox process — runs Dag-author functions the scheduler needs to call (callbacks, notifiers,
 * timetables, deadline callbacks, deferred-task triggers), so the scheduler itself never runs user code.
 * Started and restarted by sandbox/client.ts; holds no database credentials (see sandbox/env.ts).
 *
 * A function is found by re-importing its Dag file and following the recorded property path, so
 * closures over the file's imports and helpers work. If the file no longer has it there, the source
 * captured at parse time is evaluated instead (no closures).
 *
 * IPC (structured clone):
 *   parent → child  { type: 'call', id, ref, args }          call a Dag function
 *                   { type: 'trigger', id, source, ctx }     evaluate a deferred trigger: source(ctx)
 *                   { type: 'rpc-result', rpcId, ok, value?, error? }
 *                   { type: 'ping', id }
 *   child → parent  { type: 'result', id, ok, value?, error? }
 *                   { type: 'rpc', rpcId, reqId, op: 'xcom.push' | 'xcom.pull', args }   trigger reqId's ctx.xcom
 *                   { type: 'pong', id }
 */
import { readFile } from 'node:fs/promises'
import { pathToFileURL } from 'node:url'
import { hashDagSource } from '../dag/version.js'
import { expandGroups } from '../dag/taskgroups.js'
import { getDagsDir } from '../dag/loader.js'
import { resolvePath, type UserCodeRef } from '../dag/serialize.js'
import { loadProviders } from '../providers/loader.js'
import type { DagDefinition } from '../dag/types.js'

export type SandboxRequest =
  | { type: 'call'; id: number; ref: UserCodeRef; args: unknown[] }
  | { type: 'trigger'; id: number; source: string; ctx: Record<string, unknown> }
  | { type: 'rpc-result'; rpcId: number; ok: boolean; value?: unknown; error?: string }
  | { type: 'ping'; id: number }

export type SandboxReply =
  | { type: 'result'; id: number; ok: boolean; value?: unknown; error?: string }
  | { type: 'rpc'; rpcId: number; reqId: number; op: 'xcom.push' | 'xcom.pull'; args: unknown[] }
  | { type: 'pong'; id: number }

type AnyFn = (...args: unknown[]) => unknown

const modules = new Map<string, { hash: string; dag: DagDefinition | null }>()
let providersLoaded = false

async function loadDag(ref: UserCodeRef): Promise<DagDefinition | null> {
  if (!ref.fileloc) return null
  try {
    const source = await readFile(ref.fileloc, 'utf8')
    const hash = hashDagSource(source)
    const cached = modules.get(ref.fileloc)
    if (cached?.hash === hash) return cached.dag
    if (!providersLoaded || cached) { await loadProviders(getDagsDir()); providersLoaded = true }
    const mod = await import(`${pathToFileURL(ref.fileloc).href}?v=${hash}`)
    const raw = mod.default as DagDefinition | undefined
    const dag = raw?.id === ref.dagId && raw.tasks ? expandGroups(raw) : null
    modules.set(ref.fileloc, { hash, dag })
    return dag
  } catch (err) {
    console.warn(`[sandbox] cannot import ${ref.fileloc}: ${(err as Error).message} — using the parsed source`)
    return null
  }
}

function evalSource(source: string): AnyFn {
  const fn = new Function(`return (${source})`)() as unknown   // eslint-disable-line no-new-func
  if (typeof fn !== 'function') throw new Error('source does not evaluate to a function')
  return fn as AnyFn
}

async function callUserFunction(ref: UserCodeRef, args: unknown[]): Promise<unknown> {
  const dag = await loadDag(ref)
  if (dag) {
    const { parent, value } = resolvePath(dag, ref.path)
    if (typeof value === 'function') return (value as AnyFn).apply(parent, args)
  }
  return evalSource(ref.source)(...args)
}

let nextRpcId = 1
const rpcs = new Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void }>()

function rpc(reqId: number, op: 'xcom.push' | 'xcom.pull', args: unknown[]): Promise<unknown> {
  const rpcId = nextRpcId++
  return new Promise((resolve, reject) => {
    rpcs.set(rpcId, { resolve, reject })
    reply({ type: 'rpc', rpcId, reqId, op, args })
  })
}

function reply(msg: SandboxReply): void {
  try {
    process.send!(msg)
  } catch {
    // The value cannot be cloned (e.g. it holds functions) — report the call without it
    if (msg.type === 'result') process.send!({ type: 'result', id: msg.id, ok: msg.ok, error: msg.error })
  }
}

async function handle(msg: SandboxRequest): Promise<void> {
  if (msg.type === 'ping') return reply({ type: 'pong', id: msg.id })
  if (msg.type === 'rpc-result') {
    const p = rpcs.get(msg.rpcId)
    rpcs.delete(msg.rpcId)
    if (p) msg.ok ? p.resolve(msg.value) : p.reject(new Error(msg.error ?? 'rpc failed'))
    return
  }
  try {
    let value: unknown
    if (msg.type === 'call') {
      value = await callUserFunction(msg.ref, msg.args)
    } else {
      const ctx = {
        ...msg.ctx,
        xcom: {
          push: (key: string, v: unknown) => rpc(msg.id, 'xcom.push', [key, v]),
          pull: (fromTaskId: string, key: string) => rpc(msg.id, 'xcom.pull', [fromTaskId, key]),
        },
      }
      value = await evalSource(msg.source)(ctx)
    }
    reply({ type: 'result', id: msg.id, ok: true, value })
  } catch (err) {
    reply({ type: 'result', id: msg.id, ok: false, error: err instanceof Error ? err.message : String(err) })
  }
}

process.on('message', (msg: SandboxRequest) => { void handle(msg) })
// A callback's stray rejection must not take down every other in-flight call
process.on('unhandledRejection', err => console.warn('[sandbox] unhandled rejection:', err))
