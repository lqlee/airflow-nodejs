/**
 * Worker side of the execution API: how a running task reaches XCom, connections, variables, run
 * context, dataset events and logs without database credentials.
 *
 *   executionClientFromEnv()
 *     EXECUTION_API_URL + EXECUTION_API_TOKEN set → HTTP with the task token (Kubernetes pods)
 *     otherwise, forked with an IPC channel        → IPC to the parent (local executor, BullMQ consumer)
 *
 * The HTTP client refreshes its token once a third of its lifetime is left, and retries reads on
 * network errors and 5xx responses.
 */
import type { ExecutionOp } from './handlers.js'
import type { ExecRequest, ExecResult } from './ipc.js'
import { tokenExpiry } from './token.js'

export interface ExecutionClient {
  call(op: ExecutionOp, args?: unknown[]): Promise<unknown>
  close(): void
}

export class ExecutionCallError extends Error {
  constructor(public readonly status: number, message: string) { super(message) }
}

// ── IPC ────────────────────────────────────────────────────────────────────

export function ipcExecutionClient(timeoutMs = 60_000): ExecutionClient {
  if (!process.send) throw new Error('no IPC channel — set EXECUTION_API_URL and EXECUTION_API_TOKEN')
  let nextId = 1
  const pending = new Map<number, { resolve: (v: unknown) => void; reject: (e: Error) => void; timer: NodeJS.Timeout }>()
  const onMessage = (msg: ExecResult) => {
    if (msg?.type !== 'exec-result') return
    const p = pending.get(msg.id)
    if (!p) return
    pending.delete(msg.id)
    clearTimeout(p.timer)
    if (msg.ok) p.resolve(msg.value)
    else p.reject(new ExecutionCallError(msg.status ?? 500, msg.error ?? 'execution API call failed'))
  }
  process.on('message', onMessage)
  return {
    call(op, args = []) {
      const id = nextId++
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          pending.delete(id)
          reject(new ExecutionCallError(504, `execution API call '${op}' timed out after ${timeoutMs}ms`))
        }, timeoutMs)
        pending.set(id, { resolve, reject, timer })
        process.send!({ type: 'exec', id, op, args } satisfies ExecRequest)
      })
    },
    close() { process.off('message', onMessage) },
  }
}

// ── HTTP ───────────────────────────────────────────────────────────────────

interface Route { method: 'GET' | 'POST'; path: string; body?: unknown; unwrap?: (b: Record<string, unknown>) => unknown; notFoundIsNull?: boolean }

const enc = encodeURIComponent

function route(op: ExecutionOp, args: unknown[]): Route {
  switch (op) {
    case 'context':         return { method: 'GET', path: '/context' }
    case 'xcom.push':       return { method: 'POST', path: `/xcom/${enc(String(args[0]))}`, body: { value: args[1] ?? null } }
    case 'xcom.pull':       return { method: 'GET', path: `/xcom/${enc(String(args[0]))}/${enc(String(args[1]))}`, unwrap: b => b.value }
    case 'connection.get':  return { method: 'GET', path: `/connections/${enc(String(args[0]))}`, notFoundIsNull: true }
    case 'variable.get':    return { method: 'GET', path: `/variables/${enc(String(args[0]))}` }
    case 'datasets.record': return { method: 'POST', path: '/datasets', body: { events: args[0] } }
    case 'logs.append':     return { method: 'POST', path: '/logs', body: { lines: args[0] } }
    case 'task.source':     return { method: 'GET', path: '/task' }
  }
}

export interface HttpClientOptions {
  /** Retries for reads (GET) on network errors and 5xx. Default 3. */
  retries?: number
  timeoutMs?: number
  fetchImpl?: typeof fetch
}

export function httpExecutionClient(baseUrl: string, initialToken: string, opts: HttpClientOptions = {}): ExecutionClient {
  const base = `${baseUrl.replace(/\/+$/, '')}/execution/v1`
  const doFetch = opts.fetchImpl ?? fetch
  const retries = opts.retries ?? 3
  const timeoutMs = opts.timeoutMs ?? 30_000
  let token = initialToken
  let issuedAt = Date.now()
  let refreshing: Promise<void> | null = null

  const send = (method: string, path: string, body?: unknown) => doFetch(`${base}${path}`, {
    method,
    headers: { authorization: `Bearer ${token}`, ...(body !== undefined ? { 'content-type': 'application/json' } : {}) },
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(timeoutMs),
  })

  async function refreshIfDue(): Promise<void> {
    const exp = tokenExpiry(token)
    if (!exp) return
    const lifetime = Math.max(1, exp - issuedAt)
    if (exp - Date.now() > lifetime / 3) return
    refreshing ??= (async () => {
      const res = await send('POST', '/token/refresh')
      if (!res.ok) throw new ExecutionCallError(res.status, `token refresh failed: ${await errorText(res)}`)
      token = ((await res.json()) as { token: string }).token
      issuedAt = Date.now()
    })().finally(() => { refreshing = null })
    await refreshing
  }

  // Long tasks that make no calls for a while must still refresh before the token expires
  const timer = setInterval(() => { void refreshIfDue().catch(() => { /* surfaced on the next call */ }) }, 30_000)
  timer.unref()

  return {
    async call(op, args = []) {
      await refreshIfDue()
      const r = route(op, args)
      for (let attempt = 0; ; attempt++) {
        let res: Response
        try {
          res = await send(r.method, r.path, r.body)
        } catch (err) {
          if (r.method === 'GET' && attempt < retries) { await backoff(attempt); continue }
          throw new ExecutionCallError(503, `execution API unreachable (${base}): ${(err as Error).message}`)
        }
        if (res.status >= 500 && r.method === 'GET' && attempt < retries) { await backoff(attempt); continue }
        if (res.status === 404 && r.notFoundIsNull) return null
        if (!res.ok) throw new ExecutionCallError(res.status, `${op}: ${await errorText(res)}`)
        const json = res.status === 204 ? null : await res.json() as Record<string, unknown> | null
        return r.unwrap && json ? r.unwrap(json) : json
      }
    },
    close() { clearInterval(timer) },
  }
}

const backoff = (attempt: number) => new Promise(r => setTimeout(r, 200 * 2 ** attempt))

async function errorText(res: Response): Promise<string> {
  const text = await res.text().catch(() => '')
  try { return (JSON.parse(text) as { error?: string }).error ?? `HTTP ${res.status}` } catch { return text || `HTTP ${res.status}` }
}

export function executionClientFromEnv(env: NodeJS.ProcessEnv = process.env): ExecutionClient {
  if (env.EXECUTION_API_URL && env.EXECUTION_API_TOKEN) return httpExecutionClient(env.EXECUTION_API_URL, env.EXECUTION_API_TOKEN)
  return ipcExecutionClient()
}
