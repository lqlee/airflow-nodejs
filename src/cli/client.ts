/**
 * REST client for the `anode` CLI. Talks to a running deployment's API with a Bearer token
 * (an API key from API_KEYS / `POST /api-keys`, or an OIDC access token).
 */

export class ApiError extends Error {
  constructor(public readonly status: number, message: string, public readonly body?: unknown) { super(message) }
}

export interface ApiClient {
  readonly baseUrl: string
  get<T = unknown>(path: string, query?: Record<string, string | string[] | undefined>): Promise<T>
  post<T = unknown>(path: string, body?: unknown): Promise<T>
}

export interface ApiClientOptions {
  baseUrl: string
  token?: string
  fetchImpl?: typeof fetch
  timeoutMs?: number
}

function errorMessage(status: number, body: unknown): string {
  if (body && typeof body === 'object') {
    const b = body as { error?: unknown; message?: unknown; param_errors?: { param: string; message: string }[] }
    const base = String(b.error ?? b.message ?? `HTTP ${status}`)
    const params = Array.isArray(b.param_errors) ? b.param_errors.map(e => `\n  ${e.param}: ${e.message}`).join('') : ''
    return base + params
  }
  return typeof body === 'string' && body ? body : `HTTP ${status}`
}

export function createApiClient(opts: ApiClientOptions): ApiClient {
  let base: URL
  try {
    base = new URL(opts.baseUrl)
  } catch {
    throw new Error(`invalid API URL '${opts.baseUrl}'`)
  }
  if (base.protocol !== 'http:' && base.protocol !== 'https:') throw new Error(`API URL must be http(s): '${opts.baseUrl}'`)
  const baseUrl = base.href.replace(/\/+$/, '')
  const doFetch = opts.fetchImpl ?? fetch
  const timeoutMs = opts.timeoutMs ?? 30_000

  const request = async <T>(method: 'GET' | 'POST', path: string, body?: unknown, query?: Record<string, string | string[] | undefined>): Promise<T> => {
    const url = new URL(baseUrl + path)
    for (const [k, v] of Object.entries(query ?? {})) {
      for (const item of v === undefined ? [] : [v].flat()) url.searchParams.append(k, item)
    }
    const headers: Record<string, string> = { accept: 'application/json' }
    if (opts.token) headers.authorization = `Bearer ${opts.token}`
    if (body !== undefined) headers['content-type'] = 'application/json'

    let res: Response
    try {
      res = await doFetch(url, {
        method, headers, body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(timeoutMs),
      })
    } catch (err) {
      const cause = (err as { cause?: { code?: string } }).cause?.code ?? (err as Error).message
      throw new ApiError(0, `cannot reach ${baseUrl} (${cause}) — set --url or ANODE_API_URL`)
    }
    const text = await res.text()
    let parsed: unknown = text
    try { parsed = text ? JSON.parse(text) : null } catch { /* not JSON */ }
    if (!res.ok) {
      const hint = res.status === 401 ? ' — set --token or ANODE_API_TOKEN' : ''
      throw new ApiError(res.status, errorMessage(res.status, parsed) + hint, parsed)
    }
    return parsed as T
  }

  return {
    baseUrl,
    get: (path, query) => request('GET', path, undefined, query),
    post: (path, body) => request('POST', path, body ?? {}),
  }
}
