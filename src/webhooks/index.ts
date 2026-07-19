/**
 * Webhook delivery for dag run completion callbacks.
 *
 * `deliverWebhook` is the pure, injectable unit — takes a fetchFn so tests
 * can capture calls without hitting a real network.
 *
 * `fireWebhook` is the fire-and-forget scheduler wrapper: kicks off delivery
 * with a bounded timeout and logs failures without throwing.
 */

export interface WebhookPayload {
  dag_id: string
  run_id: string
  state: 'success' | 'failed'
  logical_date: Date | null
  conf: Record<string, unknown>
  tags: string[]
  ended_at: Date
}

export interface DeliverOptions {
  /** ms before aborting the POST. Default: 5000. */
  timeoutMs?: number
  /** Injected for testing. Default: global fetch. */
  fetchFn?: (url: string, init: RequestInit) => Promise<{ status: number; ok: boolean }>
}

/**
 * POST the payload to url. Returns the HTTP status code.
 * Throws on network error or timeout — caller decides how to handle.
 */
export async function deliverWebhook(
  url: string,
  payload: WebhookPayload,
  options: DeliverOptions = {},
): Promise<number> {
  const { timeoutMs = 5_000, fetchFn = fetch } = options
  const controller = new AbortController()
  const timer = setTimeout(() => controller.abort(), timeoutMs)

  try {
    const res = await fetchFn(url, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
      signal: controller.signal,
    })
    return res.status
  } finally {
    clearTimeout(timer)
  }
}

/**
 * Fire-and-forget webhook delivery.
 * Logs delivery result / failure; never throws; never blocks the caller.
 */
export function fireWebhook(
  url: string,
  payload: WebhookPayload,
  options: DeliverOptions = {},
): void {
  deliverWebhook(url, payload, options).then(status => {
    if (status >= 200 && status < 300) {
      console.log(`[webhook] ${payload.dag_id}/${payload.run_id} → ${url} — HTTP ${status}`)
    } else {
      console.warn(`[webhook] ${payload.dag_id}/${payload.run_id} → ${url} — HTTP ${status} (non-2xx)`)
    }
  }).catch((err: unknown) => {
    const msg = err instanceof Error ? err.message : String(err)
    console.error(`[webhook] ${payload.dag_id}/${payload.run_id} → ${url} — delivery failed: ${msg}`)
  })
}
