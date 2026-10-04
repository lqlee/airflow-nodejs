/**
 * In-process metric state (HTTP + scheduler tick) and Prometheus text helpers.
 * Dependency-free: the exposition format is simple enough not to need prom-client.
 */

export type Labels = Record<string, string | number>

/** Escape a label value per the Prometheus text format (backslash, quote, newline). */
export function escapeLabelValue(v: string): string {
  return v.replace(/\\/g, '\\\\').replace(/"/g, '\\"').replace(/\n/g, '\\n')
}

export function formatLabels(labels: Labels): string {
  const parts = Object.entries(labels).map(([k, v]) => `${k}="${escapeLabelValue(String(v))}"`)
  return parts.length > 0 ? `{${parts.join(',')}}` : ''
}

/** One sample line, e.g. `name{a="b"} 3`. */
export function sample(name: string, value: number, labels: Labels = {}): string {
  return `${name}${formatLabels(labels)} ${Number.isFinite(value) ? value : 0}`
}

/** `# HELP` / `# TYPE` header lines followed by the samples. */
export function family(name: string, type: 'gauge' | 'counter' | 'histogram', help: string, lines: string[]): string[] {
  return [`# HELP ${name} ${help}`, `# TYPE ${name} ${type}`, ...lines]
}

// ── HTTP request metrics ──────────────────────────────────────────────────────

export const HTTP_BUCKETS = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10]

interface HistogramSeries { buckets: number[]; sum: number; count: number }
const httpDurations = new Map<string, { labels: Labels; h: HistogramSeries }>()

export function recordHttpRequest(method: string, route: string, status: number, seconds: number): void {
  const labels = { method, route, status: String(status) }
  const key = `${method}|${route}|${status}`
  let entry = httpDurations.get(key)
  if (!entry) {
    entry = { labels, h: { buckets: HTTP_BUCKETS.map(() => 0), sum: 0, count: 0 } }
    httpDurations.set(key, entry)
  }
  HTTP_BUCKETS.forEach((le, i) => { if (seconds <= le) entry!.h.buckets[i]++ })
  entry.h.sum += seconds
  entry.h.count++
}

export function renderHttpMetrics(): string[] {
  const lines: string[] = []
  for (const { labels, h } of httpDurations.values()) {
    HTTP_BUCKETS.forEach((le, i) => lines.push(sample('airflow_http_request_duration_seconds_bucket', h.buckets[i], { ...labels, le: String(le) })))
    lines.push(sample('airflow_http_request_duration_seconds_bucket', h.count, { ...labels, le: '+Inf' }))
    lines.push(sample('airflow_http_request_duration_seconds_sum', h.sum, labels))
    lines.push(sample('airflow_http_request_duration_seconds_count', h.count, labels))
  }
  return family('airflow_http_request_duration_seconds', 'histogram', 'HTTP request duration by method, route and status.', lines)
}

// ── Scheduler tick metrics ────────────────────────────────────────────────────

const scheduler = { ticks: 0, errors: 0, lastTickEndSeconds: 0, lastDurationSeconds: 0 }

export function recordSchedulerTick(durationSeconds: number, ok: boolean): void {
  scheduler.ticks++
  if (!ok) scheduler.errors++
  scheduler.lastDurationSeconds = durationSeconds
  scheduler.lastTickEndSeconds = Date.now() / 1000
}

export function getSchedulerStats() {
  return { ...scheduler }
}

/** Test helper — clear all in-process metric state. */
export function resetMetrics(): void {
  httpDurations.clear()
  Object.assign(scheduler, { ticks: 0, errors: 0, lastTickEndSeconds: 0, lastDurationSeconds: 0 })
}
