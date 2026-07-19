import type { Db } from 'mongodb'

export interface DagStats {
  dag_id: string
  total_runs: number
  success_count: number
  failed_count: number
  cancelled_count: number
  success_rate: number | null   // 0–1, null when no terminal runs
  avg_duration_ms: number | null
  p95_duration_ms: number | null
  min_duration_ms: number | null
  max_duration_ms: number | null
}

function p95(sorted: number[]): number {
  if (sorted.length === 0) return 0
  const idx = Math.ceil(0.95 * sorted.length) - 1
  return sorted[Math.max(0, idx)]
}

export async function getDagStats(
  db: Db,
  dagId: string,
  limit = 20,
): Promise<DagStats> {
  const runs = await db
    .collection('dag_runs')
    .find({ dag_id: dagId })
    .sort({ created_at: -1 })
    .limit(limit)
    .project({ state: 1, created_at: 1, ended_at: 1 })
    .toArray()

  const total_runs = runs.length
  const success_count = runs.filter(r => r.state === 'success').length
  const failed_count = runs.filter(r => r.state === 'failed').length
  const cancelled_count = runs.filter(r => r.state === 'cancelled').length
  const terminal_count = success_count + failed_count + cancelled_count

  const success_rate = terminal_count > 0 ? success_count / terminal_count : null

  // Duration only for completed runs that have both timestamps
  const durations: number[] = runs
    .filter(r => r.ended_at != null && r.created_at != null)
    .map(r => new Date(r.ended_at).getTime() - new Date(r.created_at).getTime())
    .filter(d => d >= 0)
    .sort((a, b) => a - b)

  const avg_duration_ms =
    durations.length > 0
      ? Math.round(durations.reduce((s, d) => s + d, 0) / durations.length)
      : null

  const p95_duration_ms = durations.length > 0 ? p95(durations) : null
  const min_duration_ms = durations.length > 0 ? durations[0] : null
  const max_duration_ms = durations.length > 0 ? durations[durations.length - 1] : null

  return {
    dag_id: dagId,
    total_runs,
    success_count,
    failed_count,
    cancelled_count,
    success_rate,
    avg_duration_ms,
    p95_duration_ms,
    min_duration_ms,
    max_duration_ms,
  }
}
