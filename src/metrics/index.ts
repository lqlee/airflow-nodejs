/**
 * Prometheus exposition for GET /metrics.
 * State gauges are derived from the DB at scrape time (so they're correct across restarts and
 * replicas); HTTP and scheduler-tick metrics are in-process counters.
 */
import type { Db } from 'mongodb'
import { listDags } from '../dag/registry.js'
import { getPausedDagIds } from '../dag/pause.js'
import { getImportErrors, getDagWarnings } from '../dag/import-errors.js'
import { listPools } from '../pools/index.js'
import { activeWorkers, queueDepth } from '../scheduler/pool.js'
import { AUTH_ENABLED } from '../auth/index.js'
import { family, sample, renderHttpMetrics, getSchedulerStats } from './registry.js'

export const RUN_STATES = ['queued', 'running', 'success', 'failed', 'cancelled'] as const
export const TASK_STATES = ['queued', 'running', 'deferred', 'success', 'failed', 'skipped', 'cancelled'] as const

const startedAtSeconds = Date.now() / 1000

interface Cached { at: number; lines: string[] }
let cache: Cached | null = null

/** DB-derived metrics are cached for METRICS_CACHE_SECONDS (default 5) to protect the DB from tight scrape loops. */
function cacheTtlMs(): number {
  const s = Number(process.env.METRICS_CACHE_SECONDS ?? 5)
  return Number.isFinite(s) && s > 0 ? s * 1000 : 0
}

/** Test helper. */
export function clearMetricsCache(): void {
  cache = null
}

async function stateCounts(db: Db, collection: string, states: readonly string[], known: string[]) {
  const rows = await db.collection(collection).aggregate<{ _id: { dag_id: string; state: string }; n: number }>([
    { $group: { _id: { dag_id: '$dag_id', state: '$state' }, n: { $sum: 1 } } },
  ]).toArray()

  const counts = new Map<string, number>()
  const dagIds = new Set(known)
  for (const r of rows) {
    dagIds.add(r._id.dag_id)
    counts.set(`${r._id.dag_id}|${r._id.state}`, r.n)
  }
  // Zero-fill every known state so absent series don't break alert expressions / rate()
  const lines: string[] = []
  for (const dagId of [...dagIds].sort()) {
    for (const state of states) lines.push(sample(collection === 'dag_runs' ? 'airflow_dag_runs' : 'airflow_task_instances',
      counts.get(`${dagId}|${state}`) ?? 0, { dag_id: dagId, state }))
  }
  return lines
}

async function collectDbMetrics(db: Db): Promise<string[]> {
  const dags = listDags()
  const dagIds = dags.map(d => d.id)
  const paused = await getPausedDagIds(db)

  const [runLines, taskLines, pools, hitlPending, slaUnacked] = await Promise.all([
    stateCounts(db, 'dag_runs', RUN_STATES, dagIds),
    stateCounts(db, 'task_instances', TASK_STATES, dagIds),
    listPools(db),
    db.collection('task_instances').countDocuments({ is_hitl: true, hitl_state: 'pending', state: 'queued' }),
    db.collection('sla_alerts').countDocuments({ acked: false }),
  ])

  const out: string[] = []
  out.push(...family('airflow_dags', 'gauge', 'Dags loaded in the registry.', [sample('airflow_dags', dags.length)]))
  out.push(...family('airflow_dags_paused', 'gauge', 'Loaded Dags that are paused.',
    [sample('airflow_dags_paused', dagIds.filter(id => paused.has(id)).length)]))
  out.push(...family('airflow_dag_runs', 'gauge', 'Dag runs by Dag and state.', runLines))
  out.push(...family('airflow_task_instances', 'gauge', 'Task instances by Dag and state.', taskLines))
  out.push(...family('airflow_pool_slots', 'gauge', 'Total slots per pool.',
    pools.map(p => sample('airflow_pool_slots', p.slots, { pool: p.name }))))
  out.push(...family('airflow_pool_occupied_slots', 'gauge', 'Slots currently held per pool (this process).',
    pools.map(p => sample('airflow_pool_occupied_slots', p.occupied_slots, { pool: p.name }))))
  out.push(...family('airflow_pool_open_slots', 'gauge', 'Free slots per pool (this process).',
    pools.map(p => sample('airflow_pool_open_slots', p.open_slots, { pool: p.name }))))
  out.push(...family('airflow_hitl_pending_tasks', 'gauge', 'Tasks waiting for human approval.', [sample('airflow_hitl_pending_tasks', hitlPending)]))
  out.push(...family('airflow_sla_alerts_unacknowledged', 'gauge', 'SLA alerts not yet acknowledged.', [sample('airflow_sla_alerts_unacknowledged', slaUnacked)]))
  return out
}

/** Render the full Prometheus text exposition. */
export async function renderMetrics(db: Db): Promise<string> {
  const ttl = cacheTtlMs()
  let dbLines: string[]
  if (ttl > 0 && cache && Date.now() - cache.at < ttl) {
    dbLines = cache.lines
  } else {
    dbLines = await collectDbMetrics(db)
    cache = ttl > 0 ? { at: Date.now(), lines: dbLines } : null
  }

  const sched = getSchedulerStats()
  const mem = process.memoryUsage()
  const cpu = process.cpuUsage()
  const lines: string[] = [
    ...family('airflow_up', 'gauge', 'Always 1 while the process is serving.', [sample('airflow_up', 1)]),
    ...family('airflow_auth_enabled', 'gauge', '1 when API key auth is enabled.', [sample('airflow_auth_enabled', AUTH_ENABLED ? 1 : 0)]),
    ...dbLines,
    ...family('airflow_import_errors', 'gauge', 'Dag files that failed to import.', [sample('airflow_import_errors', getImportErrors().length)]),
    ...family('airflow_dag_warnings', 'gauge', 'Soft warnings on loaded Dags.', [sample('airflow_dag_warnings', getDagWarnings().length)]),
    ...family('airflow_workers_active', 'gauge', 'Task workers currently running (this process).', [sample('airflow_workers_active', activeWorkers())]),
    ...family('airflow_workers_queued', 'gauge', 'Tasks waiting for a worker slot (this process).', [sample('airflow_workers_queued', queueDepth())]),
    ...family('airflow_scheduler_ticks_total', 'counter', 'Scheduler ticks completed.', [sample('airflow_scheduler_ticks_total', sched.ticks)]),
    ...family('airflow_scheduler_tick_errors_total', 'counter', 'Scheduler ticks that threw.', [sample('airflow_scheduler_tick_errors_total', sched.errors)]),
    ...family('airflow_scheduler_last_tick_duration_seconds', 'gauge', 'Duration of the most recent scheduler tick.', [sample('airflow_scheduler_last_tick_duration_seconds', sched.lastDurationSeconds)]),
    ...family('airflow_scheduler_last_tick_timestamp_seconds', 'gauge', 'Unix time the most recent scheduler tick finished (alert if stale).', [sample('airflow_scheduler_last_tick_timestamp_seconds', sched.lastTickEndSeconds)]),
    ...renderHttpMetrics(),
    ...family('process_start_time_seconds', 'gauge', 'Process start time, unix seconds.', [sample('process_start_time_seconds', startedAtSeconds)]),
    ...family('process_cpu_seconds_total', 'counter', 'User + system CPU time.', [sample('process_cpu_seconds_total', (cpu.user + cpu.system) / 1e6)]),
    ...family('process_resident_memory_bytes', 'gauge', 'Resident set size.', [sample('process_resident_memory_bytes', mem.rss)]),
    ...family('nodejs_heap_used_bytes', 'gauge', 'V8 heap used.', [sample('nodejs_heap_used_bytes', mem.heapUsed)]),
  ]
  return lines.join('\n') + '\n'
}
