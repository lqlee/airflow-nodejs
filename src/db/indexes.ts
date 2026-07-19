import type { Db } from 'mongodb'

export async function ensureIndexes(db: Db): Promise<void> {
  // dag_runs: find runs by dag_id + state
  await db.collection('dag_runs').createIndexes([
    { key: { dag_id: 1, state: 1 } },
    { key: { run_after: 1 } },
  ])

  // task_instances: claim query (state + dag_run_id) + dependency checks + sensor poke gate
  await db.collection('task_instances').createIndexes([
    { key: { state: 1, dag_run_id: 1 } },
    { key: { dag_run_id: 1, task_id: 1 }, unique: true },
    { key: { dag_id: 1, state: 1 } },
    { key: { dag_run_id: 1, state: 1, next_poke_at: 1 } },  // sensor claim filter
  ])

  // xcoms: lookup by run + source task + key
  await db.collection('xcoms').createIndexes([
    { key: { dag_run_id: 1, task_id: 1, key: 1 }, unique: true },
  ])

  // task_logs: fetch logs for a task ordered by time
  await db.collection('task_logs').createIndexes([
    { key: { dag_run_id: 1, task_id: 1, ts: 1 } },
  ])

  // dag_paused: pause/resume state per dag
  await db.collection('dag_paused').createIndexes([
    { key: { dag_id: 1 }, unique: true },
  ])

  // sla_alerts: lookup by run (dedup) + unacked filter
  await db.collection('sla_alerts').createIndexes([
    { key: { dag_run_id: 1 }, unique: true },
    { key: { acked: 1, fired_at: -1 } },
  ])

  // api_keys: fast validation (revoked=false scan) + name lookup
  await db.collection('api_keys').createIndexes([
    { key: { revoked: 1, created_at: -1 } },
    { key: { name: 1 } },
  ])

  // dataset_events: aggregate latest event per URI efficiently
  await db.collection('dataset_events').createIndexes([
    { key: { uri: 1, emitted_at: -1 } },
  ])

  // dataset_watermarks: unique per (consumer, dataset) — prevents duplicate inserts
  // on concurrent first-boot ticks hitting the upsert path simultaneously
  await db.collection('dataset_watermarks').createIndexes([
    { key: { consumer_dag_id: 1, dataset_uri: 1 }, unique: true },
  ])
}
