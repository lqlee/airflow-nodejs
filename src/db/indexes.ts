import type { Db } from 'mongodb'

export async function ensureIndexes(db: Db): Promise<void> {
  // dag_runs: find runs by dag_id + state; tag filter
  await db.collection('dag_runs').createIndexes([
    { key: { dag_id: 1, state: 1 } },
    { key: { run_after: 1 } },
    { key: { dag_id: 1, tags: 1, created_at: -1 } },  // tag filter + sort
    { key: { dag_id: 1, ordering_date: -1 } },        // depends_on_past previous-run lookup (every claim)
    { key: { dag_id: 1, logical_date: -1 } },         // catch-up baseline + duplicate check (every tick)
    { key: { dag_id: 1, created_at: -1 } },           // run list + latest run per Dag (Dag list, polled)
  ])

  // task_instances: claim query (state + dag_run_id) + dependency checks + sensor poke gate
  // IMPORTANT: unique key includes map_index to support mapped tasks (N instances per task_id).
  // Drop the old 2-field unique index if it still exists (pre-dynamic-mapping deployments).
  await db.collection('task_instances').dropIndex('dag_run_id_1_task_id_1').catch(() => { /* already dropped or never existed */ })
  await db.collection('task_instances').createIndexes([
    { key: { state: 1, dag_run_id: 1 } },
    { key: { dag_run_id: 1, task_id: 1, map_index: 1 }, unique: true },
    { key: { dag_id: 1, state: 1 } },
    { key: { dag_run_id: 1, state: 1, next_poke_at: 1 } },  // sensor claim filter
    { key: { pool: 1, state: 1 } },                         // pool occupancy at claim time
    { key: { dag_id: 1, task_id: 1, state: 1 } },            // maxActiveTis count at claim time
    // results whose callbacks are not fired yet (a handful at a time — partial keeps it tiny)
    { key: { callback_pending: 1, dag_run_id: 1 }, partialFilterExpression: { callback_pending: { $type: 'string' } } },
  ])

  // xcoms: unique per (run, task, map_index, key) — map_index scopes mapped instances
  await db.collection('xcoms').createIndexes([
    { key: { dag_run_id: 1, task_id: 1, map_index: 1, key: 1 }, unique: true },
  ])

  // task_instance_tries: full history per (run, task, map_index) ordered by try_number
  await db.collection('task_instance_tries').createIndexes([
    { key: { dag_run_id: 1, task_id: 1, map_index: 1, try_number: 1 } },
  ])

  // task_logs: fetch logs for a task ordered by time
  await db.collection('task_logs').createIndexes([
    { key: { dag_run_id: 1, task_id: 1, ts: 1 } },
  ])

  // dag_paused: pause/resume state per dag
  await db.collection('dag_paused').createIndexes([
    { key: { dag_id: 1 }, unique: true },
  ])

  // deadlines: due-check (every tick), per-run prune / listing, per-Dag listing
  await db.collection('deadlines').createIndexes([
    { key: { missed: 1, deadline_time: 1 } },
    { key: { dag_run_id: 1, missed: 1 } },
    { key: { dag_id: 1, deadline_time: -1 } },
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

  // dataset_alias_links: datasets attached to an alias at runtime — one row per (alias, uri)
  await db.collection('dataset_alias_links').createIndexes([
    { key: { alias: 1, uri: 1 }, unique: true },
  ])

  // dataset_watermarks: unique per (consumer, dataset) — prevents duplicate inserts
  // on concurrent first-boot ticks hitting the upsert path simultaneously
  await db.collection('dataset_watermarks').createIndexes([
    { key: { consumer_dag_id: 1, dataset_uri: 1 }, unique: true },
  ])

  // connections: unique conn_id — fast lookup for task injection
  await db.collection('connections').createIndexes([
    { key: { conn_id: 1 }, unique: true },
    { key: { conn_type: 1 } },
  ])

  // variables: unique key — fast lookup for task injection
  await db.collection('variables').createIndexes([
    { key: { key: 1 }, unique: true },
  ])

  // dag_versions: unique (dag_id, version); list by dag sorted newest-first
  await db.collection('dag_versions').createIndexes([
    { key: { dag_id: 1, version: 1 }, unique: true },
    { key: { dag_id: 1, first_seen: -1 } },
  ])

  // serialized_dags: one row per Dag (_id = dag_id), written by the Dag processor; dag_processor
  // holds a single state row (_id 'state') and needs no index
  await db.collection('serialized_dags').createIndexes([
    { key: { dag_id: 1 }, unique: true },
  ])

  // pools: unique name + sorted list
  await db.collection('pools').createIndexes([
    { key: { name: 1 }, unique: true },
  ])

  // backfills: list by dag, filter by state
  await db.collection('backfills').createIndexes([
    { key: { dag_id: 1, created_at: -1 } },
    { key: { state: 1 } },
  ])

  // dag_runs: backfill_id for lifecycle gating
  await db.collection('dag_runs').createIndex({ backfill_id: 1 })

  // dag_runs: at most one run per (dag_id, logical_date) when the date is set — stops two schedulers
  // (or a scheduler racing a backfill) from creating the same scheduled run. Manual runs have no date.
  // Non-fatal: data that already holds duplicates must not stop the server from starting.
  await db.collection('dag_runs').createIndex(
    { dag_id: 1, logical_date: 1 },
    { name: 'dag_runs_dag_id_logical_date_uq', unique: true, partialFilterExpression: { logical_date: { $type: 'date' } } },
  ).catch((err: unknown) => {
    console.warn(`[db] unique (dag_id, logical_date) index NOT created — existing runs violate it, so duplicates are not prevented: ${(err as Error).message}`)
  })

  // scheduler_instances: liveness lookup for zombie detection
  await db.collection('scheduler_instances').createIndexes([{ key: { heartbeat_at: 1 } }])

  // ui_sessions: browser sessions + OIDC login attempts, looked up by the hash of their id; Mongo
  // drops expired rows itself (TTL index), and createSession also prunes them (the Spanner path)
  await db.collection('ui_sessions').createIndexes([
    { key: { key_hash: 1 }, unique: true },
    { key: { expires_at: 1 }, expireAfterSeconds: 0 },
  ])

  // event_logs: audit trail — query by dag, run, or event type; sorted by time
  await db.collection('event_logs').createIndexes([
    { key: { dag_id: 1, created_at: -1 } },
    { key: { dag_run_id: 1, created_at: -1 } },
    { key: { event_type: 1, created_at: -1 } },
  ])
}
