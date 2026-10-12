/**
 * Spanner table layout for the Mongo-style collections.
 *
 * Every collection is one table:  (ns, id) PRIMARY KEY, doc = full document as tagged JSON text,
 * plus a few typed "promoted" columns derived from the doc. The doc is the source of truth;
 * promoted columns only exist so hot queries/sorts/unique constraints run in SQL.
 *   ns  — logical database name (DB_NAME), so many "databases" (tests!) share one Spanner database
 *   id  — _id with a type tag: 'o:<hex>' for ObjectId, 's:<text>' for strings
 *
 * Adding a promoted column later needs ALTER TABLE + a backfill — keep this list to fields that
 * are filtered/sorted/uniquely constrained on a hot path.
 */
export type ColType = 'string' | 'float64' | 'bool' | 'timestamp'

export interface TableSchema {
  collection: string
  columns: Array<{ name: string; type: ColType }>
  indexes: Array<{ name: string; columns: string[]; unique?: boolean }>
}

const S = (name: string) => ({ name, type: 'string' as const })
const N = (name: string) => ({ name, type: 'float64' as const })
const B = (name: string) => ({ name, type: 'bool' as const })
const T = (name: string) => ({ name, type: 'timestamp' as const })
const idx = (collection: string, columns: string[], unique = false) =>
  ({ name: `${collection}_${columns.join('_')}${unique ? '_uq' : ''}`.slice(0, 120), columns, unique })

export const SCHEMAS: TableSchema[] = [
  {
    collection: 'dag_runs',
    columns: [S('dag_id'), S('state'), T('created_at'), T('ordering_date'), T('logical_date'), T('run_after'), S('backfill_id'), S('trigger_type')],
    indexes: [idx('dag_runs', ['dag_id', 'state']), idx('dag_runs', ['state']), idx('dag_runs', ['dag_id', 'ordering_date']), idx('dag_runs', ['dag_id', 'logical_date']), idx('dag_runs', ['dag_id', 'created_at']), idx('dag_runs', ['backfill_id']), idx('dag_runs', ['run_after'])],
  },
  {
    collection: 'task_instances',
    columns: [S('dag_run_id'), S('dag_id'), S('task_id'), N('map_index'), S('state'), T('next_poke_at'), B('is_dynamic_placeholder'), B('is_branch'), B('is_short_circuit'), B('is_hitl'), S('hitl_state'), T('created_at')],
    indexes: [idx('task_instances', ['dag_run_id', 'task_id', 'map_index'], true), idx('task_instances', ['state', 'dag_run_id']), idx('task_instances', ['dag_id', 'state']), idx('task_instances', ['dag_id', 'task_id', 'state'])],
  },
  { collection: 'xcoms', columns: [S('dag_run_id'), S('task_id'), N('map_index'), S('key'), T('pushed_at')], indexes: [idx('xcoms', ['dag_run_id', 'task_id', 'map_index', 'key'], true)] },
  { collection: 'task_instance_tries', columns: [S('dag_run_id'), S('task_id'), N('map_index'), N('try_number')], indexes: [idx('task_instance_tries', ['dag_run_id', 'task_id', 'map_index', 'try_number'])] },
  { collection: 'task_logs', columns: [S('dag_run_id'), S('task_id'), T('ts')], indexes: [idx('task_logs', ['dag_run_id', 'task_id', 'ts'])] },
  { collection: 'dag_paused', columns: [S('dag_id')], indexes: [idx('dag_paused', ['dag_id'], true)] },
  { collection: 'sla_alerts', columns: [S('dag_run_id'), B('acked'), T('fired_at')], indexes: [idx('sla_alerts', ['dag_run_id'], true), idx('sla_alerts', ['acked', 'fired_at'])] },
  { collection: 'api_keys', columns: [B('revoked'), T('created_at'), S('name')], indexes: [idx('api_keys', ['revoked', 'created_at']), idx('api_keys', ['name'])] },
  { collection: 'dataset_events', columns: [S('uri'), T('emitted_at')], indexes: [idx('dataset_events', ['uri', 'emitted_at'])] },
  { collection: 'dataset_watermarks', columns: [S('consumer_dag_id'), S('dataset_uri')], indexes: [idx('dataset_watermarks', ['consumer_dag_id', 'dataset_uri'], true)] },
  { collection: 'connections', columns: [S('conn_id'), S('conn_type')], indexes: [idx('connections', ['conn_id'], true), idx('connections', ['conn_type'])] },
  { collection: 'variables', columns: [S('key')], indexes: [idx('variables', ['key'], true)] },
  { collection: 'dag_versions', columns: [S('dag_id'), S('version'), T('first_seen')], indexes: [idx('dag_versions', ['dag_id', 'version'], true), idx('dag_versions', ['dag_id', 'first_seen'])] },
  { collection: 'pools', columns: [S('name')], indexes: [idx('pools', ['name'], true)] },
  { collection: 'backfills', columns: [S('dag_id'), S('state'), T('created_at')], indexes: [idx('backfills', ['dag_id', 'created_at']), idx('backfills', ['state'])] },
  { collection: 'event_logs', columns: [S('dag_id'), S('dag_run_id'), S('event_type'), T('created_at')], indexes: [idx('event_logs', ['dag_id', 'created_at']), idx('event_logs', ['dag_run_id', 'created_at']), idx('event_logs', ['event_type', 'created_at'])] },
]

export const SCHEMA_BY_COLLECTION = new Map(SCHEMAS.map(s => [s.collection, s]))

const SQL_TYPE: Record<ColType, string> = { string: 'STRING(2048)', float64: 'FLOAT64', bool: 'BOOL', timestamp: 'TIMESTAMP' }

/** GoogleSQL DDL for every table and index (ns is always the leading index column). */
export function buildDdl(): string[] {
  const out: string[] = []
  for (const s of SCHEMAS) {
    const cols = s.columns.map(c => `  ${c.name} ${SQL_TYPE[c.type]}`).join(',\n')
    out.push(`CREATE TABLE ${s.collection} (\n  ns STRING(128) NOT NULL,\n  id STRING(64) NOT NULL,\n  doc STRING(MAX) NOT NULL,\n${cols}\n) PRIMARY KEY (ns, id)`)
    for (const i of s.indexes) {
      out.push(`CREATE ${i.unique ? 'UNIQUE ' : ''}INDEX ${i.name} ON ${s.collection}(ns, ${i.columns.join(', ')})`)
    }
  }
  return out
}
