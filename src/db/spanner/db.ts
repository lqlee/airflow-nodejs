/** Mongo-style Db / client facade over Spanner. */
import { Spanner, type Database } from '@google-cloud/spanner'
import { SCHEMAS, SCHEMA_BY_COLLECTION, buildDdl } from './schema.js'
import { SpannerCollection, type Backend } from './collection.js'
import type { Doc } from './eval.js'

export class SpannerDb {
  constructor(private readonly backend: Backend, readonly databaseName: string) {}

  collection<T extends Doc = Doc>(name: string): SpannerCollection<T> {
    const schema = SCHEMA_BY_COLLECTION.get(name)
    if (!schema) throw new Error(`Unknown collection '${name}' — add it to src/db/spanner/schema.ts`)
    return new SpannerCollection<T>(this.backend, this.databaseName, schema)
  }

  /** Deletes every row of this logical database (namespace). Partitioned DML — safe for large tables. */
  async dropDatabase(): Promise<boolean> {
    for (const s of SCHEMAS) {
      await this.backend.database.runPartitionedUpdate({ sql: `DELETE FROM ${s.collection} WHERE ns = @ns`, params: { ns: this.databaseName }, types: { ns: 'string' } })
    }
    return true
  }

  listCollections(): { toArray(): Promise<Array<{ name: string }>> } {
    return {
      toArray: async () => {
        const out: Array<{ name: string }> = []
        for (const s of SCHEMAS) {
          const [rows] = await this.backend.database.run({ sql: `SELECT 1 AS x FROM ${s.collection} WHERE ns = @ns LIMIT 1`, params: { ns: this.databaseName }, types: { ns: 'string' }, json: true }) as unknown as [unknown[]]
          if (rows.length > 0) out.push({ name: s.collection })
        }
        return out
      },
    }
  }

  async command(cmd: Record<string, unknown>): Promise<{ ok: 1 }> {
    if (!('ping' in cmd)) throw new Error('Only { ping: 1 } is supported')
    await this.backend.database.run({ sql: 'SELECT 1' })
    return { ok: 1 }
  }
}

export interface SpannerConnectionConfig {
  projectId: string
  instanceId: string
  databaseId: string
}

export function readSpannerConfig(env: NodeJS.ProcessEnv): SpannerConnectionConfig {
  const projectId = env.SPANNER_PROJECT_ID ?? (env.SPANNER_EMULATOR_HOST ? 'test-project' : '')
  const instanceId = env.SPANNER_INSTANCE_ID ?? (env.SPANNER_EMULATOR_HOST ? 'test-instance' : '')
  const databaseId = env.SPANNER_DATABASE_ID ?? (env.SPANNER_EMULATOR_HOST ? 'airflow' : '')
  const missing = [['SPANNER_PROJECT_ID', projectId], ['SPANNER_INSTANCE_ID', instanceId], ['SPANNER_DATABASE_ID', databaseId]].filter(([, v]) => !v).map(([k]) => k)
  if (missing.length > 0) throw new Error(`Spanner backend needs ${missing.join(', ')}`)
  return { projectId, instanceId, databaseId }
}

const isCode = (e: unknown, ...codes: number[]) => codes.includes((e as { code?: number })?.code ?? -1)

/**
 * Creates any missing tables/indexes (and, on the emulator, the instance + database).
 * Production DDL is usually applied by a DBA: set SPANNER_AUTO_DDL=false and run
 * `npm run spanner:ddl` to get the statements.
 */
export async function ensureSchema(spanner: Spanner, cfg: SpannerConnectionConfig, database: Database): Promise<void> {
  const instance = spanner.instance(cfg.instanceId)
  if (process.env.SPANNER_EMULATOR_HOST) {
    const [exists] = await instance.exists()
    if (!exists) {
      try { const [, op] = await spanner.createInstance(cfg.instanceId, { config: 'emulator-config', nodes: 1, displayName: cfg.instanceId }); await op.promise() }
      catch (e) { if (!isCode(e, 6)) throw e }
    }
    const [dbExists] = await database.exists()
    if (!dbExists) {
      try { const [, op] = await instance.createDatabase(cfg.databaseId); await op.promise() }
      catch (e) { if (!isCode(e, 6)) throw e }
    }
  }
  const [tableRows] = await database.run({ sql: "SELECT table_name FROM information_schema.tables WHERE table_schema = ''", json: true }) as unknown as [Array<{ table_name: string }>]
  const [indexRows] = await database.run({ sql: "SELECT index_name FROM information_schema.indexes WHERE table_schema = '' AND index_type != 'PRIMARY_KEY'", json: true }) as unknown as [Array<{ index_name: string }>]
  const haveTables = new Set(tableRows.map(r => r.table_name))
  const haveIndexes = new Set(indexRows.map(r => r.index_name))

  const missing = buildDdl().filter(stmt => {
    const t = /^CREATE TABLE (\w+)/.exec(stmt)
    if (t) return !haveTables.has(t[1])
    const i = /^CREATE (?:UNIQUE )?INDEX (\w+)/.exec(stmt)
    return i ? !haveIndexes.has(i[1]) : true
  })
  if (missing.length === 0) return
  try {
    const [op] = await database.updateSchema(missing)
    await op.promise()
  } catch (e) {
    // A concurrent process created the same objects first
    if (!/already exists|duplicate/i.test((e as Error).message ?? '') && !isCode(e, 6, 9)) throw e
  }
}
