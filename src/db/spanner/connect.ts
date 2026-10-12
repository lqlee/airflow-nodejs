import { Spanner, type Database } from '@google-cloud/spanner'
import { SpannerDb, readSpannerConfig, ensureSchema } from './db.js'
import type { Backend } from './collection.js'

export interface SpannerConnectOptions {
  /** Create missing tables/indexes. Default: SPANNER_AUTO_DDL (default true on the emulator, false otherwise). */
  schema?: boolean
  /** Session pool bounds. Forked task workers use a tiny pool. */
  pool?: { min: number; max: number }
}

/** MongoClient-shaped wrapper: connect() / db(name) / close(). */
export class SpannerClient {
  private spanner: Spanner | null = null
  private backend: Backend | null = null

  async connect(opts: SpannerConnectOptions = {}): Promise<this> {
    if (this.backend) return this
    const cfg = readSpannerConfig(process.env)
    // GOOGLE_APPLICATION_CREDENTIALS (service-account JSON file) is picked up by the client library;
    // SPANNER_EMULATOR_HOST switches it to the emulator with no credentials.
    this.spanner = new Spanner({ projectId: cfg.projectId })
    const database: Database = this.spanner.instance(cfg.instanceId).database(cfg.databaseId, opts.pool ?? { min: 2, max: 50 })
    this.backend = { database }

    const autoDdl = opts.schema ?? (process.env.SPANNER_AUTO_DDL ? process.env.SPANNER_AUTO_DDL === 'true' : Boolean(process.env.SPANNER_EMULATOR_HOST))
    if (autoDdl) await ensureSchema(this.spanner, cfg, database)
    return this
  }

  db(name: string): SpannerDb {
    if (!this.backend) throw new Error('SpannerClient is not connected')
    return new SpannerDb(this.backend, name)
  }

  async close(): Promise<void> {
    const s = this.spanner
    this.spanner = null
    this.backend = null
    if (s) await s.close()
  }
}
