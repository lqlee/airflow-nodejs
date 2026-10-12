import { MongoClient, type Db } from 'mongodb'
import { SpannerClient } from './spanner/connect.js'

export type DbBackend = 'mongo' | 'spanner'

export function getBackend(env: NodeJS.ProcessEnv = process.env): DbBackend {
  const b = (env.DB_BACKEND ?? 'mongo').toLowerCase()
  if (b !== 'mongo' && b !== 'spanner') throw new Error(`DB_BACKEND must be 'mongo' or 'spanner' (got '${env.DB_BACKEND}')`)
  return b
}

export interface DbClient {
  connect(opts?: { schema?: boolean; light?: boolean }): Promise<void>
  db(name: string): Db
  close(): Promise<void>
}

/**
 * The single place that opens a database connection — the scheduler, forked task workers,
 * the BullMQ consumer and the k8s worker all go through here so they always agree on the backend.
 * `light: true` is for short-lived workers (tiny Spanner session pool, no DDL check).
 */
export function createDbClient(): DbClient {
  if (getBackend() === 'spanner') {
    const c = new SpannerClient()
    return {
      connect: async (opts) => { await c.connect(opts?.light ? { schema: false, pool: { min: 1, max: 4 } } : { schema: opts?.schema }) },
      db: (name) => c.db(name) as unknown as Db,
      close: () => c.close(),
    }
  }
  const c = new MongoClient(process.env.MONGO_URL ?? 'mongodb://localhost:27017')
  return { connect: async () => { await c.connect() }, db: (name) => c.db(name), close: () => c.close() }
}
