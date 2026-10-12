/**
 * Test-only stand-in for the 'mongodb' package (vitest.spanner.config.ts aliases it).
 * `new MongoClient(url).db(name)` returns the Spanner-backed Db, so the ENTIRE existing test-suite
 * runs against Spanner unchanged. ObjectId is the real one (shared identity with src/).
 */
import { createRequire } from 'node:module'
import { SpannerClient } from '../connect.js'

const real = createRequire(import.meta.url)('mongodb') as typeof import('mongodb')

export const ObjectId = real.ObjectId

export class MongoClient {
  private readonly inner = new SpannerClient()
  constructor(_url?: string) { /* the URL is irrelevant for Spanner */ }
  async connect(): Promise<this> { await this.inner.connect(); return this }
  db(name: string) { return this.inner.db(name) }
  async close(): Promise<void> { await this.inner.close() }
}
