/**
 * Mongo-style Collection / FindCursor on top of Spanner (see schema.ts for the table layout).
 * Only the API surface airflow-nodejs uses is implemented; everything else throws.
 */
import { Spanner, type Database } from '@google-cloud/spanner'
import { ObjectId } from 'mongodb'
import { encodeDoc, decodeDoc, cloneDoc, isObjectId, isPlainObject } from './codec.js'
import {
  matchesFilter, applyUpdate, equalityFields, sortDocs, projectDoc, getPath, compareValues,
  type Doc, type Filter, type SortSpec,
} from './eval.js'
import { buildQuery, buildOrderBy, idToKey, type SqlQuery } from './pushdown.js'
import type { TableSchema } from './schema.js'

export interface Backend { database: Database }
type Runner = { run(q: object): Promise<unknown[]> }

const BATCH = 1000
const WARN_SCAN_ROWS = 2000
const warned = new Set<string>()

export function duplicateKeyError(collection: string, cause: unknown): Error {
  const err = new Error(`E11000 duplicate key error collection: ${collection} (${(cause as Error)?.message ?? cause})`, { cause }) as Error & { code: number }
  err.name = 'MongoServerError'
  err.code = 11000
  return err
}

const isAlreadyExists = (e: unknown) => (e as { code?: number })?.code === 6
/** Primary-key (not unique-index) violation: "Table X: Row {...} already exists." */
const isPrimaryKeyViolation = (e: unknown) => isAlreadyExists(e) && /Table \w+: Row .* already exists/.test((e as Error).message ?? '')

// ── column mapping ──────────────────────────────────────────────────────────

function columnValue(schema: TableSchema, name: string, type: string, doc: Doc): unknown {
  const v = getPath(doc, name)
  if (v === undefined || v === null) return null
  if (Array.isArray(v) || isPlainObject(v)) {
    throw new TypeError(`${schema.collection}.${name} is a promoted scalar column but the document holds an ${Array.isArray(v) ? 'array' : 'object'}`)
  }
  switch (type) {
    case 'string': return typeof v === 'string' ? v : null
    case 'float64': return typeof v === 'number' && Number.isFinite(v) ? Spanner.float(v) : null
    case 'bool': return typeof v === 'boolean' ? v : null
    case 'timestamp': return v instanceof Date && !Number.isNaN(v.getTime()) ? v : null
    default: return null
  }
}

function toRow(schema: TableSchema, ns: string, doc: Doc): Record<string, unknown> {
  const row: Record<string, unknown> = { ns, id: idToKey(doc._id), doc: encodeDoc(doc) }
  for (const c of schema.columns) row[c.name] = columnValue(schema, c.name, c.type, doc)
  return row
}

// ── querying ────────────────────────────────────────────────────────────────

/** FORCE_INDEX when an index's leading column is constrained by the (pushed) filter. */
function chooseIndex(schema: TableSchema, filter: Filter): string | null {
  const keys = new Set(Object.keys(filter).filter(k => !k.startsWith('$')))
  let best: { name: string; score: number } | null = null
  for (const ix of schema.indexes) {
    let score = 0
    for (const col of ix.columns) { if (keys.has(col)) score++; else break }
    if (score > 0 && (!best || score > best.score)) best = { name: ix.name, score }
  }
  return best?.name ?? null
}

interface FindOpts { sort?: SortSpec; limit?: number; skip?: number; projection?: Record<string, 0 | 1 | boolean> }

async function sqlRun(runner: Runner, sql: string, q: SqlQuery): Promise<Array<Record<string, unknown>>> {
  const [rows] = (await runner.run({ sql, params: q.params, types: q.types, json: true, jsonOptions: { wrapNumbers: false } })) as [Array<Record<string, unknown>>]
  return rows
}

async function selectDocs(runner: Runner, schema: TableSchema, ns: string, filter: Filter, opts: FindOpts = {}): Promise<Doc[]> {
  const q = buildQuery(schema, ns, filter)
  const sortSpec = opts.sort ?? {}
  const orderBy = Object.keys(sortSpec).length > 0 ? buildOrderBy(schema, sortSpec) : ''
  const sqlSort = q.exact && orderBy !== null   // ordering in SQL only matters when SQL also does the limiting
  const pushLimit = q.exact && sqlSort && opts.limit !== undefined && opts.limit > 0

  const hint = chooseIndex(schema, filter)
  let sql = `SELECT id, doc FROM ${schema.collection}${hint ? `@{FORCE_INDEX=${hint}}` : ''} WHERE ${q.where}`
  if (sqlSort && orderBy) sql += ` ORDER BY ${orderBy}${/(^|, )id (ASC|DESC)/.test(orderBy) ? '' : ', id'}`
  if (pushLimit) sql += ` LIMIT ${Math.floor(opts.limit!) + Math.max(0, Math.floor(opts.skip ?? 0))}`

  const rows = await sqlRun(runner, sql, q)
  if (rows.length > WARN_SCAN_ROWS && !q.exact && !warned.has(`${schema.collection}:${JSON.stringify(Object.keys(filter))}`)) {
    warned.add(`${schema.collection}:${JSON.stringify(Object.keys(filter))}`)
    console.warn(`[spanner] ${schema.collection}: filter on ${JSON.stringify(Object.keys(filter))} scanned ${rows.length} rows in JS — promote a column?`)
  }

  // Insertion (≈ _id) order first: MongoDB returns unsorted results — and ties under a sort — in natural order
  let docs = rows.map(r => decodeDoc<Doc>(r.doc as string)).filter(d => matchesFilter(d, filter)).sort((a, b) => compareValues(a._id, b._id))
  if (Object.keys(sortSpec).length > 0) docs = sortDocs(docs, sortSpec)   // stable: ties keep _id order
  const skip = Math.max(0, Math.floor(opts.skip ?? 0))
  if (skip) docs = docs.slice(skip)
  if (opts.limit !== undefined && opts.limit > 0) docs = docs.slice(0, Math.floor(opts.limit))
  return docs
}

// ── cursor ──────────────────────────────────────────────────────────────────

export class SpannerFindCursor<T extends Doc = Doc> {
  private opts: FindOpts
  private buffer: T[] | null = null
  private pos = 0
  constructor(private readonly exec: (opts: FindOpts) => Promise<Doc[]>, opts: FindOpts = {}) { this.opts = { ...opts } }

  sort(spec: SortSpec | string, dir?: 1 | -1): this {
    this.opts.sort = typeof spec === 'string' ? { [spec]: dir ?? 1 } : spec
    return this
  }
  limit(n: number): this { this.opts.limit = n; return this }
  skip(n: number): this { this.opts.skip = n; return this }
  project(p: Record<string, 0 | 1 | boolean>): this { this.opts.projection = p; return this }

  private async load(): Promise<T[]> {
    if (!this.buffer) {
      const docs = await this.exec(this.opts)
      this.buffer = (this.opts.projection ? docs.map(d => projectDoc(d, this.opts.projection!)) : docs) as T[]
    }
    return this.buffer
  }
  async toArray(): Promise<T[]> { return [...(await this.load())] }
  async next(): Promise<T | null> { const b = await this.load(); return this.pos < b.length ? b[this.pos++] : null }
  async hasNext(): Promise<boolean> { const b = await this.load(); return this.pos < b.length }
  async forEach(fn: (d: T) => void | boolean | Promise<void | boolean>): Promise<void> {
    for (const d of await this.load()) { if ((await fn(d)) === false) break }
  }
  async *[Symbol.asyncIterator](): AsyncGenerator<T> { for (const d of await this.load()) yield d }
  async close(): Promise<void> { /* nothing held open */ }
}

// ── collection ──────────────────────────────────────────────────────────────

export class SpannerCollection<T extends Doc = Doc> {
  constructor(private readonly backend: Backend, private readonly ns: string, private readonly schema: TableSchema) {}

  get collectionName(): string { return this.schema.collection }

  private get db(): Database { return this.backend.database }

  private async txn<R>(fn: (tx: any) => Promise<R>): Promise<R> {
    try {
      return await this.db.runTransactionAsync(async (tx: any) => {
        const result = await fn(tx)
        await tx.commit()
        return result
      })
    } catch (e) {
      if (isAlreadyExists(e)) throw duplicateKeyError(this.schema.collection, e)
      throw e
    }
  }

  find(filter: Filter = {}, options: FindOpts = {}): SpannerFindCursor<T> {
    return new SpannerFindCursor<T>(opts => selectDocs(this.db as unknown as Runner, this.schema, this.ns, filter, opts), options)
  }

  async findOne(filter: Filter = {}, options: FindOpts = {}): Promise<T | null> {
    const [doc] = await this.find(filter, { ...options, limit: 1 }).toArray()
    return doc ?? null
  }

  async countDocuments(filter: Filter = {}): Promise<number> {
    const q = buildQuery(this.schema, this.ns, filter)
    if (q.exact) {
      const [rows] = (await this.db.run({ sql: `SELECT COUNT(*) AS c FROM ${this.schema.collection} WHERE ${q.where}`, params: q.params, types: q.types, json: true, jsonOptions: { wrapNumbers: false } })) as unknown as [Array<{ c: number | string }>]
      return Number(rows[0]?.c ?? 0)
    }
    return (await selectDocs(this.db as unknown as Runner, this.schema, this.ns, filter)).length
  }

  /**
   * Insert rows whose _id we generated. A freshly generated ObjectId cannot collide with an existing
   * row, so a primary-key violation means an earlier attempt of ours looked committed (client retry
   * after an abort / lost commit response). Don't trust it: re-read, and re-insert whatever is missing.
   */
  private async insertGenerated(docs: Doc[]): Promise<void> {
    let pending = docs
    for (let attempt = 0; pending.length > 0; attempt++) {
      try {
        await this.txn(async tx => { tx.insert(this.schema.collection, pending.map(d => toRow(this.schema, this.ns, d))) })
        return
      } catch (e) {
        const dup = (e as { code?: number }).code === 11000 && isPrimaryKeyViolation((e as { cause?: unknown }).cause)
        if (!dup || attempt >= 5) throw e
      }
      const ids = pending.map(d => idToKey(d._id))
      const [rows] = (await this.db.run({
        sql: `SELECT id FROM ${this.schema.collection} WHERE ns = @ns AND id IN UNNEST(@ids)`,
        params: { ns: this.ns, ids },
        types: { ns: 'string', ids: { type: 'array', child: 'string' } },
        json: true,
      })) as unknown as [Array<{ id: string }>]
      const present = new Set(rows.map(r => r.id))
      pending = pending.filter(d => !present.has(idToKey(d._id)))
    }
  }

  async insertOne(doc: Doc): Promise<{ acknowledged: true; insertedId: unknown }> {
    const copy = cloneDoc(doc)
    if ('_id' in copy) {
      await this.txn(async tx => { tx.insert(this.schema.collection, [toRow(this.schema, this.ns, copy)]) })
      return { acknowledged: true, insertedId: copy._id }
    }
    const withId: Doc = { _id: new ObjectId(), ...copy }
    await this.insertGenerated([withId])
    return { acknowledged: true, insertedId: withId._id }
  }

  async insertMany(docs: Doc[], _options?: { ordered?: boolean }): Promise<{ acknowledged: true; insertedCount: number; insertedIds: Record<number, unknown> }> {
    const prepared = docs.map(d => { const c = cloneDoc(d); return '_id' in c ? c : { _id: new ObjectId(), ...c } })
    const generated = docs.map(d => !('_id' in d))
    for (let i = 0; i < prepared.length; i += BATCH) {
      const chunk = prepared.slice(i, i + BATCH)
      if (generated.slice(i, i + BATCH).every(Boolean)) await this.insertGenerated(chunk)
      else await this.txn(async tx => { tx.insert(this.schema.collection, chunk.map(d => toRow(this.schema, this.ns, d))) })
    }
    return { acknowledged: true, insertedCount: prepared.length, insertedIds: Object.fromEntries(prepared.map((d, i) => [i, d._id])) }
  }

  private async updateInTxn(tx: any, filter: Filter, update: Doc, opts: { upsert?: boolean; many?: boolean; sort?: SortSpec }) {
    const matched = await selectDocs(tx as Runner, this.schema, this.ns, filter, { sort: opts.sort, limit: opts.many ? undefined : 1 })
    const targets = opts.many ? matched : matched.slice(0, 1)
    const changes: Array<{ before: Doc; after: Doc }> = []
    for (const before of targets) {
      const after = cloneDoc(before)
      applyUpdate(after, update)
      if (encodeDoc(after) !== encodeDoc(before)) {
        tx.replace(this.schema.collection, [toRow(this.schema, this.ns, after)])
        changes.push({ before, after })
      }
    }
    if (targets.length === 0 && opts.upsert) {
      const seed = equalityFields(filter)
      applyUpdate(seed, update, { isInsert: true })
      const { _id, ...rest } = seed
      const doc: Doc = { _id: _id ?? new ObjectId(), ...rest }
      tx.insert(this.schema.collection, [toRow(this.schema, this.ns, doc)])
      return { matched: 0, changes, upserted: doc }
    }
    return { matched: targets.length, changes, upserted: null as Doc | null }
  }

  async updateOne(filter: Filter, update: Doc, options: { upsert?: boolean } = {}) {
    const r = await this.txn(tx => this.updateInTxn(tx, filter, update, { upsert: options.upsert }))
    return { acknowledged: true, matchedCount: r.matched, modifiedCount: r.changes.length, upsertedCount: r.upserted ? 1 : 0, upsertedId: r.upserted?._id ?? null }
  }

  async updateMany(filter: Filter, update: Doc, options: { upsert?: boolean } = {}) {
    const r = await this.txn(tx => this.updateInTxn(tx, filter, update, { upsert: options.upsert, many: true }))
    return { acknowledged: true, matchedCount: r.matched, modifiedCount: r.changes.length, upsertedCount: r.upserted ? 1 : 0, upsertedId: r.upserted?._id ?? null }
  }

  async findOneAndUpdate(
    filter: Filter, update: Doc,
    options: { returnDocument?: 'before' | 'after'; upsert?: boolean; sort?: SortSpec; projection?: Record<string, 0 | 1 | boolean>; includeResultMetadata?: boolean } = {},
  ): Promise<T | null | { value: T | null; ok: 1 }> {
    const r = await this.txn(tx => this.updateInTxn(tx, filter, update, { upsert: options.upsert, sort: options.sort }))
    let out: Doc | null = null
    if (r.upserted) out = options.returnDocument === 'after' ? r.upserted : null
    else if (r.matched > 0) {
      const ch = r.changes[0]
      // no-op update → document unchanged; fetch it for both before/after
      out = ch ? (options.returnDocument === 'after' ? ch.after : ch.before) : (await selectDocs(this.db as unknown as Runner, this.schema, this.ns, filter, { sort: options.sort, limit: 1 }))[0] ?? null
    }
    if (out && options.projection) out = projectDoc(out, options.projection)
    return options.includeResultMetadata ? { value: out as T | null, ok: 1 } : (out as T | null)
  }

  async deleteOne(filter: Filter): Promise<{ acknowledged: true; deletedCount: number }> {
    const deleted = await this.txn(async tx => {
      const [doc] = await selectDocs(tx as Runner, this.schema, this.ns, filter, { limit: 1 })
      if (!doc) return 0
      tx.deleteRows(this.schema.collection, [[this.ns, idToKey(doc._id)]])
      return 1
    })
    return { acknowledged: true, deletedCount: deleted }
  }

  async deleteMany(filter: Filter = {}): Promise<{ acknowledged: true; deletedCount: number }> {
    const q = buildQuery(this.schema, this.ns, filter)
    const deleted = await this.txn(async tx => {
      if (q.exact) {
        const [count] = await tx.runUpdate({ sql: `DELETE FROM ${this.schema.collection} WHERE ${q.where}`, params: q.params, types: q.types })
        return Number(count)
      }
      const docs = await selectDocs(tx as Runner, this.schema, this.ns, filter)
      for (let i = 0; i < docs.length; i += BATCH) tx.deleteRows(this.schema.collection, docs.slice(i, i + BATCH).map(d => [this.ns, idToKey(d._id)]))
      return docs.length
    })
    return { acknowledged: true, deletedCount: deleted }
  }

  /** Subset of aggregation stages: $match $sort $skip $limit $group (with $first $last $sum $min $max $push $avg). */
  aggregate(pipeline: Doc[]): { toArray(): Promise<Doc[]> } {
    return { toArray: () => this.runPipeline(pipeline) }
  }

  private async runPipeline(pipeline: Doc[]): Promise<Doc[]> {
    let stages = [...pipeline]
    let docs: Doc[]
    if (stages[0] && '$match' in stages[0]) { docs = await selectDocs(this.db as unknown as Runner, this.schema, this.ns, stages[0].$match as Filter); stages = stages.slice(1) }
    else docs = await selectDocs(this.db as unknown as Runner, this.schema, this.ns, {})
    for (const stage of stages) {
      const [name] = Object.keys(stage)
      const arg = stage[name]
      switch (name) {
        case '$match': docs = docs.filter(d => matchesFilter(d, arg as Filter)); break
        case '$sort': docs = sortDocs(docs, arg as SortSpec); break
        case '$skip': docs = docs.slice(arg as number); break
        case '$limit': docs = docs.slice(0, arg as number); break
        case '$group': docs = groupDocs(docs, arg as Doc); break
        default: throw new Error(`Unsupported aggregation stage '${name}'`)
      }
    }
    return docs.map(d => cloneDoc(d))
  }

  // Index management is done by DDL (schema.ts); these exist so ensureIndexes() runs unchanged.
  async createIndexes(specs: Array<{ key: Record<string, unknown>; unique?: boolean }>): Promise<string[]> { return specs.map(s => Object.keys(s.key).join('_')) }
  async createIndex(key: Record<string, unknown>): Promise<string> { return Object.keys(key).join('_') }
  async dropIndex(_name: string): Promise<void> { /* no-op */ }
}

function groupDocs(docs: Doc[], spec: Doc): Doc[] {
  const { _id: idExpr, ...accs } = spec
  // Expressions: '$path' → field value; objects/arrays (e.g. a compound _id) are evaluated recursively
  const field = (expr: unknown, d: Doc): unknown => {
    if (typeof expr === 'string' && expr.startsWith('$')) return getPath(d, expr.slice(1)) ?? null
    if (Array.isArray(expr)) return expr.map(e => field(e, d))
    if (isPlainObject(expr)) return Object.fromEntries(Object.entries(expr).map(([k, v]) => [k, field(v, d)]))
    return expr
  }
  const groups = new Map<string, { key: unknown; docs: Doc[] }>()
  for (const d of docs) {
    const key = idExpr === null ? null : field(idExpr, d)
    const k = encodeDoc({ k: key } as Doc)
    const g = groups.get(k) ?? { key, docs: [] }
    g.docs.push(d)
    groups.set(k, g)
  }
  return [...groups.values()].map(g => {
    const out: Doc = { _id: g.key }
    for (const [name, acc] of Object.entries(accs)) {
      const [op] = Object.keys(acc as Doc)
      const expr = (acc as Doc)[op]
      const vals = g.docs.map(d => field(expr, d))
      switch (op) {
        case '$first': out[name] = vals[0] ?? null; break
        case '$last': out[name] = vals[vals.length - 1] ?? null; break
        case '$sum': out[name] = vals.reduce<number>((s, v) => s + (typeof v === 'number' ? v : 0), 0); break
        case '$avg': { const n = vals.filter(v => typeof v === 'number') as number[]; out[name] = n.length ? n.reduce((a, b) => a + b, 0) / n.length : null; break }
        case '$min': out[name] = vals.reduce((a, b) => (a === undefined || compareValues(b, a) < 0 ? b : a), undefined as unknown) ?? null; break
        case '$max': out[name] = vals.reduce((a, b) => (a === undefined || compareValues(b, a) > 0 ? b : a), undefined as unknown) ?? null; break
        case '$push': out[name] = vals; break
        default: throw new Error(`Unsupported $group accumulator '${op}'`)
      }
    }
    return out
  })
}

export { isObjectId }
