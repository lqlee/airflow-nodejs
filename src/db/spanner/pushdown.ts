/**
 * Mongo filter/sort → GoogleSQL, for promoted columns only.
 *
 * Contract: the generated WHERE is ALWAYS a superset of the true matches (callers re-apply the JS
 * matcher). `exact` says it is also a subset — only then may LIMIT/OFFSET/COUNT run in SQL.
 * Anything not understood is simply left out (→ inexact), never guessed.
 */
import { Spanner } from '@google-cloud/spanner'
import { isObjectId, isPlainObject } from './codec.js'
import type { ColType, TableSchema } from './schema.js'
import type { Filter, SortSpec } from './eval.js'

export interface SqlQuery {
  where: string
  params: Record<string, unknown>
  types: Record<string, string | { type: 'array'; child: string }>
  exact: boolean
}

const OPS: Record<string, string> = { $gt: '>', $gte: '>=', $lt: '<', $lte: '<=' }

export const idToKey = (id: unknown): string => {
  if (isObjectId(id)) return `o:${id.toHexString()}`
  if (typeof id === 'string') return `s:${id}`
  throw new TypeError(`Unsupported _id type: ${typeof id} (only ObjectId and string are supported)`)
}

function operandFits(type: ColType, v: unknown): boolean {
  switch (type) {
    case 'string': return typeof v === 'string'
    case 'float64': return typeof v === 'number' && Number.isFinite(v)
    case 'bool': return typeof v === 'boolean'
    case 'timestamp': return v instanceof Date && !Number.isNaN(v.getTime())
  }
}

class Builder {
  private n = 0
  params: Record<string, unknown> = {}
  types: Record<string, string | { type: 'array'; child: string }> = {}
  constructor(private readonly schema: TableSchema) {}

  private bindArray(values: unknown[], child: ColType | 'id'): string {
    const name = `p${this.n++}`
    this.params[name] = child === 'float64' ? values.map(v => Spanner.float(v as number)) : values
    this.types[name] = { type: 'array', child: child === 'id' ? 'string' : child }
    return `@${name}`
  }

  private bind(value: unknown, type: ColType | 'id'): string {
    const name = `p${this.n++}`
    this.params[name] = type === 'float64' ? Spanner.float(value as number) : value
    this.types[name] = type === 'id' ? 'string' : type
    return `@${name}`
  }

  /** One field condition → [sql | null, exact]. null = not expressible. */
  private field(key: string, cond: unknown): [string | null, boolean] {
    if (key === '_id') return this.idCond(cond)
    const col = this.schema.columns.find(c => c.name === key)
    if (!col) return [null, false]
    const ops: Record<string, unknown> = isPlainObject(cond) && Object.keys(cond).length > 0 && Object.keys(cond).every(k => k.startsWith('$')) ? cond : { $eq: cond }
    const parts: string[] = []
    let exact = true
    for (const [op, v] of Object.entries(ops)) {
      if (op === '$eq') {
        if (v === null || v === undefined) { parts.push(`${key} IS NULL`); exact = false }   // also catches wrong-typed values
        else if (operandFits(col.type, v)) parts.push(`${key} = ${this.bind(v, col.type)}`)
        else exact = false
      } else if (op === '$in') {
        const vals = (Array.isArray(v) ? v : []) as unknown[]
        if (vals.length > 0 && vals.every(x => x !== null && operandFits(col.type, x))) parts.push(`${key} IN UNNEST(${this.bindArray(vals, col.type)})`)
        else exact = false
      } else if (op in OPS) {
        if (v !== null && operandFits(col.type, v) && col.type !== 'bool') parts.push(`${key} ${OPS[op]} ${this.bind(v, col.type)}`)
        else exact = false
      } else exact = false   // $ne, $nin, $exists, $not, $regex ... → JS only
    }
    return parts.length > 0 ? [parts.join(' AND '), exact] : [null, false]
  }

  private idCond(cond: unknown): [string | null, boolean] {
    const ops: Record<string, unknown> = isPlainObject(cond) && Object.keys(cond).length > 0 && Object.keys(cond).every(k => k.startsWith('$')) ? cond : { $eq: cond }
    const parts: string[] = []
    let exact = true
    for (const [op, v] of Object.entries(ops)) {
      try {
        if (op === '$eq' && v !== null) parts.push(`id = ${this.bind(idToKey(v), 'id')}`)
        else if (op === '$in' && Array.isArray(v) && v.length > 0) parts.push(`id IN UNNEST(${this.bindArray(v.map(idToKey), 'string')})`)
        else if (op in OPS && v !== null) {
          // range within the same id type: bound by the tag prefix ('o:' … 'o;')
          const key = idToKey(v)
          const tag = key.slice(0, 2)
          const upper = `${tag[0]};`
          const p = this.bind(key, 'id')
          if (op === '$lt' || op === '$lte') parts.push(`id >= ${this.bind(tag, 'id')} AND id ${OPS[op]} ${p}`)
          else parts.push(`id ${OPS[op]} ${p} AND id < ${this.bind(upper, 'id')}`)
        } else exact = false
      } catch { exact = false }
    }
    return parts.length > 0 ? [parts.join(' AND '), exact] : [null, false]
  }

  /** AND of everything expressible; (sql null) when nothing is. */
  conj(filter: Filter): [string | null, boolean] {
    const parts: string[] = []
    let exact = true
    for (const [key, cond] of Object.entries(filter)) {
      if (key === '$and' && Array.isArray(cond)) {
        for (const f of cond) { const [s, e] = this.conj(f as Filter); if (s) parts.push(s); exact &&= e }
      } else if (key === '$or' && Array.isArray(cond)) {
        const clauses = cond.map(f => this.conj(f as Filter))
        if (clauses.length > 0 && clauses.every(([s]) => s !== null)) {
          parts.push(`(${clauses.map(([s]) => `(${s})`).join(' OR ')})`)
          exact &&= clauses.every(([, e]) => e)
        } else exact = false
      } else if (key.startsWith('$')) {
        exact = false
      } else {
        const [s, e] = this.field(key, cond)
        if (s) parts.push(s)
        exact &&= e
      }
    }
    return [parts.length > 0 ? parts.join(' AND ') : null, exact && parts.length === Object.keys(filter).length]
  }
}

export function buildQuery(schema: TableSchema, ns: string, filter: Filter): SqlQuery {
  const b = new Builder(schema)
  const [sql, exact] = b.conj(filter)
  b.params.ns = ns
  b.types.ns = 'string'
  return { where: `ns = @ns${sql ? ` AND (${sql})` : ''}`, params: b.params, types: b.types, exact }
}

/** ORDER BY for promoted columns / _id; null when any sort key is not expressible. */
export function buildOrderBy(schema: TableSchema, sort: SortSpec): string | null {
  const parts: string[] = []
  for (const [k, dir] of Object.entries(sort)) {
    if (k === '_id') parts.push(`id ${dir === -1 ? 'DESC' : 'ASC'}`)
    else if (schema.columns.some(c => c.name === k)) parts.push(`${k} ${dir === -1 ? 'DESC' : 'ASC'}`)
    else return null
  }
  return parts.join(', ')
}
