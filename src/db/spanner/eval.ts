/**
 * MongoDB query semantics in plain JS — the reference implementation for the Spanner backend.
 * SQL pushdown is only ever an optimisation on top of this; anything unsupported THROWS
 * (never silently mis-matches).
 */
import { ObjectId } from 'mongodb'
import { isObjectId, isPlainObject } from './codec.js'

export type Doc = Record<string, unknown>
export type Filter = Record<string, unknown>

const MISSING = Symbol('missing')
type Maybe = unknown | typeof MISSING

// ── values ─────────────────────────────────────────────────────────────────

function typeRank(v: unknown): number {
  if (v === null || v === undefined) return 1
  if (typeof v === 'number') return 2
  if (typeof v === 'string') return 3
  if (Array.isArray(v)) return 5
  if (Buffer.isBuffer(v)) return 6
  if (isObjectId(v)) return 7
  if (typeof v === 'boolean') return 8
  if (v instanceof Date) return 9
  return 4   // object
}

/** Total order matching BSON type ordering — used for sort. */
export function compareValues(a: unknown, b: unknown): number {
  const ra = typeRank(a), rb = typeRank(b)
  if (ra !== rb) return ra - rb
  switch (ra) {
    case 1: return 0
    case 2: return (a as number) - (b as number) || 0
    case 3: return (a as string) < (b as string) ? -1 : (a as string) > (b as string) ? 1 : 0
    case 7: return (a as ObjectId).toHexString() < (b as ObjectId).toHexString() ? -1 : (a as ObjectId).toHexString() > (b as ObjectId).toHexString() ? 1 : 0
    case 8: return Number(a) - Number(b)
    case 9: return (a as Date).getTime() - (b as Date).getTime()
    case 5: {
      const x = a as unknown[], y = b as unknown[]
      for (let i = 0; i < Math.min(x.length, y.length); i++) { const c = compareValues(x[i], y[i]); if (c) return c }
      return x.length - y.length
    }
    case 6: return Buffer.compare(a as Buffer, b as Buffer)
    default: {
      const x = a as Doc, y = b as Doc
      const kx = Object.keys(x), ky = Object.keys(y)
      for (let i = 0; i < Math.min(kx.length, ky.length); i++) {
        if (kx[i] !== ky[i]) return kx[i] < ky[i] ? -1 : 1
        const c = compareValues(x[kx[i]], y[ky[i]]); if (c) return c
      }
      return kx.length - ky.length
    }
  }
}

export function valuesEqual(a: unknown, b: unknown): boolean {
  if (a === undefined) a = null
  if (b === undefined) b = null
  if (typeRank(a) !== typeRank(b)) return false
  return compareValues(a, b) === 0
}

/** Same BSON comparison bracket (numbers with numbers, strings with strings, ...) — required for $lt/$gt. */
const sameBracket = (a: unknown, b: unknown) => typeRank(a) === typeRank(b)

// ── paths ──────────────────────────────────────────────────────────────────

/** Candidate values at a dotted path; arrays on the way are traversed (Mongo semantics). */
function resolvePath(value: unknown, parts: string[]): Maybe[] {
  if (parts.length === 0) return [value]
  if (Array.isArray(value)) {
    const idx = /^\d+$/.test(parts[0]) ? Number(parts[0]) : -1
    const out: Maybe[] = []
    if (idx >= 0 && idx < value.length) out.push(...resolvePath(value[idx], parts.slice(1)))
    for (const el of value) if (isPlainObject(el)) out.push(...resolvePath(el, parts))
    return out
  }
  if (isPlainObject(value)) {
    if (!(parts[0] in value)) return [MISSING]
    return resolvePath(value[parts[0]], parts.slice(1))
  }
  return [MISSING]
}

export function getPath(doc: Doc, path: string): unknown {
  const vals = resolvePath(doc, path.split('.'))
  return vals.length === 0 || vals[0] === MISSING ? undefined : vals[0]
}

// ── filter ─────────────────────────────────────────────────────────────────

const isOperatorObject = (v: unknown): v is Record<string, unknown> =>
  isPlainObject(v) && Object.keys(v).length > 0 && Object.keys(v).every(k => k.startsWith('$'))

/** Equality of one candidate against an operand: array candidates also match if they contain it. */
function eqCandidate(c: Maybe, operand: unknown): boolean {
  if (operand === null || operand === undefined) return c === MISSING || c === null || c === undefined
  if (c === MISSING) return false
  if (valuesEqual(c, operand)) return true
  return Array.isArray(c) && c.some(el => valuesEqual(el, operand))
}

function matchOperators(cands: Maybe[], ops: Record<string, unknown>, doc: Doc): boolean {
  for (const [op, operand] of Object.entries(ops)) {
    switch (op) {
      case '$eq': if (!cands.some(c => eqCandidate(c, operand))) return false; break
      case '$ne': if (cands.some(c => eqCandidate(c, operand))) return false; break
      case '$in': {
        if (!Array.isArray(operand)) throw new TypeError('$in needs an array')
        if (!cands.some(c => operand.some(o => o instanceof RegExp ? typeof c === 'string' && o.test(c) : eqCandidate(c, o)))) return false
        break
      }
      case '$nin': {
        if (!Array.isArray(operand)) throw new TypeError('$nin needs an array')
        if (cands.some(c => operand.some(o => eqCandidate(c, o)))) return false
        break
      }
      case '$gt': case '$gte': case '$lt': case '$lte': {
        const ok = cands.some(c => {
          const flat = Array.isArray(c) ? c : [c]
          return flat.some(v => {
            if (operand === null && (op === '$gte' || op === '$lte')) return v === MISSING || v === null || v === undefined
            if (v === MISSING) return false
            if (operand === null) return op === '$gte' || op === '$lte' ? v === null || v === undefined : false  // $gte/$lte null ≡ equality with null
            if (!sameBracket(v, operand)) return false
            const d = compareValues(v, operand)
            return op === '$gt' ? d > 0 : op === '$gte' ? d >= 0 : op === '$lt' ? d < 0 : d <= 0
          })
        })
        if (!ok) return false
        break
      }
      case '$exists': {
        const exists = cands.some(c => c !== MISSING)
        if (Boolean(operand) !== exists) return false
        break
      }
      case '$not': {
        if (!isOperatorObject(operand)) throw new TypeError('$not needs an operator expression')
        if (matchOperators(cands, operand, doc)) return false
        break
      }
      case '$regex': {
        const re = operand instanceof RegExp ? operand : new RegExp(String(operand), typeof ops.$options === 'string' ? ops.$options : '')
        if (!cands.some(c => (typeof c === 'string' && re.test(c)) || (Array.isArray(c) && c.some(x => typeof x === 'string' && re.test(x))))) return false
        break
      }
      case '$options': break   // consumed by $regex
      case '$size': if (!cands.some(c => Array.isArray(c) && c.length === operand)) return false; break
      case '$all': {
        if (!Array.isArray(operand)) throw new TypeError('$all needs an array')
        if (!cands.some(c => Array.isArray(c) && operand.every(o => c.some(el => valuesEqual(el, o))))) return false
        break
      }
      default: throw new Error(`Unsupported query operator '${op}'`)
    }
  }
  return true
}

export function matchesFilter(doc: Doc, filter: Filter): boolean {
  for (const [key, cond] of Object.entries(filter)) {
    if (key === '$and') { if (!(cond as Filter[]).every(f => matchesFilter(doc, f))) return false; continue }
    if (key === '$or') { if (!(cond as Filter[]).some(f => matchesFilter(doc, f))) return false; continue }
    if (key === '$nor') { if ((cond as Filter[]).some(f => matchesFilter(doc, f))) return false; continue }
    if (key.startsWith('$')) throw new Error(`Unsupported top-level query operator '${key}'`)

    const cands = resolvePath(doc, key.split('.'))
    if (cond instanceof RegExp) {
      if (!matchOperators(cands, { $regex: cond }, doc)) return false
    } else if (isOperatorObject(cond)) {
      if (!matchOperators(cands, cond, doc)) return false
    } else if (!cands.some(c => eqCandidate(c, cond))) return false
  }
  return true
}

/** Equality fields a filter pins — used to seed an upserted document. */
export function equalityFields(filter: Filter): Doc {
  const out: Doc = {}
  const take = (f: Filter) => {
    for (const [k, v] of Object.entries(f)) {
      if (k === '$and') { (v as Filter[]).forEach(take); continue }
      if (k === '$or' && (v as Filter[]).length === 1) { take((v as Filter[])[0]); continue }   // MongoDB seeds from a single-clause $or
      if (k.startsWith('$')) continue
      if (isOperatorObject(v)) { if ('$eq' in v) setPath(out, k, v.$eq); continue }
      setPath(out, k, v)
    }
  }
  take(filter)
  return out
}

// ── update ─────────────────────────────────────────────────────────────────

export function setPath(doc: Doc, path: string, value: unknown): void {
  const parts = path.split('.')
  let cur: Doc = doc
  for (let i = 0; i < parts.length - 1; i++) {
    const next = cur[parts[i]]
    if (next === undefined) cur[parts[i]] = {}
    else if (!isPlainObject(next)) throw new Error(`Cannot create field '${parts[i + 1]}' in element {${parts[i]}: ${JSON.stringify(next)}}`)
    cur = cur[parts[i]] as Doc
  }
  cur[parts[parts.length - 1]] = value
}

function unsetPath(doc: Doc, path: string): void {
  const parts = path.split('.')
  let cur: unknown = doc
  for (let i = 0; i < parts.length - 1; i++) { cur = isPlainObject(cur) ? cur[parts[i]] : undefined }
  if (isPlainObject(cur)) delete cur[parts[parts.length - 1]]
}

const getDeep = (doc: Doc, path: string): unknown => path.split('.').reduce<unknown>((c, k) => (isPlainObject(c) ? c[k] : undefined), doc)

export interface UpdateOpts { isInsert?: boolean }

/** Applies an update document in place; returns nothing — callers compare before/after for modifiedCount. */
export function applyUpdate(doc: Doc, update: Doc, opts: UpdateOpts = {}): void {
  const keys = Object.keys(update)
  if (keys.length === 0 || keys.some(k => !k.startsWith('$'))) {
    throw new Error('Replacement-style updates are not supported — use update operators ($set, $inc, ...)')
  }
  for (const [op, fields] of Object.entries(update)) {
    if (!isPlainObject(fields)) throw new TypeError(`${op} needs an object`)
    for (const [path, value] of Object.entries(fields)) {
      if (path === '_id' && op !== '$setOnInsert') throw new Error("Performing an update on the path '_id' would modify the immutable field '_id'")
      switch (op) {
        case '$set': setPath(doc, path, value === undefined ? null : value); break
        case '$setOnInsert': if (opts.isInsert) setPath(doc, path, value); break
        case '$unset': unsetPath(doc, path); break
        case '$inc': {
          const cur = getDeep(doc, path)
          if (cur !== undefined && cur !== null && typeof cur !== 'number') throw new TypeError(`Cannot apply $inc to a non-numeric value at '${path}'`)
          if (typeof value !== 'number') throw new TypeError('$inc needs a number')
          setPath(doc, path, (typeof cur === 'number' ? cur : 0) + value)
          break
        }
        case '$min': case '$max': {
          const cur = getDeep(doc, path)
          if (cur === undefined || (op === '$min' ? compareValues(value, cur) < 0 : compareValues(value, cur) > 0)) setPath(doc, path, value)
          break
        }
        case '$push': {
          const cur = getDeep(doc, path)
          if (cur !== undefined && !Array.isArray(cur)) throw new TypeError(`$push on non-array '${path}'`)
          const items = isPlainObject(value) && '$each' in value ? (value.$each as unknown[]) : [value]
          setPath(doc, path, [...((cur as unknown[] | undefined) ?? []), ...items])
          break
        }
        case '$addToSet': {
          const cur = getDeep(doc, path)
          if (cur !== undefined && !Array.isArray(cur)) throw new TypeError(`$addToSet on non-array '${path}'`)
          const arr = [...((cur as unknown[] | undefined) ?? [])]
          for (const item of isPlainObject(value) && '$each' in value ? (value.$each as unknown[]) : [value]) {
            if (!arr.some(x => valuesEqual(x, item))) arr.push(item)
          }
          setPath(doc, path, arr)
          break
        }
        case '$pull': {
          const cur = getDeep(doc, path)
          if (Array.isArray(cur)) setPath(doc, path, cur.filter(x => !valuesEqual(x, value)))
          break
        }
        default: throw new Error(`Unsupported update operator '${op}'`)
      }
    }
  }
}

// ── sort / projection ──────────────────────────────────────────────────────

export type SortSpec = Record<string, 1 | -1>

export function sortDocs<T extends Doc>(docs: T[], spec: SortSpec): T[] {
  const entries = Object.entries(spec)
  return docs.map((d, i) => [d, i] as const).sort(([a, ia], [b, ib]) => {
    for (const [path, dir] of entries) {
      const c = compareValues(getPath(a, path), getPath(b, path))
      if (c) return c * dir
    }
    return ia - ib   // stable
  }).map(([d]) => d)
}

export function projectDoc<T extends Doc>(doc: T, projection: Record<string, 0 | 1 | boolean>): Doc {
  const entries = Object.entries(projection).filter(([k]) => k !== '_id')
  const includeId = projection._id === undefined ? true : Boolean(projection._id)
  if (entries.length === 0) return includeId ? doc : (({ _id: _omit, ...r }) => r)(doc)
  const inclusive = entries.some(([, v]) => Boolean(v))
  if (inclusive) {
    const out: Doc = {}
    if (includeId && '_id' in doc) out._id = doc._id
    for (const [path, v] of entries) {
      if (!v) throw new Error('Cannot mix inclusion and exclusion in a projection')
      const val = getPath(doc, path)
      if (val !== undefined) setPath(out, path, val)
    }
    return out
  }
  const copy: Doc = { ...doc }   // callers pass decoded (unshared) docs, so nested unset is safe
  for (const [path] of entries) unsetPath(copy, path)
  if (!includeId) delete copy._id
  return copy
}

export function newId(): ObjectId { return new ObjectId() }
