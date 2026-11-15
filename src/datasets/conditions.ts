/**
 * Dataset conditions (Airflow asset expressions): which dataset events schedule a consumer Dag.
 *
 *   'uri'                       — one dataset
 *   { all: [c1, c2, …] }        — every one has a new event (Airflow `a & b`, AssetAll)
 *   { any: [c1, c2, …] }        — at least one has (Airflow `a | b`, AssetAny)
 *   { alias: 'name' }           — any dataset a task has attached to this alias at runtime (AssetAlias)
 *   ['u1', 'u2']                — shorthand for { all: ['u1', 'u2'] } (the original form)
 */
import type { DatasetCondition } from '../dag/types.js'

export const allOf = (...items: DatasetCondition[]): DatasetCondition => ({ all: items })
export const anyOf = (...items: DatasetCondition[]): DatasetCondition => ({ any: items })
export const datasetAlias = (name: string): DatasetCondition => ({ alias: name })

/** The Dag's condition in canonical form, or null when it is not dataset-scheduled. */
export function normalizeCondition(datasets: DatasetCondition | string[] | undefined): DatasetCondition | null {
  if (datasets === undefined || datasets === null) return null
  if (Array.isArray(datasets)) return datasets.length === 0 ? null : { all: datasets }
  return datasets
}

/** Throws on a malformed condition (the loader reports it as an import error). */
export function validateCondition(c: unknown, path = 'datasets'): void {
  if (typeof c === 'string') {
    if (!c) throw new Error(`${path}: empty dataset URI`)
    return
  }
  if (Array.isArray(c)) { c.forEach((x, i) => validateCondition(x, `${path}[${i}]`)); return }
  if (!c || typeof c !== 'object') throw new Error(`${path}: expected a URI, { all }, { any } or { alias }`)
  const keys = Object.keys(c)
  if (keys.length !== 1) throw new Error(`${path}: use exactly one of all / any / alias`)
  const o = c as Record<string, unknown>
  if ('alias' in o) {
    if (typeof o.alias !== 'string' || !o.alias) throw new Error(`${path}.alias: expected a name`)
  } else if ('all' in o || 'any' in o) {
    const list = o.all ?? o.any
    if (!Array.isArray(list) || list.length === 0) throw new Error(`${path}.${keys[0]}: expected a non-empty list`)
    list.forEach((x, i) => validateCondition(x, `${path}.${keys[0]}[${i}]`))
  } else {
    throw new Error(`${path}: unknown key '${keys[0]}'`)
  }
}

/** Dataset URIs named directly in the condition. */
export function conditionUris(c: DatasetCondition | null): string[] {
  const out = new Set<string>()
  const walk = (x: DatasetCondition) => {
    if (typeof x === 'string') out.add(x)
    else if ('all' in x) x.all.forEach(walk)
    else if ('any' in x) x.any.forEach(walk)
  }
  if (c) walk(c)
  return [...out]
}

/** Alias names referenced by the condition. */
export function conditionAliases(c: DatasetCondition | null): string[] {
  const out = new Set<string>()
  const walk = (x: DatasetCondition) => {
    if (typeof x === 'string') return
    if ('alias' in x) out.add(x.alias)
    else ('all' in x ? x.all : x.any).forEach(walk)
  }
  if (c) walk(c)
  return [...out]
}

/**
 * Is the condition met? `ready(uri)` says whether that dataset has an unconsumed event;
 * an alias is met when any dataset attached to it is (Airflow AssetAlias evaluation).
 */
export function evaluateCondition(c: DatasetCondition, ready: (uri: string) => boolean, aliasUris: (name: string) => string[]): boolean {
  if (typeof c === 'string') return ready(c)
  if ('alias' in c) return aliasUris(c.alias).some(ready)
  if ('all' in c) return c.all.every(x => evaluateCondition(x, ready, aliasUris))
  return c.any.some(x => evaluateCondition(x, ready, aliasUris))
}

/** Human-readable form: `a & (b | alias:c)`. */
export function describeCondition(c: DatasetCondition | null, nested = false): string {
  if (!c) return ''
  if (typeof c === 'string') return c
  if ('alias' in c) return `alias:${c.alias}`
  const [op, items] = 'all' in c ? [' & ', c.all] : [' | ', c.any]
  const s = items.map(x => describeCondition(x, true)).join(op)
  return nested && items.length > 1 ? `(${s})` : s
}
