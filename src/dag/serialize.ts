/**
 * Serialized Dags — the form a Dag takes in the database, so the scheduler and API never import user code.
 *
 * The Dag processor (dag/processor.ts) imports each Dag file in a child process and stores
 * serializeDag(dag) in `serialized_dags`. Everyone else rebuilds the Dag with hydrateDag(): plain data is
 * restored as-is, and every function (task run/poke/branch, callbacks, timetable, deadline callbacks)
 * becomes a stub that
 *   - is still `typeof 'function'`, so code checking "is this a sensor?" keeps working,
 *   - returns the function's source from toString(), which is how task functions already reach workers,
 *   - when called, runs the real function in the sandbox process (sandbox/client.ts) — never in-process.
 *
 * Encoding (JSON):  function → { "__fn": "<source>" },  Date → { "__date": "<ISO>" }.
 * Sources are normalized to an expression that evaluates to the function, so method shorthand
 * (`notify(info) { … }`) survives the round trip.
 */
import type { DagDefinition } from './types.js'

/** Marks a hydrated Dag (its functions are stubs) and carries the stub's call reference. */
export const SERIALIZED = Symbol.for('airflow-nodejs.serialized-dag')
export const USER_CODE = Symbol.for('airflow-nodejs.user-code')

/** Where a stubbed function lives: the sandbox re-imports `fileloc` and calls the function at `path`. */
export interface UserCodeRef {
  dagId: string
  fileloc: string | null
  /** Property path from the Dag object, e.g. ['tasks', 'load', 'onFailure', 0] or ['onSuccess', 'notify']. */
  path: Array<string | number>
  /** Normalized source — used when the file no longer defines the function at `path`. */
  source: string
}

export type UserCodeInvoker = (ref: UserCodeRef, args: unknown[]) => Promise<unknown>

let invoker: UserCodeInvoker = async (ref) => {
  throw new Error(`user code (${ref.dagId}.${ref.path.join('.')}) must run in the sandbox, which is not started`)
}

/** Set how stubbed functions run (sandbox/client.ts installs itself at import). */
export function setUserCodeInvoker(fn: UserCodeInvoker): void {
  invoker = fn
}

const NATIVE = /\{\s*\[native code\]\s*\}\s*$/

/** Turn a function into source text that evaluates (as an expression) to an equivalent function. */
export function normalizeFunctionSource(fn: (...args: never[]) => unknown): string {
  const src = Function.prototype.toString.call(fn)
  if (NATIVE.test(src)) throw new Error('native or bound functions cannot be serialized — use a plain function')
  // Method shorthand (`run(ctx) {}`, `async notify(i) {}`) is not an expression; wrap it in an object literal
  for (const candidate of [src, `Object.values({ ${src} })[0]`]) {
    try {
      // Compiles the text without running it — a syntax check only
      new Function(`return (${candidate})`)   // eslint-disable-line no-new-func
      return candidate
    } catch { /* next form */ }
  }
  throw new Error('function source cannot be re-parsed as an expression')
}

function isPlainObject(v: object): boolean {
  const proto = Object.getPrototypeOf(v)
  return proto === Object.prototype || proto === null
}

function encode(value: unknown, path: string, seen: Set<object>): unknown {
  if (typeof value === 'function') {
    try {
      return { __fn: normalizeFunctionSource(value as () => unknown) }
    } catch (err) {
      throw new Error(`${path}: ${(err as Error).message}`)
    }
  }
  if (value === null || typeof value !== 'object') {
    if (typeof value === 'bigint') throw new Error(`${path}: BigInt values cannot be serialized`)
    if (typeof value === 'symbol') return undefined
    return value
  }
  if (value instanceof Date) return { __date: value.toISOString() }
  if (seen.has(value)) throw new Error(`${path}: circular reference`)
  seen.add(value)
  try {
    if (Array.isArray(value)) return value.map((v, i) => encode(v, `${path}[${i}]`, seen) ?? null)
    if (!isPlainObject(value)) {
      const name = value.constructor?.name ?? 'object'
      throw new Error(`${path}: ${name} values cannot be serialized — use plain objects, arrays and functions`)
    }
    const out: Record<string, unknown> = {}
    for (const [k, v] of Object.entries(value)) {
      const encoded = encode(v, `${path}.${k}`, seen)
      if (encoded !== undefined) out[k] = encoded
    }
    return out
  } finally {
    seen.delete(value)
  }
}

/** JSON for the database. Throws (an import error for the file) on values that cannot round-trip. */
export function serializeDag(dag: DagDefinition): string {
  return JSON.stringify(encode(dag, dag.id ?? 'dag', new Set()))
}

function makeStub(ref: UserCodeRef): (...args: unknown[]) => Promise<unknown> {
  const stub = (...args: unknown[]) => invoker(ref, args)
  Object.defineProperty(stub, 'toString', { value: () => ref.source })
  Object.defineProperty(stub, USER_CODE, { value: ref })
  return stub
}

function decode(value: unknown, base: Omit<UserCodeRef, 'path' | 'source'>, path: Array<string | number>): unknown {
  if (value === null || typeof value !== 'object') return value
  if (Array.isArray(value)) return value.map((v, i) => decode(v, base, [...path, i]))
  const obj = value as Record<string, unknown>
  const keys = Object.keys(obj)
  if (keys.length === 1 && typeof obj.__fn === 'string') return makeStub({ ...base, path, source: obj.__fn })
  if (keys.length === 1 && typeof obj.__date === 'string') return new Date(obj.__date)
  const out: Record<string, unknown> = {}
  for (const k of keys) out[k] = decode(obj[k], base, [...path, k])
  return out
}

/** Rebuild a Dag from its serialized JSON; functions become sandbox stubs. */
export function hydrateDag(data: string, fileloc: string | null = null): DagDefinition {
  const parsed = JSON.parse(data) as { id: string }
  const dag = decode(parsed, { dagId: parsed.id, fileloc }, []) as DagDefinition
  Object.defineProperty(dag, SERIALIZED, { value: true })
  return dag
}

export function isSerializedDag(dag: object | undefined): boolean {
  return Boolean(dag && (dag as Record<symbol, unknown>)[SERIALIZED])
}

/** The call reference of a stubbed function, or null for a real (in-process) function. */
export function userCodeRef(fn: unknown): UserCodeRef | null {
  return typeof fn === 'function' ? ((fn as unknown as Record<symbol, UserCodeRef>)[USER_CODE] ?? null) : null
}

/** Follow a property path (as recorded in a UserCodeRef) from a Dag object. */
export function resolvePath(root: unknown, path: Array<string | number>): { parent: unknown; value: unknown } {
  let parent: unknown
  let value: unknown = root
  for (const key of path) {
    if (value === null || (typeof value !== 'object' && typeof value !== 'function')) return { parent: undefined, value: undefined }
    parent = value
    value = (value as Record<string | number, unknown>)[key]
  }
  return { parent, value }
}
