/**
 * Built-in operator factories for Dag files:
 *
 *   import { SQLExecuteQueryOperator, EmailOperator } from 'airflow-nodejs/providers'
 *
 *   tasks: {
 *     load:   SQLExecuteQueryOperator({ connId: 'warehouse', sql: "DELETE FROM sales WHERE ds = '{{ ds }}'", retries: 2 }),
 *     notify: EmailOperator({ to: 'team@example.com', subject: 'loaded {{ ds }}', text: 'done', dependsOn: ['load'] }),
 *   }
 *
 * A factory returns a plain task whose run (or poke, for sensors) function is self-contained: its
 * options are inlined as JSON and it calls ctx.runOperator(), which the worker implements. So the task
 * survives serialization and runs in any executor (forked worker, BullMQ, Kubernetes pod, tasks test).
 * Task options (dependsOn, retries, pool, timeout, callbacks, …) pass through to the task; every other
 * option must be plain data (strings, numbers, booleans, arrays, objects). String values are templated
 * at run time ({{ ds }}, {{ params.x }}, {{ map_value }}, macros — see scheduler/template.ts).
 */
import type { TaskDefinition } from '../../dag/types.js'
import { OPERATORS, type OperatorSpec } from './catalog.js'

/** TaskDefinition fields an operator call may set besides its own options. */
const TASK_KEYS = new Set([
  'dependsOn', 'group', 'pool', 'poolSlots', 'triggerRule', 'priority', 'weightRule', 'setup', 'teardown',
  'onFailureFailDagrun', 'maxActiveTis', 'maxActiveTisPerRun', 'onExecute', 'onSuccess', 'onFailure', 'onRetry',
  'dependsOnPast', 'retries', 'retryDelay', 'retryExponentialBackoff', 'maxRetryDelay', 'timeout', 'expand',
  'pokeInterval', 'sensorTimeout', 'requiresApproval', 'hitlPrompt',
])

export type OperatorOptions = Record<string, unknown> & Partial<TaskDefinition>

/** Throws unless `v` is JSON data — functions, class instances, cycles and the like cannot travel. */
function assertPlainData(v: unknown, path: string, seen = new Set<unknown>()): void {
  if (v === null || ['string', 'number', 'boolean'].includes(typeof v)) {
    if (typeof v === 'number' && !Number.isFinite(v)) throw new TypeError(`${path} must be a finite number`)
    return
  }
  if (typeof v !== 'object') throw new TypeError(`${path} must be plain data (got ${typeof v})`)
  if (seen.has(v)) throw new TypeError(`${path} contains a cycle`)
  seen.add(v)
  if (Array.isArray(v)) v.forEach((item, i) => assertPlainData(item, `${path}[${i}]`, seen))
  else {
    const proto = Object.getPrototypeOf(v)
    if (proto !== Object.prototype && proto !== null) throw new TypeError(`${path} must be a plain object or array`)
    for (const [k, item] of Object.entries(v)) if (item !== undefined) assertPlainData(item, `${path}.${k}`, seen)
  }
  seen.delete(v)
}

/** Build the task for operator `name` — validates its options at Dag parse time (import error). */
export function operatorTask(name: string, options: OperatorOptions = {}): TaskDefinition {
  const spec: OperatorSpec | undefined = OPERATORS[name]
  if (!spec) throw new Error(`unknown operator '${name}'`)
  if (options === null || typeof options !== 'object' || Array.isArray(options)) throw new TypeError(`${name}: options must be an object`)

  const task: Record<string, unknown> = {}
  const args: Record<string, unknown> = {}
  const allowed = new Set([...spec.required, ...spec.optional])
  for (const [k, v] of Object.entries(options)) {
    if (v === undefined) continue
    if (TASK_KEYS.has(k)) task[k] = v
    else if (allowed.has(k)) args[k] = v
    else throw new TypeError(`${name}: unknown option '${k}' (options: ${[...allowed].join(', ')})`)
  }
  const missing = spec.required.filter(k => args[k] === undefined || args[k] === null || args[k] === '')
  if (missing.length) throw new TypeError(`${name}: missing required option(s) ${missing.join(', ')}`)
  assertPlainData(args, name)

  // The run function carries its options as literal JSON — no closure, so its source alone runs it
  const src = `async (ctx) => ctx.runOperator(${JSON.stringify(spec.impl)}, ${JSON.stringify(args)})`
  const fn = new Function(`return (${src})`)() as TaskDefinition['run']   // eslint-disable-line no-new-func
  return { ...task, [spec.kind]: fn } as TaskDefinition
}

const factory = (name: keyof typeof OPERATORS & string) => (options: OperatorOptions = {}) => operatorTask(name, options)

export const SQLExecuteQueryOperator = factory('SQLExecuteQueryOperator')
export const SQLCheckOperator = factory('SQLCheckOperator')
export const SqlSensor = factory('SqlSensor')
export const LocalFilesystemToGCSOperator = factory('LocalFilesystemToGCSOperator')
export const GCSToLocalFilesystemOperator = factory('GCSToLocalFilesystemOperator')
export const GCSListObjectsOperator = factory('GCSListObjectsOperator')
export const GCSDeleteObjectsOperator = factory('GCSDeleteObjectsOperator')
export const GCSObjectExistenceSensor = factory('GCSObjectExistenceSensor')
export const BigQueryInsertJobOperator = factory('BigQueryInsertJobOperator')
export const BigQueryCheckOperator = factory('BigQueryCheckOperator')
export const ProduceToTopicOperator = factory('ProduceToTopicOperator')
export const ConsumeFromTopicOperator = factory('ConsumeFromTopicOperator')
export const AwaitMessageSensor = factory('AwaitMessageSensor')
export const EmailOperator = factory('EmailOperator')
