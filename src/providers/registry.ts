/**
 * Provider registry — tracks providers loaded from dags/providers/*.js
 *
 * A provider is a JS module that exports:
 *   - name: string           — unique provider id
 *   - version?: string       — semver
 *   - description?: string   — human-readable summary
 *   - operators: Record<string, OperatorFactory>
 *   - connectionTypes?: string[]  — connection type ids this provider supports
 *
 * Operators are factory functions: (opts) => TaskDefinition
 * They produce shell/python/container/run task specs — not arbitrary closures.
 */

import type { TaskDefinition } from '../dag/types.js'
import { BUILTIN_PROVIDERS, OPERATORS } from './builtin/catalog.js'
import { operatorTask } from './builtin/operators.js'

export type OperatorFactory = (opts?: Record<string, unknown>) => TaskDefinition

export interface ProviderDefinition {
  name: string
  version: string
  description: string
  operators: Record<string, OperatorFactory>
  connectionTypes: string[]
}

export interface ProviderRecord {
  name: string
  version: string
  description: string
  operator_names: string[]
  connection_types: string[]
  /** 'builtin' = ships with airflow-nodejs (providers/builtin); 'dags/providers' = a local provider file. */
  source: 'dags/providers' | 'builtin'
}

const BUILTIN_VERSION = '1.0.0'

/** The built-in providers (SQL, Google, Kafka, SMTP) — always listed, whatever loaded from dags/providers. */
function builtinRecords(): ProviderRecord[] {
  return BUILTIN_PROVIDERS.map(p => ({
    name: p.name, version: BUILTIN_VERSION, description: p.description,
    operator_names: p.operator_names, connection_types: p.connection_types, source: 'builtin' as const,
  }))
}

const _providers = new Map<string, ProviderDefinition>()

export function registerProvider(def: ProviderDefinition): void {
  _providers.set(def.name, def)
}

export function clearProviders(): void {
  _providers.clear()
}

export function getProvider(name: string): ProviderDefinition | undefined {
  return _providers.get(name)
}

/** Provider records reported by the Dag processor — what the API lists when it loads no providers itself. */
let _synced: ProviderRecord[] = []

export function setSyncedProviders(records: ProviderRecord[]): void {
  _synced = records
}

/** Built-in providers followed by the local ones (dags/providers) — what the API lists. */
export function listAllProviders(): ProviderRecord[] {
  return [...builtinRecords(), ...listProviders()]
}

/** Local providers: loaded in this process, else those the Dag processor reported. */
export function listProviders(): ProviderRecord[] {
  if (_providers.size === 0) return [..._synced]
  return [..._providers.values()].map(p => ({
    name: p.name,
    version: p.version,
    description: p.description,
    operator_names: Object.keys(p.operators),
    connection_types: p.connectionTypes,
    source: 'dags/providers' as const,
  }))
}

export function getOperator(providerName: string, operatorName: string): OperatorFactory | undefined {
  const local = _providers.get(providerName)?.operators[operatorName]
  if (local) return local
  return OPERATORS[operatorName]?.provider === providerName ? (opts => operatorTask(operatorName, opts)) : undefined
}
