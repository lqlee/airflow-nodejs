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
  source: 'dags/providers'  // always local for now
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

export function listProviders(): ProviderRecord[] {
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
  return _providers.get(providerName)?.operators[operatorName]
}
