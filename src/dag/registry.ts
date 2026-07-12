import type { DagDefinition } from './types.js'

const registry = new Map<string, DagDefinition>()

export function register(dag: DagDefinition): void {
  registry.set(dag.id, dag)
}

export function getDag(dagId: string): DagDefinition | undefined {
  return registry.get(dagId)
}

export function listDags(): DagDefinition[] {
  return [...registry.values()]
}

export function clearRegistry(): void {
  registry.clear()
}
