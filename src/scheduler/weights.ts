import type { TaskDefinition } from '../dag/types.js'

/**
 * Effective claim priority per task, honouring `weightRule`.
 * `downstream` / `upstream` add the priorities of every transitive descendant / ancestor,
 * each counted once (a set, not a sum over paths). Unknown dependency ids are ignored.
 */
export function computeEffectiveWeights(
  tasks: Record<string, Pick<TaskDefinition, 'dependsOn' | 'priority' | 'weightRule'>>,
): Map<string, number> {
  const parents = new Map<string, string[]>()
  const children = new Map<string, string[]>()
  for (const id of Object.keys(tasks)) {
    parents.set(id, [])
    children.set(id, [])
  }
  for (const [id, t] of Object.entries(tasks)) {
    for (const dep of t.dependsOn ?? []) {
      if (!tasks[dep]) continue
      parents.get(id)!.push(dep)
      children.get(dep)!.push(id)
    }
  }

  const reach = (start: string, edges: Map<string, string[]>): Set<string> => {
    const seen = new Set<string>()
    const stack = [...(edges.get(start) ?? [])]
    while (stack.length > 0) {
      const id = stack.pop()!
      if (seen.has(id) || id === start) continue
      seen.add(id)
      stack.push(...(edges.get(id) ?? []))
    }
    return seen
  }

  const out = new Map<string, number>()
  for (const [id, t] of Object.entries(tasks)) {
    let weight = t.priority ?? 0
    const rule = t.weightRule ?? 'absolute'
    if (rule !== 'absolute') {
      for (const rel of reach(id, rule === 'downstream' ? children : parents)) {
        weight += tasks[rel].priority ?? 0
      }
    }
    out.set(id, weight)
  }
  return out
}
