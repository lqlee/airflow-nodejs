/**
 * Dynamic Task Mapping — pure helpers.
 * Branch A: literal expand (static array known at authoring time).
 * Branch B (XCom-driven): planned; schema is forward-compatible.
 */

export interface MappedInstance {
  map_index: number
  map_value: unknown
}

/**
 * Pure function — no DB access.
 * Given a task's `expand` array, return one MappedInstance per element.
 * Non-mapped tasks (expand undefined/null) return empty array.
 *
 * Validation:
 * - expand must be a non-empty array
 * - values can be any JSON-serializable type (primitives, objects, arrays)
 */
export function planExpansion(expand: unknown[] | undefined | null): MappedInstance[] {
  if (!Array.isArray(expand) || expand.length === 0) return []
  return expand.map((value, index) => ({ map_index: index, map_value: value }))
}

/**
 * Return true if a task is a mapped task (has an expand array with ≥1 item).
 */
export function isMappedTask(expand: unknown[] | undefined | null): boolean {
  return Array.isArray(expand) && expand.length > 0
}
