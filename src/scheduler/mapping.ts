/**
 * Dynamic Task Mapping — pure helpers.
 * Branch A: literal expand (static array known at authoring time).
 * Branch B: XCom-driven expand (list produced at runtime by an upstream task).
 */

import type { DynamicExpand } from '../dag/types.js'

export interface MappedInstance {
  map_index: number
  map_value: unknown
}

/**
 * Pure function — no DB access.
 * Given a task's `expand` array, return one MappedInstance per element.
 * Non-mapped tasks (expand undefined/null) return empty array.
 */
export function planExpansion(expand: unknown[] | undefined | null): MappedInstance[] {
  if (!Array.isArray(expand) || expand.length === 0) return []
  return expand.map((value, index) => ({ map_index: index, map_value: value }))
}

/**
 * Return true if a task uses literal expand (static array with ≥1 item).
 */
export function isLiteralMapped(expand: unknown): expand is unknown[] {
  return Array.isArray(expand) && expand.length > 0
}

/**
 * Return true if a task uses XCom-driven dynamic expand.
 */
export function isDynamicMapped(expand: unknown): expand is DynamicExpand {
  return (
    expand !== null &&
    typeof expand === 'object' &&
    !Array.isArray(expand) &&
    typeof (expand as DynamicExpand).from === 'string' &&
    typeof (expand as DynamicExpand).key === 'string'
  )
}

/**
 * Return true if a task is mapped (either literal or dynamic).
 * Used for gating that applies to both forms.
 */
export function isMappedTask(expand: unknown): boolean {
  return isLiteralMapped(expand) || isDynamicMapped(expand)
}
