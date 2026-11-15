/**
 * This process's copy of the Dag import errors and warnings, replaced wholesale on each update.
 * The Dag processor stores them in the database (dag_processor state row) and syncDagsFromDb copies
 * them here; loadDags() (in-process loading, used by workers and dev scripts) sets them directly.
 */

import type { DagWarning } from './warnings.js'

export interface ImportError {
  filename: string
  error: string
  imported_at: Date
}

// ── Import Errors ─────────────────────────────────────────────────────────

let _errors: ImportError[] = []

/** Replace the entire error list (called at the start of each loadDags). */
export function setImportErrors(errors: ImportError[]): void {
  _errors = errors
}

/** Get the current import error list (snapshot). */
export function getImportErrors(): ImportError[] {
  return [..._errors]
}

/** Convenience: true if any errors exist. */
export function hasImportErrors(): boolean {
  return _errors.length > 0
}

// ── Dag Warnings ──────────────────────────────────────────────────────────

let _warnings: DagWarning[] = []

/** Replace the entire warnings list (called at the end of each loadDags). */
export function setDagWarnings(warnings: DagWarning[]): void {
  _warnings = warnings
}

/** Get all current dag warnings (snapshot). */
export function getDagWarnings(dagId?: string): DagWarning[] {
  const all = [..._warnings]
  return dagId ? all.filter(w => w.dag_id === dagId) : all
}

/** True if any warnings exist (optionally for a specific dag). */
export function hasDagWarnings(dagId?: string): boolean {
  return getDagWarnings(dagId).length > 0
}
