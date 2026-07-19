/**
 * In-memory registry of Dag import errors from the most recent loadDags() call.
 * Errors are replaced wholesale on each reload — a fixed file clears its entry.
 * Not persisted to DB: errors reset on process restart (source of truth is the filesystem).
 */

export interface ImportError {
  filename: string
  error: string
  imported_at: Date
}

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
