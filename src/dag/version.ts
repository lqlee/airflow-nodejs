import { createHash } from 'node:crypto'

/**
 * Compute a short sha256 hex digest of dag source bytes.
 * Returns the first 12 hex chars — enough to identify a version uniquely
 * while staying readable in logs and UI.
 */
export function hashDagSource(source: string): string {
  return createHash('sha256').update(source, 'utf8').digest('hex').slice(0, 12)
}
