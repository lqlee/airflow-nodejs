/** Output helpers for the `anode` CLI: plain-text tables (default) or JSON (`-o json`). */

export type OutputFormat = 'table' | 'json'

export interface Io {
  out: (s: string) => void
  err: (s: string) => void
}

function cell(v: unknown): string {
  if (v === null || v === undefined) return ''
  if (v instanceof Date) return v.toISOString()
  if (Array.isArray(v)) return v.map(cell).join(',')
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

/** Left-aligned columns separated by two spaces; `columns` picks and orders the fields. */
export function formatTable(rows: Record<string, unknown>[], columns: string[]): string {
  const data = rows.map(r => columns.map(c => cell(r[c]).replace(/\s+/g, ' ')))
  const widths = columns.map((c, i) => Math.max(c.length, ...data.map(row => row[i].length)))
  const line = (vals: string[]) => vals.map((v, i) => (i === vals.length - 1 ? v : v.padEnd(widths[i]))).join('  ').trimEnd()
  return [line(columns), ...data.map(line)].join('\n')
}

export function print(io: Io, format: OutputFormat, value: unknown, columns?: string[]): void {
  if (format === 'json') return io.out(JSON.stringify(value, null, 2))
  if (Array.isArray(value) && columns) {
    return io.out(value.length ? formatTable(value as Record<string, unknown>[], columns) : '(none)')
  }
  if (value && typeof value === 'object') {
    const entries = Object.entries(value as Record<string, unknown>)
    const width = Math.max(...entries.map(([k]) => k.length))
    return io.out(entries.map(([k, v]) => `${k.padEnd(width)}  ${cell(v)}`).join('\n'))
  }
  io.out(cell(value))
}
