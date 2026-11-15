/** Run-time option checks for the built-in operators (after templating). */

export function str(v: unknown, name: string): string {
  if (typeof v !== 'string' || !v.trim()) throw new TypeError(`${name} must be a non-empty string`)
  return v
}

export function optStr(v: unknown, name: string): string | undefined {
  return v === undefined || v === null || v === '' ? undefined : str(v, name)
}

export function num(v: unknown, name: string): number | undefined {
  if (v === undefined || v === null || v === '') return undefined
  const n = typeof v === 'string' ? Number(v) : v
  if (typeof n !== 'number' || !Number.isFinite(n) || n < 0) throw new TypeError(`${name} must be a non-negative number`)
  return n
}

export function bool(v: unknown, name: string): boolean | undefined {
  if (v === undefined || v === null) return undefined
  if (typeof v === 'boolean') return v
  if (v === 'true' || v === 'false') return v === 'true'
  throw new TypeError(`${name} must be true or false`)
}

/** A list of strings: an array, or one string split on commas / semicolons. */
export function strList(v: unknown, name: string): string[] {
  const list = typeof v === 'string' ? v.split(/[,;]/) : Array.isArray(v) ? v : null
  if (!list || list.some(x => typeof x !== 'string')) throw new TypeError(`${name} must be a string or an array of strings`)
  const out = (list as string[]).map(s => s.trim()).filter(Boolean)
  if (!out.length) throw new TypeError(`${name} must not be empty`)
  return out
}

export function sqlParams(v: unknown): unknown[] | Record<string, unknown> | undefined {
  if (v === undefined || v === null) return undefined
  if (Array.isArray(v) || typeof v === 'object') return v as unknown[] | Record<string, unknown>
  throw new TypeError('parameters must be an array (positional) or an object (named)')
}

/**
 * Split a SQL script on `;` outside quotes, comments and postgres dollar-quoted bodies.
 * Empty statements are dropped.
 */
export function splitSql(script: string): string[] {
  const out: string[] = []
  let cur = ''
  let i = 0
  while (i < script.length) {
    const c = script[i]
    const rest = script.slice(i)
    let m: RegExpMatchArray | null
    if (c === "'" || c === '"' || c === '`') {
      let j = i + 1
      while (j < script.length && !(script[j] === c && script[j + 1] !== c)) j += script[j] === c ? 2 : (script[j] === '\\' && c !== '"' ? 2 : 1)
      cur += script.slice(i, j + 1); i = j + 1
    } else if (rest.startsWith('--')) {
      const end = script.indexOf('\n', i); const j = end < 0 ? script.length : end
      cur += script.slice(i, j); i = j
    } else if (rest.startsWith('/*')) {
      const end = script.indexOf('*/', i + 2); const j = end < 0 ? script.length : end + 2
      cur += script.slice(i, j); i = j
    } else if ((m = /^\$[A-Za-z_]*\$/.exec(rest))) {
      const end = script.indexOf(m[0], i + m[0].length); const j = end < 0 ? script.length : end + m[0].length
      cur += script.slice(i, j); i = j
    } else if (c === ';') {
      out.push(cur); cur = ''; i++
    } else {
      cur += c; i++
    }
  }
  out.push(cur)
  return out.map(s => s.trim()).filter(Boolean)
}

export function sqlStatements(sql: unknown, split: boolean): string[] {
  const list = Array.isArray(sql) ? sql : [sql]
  if (!list.length || list.some(s => typeof s !== 'string' || !s.trim())) throw new TypeError('sql must be a non-empty string or array of strings')
  const statements = split ? (list as string[]).flatMap(splitSql) : (list as string[])
  if (!statements.length) throw new TypeError('sql has no statements')
  return statements
}
