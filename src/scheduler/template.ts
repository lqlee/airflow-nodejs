/**
 * Template rendering for static task fields — a small, dependency-free subset of Airflow's Jinja.
 *
 * `{{ expression }}` where an expression is a value followed by optional filters:
 *   value   := dotted.path | macros.fn(arg, …) | 'string' | "string" | number
 *   filters := value | ds | ds_nodash | ts | ts_nodash | default('x') | upper | lower
 * Unknown paths render as '' (Airflow's default). A `{{ … }}` that does not parse is left as written.
 *
 * Variables:
 *   dag_id, run_id, task_id
 *   ds, ds_nodash         — logical date as YYYY-MM-DD / YYYYMMDD (falls back to the run's creation time)
 *   ts, ts_nodash         — the same instant as ISO-8601 / 20240101T120000Z
 *   logical_date          — ISO string, '' for runs without one (manual, dataset)
 *   data_interval_start   — ISO string of the run's data interval, '' when it has none
 *   data_interval_end
 *   conf.key, params.key  — trigger-time conf (params defaults are merged into it at trigger time)
 *   map_index, map_value  — mapped provider-operator instances (null otherwise)
 * Macros:
 *   macros.ds_add(ds, days)                     — '2024-01-01', 5 → '2024-01-06'
 *   macros.ds_format(ds, in_fmt, out_fmt)       — strftime subset: %Y %m %d %H %M %S %j %y %b
 *   macros.datetime_diff_days(a, b)             — whole days from a to b
 *
 * Templated fields: shell.command/env, python.code/args/env, java.jvmArgs/args/env,
 * container.command/env/image/workdir, kubernetes.image/command/args/env/labels/annotations.
 */

export interface TemplateContext {
  dag_id: string
  run_id: string
  task_id: string
  logical_date: Date | null
  created_at: Date
  conf: Record<string, unknown>
  data_interval_start?: Date | null
  data_interval_end?: Date | null
  /** Mapped task instances (provider operators): the instance's index and input value. */
  map_index?: number | null
  map_value?: unknown
}

// ── date helpers ──────────────────────────────────────────────────────────────

const pad = (n: number, w = 2) => String(n).padStart(w, '0')
const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
const toDs = (d: Date) => d.toISOString().slice(0, 10)
const toTs = (d: Date) => d.toISOString()
const toTsNodash = (d: Date) => toTs(d).replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z')

/** Parse a date value from a template: Date, ISO string or YYYY-MM-DD (UTC). */
function asDate(v: unknown): Date {
  const d = v instanceof Date ? v : new Date(String(v))
  if (Number.isNaN(d.getTime())) throw new TemplateError(`not a date: '${String(v)}'`)
  return d
}

function strftime(d: Date, fmt: string): string {
  const start = Date.UTC(d.getUTCFullYear(), 0, 1)
  const doy = Math.floor((d.getTime() - start) / 86_400_000) + 1
  return fmt.replace(/%([YmdHMSjyb%])/g, (_, c: string) => ({
    Y: String(d.getUTCFullYear()), y: pad(d.getUTCFullYear() % 100), m: pad(d.getUTCMonth() + 1), d: pad(d.getUTCDate()),
    H: pad(d.getUTCHours()), M: pad(d.getUTCMinutes()), S: pad(d.getUTCSeconds()), j: pad(doy, 3), b: MONTHS[d.getUTCMonth()], '%': '%',
  }[c] as string))
}

/** Inverse of strftime for the numeric directives; enough for ds_format's usual inputs. */
function strptime(s: string, fmt: string): Date {
  const parts: Record<string, number> = { Y: 1970, m: 1, d: 1, H: 0, M: 0, S: 0 }
  let re = ''
  const order: string[] = []
  for (let i = 0; i < fmt.length; i++) {
    if (fmt[i] === '%' && i + 1 < fmt.length) {
      const c = fmt[++i]
      if (c === 'Y') { re += '(\\d{4})'; order.push(c) } else if ('mdHMS'.includes(c)) { re += '(\\d{1,2})'; order.push(c) } else re += c === '%' ? '%' : '.*?'
    } else re += fmt[i].replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  }
  const m = new RegExp(`^${re}$`).exec(s)
  if (!m) throw new TemplateError(`'${s}' does not match format '${fmt}'`)
  order.forEach((c, i) => { parts[c] = Number(m[i + 1]) })
  return new Date(Date.UTC(parts.Y, parts.m - 1, parts.d, parts.H, parts.M, parts.S))
}

const MACROS: Record<string, (...args: unknown[]) => unknown> = {
  ds_add: (ds, days) => toDs(new Date(asDate(ds).getTime() + Number(days) * 86_400_000)),
  ds_format: (ds, inFmt, outFmt) => strftime(strptime(String(ds), String(inFmt)), String(outFmt)),
  datetime_diff_days: (a, b) => Math.trunc((asDate(b).getTime() - asDate(a).getTime()) / 86_400_000),
}

const FILTERS: Record<string, (v: unknown, ...args: unknown[]) => unknown> = {
  ds: v => toDs(asDate(v)),
  ds_nodash: v => toDs(asDate(v)).replace(/-/g, ''),
  ts: v => toTs(asDate(v)),
  ts_nodash: v => toTsNodash(asDate(v)),
  default: (v, d = '') => (v === undefined || v === null || v === '' ? d : v),
  upper: v => String(v ?? '').toUpperCase(),
  lower: v => String(v ?? '').toLowerCase(),
}

// ── expression evaluation ────────────────────────────────────────────────────

class TemplateError extends Error {}

type Scope = Record<string, unknown>

function buildScope(ctx: TemplateContext): Scope {
  const execDate = ctx.logical_date ?? ctx.created_at
  const iso = (d: Date | null | undefined) => (d ? d.toISOString() : '')
  return {
    dag_id: ctx.dag_id,
    run_id: ctx.run_id,
    task_id: ctx.task_id,
    ds: toDs(execDate),
    ds_nodash: toDs(execDate).replace(/-/g, ''),
    ts: toTs(execDate),
    ts_nodash: toTsNodash(execDate),
    logical_date: iso(ctx.logical_date),
    data_interval_start: iso(ctx.data_interval_start),
    data_interval_end: iso(ctx.data_interval_end),
    conf: ctx.conf,
    params: ctx.conf,
    map_index: ctx.map_index ?? null,
    map_value: ctx.map_value ?? null,
    macros: MACROS,
  }
}

/** Tokenizer + recursive-descent parser for one `{{ … }}` body. */
function evaluate(src: string, scope: Scope): unknown {
  const tokens = src.match(/\s*('(?:[^'\\]|\\.)*'|"(?:[^"\\]|\\.)*"|-?\d+(?:\.\d+)?|[A-Za-z_][\w]*|[.(),|])\s*/gy)
  if (!tokens || tokens.join('').length !== src.length) throw new TemplateError('unparsable')
  const toks = tokens.map(t => t.trim())
  let i = 0
  const peek = () => toks[i]
  const take = (want?: string) => {
    const t = toks[i++]
    if (t === undefined || (want !== undefined && t !== want)) throw new TemplateError('unexpected token')
    return t
  }
  const isIdent = (t: string | undefined) => !!t && /^[A-Za-z_]/.test(t)

  function args(): unknown[] {
    const out: unknown[] = []
    take('(')
    if (peek() !== ')') {
      out.push(value())
      while (peek() === ',') { take(','); out.push(value()) }
    }
    take(')')
    return out
  }

  function value(): unknown {
    const t = take()
    if (/^['"]/.test(t)) return t.slice(1, -1).replace(/\\(.)/g, '$1')
    if (/^-?\d/.test(t)) return Number(t)
    if (!isIdent(t)) throw new TemplateError('expected a value')
    let cur: unknown = scope[t]
    let path = t
    while (peek() === '.') {
      take('.')
      const key = take()
      if (!isIdent(key) && !/^\d+$/.test(key)) throw new TemplateError('expected a name')
      path += `.${key}`
      cur = cur !== null && typeof cur === 'object' ? (cur as Record<string, unknown>)[key] : undefined
    }
    if (peek() === '(') {
      if (typeof cur !== 'function') throw new TemplateError(`'${path}' is not a macro`)
      return cur(...args())
    }
    return cur
  }

  let result = value()
  while (peek() === '|') {
    take('|')
    const name = take()
    const filter = FILTERS[name]
    if (!filter) throw new TemplateError(`unknown filter '${name}'`)
    result = filter(result, ...(peek() === '(' ? args() : []))
  }
  if (i !== toks.length) throw new TemplateError('trailing input')
  return result
}

function stringify(v: unknown): string {
  if (v === undefined || v === null) return ''
  if (v instanceof Date) return v.toISOString()
  if (typeof v === 'object') return JSON.stringify(v)
  return String(v)
}

/**
 * Render a template string. Unknown paths render as ''; a malformed `{{ … }}` is left as written;
 * a macro called with bad input renders as '' and logs a warning.
 */
export function renderTemplate(template: string, ctx: TemplateContext): string {
  if (!template.includes('{{')) return template
  const scope = buildScope(ctx)
  return template.replace(/\{\{([^{}]*?)\}\}/g, (match, body: string) => {
    if (!body.trim()) return match
    try {
      return stringify(evaluate(body.trim(), scope))
    } catch (err) {
      if (err instanceof TemplateError && /unparsable|unexpected token|expected|trailing/.test(err.message)) return match
      console.warn(`[template] ${ctx.dag_id}.${ctx.task_id}: '${match}' → ${err instanceof Error ? err.message : String(err)}`)
      return ''
    }
  })
}

/** Render all string values of an object (values only, not keys) — env maps, labels, annotations. */
export function renderEnv(env: Record<string, string>, ctx: TemplateContext): Record<string, string> {
  const result: Record<string, string> = {}
  for (const [k, v] of Object.entries(env)) result[k] = typeof v === 'string' ? renderTemplate(v, ctx) : v
  return result
}

/** Render every item of a string array — args / command lists. */
export function renderArgs(args: string[], ctx: TemplateContext): string[] {
  return args.map(a => (typeof a === 'string' ? renderTemplate(a, ctx) : a))
}
