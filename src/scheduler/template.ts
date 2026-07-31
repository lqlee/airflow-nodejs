/**
 * Template rendering for static task fields.
 *
 * Supports `{{ dotted.path }}` syntax — no dependencies (no Jinja2/nunjucks).
 * Undefined paths render as empty string ''.
 *
 * Template context variables:
 *
 *   Scheduling:
 *     {{ dag_id }}           — DAG id
 *     {{ run_id }}           — run id (MongoDB ObjectId hex)
 *     {{ task_id }}          — task id
 *     {{ ds }}               — execution date as YYYY-MM-DD (from logical_date or created_at)
 *     {{ ts }}               — ISO-8601 timestamp (from logical_date or created_at)
 *     {{ ts_nodash }}        — ISO timestamp without dashes/colons (e.g. 20240101T120000)
 *     {{ logical_date }}     — logical_date ISO string or '' for manual runs
 *
 *   Run conf (trigger-time config):
 *     {{ conf.key }}         — ctx.conf.key
 *     {{ conf.nested.key }}  — deep path into conf object
 *
 *   Templated fields:
 *     shell.command, shell.env values
 *     python.code, python.args items
 *     java.args items
 *     container.command items, container.env values
 *
 *   Undefined path behavior: renders as '' (empty string, Airflow default)
 *
 *   Note: logical_date is null for manual/ad-hoc triggers. Use {{ ds }} which
 *   falls back to created_at so you always get a valid date string.
 */

export interface TemplateContext {
  dag_id: string
  run_id: string
  task_id: string
  logical_date: Date | null
  created_at: Date
  conf: Record<string, unknown>
}

/**
 * Render a single template string, replacing `{{ path }}` expressions.
 * Path is dot-separated (e.g. `conf.env`, `ds`, `dag_id`).
 * Unknown paths render as ''.
 */
export function renderTemplate(template: string, ctx: TemplateContext): string {
  const execDate = ctx.logical_date ?? ctx.created_at
  const ds = execDate.toISOString().slice(0, 10)                    // YYYY-MM-DD
  const ts = execDate.toISOString()                                   // full ISO
  const tsNodash = ts.replace(/[-:]/g, '').replace(/\.\d{3}Z$/, 'Z') // 20240101T120000Z

  // Flat lookup table — handles well-known top-level keys first
  const topLevel: Record<string, string> = {
    dag_id:       ctx.dag_id,
    run_id:       ctx.run_id,
    task_id:      ctx.task_id,
    ds,
    ts,
    ts_nodash:    tsNodash,
    logical_date: ctx.logical_date ? ctx.logical_date.toISOString() : '',
  }

  return template.replace(/\{\{\s*([\w.]+)\s*\}\}/g, (match, path) => {
    // Top-level shorthand
    if (path in topLevel) return topLevel[path]

    // Dotted path into conf or other nested objects
    const parts = path.split('.')
    const root = parts[0]

    let obj: unknown
    if (root === 'conf') {
      obj = ctx.conf
      const subPath = parts.slice(1)
      for (const key of subPath) {
        if (obj === null || typeof obj !== 'object') return ''
        obj = (obj as Record<string, unknown>)[key]
      }
      return obj === undefined || obj === null ? '' : String(obj)
    }

    // Unknown path → ''
    return ''
  })
}

/**
 * Render all string values in an object (shallow — values only, not keys).
 * Used for env objects: { KEY: '{{ conf.env }}' } → { KEY: 'prod' }
 */
export function renderEnv(
  env: Record<string, string>,
  ctx: TemplateContext,
): Record<string, string> {
  const result: Record<string, string> = {}
  for (const [k, v] of Object.entries(env)) {
    result[k] = renderTemplate(v, ctx)
  }
  return result
}

/**
 * Render all items in a string array.
 * Used for args arrays: ['--date', '{{ ds }}'] → ['--date', '2024-01-01']
 */
export function renderArgs(args: string[], ctx: TemplateContext): string[] {
  return args.map(a => renderTemplate(a, ctx))
}
