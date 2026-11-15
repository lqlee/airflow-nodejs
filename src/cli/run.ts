/**
 * `anode` — command-line client for an airflow-nodejs deployment, on top of the REST API.
 * Command names follow the Airflow CLI (`airflow dags list`, `airflow tasks test`, …).
 *
 *   ANODE_API_URL    API base URL (default http://localhost:3000)   or --url
 *   ANODE_API_TOKEN  API key or OIDC access token                   or --token
 *   DAG_DIR          Dag folder for `tasks test` (default ./dags)  or --dags-dir
 */
import { readFileSync } from 'node:fs'
import { parseArgs } from 'node:util'
import { setTimeout as sleep } from 'node:timers/promises'
import { getDagsDir } from '../dag/loader.js'
import { ApiError, createApiClient, type ApiClient } from './client.js'
import { print, type Io, type OutputFormat } from './output.js'
import { testTask } from './task-test.js'

export interface CliEnv {
  io: Io
  env?: NodeJS.ProcessEnv
  fetchImpl?: typeof fetch
  /** `dags trigger --wait` poll interval (ms). */
  pollMs?: number
}

class UsageError extends Error {}

const OPTIONS = {
  url:            { type: 'string' },
  token:          { type: 'string' },
  output:         { type: 'string', short: 'o' },
  help:           { type: 'boolean', short: 'h' },
  version:        { type: 'boolean' },
  tag:            { type: 'string', multiple: true },
  conf:           { type: 'string' },
  note:           { type: 'string' },
  wait:           { type: 'boolean' },
  timeout:        { type: 'string' },
  limit:          { type: 'string' },
  'logical-date': { type: 'string' },
  'map-index':    { type: 'string' },
  'map-value':    { type: 'string' },
  'run-id':       { type: 'string' },
  'dags-dir':     { type: 'string' },
  file:           { type: 'string' },
} as const

type Values = ReturnType<typeof parseArgs<{ options: typeof OPTIONS; allowPositionals: true }>>['values']

interface Command {
  args: string[]
  options?: (keyof typeof OPTIONS)[]
  summary: string
  run: (c: Ctx, args: string[], v: Values) => Promise<number>
}

interface Ctx { io: Io; format: OutputFormat; api: () => ApiClient; env: NodeJS.ProcessEnv; pollMs: number }

const TERMINAL_RUN_STATES = new Set(['success', 'failed', 'cancelled'])
const seg = encodeURIComponent

function parseJsonObject(raw: string | undefined, flag: string): Record<string, unknown> | undefined {
  if (raw === undefined) return undefined
  let v: unknown
  try { v = JSON.parse(raw) } catch { throw new UsageError(`${flag} must be valid JSON`) }
  if (!v || typeof v !== 'object' || Array.isArray(v)) throw new UsageError(`${flag} must be a JSON object`)
  return v as Record<string, unknown>
}

function parseInteger(raw: string | undefined, flag: string, min: number): number | undefined {
  if (raw === undefined) return undefined
  const n = Number(raw)
  if (!Number.isInteger(n) || n < min) throw new UsageError(`${flag} must be an integer ≥ ${min}`)
  return n
}

function parseDate(raw: string | undefined, flag: string): Date | undefined {
  if (raw === undefined) return undefined
  const d = new Date(raw)
  if (Number.isNaN(d.getTime())) throw new UsageError(`${flag} must be an ISO date, e.g. 2026-01-31 or 2026-01-31T06:00:00Z`)
  return d
}

const COMMANDS: Record<string, Record<string, Command>> = {
  dags: {
    list: {
      args: [], options: ['tag'], summary: 'List Dags (--tag to filter; repeat for AND)',
      run: async (c, _a, v) => {
        const dags = await c.api().get<Record<string, unknown>[]>('/dags', { tag: v.tag })
        print(c.io, c.format, dags, ['id', 'schedule', 'is_paused', 'owner', 'tags'])
        return 0
      },
    },
    show: {
      args: ['dag_id'], summary: 'Show one Dag',
      run: async (c, [dagId]) => {
        print(c.io, c.format, await c.api().get(`/dags/${seg(dagId)}`))
        return 0
      },
    },
    trigger: {
      args: ['dag_id'], options: ['conf', 'tag', 'note', 'wait', 'timeout'],
      summary: 'Trigger a run (--conf JSON, --tag, --note; --wait [--timeout SEC] until it finishes)',
      run: async (c, [dagId], v) => {
        const timeoutSec = parseInteger(v.timeout, '--timeout', 1)
        if (timeoutSec !== undefined && !v.wait) throw new UsageError('--timeout needs --wait')
        const body = { conf: parseJsonObject(v.conf, '--conf'), tags: v.tag, note: v.note }
        const run = await c.api().post<{ run_id: string; state: string }>(`/dags/${seg(dagId)}/trigger`, body)
        if (!v.wait) { print(c.io, c.format, run); return 0 }
        if (c.format === 'table') c.io.out(`triggered ${dagId} run ${run.run_id} — waiting…`)
        const final = await waitForRun(c, run.run_id, timeoutSec)
        print(c.io, c.format, c.format === 'json' ? final : summarizeRun(final))
        return final.state === 'success' ? 0 : 1
      },
    },
    pause: {
      args: ['dag_id'], summary: 'Pause scheduling of a Dag',
      run: async (c, [dagId]) => { print(c.io, c.format, await c.api().post(`/dags/${seg(dagId)}/pause`)); return 0 },
    },
    unpause: {
      args: ['dag_id'], summary: 'Resume scheduling of a Dag',
      run: async (c, [dagId]) => { print(c.io, c.format, await c.api().post(`/dags/${seg(dagId)}/resume`)); return 0 },
    },
    'list-runs': {
      args: ['dag_id'], options: ['limit'], summary: 'Recent runs of a Dag, newest first (--limit, default 20)',
      run: async (c, [dagId], v) => {
        const limit = parseInteger(v.limit, '--limit', 1)
        const page = await c.api().get<{ items: Record<string, unknown>[] }>(
          `/dags/${seg(dagId)}/runs`, { limit: limit === undefined ? undefined : String(limit) })
        print(c.io, c.format, page.items, ['run_id', 'state', 'logical_date', 'created_at', 'tags'])
        return 0
      },
    },
    'list-import-errors': {
      args: [], summary: 'Dag files that failed to import',
      run: async (c) => {
        print(c.io, c.format, await c.api().get<Record<string, unknown>[]>('/import-errors'), ['filename', 'error'])
        return 0
      },
    },
  },
  tasks: {
    list: {
      args: ['dag_id'], summary: 'Tasks of a Dag',
      run: async (c, [dagId]) => {
        const tasks = await c.api().get<Record<string, unknown>[]>(`/dags/${seg(dagId)}/tasks`)
        print(c.io, c.format, tasks, ['task_id', 'depends_on', 'retries', 'pool', 'is_sensor', 'is_mapped'])
        return 0
      },
    },
    'states-for-dag-run': {
      args: ['run_id'], summary: 'Task instance states of a run',
      run: async (c, [runId]) => {
        const tis = await c.api().get<Record<string, unknown>[]>(`/dag-runs/${seg(runId)}/tasks`)
        print(c.io, c.format, tis, ['task_id', 'map_index', 'state', 'try_number', 'started_at', 'ended_at', 'error'])
        return 0
      },
    },
    test: {
      args: ['dag_id', 'task_id'], options: ['logical-date', 'conf', 'map-index', 'map-value', 'run-id', 'dags-dir', 'file'],
      summary: 'Run one task locally without recording state (imports the Dag file here)',
      run: async (c, [dagId, taskId], v) => {
        let mapValue: unknown
        if (v['map-value'] !== undefined) {
          try { mapValue = JSON.parse(v['map-value']) } catch { throw new UsageError('--map-value must be valid JSON') }
        }
        const opts = {
          dagId, taskId,
          dagsDir: v['dags-dir'] ?? c.env.DAG_DIR ?? getDagsDir(),
          file: v.file,
          logicalDate: parseDate(v['logical-date'], '--logical-date'),
          conf: parseJsonObject(v.conf, '--conf'),
          mapIndex: parseInteger(v['map-index'], '--map-index', 0),
          mapValue,
          runId: v['run-id'],
        }
        let api: ApiClient | null = null
        try { api = c.api() } catch { /* bad URL: local secrets backend only */ }
        const result = await testTask({ ...opts, api, io: c.io })
        if (c.format === 'json') c.io.out(JSON.stringify(result, null, 2))
        return result.ok ? 0 : 1
      },
    },
  },
}

async function waitForRun(c: Ctx, runId: string, timeoutSec: number | undefined): Promise<Record<string, unknown> & { state: string }> {
  const deadline = timeoutSec === undefined ? Infinity : Date.now() + timeoutSec * 1000
  for (;;) {
    const run = await c.api().get<Record<string, unknown> & { state: string }>(`/dag-runs/${seg(runId)}`)
    if (TERMINAL_RUN_STATES.has(run.state)) return run
    if (Date.now() + c.pollMs > deadline) throw new Error(`run ${runId} still ${run.state} after ${timeoutSec}s`)
    await sleep(c.pollMs)
  }
}

function summarizeRun(run: Record<string, unknown>): Record<string, unknown> {
  const tasks = (run.tasks as { task_id: string; map_index: number | null; state: string; error: string | null }[] | undefined) ?? []
  const failed = tasks.filter(t => t.error).map(t => `${t.task_id}${t.map_index !== null ? `[${t.map_index}]` : ''}: ${t.error}`)
  return {
    run_id: run.run_id, dag_id: run.dag_id, state: run.state,
    tasks: Object.entries(tasks.reduce<Record<string, number>>((acc, t) => ({ ...acc, [t.state]: (acc[t.state] ?? 0) + 1 }), {}))
      .map(([s, n]) => `${s}=${n}`).join(' '),
    ...(failed.length ? { errors: failed.join(' | ') } : {}),
  }
}

function packageVersion(): string {
  try {
    return (JSON.parse(readFileSync(new URL('../../package.json', import.meta.url), 'utf8')) as { version: string }).version
  } catch {
    return 'unknown'
  }
}

export function usage(): string {
  const lines = ['Usage: anode [--url URL] [--token TOKEN] [-o table|json] <group> <command> [args]', '']
  for (const [group, cmds] of Object.entries(COMMANDS)) {
    for (const [name, cmd] of Object.entries(cmds)) {
      lines.push(`  ${`${group} ${name} ${cmd.args.map(a => `<${a}>`).join(' ')}`.padEnd(42)} ${cmd.summary}`)
    }
  }
  lines.push('', 'Environment: ANODE_API_URL (default http://localhost:3000), ANODE_API_TOKEN, DAG_DIR')
  return lines.join('\n')
}

/** Run the CLI; resolves to the process exit code (0 ok, 1 failed, 2 usage error). */
export async function runCli(argv: string[], cli: CliEnv): Promise<number> {
  const { io } = cli
  const env = cli.env ?? process.env
  let parsed: { values: Values; positionals: string[] }
  try {
    parsed = parseArgs({ args: argv, options: OPTIONS, allowPositionals: true, strict: true })
  } catch (err) {
    io.err(`${(err as Error).message}\n\n${usage()}`)
    return 2
  }
  const { values: v, positionals } = parsed
  if (v.version) { io.out(packageVersion()); return 0 }
  const [group, name, ...args] = positionals
  if (v.help || !group) { io.out(usage()); return v.help ? 0 : 2 }

  const cmd = COMMANDS[group]?.[name ?? '']
  if (!cmd) {
    io.err(`unknown command '${[group, name].filter(Boolean).join(' ')}'\n\n${usage()}`)
    return 2
  }
  const usageLine = `usage: anode ${group} ${name} ${cmd.args.map(a => `<${a}>`).join(' ')}`
  if (args.length !== cmd.args.length) {
    io.err(`${usageLine}\n${cmd.summary}`)
    return 2
  }
  const global = new Set(['url', 'token', 'output', 'help', 'version'])
  const foreign = Object.keys(v).filter(k => !global.has(k) && !(cmd.options as string[] | undefined)?.includes(k))
  if (foreign.length) {
    io.err(`'${group} ${name}' does not take ${foreign.map(f => `--${f}`).join(', ')}\n${usageLine}`)
    return 2
  }
  const format = (v.output ?? 'table') as OutputFormat
  if (format !== 'table' && format !== 'json') { io.err('--output must be table or json'); return 2 }

  let client: ApiClient | null = null
  const ctx: Ctx = {
    io, format, env,
    pollMs: cli.pollMs ?? 2_000,
    api: () => (client ??= createApiClient({
      baseUrl: v.url ?? env.ANODE_API_URL ?? 'http://localhost:3000',
      token: v.token ?? env.ANODE_API_TOKEN,
      fetchImpl: cli.fetchImpl,
    })),
  }
  try {
    return await cmd.run(ctx, args, v)
  } catch (err) {
    if (err instanceof UsageError) { io.err(`${err.message}\n${usageLine}`); return 2 }
    io.err(`error: ${err instanceof ApiError || err instanceof Error ? err.message : String(err)}`)
    return 1
  }
}
