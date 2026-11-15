/**
 * Dag processor (Airflow's DagFileProcessor): parses Dag files in a child process and stores the
 * serialized Dags, import errors and warnings in the database. The scheduler and API read those
 * (dag/sync.ts) and never import user code.
 *
 *   serialized_dags   one row per Dag: { _id: dag_id, dag_id, version, fileloc, data, data_hash, task_ids, updated_at }
 *   dag_processor     one state row:   { _id: 'state', import_errors, warnings, providers, parsed_at, duration_ms, files }
 *
 * A file that fails to import keeps its last good Dags (as in Airflow) and shows an import error; a Dag
 * disappears only when its file is gone or no longer defines it. A Dag id defined by two files keeps
 * the first (by file name) and reports the second as an import error.
 *
 * Runs embedded (the leader scheduler re-parses when files change: DAG_PROCESSOR=embedded, the default)
 * or as its own process (`npm run dag-processor`, with DAG_PROCESSOR=external on the schedulers).
 *
 *   DAG_PARSE_TIMEOUT_MS     per-file import timeout (default 30s); a hung file becomes an import error
 *   DAG_REPARSE_INTERVAL_MS  re-parse even without file changes this often (default 5 min) — catches
 *                            changes outside the Dag folder, such as an updated npm dependency
 */
import { createHash } from 'node:crypto'
import { readdir, stat } from 'node:fs/promises'
import { join, resolve } from 'node:path'
import type { Db } from 'mongodb'
import { getDagsDir, listDagFiles } from './loader.js'
import { recordDagVersion } from './version.js'
import { ensureImages } from './images.js'
import type { ImportError } from './import-errors.js'
import type { DagWarning } from './warnings.js'
import type { ProviderRecord } from '../providers/registry.js'
import type { ParseChildMsg, ParsedDagMsg, ParseRequest } from './parse-worker.js'
import { forkUserCodeProcess } from '../sandbox/fork.js'

export interface SerializedDagDoc {
  _id: string
  dag_id: string
  version: string
  fileloc: string
  data: string
  data_hash: string
  task_ids: string[]
  updated_at: Date
}

export interface ProcessorStateDoc {
  _id: 'state'
  import_errors: ImportError[]
  warnings: DagWarning[]
  providers: ProviderRecord[]
  parsed_at: Date
  duration_ms: number
  files: number
}

export interface ProcessorResult {
  dags: number
  changed: string[]
  deleted: string[]
  errors: ImportError[]
}

export const SERIALIZED_DAGS = 'serialized_dags'
export const PROCESSOR_STATE = 'dag_processor'

const parseTimeoutMs = () => Number(process.env.DAG_PARSE_TIMEOUT_MS ?? 30_000)
const reparseIntervalMs = () => Number(process.env.DAG_REPARSE_INTERVAL_MS ?? 300_000)

export function isProcessorEmbedded(): boolean {
  return (process.env.DAG_PROCESSOR ?? 'embedded') !== 'external'
}

type FileResult = { ok: true; dag: ParsedDagMsg } | { ok: false; error: string }

const liveChildren = new Set<import('node:child_process').ChildProcess>()

/**
 * Parse `files` in child processes. A file that hangs past the timeout, or crashes the child, becomes
 * an import error and parsing resumes with the next file in a fresh child.
 */
export async function parseDagFiles(dagsDir: string, files: string[], timeoutMs = parseTimeoutMs()):
  Promise<{ results: Map<string, FileResult>; providers: ProviderRecord[] }> {
  const results = new Map<string, FileResult>()
  let providers: ProviderRecord[] | null = null
  let remaining = [...files]

  while (remaining.length > 0) {
    const batch = remaining
    const outcome = await new Promise<{ current: string | null; reason: string | null }>((done) => {
      const child = forkUserCodeProcess('dag/parse-worker')
      liveChildren.add(child)
      let current: string | null = null
      let timer: NodeJS.Timeout | null = null
      let finished = false
      const finish = (reason: string | null) => {
        if (finished) return
        finished = true
        if (timer) clearTimeout(timer)
        liveChildren.delete(child)
        if (child.exitCode === null && child.signalCode === null) child.kill('SIGKILL')
        done({ current, reason })
      }
      child.on('message', (msg: ParseChildMsg) => {
        if (msg.type === 'providers') providers ??= msg.records
        else if (msg.type === 'start') {
          current = msg.file
          if (timer) clearTimeout(timer)
          timer = setTimeout(() => finish(`timed out after ${timeoutMs}ms (DAG_PARSE_TIMEOUT_MS)`), timeoutMs)
        } else if (msg.type === 'file') {
          results.set(msg.file, msg.ok ? { ok: true, dag: msg.dag } : { ok: false, error: msg.error })
          current = null
        } else if (msg.type === 'done') finish(null)
      })
      child.on('exit', (code, signal) => finish(`parse process exited (${signal ?? `code ${code}`})`))
      child.on('error', err => finish(`parse process failed: ${err.message}`))
      child.send({ type: 'parse', dagsDir, files: batch } satisfies ParseRequest)
    })

    if (outcome.reason) {
      // Blame the file being imported; if none was (e.g. a provider crashed the child), fail them all
      const blamed = outcome.current ? [outcome.current] : batch.filter(f => !results.has(f))
      for (const f of blamed) if (!results.has(f)) results.set(f, { ok: false, error: outcome.reason })
    }
    remaining = batch.filter(f => !results.has(f))
  }
  return { results, providers: providers ?? [] }
}

/** Kill parse children still running (shutdown). */
export function stopDagProcessor(): void {
  for (const child of liveChildren) child.kill('SIGKILL')
  liveChildren.clear()
}

const sha256 = (s: string) => createHash('sha256').update(s).digest('hex')

/** Dags whose images / versions were already handled by this process (they only need it once). */
const handled = new Set<string>()

/** Parse every Dag file and bring the database in line. */
export async function runDagProcessor(db: Db, opts: { dagsDir?: string } = {}): Promise<ProcessorResult> {
  const startedAt = performance.now()
  const dagsDir = opts.dagsDir ?? getDagsDir()
  const files = await listDagFiles(dagsDir)
  if (files === null) console.warn(`[dag-processor] Dag folder not found at ${dagsDir}`)
  const fileList = files ?? []
  const { results, providers } = fileList.length
    ? await parseDagFiles(dagsDir, fileList)
    : { results: new Map<string, FileResult>(), providers: [] as ProviderRecord[] }

  const now = new Date()
  const errors: ImportError[] = []
  const warnings: DagWarning[] = []
  const failedFilelocs = new Set<string>()
  const parsed = new Map<string, { fileloc: string; dag: ParsedDagMsg }>()

  for (const file of fileList) {
    const r = results.get(file)!
    const fileloc = resolve(dagsDir, file)
    if (!r.ok) {
      errors.push({ filename: file, error: r.error, imported_at: now })
      failedFilelocs.add(fileloc)
      continue
    }
    const first = parsed.get(r.dag.dag_id)
    if (first) {
      errors.push({ filename: file, error: `Dag id '${r.dag.dag_id}' is already defined in ${first.fileloc.slice(dagsDir.length + 1)}`, imported_at: now })
      continue
    }
    parsed.set(r.dag.dag_id, { fileloc, dag: r.dag })
    warnings.push(...r.dag.warnings)
  }

  const coll = db.collection<SerializedDagDoc>(SERIALIZED_DAGS)
  const existing = await coll.find({}, { projection: { dag_id: 1, data_hash: 1, fileloc: 1 } }).toArray()
  const existingById = new Map(existing.map(d => [d.dag_id, d]))

  const changed: string[] = []
  for (const [dagId, { fileloc, dag }] of parsed) {
    const dataHash = sha256(`${fileloc}\n${dag.data}`)
    if (existingById.get(dagId)?.data_hash !== dataHash) {
      await coll.updateOne({ _id: dagId }, { $set: {
        dag_id: dagId, version: dag.version, fileloc, data: dag.data, data_hash: dataHash,
        task_ids: dag.task_ids, updated_at: now,
      } }, { upsert: true })
      changed.push(dagId)
    }
    const key = `${dagId}@${dag.version}`
    if (!handled.has(key)) {
      handled.add(key)
      await recordDagVersion(db, dagId, dag.version, dag.source, dag.task_ids)
      if (dag.required_images.length) void ensureImages(dagId, dag.required_images)
    }
  }

  const deleted = existing
    .filter(d => !parsed.has(d.dag_id) && !failedFilelocs.has(d.fileloc))
    .map(d => d.dag_id)
  if (deleted.length) await coll.deleteMany({ dag_id: { $in: deleted } })

  await db.collection<ProcessorStateDoc>(PROCESSOR_STATE).updateOne({ _id: 'state' }, { $set: {
    import_errors: errors, warnings, providers, parsed_at: now,
    duration_ms: Math.round(performance.now() - startedAt), files: fileList.length,
  } }, { upsert: true })

  if (changed.length || deleted.length || errors.length) {
    console.log(`[dag-processor] ${parsed.size} Dag(s) from ${fileList.length} file(s)` +
      `${changed.length ? `; updated: ${changed.join(', ')}` : ''}${deleted.length ? `; removed: ${deleted.join(', ')}` : ''}` +
      `${errors.length ? `; ${errors.length} import error(s): ${errors.map(e => e.filename).join(', ')}` : ''}`)
  }
  return { dags: parsed.size, changed, deleted, errors }
}

// ── Change detection ───────────────────────────────────────────────────────

/** Hash of every file's path, size and mtime under the Dag folder (providers and helpers included). */
export async function dagsFingerprint(dir: string): Promise<string> {
  const parts: string[] = []
  const walk = async (d: string, depth: number): Promise<void> => {
    let entries: import('node:fs').Dirent[]
    try { entries = await readdir(d, { withFileTypes: true }) } catch { return }
    for (const e of entries) {
      if (e.name.startsWith('.') || e.name === 'node_modules') continue
      const p = join(d, e.name)
      if (e.isDirectory()) { if (depth < 8) await walk(p, depth + 1); continue }
      try { const s = await stat(p); parts.push(`${p}:${s.size}:${s.mtimeMs}`) } catch { /* vanished */ }
    }
  }
  await walk(dir, 0)
  return sha256(parts.sort().join('\n'))
}

let lastFingerprint: string | null = null
let lastRunAt = 0
let inFlight: Promise<ProcessorResult | null> | null = null

/**
 * Re-parse when the Dag folder changed (or DAG_REPARSE_INTERVAL_MS passed). One run at a time:
 * a call while a run is in flight returns that run. Resolves null when nothing needed parsing.
 */
export function maybeRunDagProcessor(db: Db, opts: { force?: boolean; dagsDir?: string } = {}): Promise<ProcessorResult | null> {
  if (inFlight) return inFlight
  inFlight = (async () => {
    const dagsDir = opts.dagsDir ?? getDagsDir()
    const fingerprint = await dagsFingerprint(dagsDir)
    const due = Date.now() - lastRunAt >= reparseIntervalMs()
    if (!opts.force && !due && fingerprint === lastFingerprint) return null
    const result = await runDagProcessor(db, { dagsDir })
    lastFingerprint = fingerprint
    lastRunAt = Date.now()
    return result
  })().finally(() => { inFlight = null })
  return inFlight
}

/** Forget change-detection state (tests). */
export function resetDagProcessor(): void {
  lastFingerprint = null
  lastRunAt = 0
  handled.clear()
}
