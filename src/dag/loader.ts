import { readdir, readFile } from 'node:fs/promises'
import { resolve, extname, basename } from 'node:path'
import { pathToFileURL } from 'node:url'
import type { Db } from 'mongodb'
import { register, clearRegistry } from './registry.js'
import { hashDagSource, recordDagVersion } from './version.js'
import { expandGroups } from './taskgroups.js'
import { setImportErrors, setDagWarnings, type ImportError } from './import-errors.js'
import { analyzeWarnings, type DagWarning } from './warnings.js'
import { ensureImages } from './images.js'
import { loadProviders } from '../providers/loader.js'
import type { DagDefinition } from './types.js'
import { validateCondition } from '../datasets/conditions.js'

// DAG_DIR env var allows overriding the default location — used in WCNP where
// DAGs are bundled inside the npm package rather than mounted at ./dags/
export function getDagsDir(): string {
  return process.env.DAG_DIR ?? resolve(process.cwd(), 'dags')
}

const IS_COMPILED = import.meta.url.endsWith('.js')

/**
 * Dag files in `dir` (file names), or null when the directory does not exist.
 * Compiled (prod): .js only. Dev (tsx): .ts, plus .js files without a .ts twin.
 */
export async function listDagFiles(dir: string): Promise<string[] | null> {
  let entries: string[]
  try {
    entries = await readdir(dir)
  } catch {
    return null
  }
  const allDagFiles = entries.filter(f => extname(f) === '.ts' || extname(f) === '.js').sort()
  const tsBasenames = new Set(allDagFiles.filter(f => extname(f) === '.ts').map(f => f.slice(0, -3)))
  return IS_COMPILED
    ? allDagFiles.filter(f => extname(f) === '.js')
    : allDagFiles.filter(f => extname(f) === '.ts' || !tsBasenames.has(f.slice(0, -3)))
}

export interface ParsedDagFile {
  /** The Dag with group dependencies expanded — what gets registered. */
  dag: DagDefinition
  source: string
  version: string
  warnings: DagWarning[]
}

/**
 * Import one Dag file and validate it. Runs the file's top-level code — call it only in a process
 * meant to run user code (the Dag processor's parse child, a worker, or a dev script).
 * Throws an Error whose message is the import error.
 */
export async function parseDagFile(filePath: string, now = new Date()): Promise<ParsedDagFile> {
  // Read source for hashing before import (cache-bust ensures fresh bytes)
  const source = await readFile(filePath, 'utf8')
  const version = hashDagSource(source)

  // Cache-bust: append the content hash to the URL so each unique file version
  // gets a unique module URL — Bun (unlike Node) ignores ?query in file URLs
  // but treats different full URLs as distinct modules.
  const mod = await import(`${pathToFileURL(filePath).href}?v=${version}`)
  const dag: DagDefinition = mod.default
  if (!dag?.id || !dag?.tasks) {
    throw new Error(`${basename(filePath)} has no valid default export (expected { id, tasks, schedule })`)
  }
  // A malformed dataset condition is an import error, not a silently never-firing Dag
  if (dag.datasets !== undefined) validateCondition(dag.datasets)
  // Stamp version onto the dag object (overwrites any author-set version)
  dag.version = version
  // Expand group→group dependencies into task-level edges before registration
  const expanded = expandGroups(dag)
  return { dag: expanded, source, version, warnings: analyzeWarnings(expanded, now) }
}

/**
 * Import every Dag file IN THIS PROCESS and register it (no serialization).
 * The scheduler and API do not use this — they read serialized Dags (dag/processor.ts, dag/sync.ts).
 * Kept for Kubernetes worker pods and dev/verification scripts, which run user code anyway.
 * When db is provided, new (dag_id, version) pairs are persisted to dag_versions.
 */
export async function loadDags(db?: Db): Promise<void> {
  clearRegistry()
  const dagsDir = getDagsDir()

  // Load providers before DAGs so DAG files can use operator factories
  await loadProviders(dagsDir)

  const dagFiles = await listDagFiles(dagsDir)
  if (!dagFiles) {
    console.warn(`[loader] dags/ directory not found at ${dagsDir}`)
    setImportErrors([])
    return
  }
  const errors: ImportError[] = []
  const allWarnings: DagWarning[] = []
  const now = new Date()

  for (const file of dagFiles) {
    try {
      const { dag, source, version, warnings } = await parseDagFile(resolve(dagsDir, file), now)
      register(dag)
      const groupSuffix = dag.groups ? ` (groups: ${Object.keys(dag.groups).join(', ')})` : ''
      console.log(`[loader] loaded Dag: ${dag.id} v${version} (tasks: ${Object.keys(dag.tasks).join(', ')})${groupSuffix}`)

      // Persist new version — fire-and-forget, idempotent; skipped when db not provided
      if (db) {
        void recordDagVersion(db, dag.id, version, source, Object.keys(dag.tasks))
          .catch(() => {/* stub db in tests — swallow silently */})
      }
      // Load any user-supplied Docker image tarballs declared in requiredImages
      if (dag.requiredImages?.length) void ensureImages(dag.id, dag.requiredImages)

      allWarnings.push(...warnings)
      if (warnings.length > 0) {
        console.warn(`[loader] ${warnings.length} warning(s) in dag '${dag.id}': ${warnings.map(w => w.warning_type).join(', ')}`)
      }
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      console.error(`[loader] failed to load ${file}:`, err)
      errors.push({ filename: file, error: msg, imported_at: now })
    }
  }

  // Replace error and warning lists atomically
  setImportErrors(errors)
  setDagWarnings(allWarnings)

  if (errors.length > 0) {
    console.warn(`[loader] ${errors.length} file(s) failed to import`)
  }
}
