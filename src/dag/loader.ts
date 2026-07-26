import { readdir, readFile } from 'node:fs/promises'
import { resolve, extname } from 'node:path'
import { pathToFileURL } from 'node:url'
import type { Db } from 'mongodb'
import { register, clearRegistry } from './registry.js'
import { hashDagSource, recordDagVersion } from './version.js'
import { expandGroups } from './taskgroups.js'
import { setImportErrors, setDagWarnings, type ImportError } from './import-errors.js'
import { analyzeWarnings, type DagWarning } from './warnings.js'
import type { DagDefinition } from './types.js'

const DAGS_DIR = resolve(process.cwd(), 'dags')

/**
 * Load and register all Dag files from the dags/ directory.
 * When db is provided, new (dag_id, version) pairs are persisted to dag_versions.
 * db is optional so callers without a DB (dev scripts, some tests) remain unchanged.
 */
export async function loadDags(db?: Db): Promise<void> {
  clearRegistry()

  let entries: string[]
  try {
    entries = await readdir(DAGS_DIR)
  } catch {
    console.warn(`[loader] dags/ directory not found at ${DAGS_DIR}`)
    setImportErrors([])
    return
  }

  // In production (compiled, no tsx available) load only .js files.
  // In development (tsx available) load .ts files; skip .js if a .ts with same basename exists.
  const IS_COMPILED = import.meta.url.endsWith('.js')
  const allDagFiles = entries.filter(f => extname(f) === '.ts' || extname(f) === '.js')
  const tsBasenames = new Set(allDagFiles.filter(f => extname(f) === '.ts').map(f => f.slice(0, -3)))
  const dagFiles = IS_COMPILED
    ? allDagFiles.filter(f => extname(f) === '.js')   // prod: .js only
    : allDagFiles.filter(f =>
        extname(f) === '.ts' ||                        // dev: prefer .ts
        (extname(f) === '.js' && !tsBasenames.has(f.slice(0, -3)))  // .js only if no .ts twin
      )
  const errors: ImportError[] = []
  const allWarnings: DagWarning[] = []
  const now = new Date()

  for (const file of dagFiles) {
    const filePath = resolve(DAGS_DIR, file)
    try {
      // Read source for hashing before import (cache-bust ensures fresh bytes)
      const source = await readFile(filePath, 'utf8')
      const version = hashDagSource(source)

      // Cache-bust: append the content hash to the URL so each unique file version
      // gets a unique module URL — Bun (unlike Node) ignores ?query in file URLs
      // but treats different full URLs as distinct modules.
      // Using the version (content hash) ensures a changed file always re-imports.
      const mod = await import(`${pathToFileURL(filePath).href}?v=${version}`)
      const dag: DagDefinition = mod.default
      if (!dag?.id || !dag?.tasks) {
        const msg = `${file} has no valid default export (expected { id, tasks, schedule })`
        console.warn(`[loader] ${msg}`)
        errors.push({ filename: file, error: msg, imported_at: now })
        continue
      }
      // Stamp version onto the dag object (overwrites any author-set version)
      dag.version = version
      // Expand group→group dependencies into task-level edges before registration
      const expanded = expandGroups(dag)
      register(expanded)
      const groupSuffix = dag.groups ? ` (groups: ${Object.keys(dag.groups).join(', ')})` : ''
      console.log(`[loader] loaded Dag: ${dag.id} v${version} (tasks: ${Object.keys(dag.tasks).join(', ')})${groupSuffix}`)

      // Persist new version — fire-and-forget, idempotent; skipped when db not provided
      if (db) {
        void recordDagVersion(db, dag.id, version, source, Object.keys(dag.tasks))
          .catch(() => {/* stub db in tests — swallow silently */})
      }

      // Analyze soft warnings on the successfully-loaded dag
      const dagWarnings = analyzeWarnings(expanded, now)
      allWarnings.push(...dagWarnings)
      if (dagWarnings.length > 0) {
        console.warn(`[loader] ${dagWarnings.length} warning(s) in dag '${dag.id}': ${dagWarnings.map(w => w.warning_type).join(', ')}`)
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
