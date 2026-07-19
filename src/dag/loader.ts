import { readdir, readFile } from 'node:fs/promises'
import { resolve, extname } from 'node:path'
import { pathToFileURL } from 'node:url'
import { register, clearRegistry } from './registry.js'
import { hashDagSource } from './version.js'
import { expandGroups } from './taskgroups.js'
import { setImportErrors, type ImportError } from './import-errors.js'
import type { DagDefinition } from './types.js'

const DAGS_DIR = resolve(process.cwd(), 'dags')

export async function loadDags(): Promise<void> {
  clearRegistry()

  let entries: string[]
  try {
    entries = await readdir(DAGS_DIR)
  } catch {
    console.warn(`[loader] dags/ directory not found at ${DAGS_DIR}`)
    setImportErrors([])
    return
  }

  const dagFiles = entries.filter(f => extname(f) === '.ts' || extname(f) === '.js')
  const errors: ImportError[] = []
  const now = new Date()

  for (const file of dagFiles) {
    const filePath = resolve(DAGS_DIR, file)
    try {
      // Read source for hashing before import (cache-bust ensures fresh bytes)
      const source = await readFile(filePath, 'utf8')
      const version = hashDagSource(source)

      // Cache-bust so re-loads pick up file changes
      const mod = await import(`${pathToFileURL(filePath).href}?t=${Date.now()}`)
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
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      console.error(`[loader] failed to load ${file}:`, err)
      errors.push({ filename: file, error: msg, imported_at: now })
    }
  }

  // Replace error list atomically — a fixed file will no longer appear here
  setImportErrors(errors)

  if (errors.length > 0) {
    console.warn(`[loader] ${errors.length} file(s) failed to import`)
  }
}
