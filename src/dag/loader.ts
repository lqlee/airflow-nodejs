import { readdir } from 'node:fs/promises'
import { resolve, extname } from 'node:path'
import { pathToFileURL } from 'node:url'
import { register, clearRegistry } from './registry.js'
import type { DagDefinition } from './types.js'

const DAGS_DIR = resolve(process.cwd(), 'dags')

export async function loadDags(): Promise<void> {
  clearRegistry()

  let entries: string[]
  try {
    entries = await readdir(DAGS_DIR)
  } catch {
    console.warn(`[loader] dags/ directory not found at ${DAGS_DIR}`)
    return
  }

  const dagFiles = entries.filter(f => extname(f) === '.ts' || extname(f) === '.js')

  for (const file of dagFiles) {
    const filePath = resolve(DAGS_DIR, file)
    try {
      // Cache-bust so re-loads pick up file changes
      const mod = await import(`${pathToFileURL(filePath).href}?t=${Date.now()}`)
      const dag: DagDefinition = mod.default
      if (!dag?.id || !dag?.tasks) {
        console.warn(`[loader] ${file} has no valid default export — skipping`)
        continue
      }
      register(dag)
      console.log(`[loader] loaded Dag: ${dag.id} (tasks: ${Object.keys(dag.tasks).join(', ')})`)
    } catch (err) {
      console.error(`[loader] failed to load ${file}:`, err)
    }
  }
}
