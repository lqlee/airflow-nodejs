/**
 * Provider loader — auto-discovers and loads provider files from dags/providers/.
 *
 * Called once at scheduler startup (from loadDags) and on each scheduler tick.
 * Providers are idempotent to register — re-loading the same file is a no-op
 * unless the content changes (cache-busted by content hash like DAG files).
 *
 * Provider file format (dags/providers/my-provider.js):
 *
 *   import { provider } from 'airflow-nodejs/providers'
 *
 *   export default provider({
 *     name: 'my-provider',
 *     version: '1.0.0',
 *     description: 'My custom operators',
 *     operators: {
 *       MyOperator: (opts = {}) => ({
 *         shell: { command: `echo ${opts.message ?? 'hello'}`, interpreter: 'sh' }
 *       }),
 *     },
 *     connectionTypes: ['my-service'],
 *   })
 */

import { readdir, readFile } from 'node:fs/promises'
import { resolve, extname } from 'node:path'
import { pathToFileURL } from 'node:url'
import { createHash } from 'node:crypto'
import { registerProvider, clearProviders, type ProviderDefinition } from './registry.js'

const PROVIDERS_DIR_NAME = 'providers'

let _lastHashes = new Map<string, string>()

/**
 * Load all provider files from dags/providers/.
 * Re-registers all providers on each call (clears then re-loads).
 * dagsDir is the resolved dags/ directory path.
 */
export async function loadProviders(dagsDir: string): Promise<void> {
  const providersDir = resolve(dagsDir, PROVIDERS_DIR_NAME)

  let entries: string[]
  try {
    entries = await readdir(providersDir)
  } catch {
    // dags/providers/ doesn't exist — not an error, just no providers
    clearProviders()
    _lastHashes = new Map()
    return
  }

  const IS_COMPILED = import.meta.url.endsWith('.js')
  const jsFiles = entries.filter(f => extname(f) === (IS_COMPILED ? '.js' : '.ts') || extname(f) === '.js')

  clearProviders()
  const newHashes = new Map<string, string>()

  for (const file of jsFiles) {
    const filePath = resolve(providersDir, file)
    try {
      const source = await readFile(filePath, 'utf8')
      const hash = createHash('sha256').update(source).digest('hex').slice(0, 12)
      newHashes.set(file, hash)

      const mod = await import(`${pathToFileURL(filePath).href}?v=${hash}`)
      const def: ProviderDefinition = mod.default

      if (!def?.name || !def?.operators) {
        console.warn(`[providers] ${file} has no valid default export (expected { name, operators, ... })`)
        continue
      }

      // Validate: operators must be functions (factory pattern, not arbitrary run: closures)
      const invalidOps = Object.entries(def.operators)
        .filter(([, fn]) => typeof fn !== 'function')
        .map(([k]) => k)
      if (invalidOps.length > 0) {
        console.warn(`[providers] ${file}: operators must be factory functions, got non-function: ${invalidOps.join(', ')}`)
        continue
      }

      registerProvider({
        name: def.name,
        version: def.version ?? '0.0.0',
        description: def.description ?? '',
        operators: def.operators,
        connectionTypes: def.connectionTypes ?? [],
      })

      console.log(`[providers] loaded provider '${def.name}' v${def.version ?? '0.0.0'} (operators: ${Object.keys(def.operators).join(', ')})`)
    } catch (err) {
      console.error(`[providers] failed to load ${file}:`, err)
    }
  }

  _lastHashes = newHashes
}
