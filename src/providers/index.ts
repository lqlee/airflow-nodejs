/**
 * Public API for the providers system.
 * Re-exported from airflow-nodejs/providers for use in provider files.
 */

export { listProviders, getProvider, getOperator, registerProvider } from './registry.js'
export { loadProviders } from './loader.js'
export type { ProviderDefinition, ProviderRecord, OperatorFactory } from './registry.js'

/**
 * Helper to define a provider with full type inference.
 * Use in dags/providers/*.js files:
 *
 *   import { provider } from 'airflow-nodejs/providers'
 *   export default provider({ name: 'my-provider', ... })
 */
export function provider(def: import('./registry.js').ProviderDefinition): import('./registry.js').ProviderDefinition {
  return def
}
