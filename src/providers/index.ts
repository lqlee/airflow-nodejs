/**
 * Public API for the providers system.
 * Re-exported from airflow-nodejs/providers for use in provider files.
 */

export { listProviders, getProvider, getOperator, registerProvider } from './registry.js'
export { loadProviders } from './loader.js'
export type { ProviderDefinition, ProviderRecord, OperatorFactory } from './registry.js'

// Built-in operators (SQL, Google Cloud, Kafka, email) — see providers/builtin/
export {
  operatorTask,
  SQLExecuteQueryOperator, SQLCheckOperator, SqlSensor,
  LocalFilesystemToGCSOperator, GCSToLocalFilesystemOperator, GCSListObjectsOperator, GCSDeleteObjectsOperator,
  GCSObjectExistenceSensor, BigQueryInsertJobOperator, BigQueryCheckOperator,
  ProduceToTopicOperator, ConsumeFromTopicOperator, AwaitMessageSensor,
  EmailOperator,
} from './builtin/operators.js'
export type { OperatorOptions } from './builtin/operators.js'
export { CONNECTION_TYPES } from './builtin/catalog.js'

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
