/**
 * Built-in providers — what ships with airflow-nodejs, as data: operators (with their options),
 * providers and connection types. Shared by the operator factories (Dag files), the worker-side
 * runtime and the API (GET /providers, GET /connection-types). No driver imports here.
 */

export type OperatorKind = 'run' | 'poke'

export interface OperatorSpec {
  provider: string
  /** Implementation id run by the worker (providers/builtin/runtime.ts). */
  impl: string
  /** 'poke' = a sensor: true when the condition holds, polled at pokeInterval. */
  kind: OperatorKind
  required: string[]
  optional: string[]
  description: string
}

/** Options every `run` operator accepts. */
const RUN_COMMON = ['doXcomPush']

export const OPERATORS: Record<string, OperatorSpec> = {
  // ── SQL (common-sql; drivers: pg, mysql2) ──────────────────────────────────
  SQLExecuteQueryOperator: {
    provider: 'common-sql', impl: 'sql.execute', kind: 'run',
    required: ['connId', 'sql'], optional: ['parameters', 'splitStatements', 'returnLast', 'autocommit', 'maxRows', ...RUN_COMMON],
    description: 'Run SQL on a postgres or mysql connection; XCom return_value = rows (SELECT) or affected row count',
  },
  SQLCheckOperator: {
    provider: 'common-sql', impl: 'sql.check', kind: 'run',
    required: ['connId', 'sql'], optional: ['parameters'],
    description: 'Fail unless the first row exists and every value in it is truthy',
  },
  SqlSensor: {
    provider: 'common-sql', impl: 'sql.sensor', kind: 'poke',
    required: ['connId', 'sql'], optional: ['parameters'],
    description: 'Wait until the first cell of the first row is truthy',
  },
  // ── Google Cloud (google; REST via google-auth-library) ─────────────────────
  LocalFilesystemToGCSOperator: {
    provider: 'google', impl: 'gcs.upload', kind: 'run',
    required: ['bucket', 'dst'], optional: ['connId', 'src', 'data', 'mimeType', ...RUN_COMMON],
    description: 'Upload a local file (src) or a string (data) to gs://bucket/dst',
  },
  GCSToLocalFilesystemOperator: {
    provider: 'google', impl: 'gcs.download', kind: 'run',
    required: ['bucket', 'objectName'], optional: ['connId', 'filename', 'maxBytes', ...RUN_COMMON],
    description: 'Download an object to filename, or (without filename) return its text as XCom',
  },
  GCSListObjectsOperator: {
    provider: 'google', impl: 'gcs.list', kind: 'run',
    required: ['bucket'], optional: ['connId', 'prefix', 'matchGlob', ...RUN_COMMON],
    description: 'List object names (XCom return_value)',
  },
  GCSDeleteObjectsOperator: {
    provider: 'google', impl: 'gcs.delete', kind: 'run',
    required: ['bucket'], optional: ['connId', 'objects', 'prefix', ...RUN_COMMON],
    description: 'Delete the given objects, or every object under prefix',
  },
  GCSObjectExistenceSensor: {
    provider: 'google', impl: 'gcs.exists', kind: 'poke',
    required: ['bucket', 'object'], optional: ['connId'],
    description: 'Wait until gs://bucket/object exists',
  },
  BigQueryInsertJobOperator: {
    provider: 'google', impl: 'bigquery.job', kind: 'run',
    required: [], optional: ['connId', 'sql', 'configuration', 'projectId', 'location', 'maxResults', ...RUN_COMMON],
    description: 'Run a BigQuery job (sql shorthand or a jobs.insert configuration); XCom = rows of a query',
  },
  BigQueryCheckOperator: {
    provider: 'google', impl: 'bigquery.check', kind: 'run',
    required: ['sql'], optional: ['connId', 'projectId', 'location'],
    description: 'Fail unless the first row exists and every value in it is truthy',
  },
  // ── Kafka (apache-kafka; driver: kafkajs) ──────────────────────────────────
  ProduceToTopicOperator: {
    provider: 'apache-kafka', impl: 'kafka.produce', kind: 'run',
    required: ['connId', 'topic', 'messages'], optional: [...RUN_COMMON],
    description: 'Produce messages ({ key?, value, headers?, partition? } or plain values) to a topic',
  },
  ConsumeFromTopicOperator: {
    provider: 'apache-kafka', impl: 'kafka.consume', kind: 'run',
    required: ['connId', 'topics'], optional: ['groupId', 'maxMessages', 'maxWaitMs', 'fromBeginning', ...RUN_COMMON],
    description: 'Consume up to maxMessages (committing offsets); XCom = the messages',
  },
  AwaitMessageSensor: {
    provider: 'apache-kafka', impl: 'kafka.await', kind: 'poke',
    required: ['connId', 'topics'], optional: ['groupId', 'match', 'maxWaitMs', 'fromBeginning'],
    description: 'Wait for a message whose value contains match (or any message)',
  },
  // ── Email (smtp; driver: nodemailer) ───────────────────────────────────────
  EmailOperator: {
    provider: 'smtp', impl: 'email.send', kind: 'run',
    required: ['to', 'subject'], optional: ['connId', 'cc', 'bcc', 'from', 'replyTo', 'html', 'text', 'files', ...RUN_COMMON],
    description: 'Send an email through an SMTP connection (default smtp_default)',
  },
}

export interface ConnectionField { name: 'host' | 'port' | 'schema' | 'login' | 'password' | 'extra'; label: string; default?: string | number }

export interface ConnectionTypeInfo {
  conn_type: string
  label: string
  provider: string
  /** Connection fields the provider reads (others are ignored) — for forms and docs. */
  fields: ConnectionField[]
  /** Keys read from `extra`. */
  extra_keys: string[]
  /** npm driver needed at task runtime (an optional peer dependency); null = none. */
  driver: string | null
}

export const CONNECTION_TYPES: ConnectionTypeInfo[] = [
  {
    conn_type: 'postgres', label: 'PostgreSQL', provider: 'common-sql', driver: 'pg',
    fields: [{ name: 'host', label: 'Host' }, { name: 'port', label: 'Port', default: 5432 }, { name: 'schema', label: 'Database' },
      { name: 'login', label: 'User' }, { name: 'password', label: 'Password' }],
    extra_keys: ['sslmode', 'connect_timeout', 'application_name', 'statement_timeout'],
  },
  {
    conn_type: 'mysql', label: 'MySQL / MariaDB', provider: 'common-sql', driver: 'mysql2',
    fields: [{ name: 'host', label: 'Host' }, { name: 'port', label: 'Port', default: 3306 }, { name: 'schema', label: 'Database' },
      { name: 'login', label: 'User' }, { name: 'password', label: 'Password' }],
    extra_keys: ['ssl', 'charset', 'connect_timeout'],
  },
  {
    conn_type: 'google_cloud_platform', label: 'Google Cloud', provider: 'google', driver: null,
    fields: [{ name: 'extra', label: 'Extra (JSON)' }],
    extra_keys: ['project', 'keyfile_dict', 'key_path', 'scope'],
  },
  {
    conn_type: 'kafka', label: 'Apache Kafka', provider: 'apache-kafka', driver: 'kafkajs',
    fields: [{ name: 'host', label: 'Bootstrap servers (comma-separated host:port)' }, { name: 'login', label: 'SASL username' },
      { name: 'password', label: 'SASL password' }, { name: 'extra', label: 'Extra (JSON)' }],
    extra_keys: ['bootstrap.servers', 'client.id', 'group.id', 'security.protocol', 'sasl.mechanism', 'sasl.username', 'sasl.password'],
  },
  {
    conn_type: 'smtp', label: 'SMTP (email)', provider: 'smtp', driver: 'nodemailer',
    fields: [{ name: 'host', label: 'Host' }, { name: 'port', label: 'Port', default: 465 }, { name: 'login', label: 'User' },
      { name: 'password', label: 'Password' }, { name: 'extra', label: 'Extra (JSON)' }],
    extra_keys: ['from_email', 'disable_ssl', 'disable_tls', 'timeout'],
  },
]

export const BUILTIN_PROVIDERS = [
  { name: 'common-sql', description: 'SQL operators and sensor for PostgreSQL and MySQL connections' },
  { name: 'google', description: 'Google Cloud Storage and BigQuery operators and sensor' },
  { name: 'apache-kafka', description: 'Kafka produce / consume operators and await-message sensor' },
  { name: 'smtp', description: 'Email operator over SMTP' },
].map(p => ({
  ...p,
  operator_names: Object.entries(OPERATORS).filter(([, s]) => s.provider === p.name).map(([n]) => n),
  connection_types: CONNECTION_TYPES.filter(c => c.provider === p.name).map(c => c.conn_type),
}))
