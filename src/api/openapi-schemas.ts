/**
 * Request documentation for /openapi.json — query parameters and request bodies per operation.
 *
 * Routes carry no Fastify schemas (adding them would change validation behaviour), so the shapes live
 * here, keyed by "METHOD /fastify/path". openapi.test.ts fails when a write route has no entry or an
 * entry no longer matches a route, so this file cannot silently drift from the API.
 */
import { TI_STATES, TI_SETTABLE } from '../scheduler/states.js'
import { EVENT_TYPES } from '../events/index.js'
import { LAST_RUN_STATES, RUN_STATES } from '../scheduler/runs.js'

export type JsonSchema = Record<string, unknown>

export interface OperationDoc {
  summary?: string
  query?: Record<string, JsonSchema & { description?: string }>
  /** JSON request body. */
  body?: JsonSchema
  /** Non-JSON body (e.g. a file upload) — keyed by media type. */
  content?: Record<string, { schema: JsonSchema }>
  /** Explicitly no request body (write routes must say so, see openapi.test.ts). */
  noBody?: true
}

const str = (description?: string): JsonSchema => (description ? { type: 'string', description } : { type: 'string' })
const int = (description?: string): JsonSchema => ({ type: 'integer', ...(description ? { description } : {}) })
const obj = (properties: Record<string, JsonSchema>, required: string[] = []): JsonSchema =>
  ({ type: 'object', properties, ...(required.length ? { required } : {}), additionalProperties: false })

const bool: JsonSchema = { type: 'boolean' }
const mapIndex = { map_index: int('Mapped instance index; omit for all instances / non-mapped tasks') }
const page = { limit: int('Page size'), cursor: str('Opaque cursor from the previous page') }

export const OPERATION_DOCS: Record<string, OperationDoc> = {
  'GET /connection-types': { summary: 'Connection types read by the built-in providers: fields, extra keys and the npm driver each needs' },
  // ── Dags ──
  'GET /auth/session': { summary: 'Who am I: auth mode, role, user, CSRF token and the sign-in methods on offer' },
  'GET /auth/login': { summary: 'Start the OIDC browser login (authorization code + PKCE) — redirects to the identity provider', query: { return_to: str('UI path to come back to (same-origin paths only)') } },
  'GET /auth/callback': { summary: 'OIDC redirect target — exchanges the code, creates the session, redirects into the UI', query: { code: str(), state: str(), error: str() } },
  'POST /auth/key-login': { summary: 'Exchange an API key for a UI session cookie', body: obj({ key: str() }, ['key']) },
  'POST /auth/logout': { summary: 'End the UI session (needs X-CSRF-Token); returns the identity provider logout URL, if any', noBody: true },
  // ── Execution API (workers; task token, not user auth) ──
  'GET /execution/v1/context': { summary: 'Execution API: run conf, params, dates and mapValue of the token’s task instance' },
  'POST /execution/v1/xcom/:key': { summary: 'Execution API: push an XCom as the token’s task instance', body: obj({ value: {} }) },
  'GET /execution/v1/xcom/:taskId/:key': { summary: 'Execution API: pull an XCom from a task of the same run' },
  'GET /execution/v1/connections/:connId': { summary: 'Execution API: a decrypted connection (DB, then the secrets backend)' },
  'GET /execution/v1/variables/:key': { summary: 'Execution API: a variable value, and whether the worker must mask it' },
  'POST /execution/v1/datasets': {
    summary: 'Execution API: record the dataset events the task emitted',
    body: obj({ events: { type: 'array', items: obj({ uri: str(), alias: str(), extra: { type: 'object', additionalProperties: true } }, ['uri']) } }, ['events']),
  },
  'POST /execution/v1/logs': {
    summary: 'Execution API: append task log lines',
    body: obj({ lines: { type: 'array', maxItems: 1000, items: obj({ stream: { enum: ['stdout', 'stderr'] }, line: str(), level: { enum: ['debug', 'info', 'warn', 'error'] } }, ['line']) } }, ['lines']),
  },
  'GET /execution/v1/task': { summary: 'Execution API: the task’s Dag file name and run-function source' },
  'POST /execution/v1/token/refresh': { summary: 'Execution API: a fresh task token for the same running try', noBody: true },
  'GET /dags': {
    summary: 'List Dags with their newest run',
    query: {
      tag: { oneOf: [{ type: 'string' }, { type: 'array', items: { type: 'string' } }], description: 'Only Dags with this tag (repeatable)' },
      search: str('Case-insensitive substring of the Dag id, owner or a tag'),
      paused: { type: 'string', enum: ['true', 'false'], description: 'Only paused / active Dags' },
      last_run_state: { type: 'string', enum: [...LAST_RUN_STATES], description: 'Newest run in this state; none = never run' },
    },
  },
  'POST /dags/:dagId/trigger': {
    summary: 'Trigger a manual run (conf is validated against the Dag params; defaults are merged in)',
    body: obj({ conf: { type: 'object', additionalProperties: true }, tags: { type: 'array', items: { type: 'string' } }, note: str() }),
  },
  'POST /dags/:dagId/pause': { summary: 'Pause scheduling (manual triggers still work)', noBody: true },
  'POST /dags/:dagId/resume': { summary: 'Resume scheduling', noBody: true },
  'POST /dags/:dagId/backfill': {
    summary: 'Create runs for every scheduled date in [start, end]; dates that already have a run are skipped',
    body: obj({ start: { type: 'string', format: 'date-time' }, end: { type: 'string', format: 'date-time' } }, ['start', 'end']),
  },
  'GET /dags/:dagId/stats': { query: { limit: int('Number of recent runs to aggregate') } },
  'GET /dags/:dagId/source': { query: { version: str('Dag version hash; latest when omitted') } },
  'GET /dags/:dagId/runs': { summary: 'List runs, newest first', query: { ...page, tag: str('Only runs with this tag'), state: { type: 'string', enum: [...RUN_STATES], description: 'Only runs in this state' } } },

  // ── Runs, task instances, XCom ──
  'POST /dag-runs/:runId/cancel': { summary: 'Cancel a run and its unfinished tasks', noBody: true },
  'POST /dag-runs/:runId/note': { body: obj({ note: str() }, ['note']) },
  'GET /dag-runs/:runId/tasks': { query: { state: { type: 'string', enum: [...TI_STATES] } } },
  'GET /dag-runs/:runId/tasks/:taskId': { query: mapIndex },
  'GET /dag-runs/:runId/tasks/:taskId/tries': { query: mapIndex },
  'GET /dag-runs/:runId/tasks/:taskId/logs': {
    query: { level: { type: 'string', enum: ['debug', 'info', 'warn', 'error'] }, stream: { type: 'string', enum: ['stdout', 'stderr'] } },
  },
  'POST /dag-runs/:runId/tasks/:taskId/clear': {
    summary: 'Re-queue finished instance(s) of a task (optionally its downstream / upstream tasks) and re-open the run; 409 when nothing is finished',
    query: mapIndex,
    body: obj({ include_downstream: bool, include_upstream: bool, only_failed: bool, dry_run: bool }),
  },
  'POST /dag-runs/:runId/clear': {
    summary: 'Clear every finished instance of the run (re-run it), or only the failed ones',
    body: obj({ only_failed: bool, dry_run: bool }),
  },
  'PATCH /dag-runs/:runId/tasks/:taskId': {
    summary: 'Mark task instances success / failed / skipped (a running instance keeps running; its result is dropped)',
    body: obj({ state: { type: 'string', enum: [...TI_SETTABLE] }, map_index: int(), include_downstream: bool, include_upstream: bool, dry_run: bool }, ['state']),
  },
  'PATCH /dag-runs/:runId': {
    summary: 'Mark a run success (all instances → success) or failed (running → failed, waiting → skipped)',
    body: obj({ state: { type: 'string', enum: ['success', 'failed'] }, dry_run: bool }, ['state']),
  },
  'GET /dag-runs/:runId/xcoms': { query: { task_id: str(), key: str() } },
  'GET /dag-runs/:runId/xcoms/:taskId/:key': { query: mapIndex },
  'POST /dag-runs/:runId/xcoms': {
    body: obj({ task_id: str(), key: str(), value: { description: 'Any JSON value' }, map_index: { type: ['integer', 'null'] } }, ['task_id', 'key', 'value']),
  },
  'DELETE /dag-runs/:runId/xcoms/:taskId/:key': { query: mapIndex },

  // ── Backfills ──
  'GET /backfills': { query: { dag_id: str(), state: { type: 'string', enum: ['active', 'paused', 'cancelled'] }, ...page } },
  'POST /backfills/:backfillId/pause': { noBody: true },
  'POST /backfills/:backfillId/resume': { noBody: true },
  'POST /backfills/:backfillId/cancel': { noBody: true },

  // ── Pools, connections, variables ──
  'POST /pools': { summary: 'Create a pool (409 if the name exists)', body: obj({ name: str(), slots: { type: 'integer', minimum: 1 }, description: str() }, ['name', 'slots']) },
  'PATCH /pools/:name': { body: obj({ slots: int(), description: str() }) },
  'POST /connections': {
    summary: 'Create or replace a connection (password and extra are encrypted at rest)',
    body: obj({
      conn_id: str(), conn_type: str(), host: str(), port: { type: ['integer', 'null'] }, schema: str(),
      login: str(), password: str(), extra: str('JSON string'), description: str(),
    }, ['conn_id', 'conn_type']),
  },
  'POST /variables': {
    summary: 'Create or replace a variable (is_secret values are encrypted and never returned)',
    body: obj({ key: str(), value: str(), is_secret: { type: 'boolean' }, description: str() }, ['key', 'value']),
  },

  // ── Human-in-the-loop ──
  'GET /hitl': { summary: 'Tasks waiting for approval', query: { dag_id: str(), dag_run_id: str() } },
  'GET /hitl/:runId/:taskId': { query: mapIndex },
  'POST /hitl/:runId/:taskId': {
    query: mapIndex,
    body: obj({ decision: { type: 'string', enum: ['approve', 'reject'] }, note: str() }, ['decision']),
  },

  // ── Admin & observability ──
  'POST /api-keys': { summary: 'Create an API key — the plaintext key is returned once', body: obj({ name: str(), role: { type: 'string', enum: ['viewer', 'editor', 'admin'] } }, ['name']) },
  'POST /images/upload': {
    summary: 'Upload a Docker image tarball (.tar) for container tasks',
    content: { 'multipart/form-data': { schema: { type: 'object', properties: { file: { type: 'string', format: 'binary' } }, required: ['file'] } } },
  },
  'POST /sla-alerts/:alertId/ack': { noBody: true },
  'GET /sla-alerts': { query: { unacked: { type: 'string', enum: ['true', 'false'], description: 'Only unacknowledged alerts' } } },
  'GET /deadlines': { query: { dag_id: str(), dag_run_id: str(), missed: { type: 'string', enum: ['true', 'false'], description: 'Only missed (true) or pending (false) deadlines' }, limit: int() } },
  'GET /datasets': { query: { uri: str(), limit: int() } },
  'GET /dataset-aliases': { query: { alias: str() } },
  'GET /lineage': { summary: 'Dataset lineage graph', query: { dag_id: str('Only what is upstream or downstream of this Dag'), days: int('Days of runtime dataset events to include (default 30)') } },
  'GET /event-logs/types': { summary: 'Event types recorded in the event log' },
  'GET /dag-warnings': { query: { warning_type: str() } },
  'GET /event-logs': { query: { dag_id: str(), dag_run_id: str(), task_id: str(), event_type: { type: 'string', enum: [...EVENT_TYPES] }, ...page } },
}
