/**
 * Request documentation for /openapi.json — query parameters and request bodies per operation.
 *
 * Routes carry no Fastify schemas (adding them would change validation behaviour), so the shapes live
 * here, keyed by "METHOD /fastify/path". openapi.test.ts fails when a write route has no entry or an
 * entry no longer matches a route, so this file cannot silently drift from the API.
 */
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

const mapIndex = { map_index: int('Mapped instance index; omit for all instances / non-mapped tasks') }
const page = { limit: int('Page size'), cursor: str('Opaque cursor from the previous page') }

export const OPERATION_DOCS: Record<string, OperationDoc> = {
  // ── Dags ──
  'GET /dags': { summary: 'List Dags', query: { tag: { oneOf: [{ type: 'string' }, { type: 'array', items: { type: 'string' } }], description: 'Only Dags with this tag (repeatable)' } } },
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
  'GET /dags/:dagId/runs': { summary: 'List runs, newest first', query: { ...page, tag: str('Only runs with this tag') } },

  // ── Runs, task instances, XCom ──
  'POST /dag-runs/:runId/cancel': { summary: 'Cancel a run and its unfinished tasks', noBody: true },
  'POST /dag-runs/:runId/note': { body: obj({ note: str() }, ['note']) },
  'GET /dag-runs/:runId/tasks': { query: { state: { type: 'string', enum: ['queued', 'running', 'success', 'failed', 'cancelled'] } } },
  'GET /dag-runs/:runId/tasks/:taskId': { query: mapIndex },
  'GET /dag-runs/:runId/tasks/:taskId/tries': { query: mapIndex },
  'GET /dag-runs/:runId/tasks/:taskId/logs': {
    query: { level: { type: 'string', enum: ['debug', 'info', 'warn', 'error'] }, stream: { type: 'string', enum: ['stdout', 'stderr'] } },
  },
  'POST /dag-runs/:runId/tasks/:taskId/clear': { summary: 'Re-queue finished instance(s) of a task and re-open the run', query: mapIndex, noBody: true },
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
  'GET /datasets': { query: { uri: str(), limit: int() } },
  'GET /dag-warnings': { query: { warning_type: str() } },
  'GET /event-logs': { query: { dag_id: str(), dag_run_id: str(), task_id: str(), event_type: str(), ...page } },
}
