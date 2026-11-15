# airflow-nodejs

A production-grade reimplementation of Apache Airflow's core concepts in **Node.js + Fastify + MongoDB (or Cloud Spanner)**, built to be lightweight, self-contained, and deployable as a single Docker image.

1,580+ tests · 22 API route modules · `anode` CLI · multi-user RBAC + OIDC SSO · multi-replica safe · user code isolated from the control plane · Docker-ready

---

## Features

| Category | What's included |
|----------|----------------|
| **Scheduling** | Cron schedules (trigger or data-interval mode), manual triggers, catch-up, backfill with lifecycle (pause/resume/cancel), data-aware scheduling (dataset AND / OR conditions, aliases, dataset-or-time) |
| **Task execution** | Local child-process fork, BullMQ distributed mode, retries, timeouts, sensors, dynamic task mapping, task groups, resource pools |
| **Observability** | Deadline alerts (SLA as an alias), run statistics + duration histogram, event/audit log, task try history, dag warnings, import errors |
| **Data flow** | XCom push/pull (full CRUD), connections store (encrypted), variables store (encrypted), per-run conf/tags/notes |
| **Lifecycle** | Task and Dag callbacks / notifiers (functions, `{ notify }` objects, webhooks), templated fields with macros, Human-in-the-Loop (HITL) approval gates, clear (with downstream / upstream / failed only), re-run, mark task or run success / failed |
| **Operations** | Graceful shutdown with worker drain, config API, providers API, plugins API, `anode` command-line client (`dags list/trigger/pause`, `tasks test`) |
| **Auth** | Multi-user RBAC (viewer / editor / admin roles), OIDC SSO for the UI (authorization code + PKCE) and OIDC bearer tokens for the API, scrypt-hashed DB-backed API keys, env-key fallback |
| **Security** | Dag files parsed in a separate process into serialized Dags — the scheduler and API never import user code; callbacks and timetables run in a sandbox process; task workers hold no database credentials (execution API with task-scoped tokens); encryption key rotation; secret masking in task logs; Vault / Akeyless secrets backends; HttpOnly session cookie + CSRF token; strict Content-Security-Policy with React served locally |
| **Versioning** | Dag version history (sha256 hash), source code snapshots, backfill_id linkage on runs |
| **Docker** | 56 MB image, multi-stage build, Bun runtime, Walmart Artifactory registry |

---

## Prerequisites

- **Bun** 1.3+ — [bun.sh](https://bun.sh) (used as runtime; `npm` scripts delegate to it)
- **Docker Desktop** — [docker.com](https://www.docker.com/products/docker-desktop/)

MongoDB and Redis run inside Docker — no local installs needed.

---

## Quick Start

```bash
git clone <repo-url>
cd airflow-nodejs
npm install
docker-compose up -d mongo   # start MongoDB
npm run dev                  # scheduler + API + UI on :3000
```

Open **http://localhost:3000** for the web UI.

---

## Running Modes

### Local mode (default — no Redis needed)

Tasks execute as forked child processes on the same machine.

```bash
npm run dev
```

### Distributed mode (BullMQ via Redis)

Tasks are queued in Redis and picked up by worker processes — scale horizontally.

```bash
docker-compose up -d         # start MongoDB + Redis
npm run dev:bullmq
```

### Standalone BullMQ worker

Run additional workers anywhere with network access to Redis + MongoDB:

```bash
REDIS_URL=redis://<host>:6379 \
MONGO_URL=mongodb://<host>:27017 \
npm run worker

# Scale concurrency (default: 4)
WORKER_CONCURRENCY=8 npm run worker
```

---

## Authentication & RBAC

Auth is **disabled by default**. Enable by setting `API_KEYS` or `ADMIN_KEY`:

```bash
# Simple env-key auth (all keys have admin role)
API_KEYS=key1,key2 npm run dev

# DB-backed keys with roles (requires ADMIN_KEY bootstrap)
ADMIN_KEY=bootstrap-key npm run dev
```

### Roles

| Role | Capabilities |
|------|-------------|
| `viewer` | All `GET` endpoints |
| `editor` | `viewer` + trigger runs, pause/resume dags, backfill, clear tasks, HITL approve/reject |
| `admin` | `editor` + create/revoke API keys, read config |

```bash
# Create a scoped API key
curl -X POST http://localhost:3000/api-keys \
  -H "Authorization: Bearer $ADMIN_KEY" \
  -d '{"name": "ci-bot", "role": "editor"}'
```

### OIDC (SSO)

API clients can send an OIDC access token instead of an API key; the browser UI signs in through
the identity provider. A claim maps users to roles:

```bash
OIDC_ISSUER=https://login.example.com/realms/data       # enables OIDC bearer tokens
OIDC_ROLE_CLAIM=groups                                   # default roles; dotted paths ok (realm_access.roles)
OIDC_ADMIN_VALUES=airflow-admins  OIDC_EDITOR_VALUES=airflow-editors  OIDC_VIEWER_VALUES=everyone
OIDC_CLIENT_ID=airflow-nodejs                            # + this enables the UI's "Sign in with SSO"
OIDC_CLIENT_SECRET=...                                   # only for a confidential client
```

The UI login is the authorization-code flow with PKCE (S256), `state` bound to the browser and a
`nonce` checked in the ID token. The IdP must allow the redirect URI `https://<host>/auth/callback`
(or set `OIDC_REDIRECT_URI`). Sign-out also ends the IdP session when it publishes an
`end_session_endpoint`.

### Browser sessions

The UI never keeps a token or API key in the browser. Signing in (SSO, or pasting an API key once)
creates a server-side session (`ui_sessions`, only its hash is stored) and an `HttpOnly`,
`SameSite=Strict` cookie (`Secure` behind HTTPS). Every write from the UI carries an `X-CSRF-Token`
header matching the session. Requests with `Authorization: Bearer …` (scripts, CI) are unaffected.

Every response carries a strict `Content-Security-Policy` (`script-src 'self'`, no inline scripts,
no CDN): React is served from the package's own `node_modules` and the UI is compiled from
`src/ui/app.jsx` at build time (`npm run build` writes `dist/public/app.js`; `npm run dev` compiles on request).

---

## Writing a Dag

Create a `.ts` (or `.js`) file in `dags/`:

```typescript
import { dag } from '../src/dag/types.js'

export default dag({
  id: 'my_pipeline',
  schedule: '0 9 * * *',   // cron, or null for manual-only
  onSuccess: 'https://hooks.example.com/success',  // Dag callback: function, { notify }, URL or a list
  deadline: { reference: 'queued_at', interval: 30 * 60_000, callback: info => console.warn('late', info.runId) },
  tasks: {
    extract: {
      pool: 'db_pool',      // limit concurrency via resource pool
      retries: 2,
      retryDelay: 5000,
      onFailure: info => console.error(`${info.taskId} failed: ${info.error}`),   // also onExecute / onSuccess / onRetry
      run: async (ctx) => {
        const conn = await ctx.connections.get('my_db')   // ctx.ds, ctx.logicalDate, ctx.dataIntervalStart/End, ctx.params
        const limit = await ctx.variables.get('batch_size')
        await ctx.xcom.push('rows', 42)
      }
    },
    transform: {
      dependsOn: ['extract'],
      run: async (ctx) => {
        const rows = await ctx.xcom.pull('extract', 'rows')
        await ctx.xcom.push('result', { processed: rows })
      }
    },
    review: {
      dependsOn: ['transform'],
      requiresApproval: true,       // HITL gate — pauses until human approves
      hitlPrompt: 'Check the output before loading',
    },
    load: {
      dependsOn: ['review'],
      run: async (ctx) => {
        const result = await ctx.xcom.pull('transform', 'result')
        console.log('loading', result)
      }
    }
  }
})
```

Dags hot-reload every 5 seconds — no restart needed.

### Schedules and data intervals

`schedule: '0 0 * * *'` runs at each cron time T with `logical_date = T` (Airflow 3's CronTriggerTimetable;
`triggerInterval: ms` makes the data interval `[T - ms, T]`). `scheduleMode: 'interval'` instead runs once per
completed period (Airflow's CronDataIntervalTimetable): fired at T, the run covers `[prev(T), T]` and its
logical date is `prev(T)`. Catch-up and backfill follow the same mode. Manual and dataset runs have no
logical date or data interval.

Shell, Python, Java, container and Kubernetes task fields are templated: `{{ ds }}`, `{{ ds_nodash }}`,
`{{ ts }}`, `{{ data_interval_start }}`, `{{ data_interval_end }}`, `{{ conf.x }}` / `{{ params.x }}`,
macros `{{ macros.ds_add(ds, -1) }}` and `{{ macros.ds_format(ds, '%Y-%m-%d', '%Y%m%d') }}`, and filters
`{{ data_interval_end | ds_nodash }}`, `| default('x')`, `| upper`.

### Callbacks and deadlines

Tasks take `onExecute`, `onSuccess`, `onFailure` (final failure only) and `onRetry`; Dags take `onSuccess` and
`onFailure`. Each accepts a function, a `{ notify(info) }` object, a webhook URL, or a list. They fire once, in
the scheduler, whichever executor ran the task (local, BullMQ, Kubernetes, or the zombie reaper); a callback
error is logged and never changes the task. Marking a task by hand fires no callback.

`deadline` (one or a list) alerts when a run has not succeeded by `reference + interval` — references
`'logical_date'`, `'queued_at'` (re-counted from a clear), `{ fixed: date }` and
`{ averageRuntime: { maxRuns } }`. A miss is recorded once (event `deadline_missed`, `GET /deadlines`, the
`airflow_deadlines_missed` metric) and fires `callback`. A successful run drops its pending deadlines; a
failed one still alerts. `sla: ms` is an alias for a `queued_at` deadline and also lands in `GET /sla-alerts`.

### Datasets

`outlets: [uri]` emits an event when the Dag's run succeeds; a task can emit at runtime with
`ctx.emitDataset(uri, { alias?, extra? })` (recorded only if the task succeeds). A consumer's `datasets` is a
list (all must have new events) or a condition: `{ all: [...] }`, `{ any: [...] }`, `{ alias: 'name' }`
(any dataset a task attached to that alias), nested freely. Events since the last dataset run make one run.
With a cron `schedule` as well, the Dag runs on either.

---

## Full API Reference

### Core

```
GET  /health                                   server status + worker pool stats
GET  /dags                                     list all loaded dags
GET  /dags/:dagId                              dag detail + task graph
GET  /dags/:dagId/tasks                        task metadata (retries, pool, sensor flags)
POST /dags/:dagId/trigger                      manually trigger a run
POST /dags/:dagId/pause | /resume              pause/resume scheduled execution
GET  /dags/:dagId/runs                         recent runs (?tag= ?cursor= ?limit=)
GET  /dags/:dagId/stats                        run statistics + duration histogram
GET  /dags/:dagId/versions                     version history (dag source hash)
GET  /dags/:dagId/source?version=              source code snapshot
POST /dags/:dagId/backfill                     create runs for a date range
```

### Dag Runs & Tasks

```
GET  /dag-runs/:runId                          run state + all task states
POST /dag-runs/:runId/cancel                   cancel a queued/running run
POST /dag-runs/:runId/note                     add/update a note
GET  /dag-runs/:runId/tasks                    all task instances (?state=)
GET  /dag-runs/:runId/tasks/:taskId            single task (?map_index=)
GET  /dag-runs/:runId/tasks/:taskId/tries      retry history
GET  /dag-runs/:runId/tasks/:taskId/logs       log lines (?level=debug|info|warn|error &stream=stdout|stderr)
POST /dag-runs/:runId/tasks/:taskId/clear      reset finished instance(s) to queued + re-run
                                               body: include_downstream, include_upstream, only_failed, dry_run
POST /dag-runs/:runId/clear                    re-run the whole run (body: only_failed, dry_run)
PATCH /dag-runs/:runId/tasks/:taskId           mark success | failed | skipped
                                               body: state, map_index, include_downstream, include_upstream, dry_run
PATCH /dag-runs/:runId                         mark the run success | failed (body: state, dry_run)
GET  /dag-runs/:runId/xcoms                    all xcoms (?task_id= ?key=)
GET  /dag-runs/:runId/xcoms/:taskId/:key       single xcom (?map_index=)
POST /dag-runs/:runId/xcoms                    push xcom value via API
DELETE /dag-runs/:runId/xcoms/:taskId/:key     delete one xcom
DELETE /dag-runs/:runId/xcoms                  delete all xcoms for run
```

### HITL (Human-in-the-Loop)

```
GET  /hitl                                     pending approval tasks (?dag_id= ?dag_run_id=)
GET  /hitl/:runId/:taskId                      task approval detail
POST /hitl/:runId/:taskId                      approve or reject {decision: 'approve'|'reject', note?}
```

### Backfills

```
GET  /backfills                                list backfills (?dag_id= ?state= ?cursor=)
GET  /backfills/:backfillId                    single backfill + completed flag
POST /backfills/:backfillId/pause              pause advancement
POST /backfills/:backfillId/resume             resume advancement
POST /backfills/:backfillId/cancel             cancel + cancel all runs
```

### Connections, Variables, Pools

```
GET  /connections | POST                       list / create connection (encrypted)
GET  /connections/:connId | DELETE             get / delete connection
GET  /variables | POST                         list / create variable (secrets masked)
GET  /variables/:key | DELETE                  get / delete variable
GET  /pools | POST                             list / create resource pool
GET  /pools/:name | PATCH | DELETE             get / update / delete pool
```

### Observability

```
GET  /deadlines                                deadline alerts (?dag_id= ?dag_run_id= ?missed=)
GET  /datasets                                 dataset events (?uri= ?limit=)
GET  /dataset-aliases                          datasets attached to each alias (?alias=)
GET  /sla-alerts                               SLA breach alerts (?unacked=true)
POST /sla-alerts/:alertId/ack                  acknowledge alert
GET  /event-logs                               audit trail (?dag_id= ?event_type= ?cursor=)
GET  /dag-warnings                             soft validation issues (?warning_type=)
GET  /dag-warnings/:dagId                      warnings for a specific dag
GET  /import-errors                            dag load failures
```

### Auth, Discovery, Config

```
GET  /api-keys | POST                          list / create API key (admin only)
DELETE /api-keys/:keyId                        revoke API key (admin only)
GET  /providers                                installed npm runtime packages
GET  /plugins                                  registered route modules
GET  /config | /config/:section                runtime configuration (admin only)
GET  /auth/session                             who am I + CSRF token + sign-in methods (UI)
GET  /auth/login | /auth/callback              OIDC browser login (authorization code + PKCE)
POST /auth/key-login | /auth/logout            API-key sign-in → session cookie; sign out
```

### Execution API (task workers only)

Called by task workers with a task token (not a user credential) — see *Security model* below.

```
GET  /execution/v1/context                     run conf, params, dates, mapValue
POST /execution/v1/xcom/:key                   push an XCom as this task instance
GET  /execution/v1/xcom/:taskId/:key           pull an XCom from the same run
GET  /execution/v1/connections/:connId         decrypted connection
GET  /execution/v1/variables/:key              variable value (+ whether to mask it)
POST /execution/v1/datasets | /logs            dataset events; task log lines
GET  /execution/v1/task                        Dag file name + run-function source (Kubernetes pods)
POST /execution/v1/token/refresh               fresh token for the same running try
```

---

## Architecture

```
Liveness loop (HEARTBEAT_INTERVAL_MS)
  ├── Heartbeat          scheduler_instances.heartbeat_at for this process
  └── Leader lease       scheduler_leases — one replica leads at a time

Dag processor (leader, or `npm run dag-processor`)
  └── on file change     parse child process imports every Dag file → serialized_dags,
                         import errors + warnings → dag_processor (per-file timeout)

Scheduler loop (5s poll)
  ├── Dag sync           loads changed serialized Dags from the DB (no user code imported)
  ├── Leader only        cron, catch-up, timetables, deferred polling, deadlines / SLA,
  │                      dataset triggers, zombie reaping, retention purge
  ├── Cron scheduler     node-cron fires runs on schedule (logical_date = fire time)
  ├── Claim              atomic findOneAndUpdate (safe for concurrent schedulers)
  │     └── Gates: dependency, retry delay, sensor poke_at, HITL approval, backfill pause
  │     └── Limits: pool slots + maxActiveTis counted in the DB under short claim locks;
  │                 at most as many claims as this process has free workers (MAX_WORKERS)
  │     └── Stamps claimed_by = this process, so its heartbeat vouches for the task
  ├── Callbacks          results flag callback_pending; the scheduler fires each flag once (CAS)
  ├── Sandbox process    runs callbacks, notifiers, timetables, deadline callbacks and deferred
  │                      triggers for the scheduler (re-imports the Dag file, so closures work)
  └── Executor
        ├── local mode   fork child_process per task (tsx loader in dev, node when built)
        ├── BullMQ mode  enqueue to Redis → worker picks up and forks
        └── Kubernetes   kubectl run per task → worker-cli in the pod

Worker process (isolated, no database credentials or encryption keys)
  ├── Injects: ctx.conf / params, run dates (ds, logicalDate, data interval), ctx.connections,
  │            ctx.variables, ctx.xcom, ctx.emitDataset
  ├── Reaches all of them through the execution API — IPC to the forking process, or HTTP with a
  │   task token from a Kubernetes pod
  └── Reports outcome via IPC message / exit code → executor records try history

MongoDB collections:
  dag_runs             each dag execution + state + backfill_id
  task_instances       per-task state + HITL fields + sensor fields
  task_instance_tries  retry history (one row per attempt)
  xcoms                cross-task data
  task_logs            stdout/stderr lines
  connections          encrypted credentials
  variables            key/value config (secrets encrypted)
  pools                resource pool slot limits
  backfills            backfill lifecycle entity
  dag_versions         source hash history
  event_logs           audit trail
  sla_alerts           breach records
  deadlines            per-run deadline alerts (pending / missed)
  dataset_events       dataset events (Dag outlets, task emits)
  dataset_watermarks   per-consumer consumed position per dataset / alias
  dataset_alias_links  datasets attached to each alias at runtime
  api_keys             scrypt-hashed tokens + roles
  scheduler_instances  process registry + heartbeats (zombie detection)
  scheduler_leases     leader lease
  claim_locks          short locks serializing claims per pool / capped task
  serialized_dags      Dags as the Dag processor parsed them (functions as source)
  dag_processor        last parse: import errors, warnings, provider records
  ui_sessions          browser sessions + in-flight OIDC logins (hashed ids)
```

### Security model

What runs where — the Airflow 3 split, so a Dag author's code cannot reach the metadata database
or the encryption keys:

| Process | Runs user code? | Database / keys |
|---|---|---|
| API + scheduler | **No** — reads serialized Dags | yes |
| Dag parse child (per parse) | yes — imports Dag files and providers | **no** |
| Sandbox (callbacks, timetables, triggers) | yes | **no** |
| Task worker (fork, BullMQ fork, Kubernetes pod) | yes | **no** — execution API only |

- **Dag processor.** Dag files are imported only in a short-lived child process; the result is
  stored as JSON in `serialized_dags` (functions as their source). The scheduler and API rebuild Dags
  from it — every function is a stub that cannot run in-process. A file that fails to import keeps
  its last good Dags and shows an import error; a file that hangs past `DAG_PARSE_TIMEOUT_MS` or
  crashes the child is reported and skipped. Values that cannot be serialized (class instances,
  `Map`, bound functions) are import errors.
- **Sandbox.** Callbacks, notifiers, timetables, deadline callbacks and deferred-task triggers run
  in one long-lived child process that re-imports the Dag file (closures over the file's imports
  work). A call that blocks it past `SANDBOX_CALL_TIMEOUT_MS` gets the process replaced.
- **Execution API.** Workers get no `MONGO_URL` / Spanner settings / `ENCRYPTION_KEYS` / API keys.
  Forked workers call their parent over IPC; Kubernetes pods call `/execution/v1/*` with a JWT scoped
  to one try of one task instance (`EXECUTION_TOKEN_TTL_MS`, refreshed while the task runs). Every
  call checks that this try is still running, so a token dies with its attempt. Tokens are signed
  with `EXECUTION_API_SECRET`, else a key derived from `ENCRYPTION_KEYS`.
- **Secrets.** Connections and secret variables are decrypted by the control plane and masked as
  `***` in task logs (worker output and stored lines), including JSON-escaped and URL-encoded forms.


### Task instance states

Airflow's model: `queued` → `running` → `success` / `failed`, plus

| State | Meaning |
|-------|---------|
| `up_for_retry` | A try failed and retries are left; claimable again once `next_retry_at` passes (the delay lives in the DB, so it survives restarts and any replica can pick it up) |
| `up_for_reschedule` | A sensor poke returned false; claimable again at `next_poke_at` |
| `deferred` | Waiting on a trigger; holds no worker or pool slot |
| `upstream_failed` | Can never run because an upstream failed (`skipped` is used when skips are the reason) — follows Airflow's trigger-rule outcomes, e.g. `one_failed` fires on an `upstream_failed` upstream |
| `removed` | The task was deleted from the Dag while the run was active (restored to `queued` if it comes back) |
| `skipped` / `cancelled` | Branch / short-circuit skip; run cancelled |

A late result from an executor (after a cancel, timeout, mark or zombie hand-over) is dropped rather than
overwriting the newer state.

### Running several replicas

Any number of replicas can share one database:

- **Every replica** claims and executes tasks — a claim is an atomic per-task update.
- **One replica (the leader)** creates scheduled work: cron, catch-up, timetables, dataset triggers,
  deferred-task polling, deadline / SLA checks, zombie reaping and retention. Leadership is a lease renewed every
  `HEARTBEAT_INTERVAL_MS`; if the leader dies, another replica takes over within `SCHEDULER_LEASE_TTL_MS`.
- **One run per `(dag_id, logical_date)`** is enforced by a unique index, so a leadership hand-over or a
  backfill racing the scheduler cannot create the same scheduled run twice. Manual runs have no logical date.
- **Pools and task concurrency** (`pool`, `maxActiveTis`, `maxActiveTisPerRun`) are counted from the running
  task instances in the DB when a task is claimed, so they hold across replicas and in BullMQ / Kubernetes mode.
- **Zombie tasks:** a task stays `running` only while the process that claimed it keeps heartbeating.
  Once its owner is silent for `ZOMBIE_THRESHOLD_MS`, the task goes `up_for_retry` (if it has retries left) or fails,
  with the lost attempt recorded in its try history. A graceful shutdown hands its running tasks back
  to the queue instead.

Not covered: pods started by the Kubernetes executor keep running if the replica that launched them dies;
the task is then retried elsewhere while the old pod finishes.

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URL` | `mongodb://localhost:27017` | MongoDB connection URL |
| `DB_NAME` | `airflow` | MongoDB database name |
| `ENCRYPTION_KEYS` | _(required for secrets)_ | Comma-separated 64-hex-char AES-256-GCM keys; the first encrypts, all decrypt (rotation). `ENCRYPTION_KEY` (one key) still works |
| `API_KEYS` | _(unset)_ | Comma-separated static API keys (all have admin role) |
| `ADMIN_KEY` | _(unset)_ | Bootstrap admin key for DB-backed key management |
| `REDIS_URL` | _(unset)_ | Redis URL — enables BullMQ distributed mode |
| `WORKER_CONCURRENCY` | `4` | Concurrent tasks per BullMQ worker |
| `MAX_WORKERS` | `8` | Max concurrent tasks per process in local fork mode — a scheduler claims no more than its free slots |
| `DRAIN_TIMEOUT_MS` | `20000` | Graceful shutdown worker drain timeout |
| `PORT` | `3000` | API + UI port |
| `HOST` | `0.0.0.0` | API bind address |
| `RATE_LIMIT_MAX` | `120` | Requests per minute per IP (global) |
| `GLOBAL_SLACK_WEBHOOK` | _(unset)_ | Slack incoming webhook URL — fires for **all** DAG runs (success + failure) |
| `GLOBAL_SLACK_SUCCESS_WEBHOOK` | _(unset)_ | Slack webhook — fires on success only |
| `GLOBAL_SLACK_FAILURE_WEBHOOK` | _(unset)_ | Slack webhook — fires on failure only |
| `GLOBAL_SUCCESS_WEBHOOK` | _(unset)_ | Generic JSON webhook — fires on every success run |
| `GLOBAL_FAILURE_WEBHOOK` | _(unset)_ | Generic JSON webhook — fires on every failure run |
| `LOG_BACKEND` | `file` | Task log storage: `file` (default) or `mongodb` |
| `LOG_DIR` | `./logs` | Directory for task log files (when `LOG_BACKEND=file`) |
| `SECRETS_BACKEND` | `none` | Secrets fallback: `none`, `env`, `file`, `vault`, `akeyless` |
| `SECRETS_FILE_PATH` | _(unset)_ | Path to secrets JSON file (when `SECRETS_BACKEND=file`) |
| `KUBERNETES_EXECUTOR` | _(unset)_ | Set to `true` to dispatch all tasks as K8s pods |
| `KUBERNETES_NAMESPACE` | `default` | Kubernetes namespace for executor pods |
| `KUBERNETES_IMAGE` | `airflow-nodejs:local` | Container image used by K8s executor pods |
| `DB_BACKEND` | `mongo` | `mongo` or `spanner` (Cloud Spanner, GoogleSQL) |
| `HEARTBEAT_INTERVAL_MS` | `10000` | How often each process heartbeats and the leader renews its lease |
| `ZOMBIE_THRESHOLD_MS` | `60000` | Heartbeat age after which a task's owner counts as dead (min 3× interval) |
| `SCHEDULER_LEASE_TTL_MS` | `30000` | Leader lease lifetime — failover time after a leader crash (min 2× interval) |
| `RETENTION_DAYS` | _(unset)_ | Purge finished runs (and their tasks, tries, XComs, logs, events) older than this; unset = keep forever |
| `RETENTION_INTERVAL_MS` | `3600000` | How often the leader runs the retention purge |
| `SECRETS_TIMEOUT_MS` | `10000` | Per-request timeout for the Vault / Akeyless backends |
| `SECRETS_CACHE_TTL_MS` | `0` | Cache found Vault / Akeyless secrets in-process (0 = off) |
| `MASK_MIN_LENGTH` | `5` | Secrets shorter than this are not masked in task logs |
| `SENSITIVE_FIELD_NAMES` | _(unset)_ | Extra name fragments (comma-separated) whose values are masked, on top of Airflow's defaults (`password`, `secret`, `token`, `api_key`, …) |
| `OIDC_ISSUER` | _(unset)_ | Enables OIDC bearer tokens (discovery at `/.well-known/openid-configuration`) |
| `OIDC_AUDIENCE` | `OIDC_CLIENT_ID` | Required `aud` of access tokens |
| `OIDC_ROLE_CLAIM` / `OIDC_*_VALUES` / `OIDC_DEFAULT_ROLE` | `roles` | Claim → role mapping (see *OIDC (SSO)*) |
| `OIDC_CLIENT_ID` / `OIDC_CLIENT_SECRET` | _(unset)_ | Enables the UI's SSO login; the secret only for confidential clients |
| `OIDC_REDIRECT_URI` | `<origin>/auth/callback` | Redirect URI registered at the IdP |
| `OIDC_SCOPES` | `openid profile email` | Scopes requested at login |
| `OIDC_POST_LOGOUT_REDIRECT_URI` | _(unset)_ | Where the IdP returns after sign-out |
| `SESSION_TTL_MS` | `28800000` | UI session lifetime (8 h) |
| `SESSION_COOKIE_SECURE` | _(auto)_ | Force the `Secure` cookie flag (automatic behind HTTPS / `X-Forwarded-Proto: https`) |
| `DAG_PROCESSOR` | `embedded` | `embedded`: the leader re-parses changed Dag files; `external`: run `npm run dag-processor` separately |
| `DAG_PARSE_TIMEOUT_MS` | `30000` | Per-file import timeout in the parse child |
| `DAG_REPARSE_INTERVAL_MS` | `300000` | Re-parse even without file changes this often |
| `DAG_PROCESSOR_POLL_MS` | `5000` | External processor: how often it checks the Dag folder |
| `SANDBOX_CALL_TIMEOUT_MS` | `30000` | Limit per callback / timetable / trigger call in the sandbox |
| `EXECUTION_API_URL` | _(unset)_ | This deployment as reached from Kubernetes task pods (required for the Kubernetes executor) |
| `EXECUTION_API_SECRET` | _(derived)_ | Task-token signing secret; default: derived from `ENCRYPTION_KEYS` (same on every replica) |
| `EXECUTION_TOKEN_TTL_MS` | `1800000` | Task-token lifetime; workers refresh it while the try runs |

---

## Scripts

```bash
npm run dev          # local mode with hot reload
npm run dev:bullmq   # BullMQ distributed mode with hot reload
npm run build        # compile TypeScript → dist/
npm start            # production: node dist/main.js
npm run worker       # standalone BullMQ worker: node dist/queue/consumer.js
npm run dag-processor  # standalone Dag processor (use with DAG_PROCESSOR=external)
npm run rotate-keys [-- --dry-run]
                     # re-encrypt stored secrets with the first key in ENCRYPTION_KEYS
npm test             # run the test suite (requires MongoDB at localhost:27017)
npm run test:spanner # run the same suite against the Spanner emulator (localhost:9010)
npm run spanner:ddl  # print the Spanner DDL (for DBA-managed schemas)
npm run db:cleanup -- --days 30 [--dry-run] [--batch-size 100]
                     # purge finished runs older than N days (needs npm run build first)
npm run anode -- <group> <command>   # the CLI from source (see below)
```

## Command-line client (`anode`)

`anode` talks to a running deployment through the REST API; command names follow the Airflow CLI.
Installing the package puts it on the `PATH` (`bin`); from a checkout use `npm run anode --` or
`node dist/cli/main.js`.

```bash
export ANODE_API_URL=http://localhost:3000   # default
export ANODE_API_TOKEN=<api key or OIDC access token>   # when auth is on (or --url / --token)

anode dags list [--tag etl]                  # -o json for machine-readable output
anode dags show <dag_id>
anode dags trigger <dag_id> [--conf '{"region":"eu"}'] [--tag T] [--note N] [--wait [--timeout SEC]]
anode dags pause <dag_id> / anode dags unpause <dag_id>
anode dags list-runs <dag_id> [--limit 20]
anode dags list-import-errors
anode tasks list <dag_id>
anode tasks states-for-dag-run <run_id>
anode tasks test <dag_id> <task_id> [--logical-date 2026-03-02] [--conf JSON]
                 [--map-index N | --map-value JSON] [--run-id ID] [--dags-dir DIR | --file F]
```

Exit codes: `0` success, `1` failure (API error, failed run with `--wait`, failed task), `2` usage error.

`tasks test` runs one task's function **in the CLI process** without dependency checks and without
writing any state — like `airflow tasks test`. It imports the Dag file from `DAG_DIR` (or `--dags-dir` /
`--file`), so run it where the Dag code is checked out, never on the control plane.

| Task reads | Comes from |
|---|---|
| XCom | its own pushes (printed, kept in memory); other tasks' XComs of a real run with `--run-id` (REST), else `null` |
| variables / connections | the local secrets backend first (`SECRETS_BACKEND=env` with `AIRFLOW_VAR_*` / `AIRFLOW_CONN_*`), then the REST API — which never returns secret variable values or connection passwords/extras |
| conf / params | `--conf`, validated and merged with the Dag's param defaults |
| logical date / data interval | `--logical-date` and the Dag's schedule; none by default (like a manual run) |

`run`, `poke` (sensor, polled at its interval), `branch` and `shortCircuit` tasks are supported; a
`ctx.defer()` trigger is polled in-process. Shell, Python, Java, container, Kubernetes and cross-Dag tasks
need the scheduler — trigger the Dag for those. Secrets the task reads are masked in the CLI's output.

---

## Docker

### Image variants

A single `Dockerfile` produces all variants via the `--variant` flag:

| Image tag | `--variant` | Runtimes | Size |
|---|---|---|---|
| `airflow-nodejs:local` | *(default)* | sh, bash, zsh, tcsh, docker CLI | ~105 MB |
| `airflow-nodejs:python` | `python` | + Python 3.13 | ~114 MB |
| `airflow-nodejs:java21` | `java` | + Python 3.13 + JDK 21 LTS | ~225 MB |
| `airflow-nodejs:java25` | `java --jdk 25` | + Python 3.13 + JDK 25 | ~249 MB |

> **All variants include the Docker CLI** — container tasks (`container:` field) work in any variant as long as the Docker socket is mounted.
> **JDK 25** runs JARs compiled for any Java version (8/11/17/21/25).

### Prerequisites (one-time, on public WiFi)

The Dockerfile copies pre-downloaded `.deb` packages instead of running `apt-get` (blocked on Walmart network). Download them once and they stay locally in git-ignored folders:

```bash
# Required for ALL variants
./scripts/download-shell-debs.sh

# Required for python + java variants
./scripts/download-shell-debs.sh --python

# Required for java variant — choose JDK version:
./scripts/download-shell-debs.sh --java            # JDK 21 LTS (default)
./scripts/download-shell-debs.sh --java --jdk 25   # JDK 25 (runs Java 8–25 bytecode)

# Download everything at once:
./scripts/download-shell-debs.sh --python --java --jdk 25
```

### Build the image

`docker-build.sh` compiles TypeScript, validates prerequisites, and runs `docker build`:

```bash
# Base image — shells only
./docker-build.sh                               # arm64 (auto-detected from host)
./docker-build.sh --platform linux/amd64        # x86 / Intel deployment servers

# Python variant
./docker-build.sh --variant python
./docker-build.sh --variant python --platform linux/amd64

# Java variant — JDK 21 LTS (default)
./docker-build.sh --variant java
./docker-build.sh --variant java --platform linux/amd64

# Java variant — JDK 25 (backwards-compatible with Java 8–25)
./docker-build.sh --variant java --jdk 25
./docker-build.sh --variant java --jdk 25 --platform linux/amd64
```

`docker-build.sh` handles TypeScript compilation automatically (`npm run build`) — no need to run it separately.

> **Flags:**
> - `--variant base|python|java` — runtime stack to include (default: `base`)
> - `--jdk 21|25` — JDK version for the java variant (default: `21`)
> - `--platform linux/arm64|linux/amd64` — target CPU (default: auto-detect from host)

### Build directly with Docker (advanced)

```bash
# Base
docker build --build-arg VARIANT=base --build-arg TARGETPLATFORM=linux/arm64 -t airflow-nodejs:local .

# Python
docker build --build-arg VARIANT=python --build-arg TARGETPLATFORM=linux/amd64 -t airflow-nodejs:python .

# Java JDK 21
docker build --build-arg VARIANT=java --build-arg JDK_VER=21 --build-arg TARGETPLATFORM=linux/arm64 -t airflow-nodejs:java21 .

# Java JDK 25
docker build --build-arg VARIANT=java --build-arg JDK_VER=25 --build-arg TARGETPLATFORM=linux/arm64 -t airflow-nodejs:java25 .
```

### Quick start (local / dev)

A `.env` file ships with simple fixed credentials for local testing:

```bash
# .env (already in repo)
MONGO_URL=mongodb://host.docker.internal:27017
ENCRYPTION_KEY=0000000000000000000000000000000000000000000000000000000000000000
ADMIN_KEY=airflow
```

```bash
# 1. Start MongoDB
docker-compose up -d mongo

# 2. Run the app (shell/python/java tasks only)
docker run -p 3000:3000 --env-file .env \
  -v $(pwd)/dags:/app/dags \
  airflow-nodejs:local

# 3. Run with container task support (mounts Docker socket)
DOCKER_GID=$(stat -c %g /var/run/docker.sock 2>/dev/null || stat -f %g /var/run/docker.sock)
docker run -p 3000:3000 --env-file .env \
  -v $(pwd)/dags:/app/dags \
  -v /var/run/docker.sock:/var/run/docker.sock \
  --group-add $DOCKER_GID \
  airflow-nodejs:local
```

Open **http://localhost:3000** — use `Authorization: Bearer airflow` for authenticated endpoints.

> **Production:** replace `ENCRYPTION_KEY` with `openssl rand -hex 32` and use a strong `ADMIN_KEY`. The encryption key must be kept stable across restarts — it decrypts stored connections and variables.

#### Server resource limits

The server itself is lightweight (scheduler + HTTP API). Set limits at `docker run` time:

```bash
docker run -p 3000:3000 --env-file .env \
  -v $(pwd)/dags:/app/dags \
  --memory 512m \       # server hard memory cap (OOM-killed if exceeded)
  --cpus 1.0 \          # max CPU cores the server may use
  airflow-nodejs:local
```

Or in `docker-compose.yml`:
```yaml
services:
  app:
    image: airflow-nodejs:local
    mem_limit: 512m
    cpus: 1.0
```

> These limits apply to the **server process only**. Each task container has its own independent limits set in the DAG file (see [Container task resource limits](#container-task-resource-limits) below).

Suggested sizing:

| Component | Suggested limit |
|---|---|
| `airflow-nodejs` server | `--memory 512m --cpus 1.0` |
| MongoDB | `--memory 1g` |
| Task containers | set per-task in DAG (workload-dependent) |

### Container tasks

Container tasks run each task in its own Docker container — any language, any image, zero changes to the server. The server image stays lean; each task brings its own runtime.

`DAG_ID`, `RUN_ID`, and `TASK_ID` are injected as environment variables into every container. stdout/stderr are captured line-by-line to task logs.

#### Language examples

```js
export default dag({
  id: 'polyglot_pipeline',
  tasks: {

    // ── Python ──────────────────────────────────────────────────────────────
    python_step: {
      container: {
        image: 'python:3.13-slim',
        command: ['python3', '-c', `
import os, json
print(f"Python task: DAG={os.environ['DAG_ID']} RUN={os.environ['RUN_ID']}")
result = {"rows": 42, "status": "ok"}
print(json.dumps(result))
        `.trim()],
      }
    },

    // ── Ruby ────────────────────────────────────────────────────────────────
    ruby_step: {
      dependsOn: ['python_step'],
      container: {
        image: 'ruby:3.3-slim',
        command: ['ruby', '-e', `
require 'json'
puts "Ruby #{RUBY_VERSION} — task=#{ENV['TASK_ID']}"
puts JSON.generate({status: 'processed', run: ENV['RUN_ID']})
        `.trim()],
      }
    },

    // ── Perl ────────────────────────────────────────────────────────────────
    perl_step: {
      dependsOn: ['python_step'],
      container: {
        image: 'perl:5.40-slim',
        command: ['perl', '-e', `
use strict; use warnings; use JSON::PP;
printf "Perl %s — task=%s\\n", $], $ENV{TASK_ID};
print encode_json({status => 'ok', run => $ENV{RUN_ID}}), "\\n";
        `.trim()],
      }
    },

    // ── Rust (pre-compiled binary in dags/jobs/) ─────────────────────────
    // Build: GOOS=linux GOARCH=arm64 cargo build --release
    rust_step: {
      dependsOn: ['ruby_step'],
      container: {
        image: 'alpine:3.20',               // musl-linked binary runs on Alpine
        command: ['/jobs/my-rust-tool', '--run-id', '$RUN_ID'],
        volumes: ['$(pwd)/dags/jobs:/jobs'], // mount compiled binary
      }
    },

    // ── Go (pre-compiled binary) ─────────────────────────────────────────
    go_step: {
      dependsOn: ['perl_step'],
      container: {
        image: 'alpine:3.20',
        command: ['/jobs/my-go-tool', '--dag', '$DAG_ID'],
        volumes: ['$(pwd)/dags/jobs:/jobs'],
      }
    },

    // ── R ───────────────────────────────────────────────────────────────────
    r_step: {
      dependsOn: ['ruby_step'],
      container: {
        image: 'r-base:4.4',
        command: ['Rscript', '-e', `
cat("R", R.version$major, ".", R.version$minor, "\\n", sep="")
cat("task:", Sys.getenv("TASK_ID"), "\\n")
        `.trim()],
      }
    },

    // ── Node.js (different version than server) ──────────────────────────
    node_step: {
      dependsOn: ['go_step'],
      container: {
        image: 'node:22-alpine',
        command: ['node', '-e', `
const {DAG_ID, RUN_ID, TASK_ID} = process.env;
console.log('Node', process.version, '— task:', TASK_ID);
console.log(JSON.stringify({dagId: DAG_ID, runId: RUN_ID}));
        `.trim()],
      }
    },

    // ── PHP ──────────────────────────────────────────────────────────────────
    php_step: {
      dependsOn: ['node_step'],
      container: {
        image: 'php:8.3-cli-alpine',
        command: ['php', '-r', `
echo "PHP " . PHP_VERSION . " — task=" . getenv("TASK_ID") . "\n";
echo json_encode(["status" => "ok", "run" => getenv("RUN_ID")]) . "\n";
        `.trim()],
      }
    },

    // ── With custom env vars and shared volume ───────────────────────────
    final_step: {
      dependsOn: ['r_step', 'php_step'],
      container: {
        image: 'alpine:3.20',
        command: ['sh', '-c', 'echo "All done. DB=$DB_HOST ENV=$APP_ENV RUN=$RUN_ID"'],
        env: { DB_HOST: 'prod-db.internal', APP_ENV: 'production' },
        volumes: ['/shared/output:/output'],   // share results with host
        timeout: 30000,                         // 30s timeout
      }
    },

  }
});
```

> **On Walmart network:** public registries (`python:3.13-slim`, `ruby:3.3-slim`, etc.) are blocked.
> Prefix images with the internal mirror: `generic.ci.artifacts.walmart.com/hub-docker-release-remote/<image>`.
> Pull images on public WiFi first, then retag.

> **Compiled binaries (Rust/Go/C):** cross-compile for the container's CPU arch and drop in `dags/jobs/`.
> The `dags/` volume is already mounted — binaries are available immediately without a server rebuild.
> ```bash
> # Rust for ARM64 (Apple Silicon host → ARM container)
> cargo build --release --target aarch64-unknown-linux-musl
> cp target/aarch64-unknown-linux-musl/release/my-tool dags/jobs/
>
> # Go for ARM64
> GOOS=linux GOARCH=arm64 go build -o dags/jobs/my-tool ./cmd/tool
> ```

#### Container task resource limits

Each container task can declare its own memory, CPU, and disk limits independently of the server:

```js
tasks: {
  ml_training: {
    container: {
      image: 'pytorch/pytorch:2.3.0-cuda12.1-cudnn8-runtime',
      command: ['python3', 'train.py'],

      memory: '8g',       // hard limit — OOM-killed if exceeded. Formats: '512m', '2g', '8g'
      memorySwap: '8g',   // memory + swap total. '8g' = same as memory → no swap (recommended)
      cpus: '4.0',        // max CPU cores (fractional ok: '0.5', '2.0', '4.0')
      storageSize: '20g', // writable layer cap — requires dm/xfs Docker storage driver
    }
  },

  // Lightweight task — small limits to prevent runaway processes
  data_check: {
    container: {
      image: 'alpine:3.20',
      command: ['sh', '-c', 'echo "ok"'],
      memory: '64m',
      cpus: '0.1',
    }
  },
}
```

| Field | Docker flag | Notes |
|---|---|---|
| `memory` | `--memory` | Hard limit. OOM-kills process if exceeded. |
| `memorySwap` | `--memory-swap` | Total of RAM + swap. Set equal to `memory` to disable swap. |
| `cpus` | `--cpus` | Fractional CPUs allowed (`0.5` = half a core). |
| `storageSize` | `--storage-opt size=` | Writable layer. Requires overlay2+xfs quota on Docker daemon. |

> **Server vs task limits:** `--memory` on `docker run airflow-nodejs:local` limits the server. `memory:` in the DAG limits individual task containers. They are completely independent.

**Requirements for container tasks:**
- Server started with Docker socket mounted and `--group-add $(stat -c %g /var/run/docker.sock)`
- Images must be pre-pulled or available in your Docker registry
- XCom is not available in container tasks — use shared volumes or env vars to pass data between tasks

#### Uploading Docker images via the Web UI

Users can upload Docker image `.tar` files directly from the browser — no registry access needed at runtime.

**Workflow (fully offline):**

```bash
# 1. On a machine with internet access — export the image
docker pull python:3.13-slim
docker save python:3.13-slim -o python-3.13-slim.tar

# 2. Upload via the Web UI
#    Click the 🐳 button in the header → "⬆ Upload .tar"
#    Or via API:
curl -X POST http://localhost:3000/images/upload \
  -H "Authorization: Bearer <key>" \
  -F "file=@python-3.13-slim.tar"

# 3. Copy the requiredImages snippet from the UI into your DAG file
```

```js
export default dag({
  id: 'my_pipeline',
  requiredImages: [
    './images/python-3.13-slim.tar',  // loaded automatically when dag is registered
  ],
  tasks: {
    step: {
      container: {
        image: 'python:3.13-slim',
        command: ['python3', '-c', 'print("hello from container")'],
      }
    }
  }
});
```

The server loads the image via `docker load` in the background when the DAG file is registered. Subsequent loads of the same image are instant (idempotent).

**Image management API:**

| Method | Path | Description |
|---|---|---|
| `GET` | `/images` | List uploaded `.tar` files with size and `requiredImages` path |
| `POST` | `/images/upload` | Upload a `.tar` file (multipart/form-data) |
| `DELETE` | `/images/:name` | Remove a `.tar` file (image stays loaded in Docker) |

### Kubernetes tasks

Kubernetes tasks run as ephemeral Pods — any cluster, any language. kubectl blocks until the Pod exits (stdout/stderr streamed to task logs), then auto-deletes the Pod.

**Supported clusters:** minikube, kind, k3d, Rancher Desktop (local) · EKS, GKE, AKS (cloud) · any cluster with a valid kubeconfig.

```js
export default dag({
  id: 'k8s_pipeline',
  tasks: {

    // ── Simple task ──────────────────────────────────────────────────────────
    hello: {
      kubernetes: {
        image: 'alpine:latest',
        command: ['sh', '-c', 'echo "Hello from $DAG_ID on Kubernetes"'],
        namespace: 'default',   // optional — default: 'default'
      },
    },

    // ── Resource-limited Python job ──────────────────────────────────────────
    ml_job: {
      dependsOn: ['hello'],
      kubernetes: {
        image: 'python:3.13-slim',
        command: ['python3', '-c', 'print("ML job done")'],
        memory: '2Gi',    // request AND limit (guaranteed QoS)
        cpu: '1',         // 1 full core
        namespace: 'airflow',
      },
    },

    // ── Cloud IAM via IRSA (AWS) or Workload Identity (GCP) ─────────────────
    s3_export: {
      dependsOn: ['ml_job'],
      kubernetes: {
        image: 'amazon/aws-cli:latest',
        command: ['aws', 's3', 'cp', '/data/output.csv', 's3://my-bucket/'],
        namespace: 'airflow',
        serviceAccount: 'airflow-sa',    // bound to IAM role via IRSA
        context: 'arn:aws:eks:us-east-1:123456789:cluster/prod',
      },
    },

  },
});
```

**Field reference:**

| Field | Description |
|---|---|
| `image` | Container image (required) |
| `command` | Command + args override — placed after `--` separator |
| `namespace` | Kubernetes namespace. Default: `'default'` |
| `memory` | Memory request + limit. K8s format: `'512Mi'`, `'2Gi'` |
| `cpu` | CPU request + limit. K8s format: `'500m'`, `'1'` |
| `serviceAccount` | Pod service account (for IRSA / Workload Identity) |
| `env` | Extra env vars merged with `DAG_ID`/`RUN_ID`/`TASK_ID` |
| `kubeconfig` | Path to kubeconfig file. Default: `~/.kube/config` |
| `context` | kubectl context override |
| `podName` | Pod name prefix (RFC-1123). Default: `airflow-<dag>-<task>` |
| `timeout` | ms timeout — on expiry the Pod is force-deleted |

**Note on ports:** `kubectl run` does not support host-port mapping (`-p host:container`). Use a Kubernetes `Service` or `kubectl port-forward` separately if you need port access to a running Pod.

**Quick start (local):**
```bash
minikube start
# or: kind create cluster --name airflow
kubectl config use-context minikube
# Trigger the dag — kubectl must be on PATH
curl -X POST http://localhost:3000/dags/kubernetes_demo/trigger \
  -H "Authorization: Bearer <key>" -d '{}'
```

See `dags/kubernetes_demo.js` for a full working example.

---

### Full stack (MongoDB + app)

Add the app to `docker-compose.yml`:

```yaml
services:
  app:
    image: airflow-nodejs:local
    ports: ["3000:3000"]
    env_file: .env   # or set environment vars directly
    volumes:
      - ./dags:/app/dags
    depends_on: [mongo]
  mongo:
    image: mongo:7
    volumes: [mongo_data:/data/db]

volumes:
  mongo_data:
```

### Dag files in Docker

Dag files are mounted at `/app/dags`. In production, compile `.ts` dags to `.js` and mount the compiled output:

```bash
# Compile dag files
tsc --module NodeNext --outDir compiled-dags dags/*.ts

docker run ... -v $(pwd)/compiled-dags:/app/dags airflow-nodejs:local
```

---

## Running Tests

Tests require MongoDB at `localhost:27017`:

```bash
docker-compose up -d mongo
npm test
```

Each test suite uses an isolated database (`airflow_test_*`) and cleans up after itself.

---

## Graceful Shutdown

The server handles `SIGTERM` and `SIGINT` with a bounded drain sequence:

1. Stop scheduler (no new ticks) and release the leader lease, so another replica takes over at once
2. Close HTTP server (drain in-flight requests)
3. Drain worker pool (wait for active forks + queue to reach 0)
4. Close the BullMQ queue
5. Hand any task this process still owns back to the queue (no retry spent), deregister the process
6. Close the database connection and exit 0

Timeout: 20s (`DRAIN_TIMEOUT_MS`). Second signal forces exit 1.

---

## Running Apache Airflow 3.x Locally (Side-by-Side Comparison)

The `apache-airflow/` project ships a `docker-compose.local.yml` for running the real Airflow 3.x alongside airflow-nodejs on port 8080.

**First run (one-time DB init, ~2-3 min):**

```bash
cd ../apache-airflow   # or wherever your apache-airflow clone lives

# 1. Init DB + write credentials
docker-compose -f docker-compose.local.yml run --rm airflow-init

# 2. Start all services
docker-compose -f docker-compose.local.yml up -d
```

**Day-to-day start/stop:**

```bash
cd ../apache-airflow

# Start
docker-compose -f docker-compose.local.yml up -d

# Stop (keeps data)
docker-compose -f docker-compose.local.yml down

# Stop + wipe all data
docker-compose -f docker-compose.local.yml down -v
```

**Ports:**

| Service | URL | Credentials |
|---|---|---|
| **Apache Airflow 3.x** | http://localhost:8080 | `airflow` / `airflow` |
| **airflow-nodejs** | http://localhost:3000 | API key or open (if `API_KEYS` unset) |

Both can run simultaneously — different ports, independent databases.

**On Walmart network** — image is pulled from Artifactory:
```bash
docker pull generic.ci.artifacts.walmart.com/hub-docker-release-remote/apache/airflow:3.0.0
```

**Architecture:** LocalExecutor + SQLite — no Redis/Celery needed. Three containers: `airflow-apiserver` (port 8080), `airflow-scheduler`, `airflow-dag-processor`.

---

## UI TODO — Feature Gaps vs Apache Airflow 3.x

Comparison against the Apache Airflow 3.x web UI. Items are grouped by priority.

### 🔴 High value

| Feature | Airflow 3.x | Status |
|---|---|---|
| **DAG search / filter bar** | Search + tag + owner filters on DAG list | ❌ not implemented |
| **Grid view** | Per-task color grid across historical runs (run × task heatmap) | ✅ implemented — ⊞ Grid tab in run detail |
| **Task instance actions** | Re-run, clear, mark success/failed per task | ✅ task panel: clear (downstream / upstream / failed only), mark success / failed / skipped |
| **Run clear / re-run** | Clear whole run or subset of tasks | ✅ re-run, clear failed, mark run success / failed |
| **Last run badge on DAG card** | Colored last-run state badge directly on each DAG card | ❌ shown only inside runs list |
| **DAG tags display** | Tags shown on DAG card, filterable | ❌ stored in DB, not shown in UI |

### 🟡 Medium value

| Feature | Airflow 3.x | Status |
|---|---|---|
| **Event log page** | Searchable audit log of all Airflow events | ❌ API exists (`/event-logs`), no UI page |
| **Dataset/Asset lineage view** | Visual dependency graph across datasets | ❌ API exists (`/datasets`), no UI |
| **DAG code viewer** | Show raw DAG source file in UI | ❌ not exposed |
| **Calendar view** | Run frequency heatmap calendar per DAG | ✅ implemented — 📅 collapsible below runs table |
| **Gantt chart** | Task duration Gantt chart per run | ✅ implemented — ▬ Gantt tab in run detail |

### 🟢 Nice-to-have

| Feature | Airflow 3.x | Status |
|---|---|---|
| **Dark / light theme toggle** | User-selectable theme | ❌ dark only |
| **Cluster activity panel** | Live breakdown of scheduler / worker health | ⚠️ partial (workers badge in header) |
| **DAG owner column** | Owner shown on DAG list and card | ❌ not shown |

---

## Templating

Use `{{ variable }}` syntax in static string fields of shell, python, java, and container tasks. Variables are substituted at execution time — the task sees the final rendered string.

**Available variables:**

| Variable | Value |
|---|---|
| `{{ dag_id }}` | DAG id |
| `{{ run_id }}` | Run id |
| `{{ task_id }}` | Task id |
| `{{ ds }}` | Execution date as `YYYY-MM-DD` |
| `{{ ts }}` | Full ISO-8601 timestamp |
| `{{ ts_nodash }}` | Timestamp without dashes (`20240101T120000Z`) |
| `{{ logical_date }}` | `logical_date` ISO string, or `''` for manual runs |
| `{{ conf.key }}` | Trigger-time conf value |
| `{{ conf.nested.key }}` | Nested conf path |

Undefined paths render as `''` (empty string, not an error).

**Templated fields:**

| Task type | Templated fields |
|---|---|
| `shell` | `command`, `env` values |
| `python` | `code`, `args` items, `env` values |
| `java` | `args` items, `jvmArgs` items, `env` values |
| `container` | `command` items, `env` values |

```js
export default dag({
  id: 'my_pipeline',
  schedule: '0 6 * * *',   // runs daily
  tasks: {
    export: {
      shell: {
        // {{ ds }} = today's date, {{ conf.env }} = trigger-time config
        command: 'aws s3 cp /data/output-{{ ds }}.csv s3://{{ conf.bucket }}/{{ ds }}/',
        env: { AWS_PROFILE: '{{ conf.env }}' },
      }
    },
    process: {
      dependsOn: ['export'],
      python: {
        args: ['--date', '{{ ds }}', '--env', '{{ conf.env }}'],
        script: '/app/dags/scripts/process.py',
      }
    },
  }
})
```

**Note:** `run:` JS tasks don't need templating — use `ctx.conf.key`, `ctx.xcom.pull()`, and `new Date()` directly in the function body. Templating is for static string fields that can't be closures.

See `dags/templating_demo.js` for a full working example.

---

## Kubernetes Executor

Runs every task as an ephemeral Kubernetes Pod — the equivalent of Airflow's `KubernetesExecutor`. Enable by setting `KUBERNETES_EXECUTOR=true`.

**How it works:** Instead of forking a child process, the scheduler dispatches each task as a `kubectl run --restart=Never --rm --attach` pod. The pod runs the same `worker-cli.js` entry point: it imports the task's Dag file from the image (falling back to the run-function source from the API) and reaches XCom, connections, variables and run context only through the execution API, with a token scoped to that try (`EXECUTION_API_URL` + `EXECUTION_API_TOKEN` in the pod env). The pod gets no database credentials or encryption keys.

**Configuration:**

```bash
KUBERNETES_EXECUTOR=true
KUBERNETES_NAMESPACE=airflow          # default: 'default'
KUBERNETES_IMAGE=airflow-nodejs:local # image containing dist/
KUBERNETES_KUBECONFIG=/path/to/kubeconfig  # optional
KUBERNETES_CONTEXT=my-cluster         # optional
KUBERNETES_SERVICE_ACCOUNT=airflow-sa # optional (IRSA/Workload Identity)
KUBERNETES_CPU_REQUEST=100m           # optional
KUBERNETES_MEMORY_REQUEST=256Mi       # optional
EXECUTION_API_URL=http://airflow-nodejs.airflow.svc:3000   # required: this deployment, reachable from pods
```

**Requirements:**
- `kubectl` on PATH, configured for the target cluster
- Scheduler service account has `pods/create` + `pods/delete` RBAC permissions
- The API service reachable from task pods at `EXECUTION_API_URL` (pods no longer need database access)
- `KUBERNETES_IMAGE` must be pullable from within the cluster

**Limitations:**
- Sensors and deferrable tasks are not supported (require local mode)
- `run:` JS tasks only — shell/python/java/container tasks still use their respective executors regardless of this setting
- `KUBERNETES_EXECUTOR` and `REDIS_URL` (BullMQ) are mutually exclusive

**Comparison of execution modes:**

| Mode | Enable | Tasks run as |
|---|---|---|
| Local (default) | — | Forked child processes |
| BullMQ | `REDIS_URL=redis://...` | BullMQ workers (Redis queue) |
| **Kubernetes** | `KUBERNETES_EXECUTOR=true` | K8s pods via kubectl |

⚠️ **Live execution requires cluster RBAC:** the GKE cluster used in this project requires `pods/create` permission in the target namespace. Argv construction is verified via unit tests.

---

## Secrets Backends

Connections and variables are resolved via a fallback chain: **DB first, then the configured secrets backend**. This is the equivalent of Airflow's secrets backends (HashiCorp Vault, AWS Secrets Manager, GCP Secret Manager).

**Configure via environment variables:**

```bash
# File backend — read from a JSON file
SECRETS_BACKEND=file
SECRETS_FILE_PATH=/secrets/airflow-secrets.json

# Env backend — read from environment variables
SECRETS_BACKEND=env
AIRFLOW_VAR_API_KEY=my-secret-key
AIRFLOW_CONN_MY_DB='{"conn_type":"postgres","host":"db.example.com","port":5432,"login":"user","password":"secret"}'
```

**Secrets file format (`SECRETS_FILE_PATH`):**

```json
{
  "connections": {
    "my_db": {
      "conn_type": "postgres",
      "host": "db.example.com",
      "port": 5432,
      "login": "user",
      "password": "secret123",
      "schema": "public"
    }
  },
  "variables": {
    "api_key": "my-secret-api-key",
    "region": "us-east-1"
  }
}
```

**Fallback chain:**
1. Check MongoDB (DB-stored connections/variables win)
2. If not found → query the configured backend
3. If still not found → return `null`

**In DAG tasks:**

```js
run: async (ctx) => {
  // Both check DB then fall through to secrets backend automatically
  const apiKey = await ctx.variables.get('api_key')
  const conn   = await ctx.connections.get('my_db')
}
```

**Backends:**

| `SECRETS_BACKEND` | Source |
|---|---|
| `none` (default) | DB only — no fallback |
| `env` | `AIRFLOW_VAR_<KEY>` and `AIRFLOW_CONN_<ID>` env vars |
| `file` | JSON file at `SECRETS_FILE_PATH` |
| `vault` | HashiCorp Vault KV v1/v2 — token, AppRole or Kubernetes auth (same layout as Airflow's `VaultBackend`) |
| `akeyless` | Akeyless static secrets — token, access key or Kubernetes auth |

```bash
# Vault: secret/connections/<conn_id> holds {conn_uri} or {conn_type, host, login, password, …};
#        secret/variables/<key> holds {value}
SECRETS_BACKEND=vault
VAULT_ADDR=https://vault.example.com
VAULT_ROLE=airflow            # Kubernetes auth with the pod's service account (or VAULT_TOKEN, or VAULT_ROLE_ID + VAULT_SECRET_ID)
VAULT_MOUNT=secret  VAULT_KV_VERSION=2  VAULT_NAMESPACE=team-a   # optional

# Akeyless: /airflow/connections/<conn_id> holds JSON or a connection URI; /airflow/variables/<key> the value
SECRETS_BACKEND=akeyless
AKEYLESS_API_URL=https://gateway.example.com/api/v2
AKEYLESS_ACCESS_ID=p-xxxx  AKEYLESS_ACCESS_KEY=...   # or AKEYLESS_K8S_AUTH_CONFIG_NAME (+ AKEYLESS_GATEWAY_URL), or AKEYLESS_TOKEN
```

A backend that cannot be reached fails the lookup (and the task) with the real cause, rather than
reporting the secret as missing.

See `dags/secrets_demo.js` for a working example.

### Masking secrets in task logs

A connection's password and sensitive `extra` fields, secret variables, and variables with a
sensitive name are masked as `***` in everything the task prints and in its error message — also
their JSON-escaped and URL-encoded forms, and each line of a multi-line secret. Tune with
`MASK_MIN_LENGTH` and `SENSITIVE_FIELD_NAMES`.

### Rotating the encryption key

```bash
# 1. Put the new key first; keep the old one so existing values still decrypt
ENCRYPTION_KEYS=<new-64-hex>,<old-64-hex>      # restart every replica with this
# 2. Re-encrypt every stored connection and variable with the new key
npm run rotate-keys -- --dry-run && npm run rotate-keys
# 3. Drop the old key
ENCRYPTION_KEYS=<new-64-hex>
```

Each value records the id of the key that encrypted it (`GET /config/encryption` shows the key ids).
`rotate-keys` is safe to re-run and to run while the scheduler is up.

---

## Priority Weights

Tasks with higher `priority` are claimed before lower-priority ones when multiple tasks are ready simultaneously (e.g. under a shared pool or global concurrency limit). Default is `0`. Negative values are allowed.

```js
export default dag({
  id: 'my_pipeline',
  schedule: null,
  tasks: {
    critical:   { priority: 100, run: async () => doUrgentWork() },
    important:  { priority: 50,  run: async () => doImportantWork() },
    normal:     {                 run: async () => doNormalWork() },   // priority: 0 (default)
    background: { priority: -10, run: async () => doCleanup() },
  }
})
```

**Behavior:**
- Higher `priority` → claimed first when slots are available
- Equal priority → FIFO (first created runs first)
- Priority has no effect on tasks with unmet dependencies (they can't run regardless)
- Works across pools and the global concurrency limit

See `dags/priority_demo.js` for a working example.

---

## Typed DAG Params

DAG params add type safety and validation to trigger-time configuration. Validated before the run is created — bad params return `400` without touching the DB.

```js
export default dag({
  id: 'my_pipeline',
  schedule: null,

  params: {
    name:       { type: 'string', description: 'Required — no default' },
    env:        { type: 'string', enum: ['dev', 'staging', 'prod'], default: 'dev' },
    batch_size: { type: 'integer', minimum: 1, maximum: 10000, default: 100 },
    dry_run:    { type: 'boolean', default: false },
    prefix:     { type: 'string', pattern: '^[a-z]+$', default: 'output' },
  },

  tasks: {
    run: {
      run: async (ctx) => {
        // ctx.conf has caller values + defaults merged in
        const { name, env, batch_size, dry_run } = ctx.conf
        return { name, env, batch_size, dry_run }
      }
    }
  }
})
```

**Trigger:**
```bash
# Valid — defaults merged automatically
curl -X POST http://localhost:3000/dags/my_pipeline/trigger \
  -H 'Content-Type: application/json' \
  -d '{"conf": {"name": "alice"}}'
# → 201 { run_id: "...", conf: { name: "alice", env: "dev", batch_size: 100, dry_run: false } }

# Missing required param → 400
curl -X POST .../trigger -d '{}'
# → 400 { "error": "Param validation failed", "param_errors": [{ "param": "name", "message": "Required param 'name' not provided" }] }
```

**Param fields:**

| Field | Description |
|---|---|
| `type` | `string` \| `number` \| `integer` \| `boolean` \| `array` \| `object` |
| `default` | Default value. Omit to make param required. |
| `description` | Shown in `GET /dags/:id` response |
| `enum` | Restrict to exact values: `['dev','staging','prod']` |
| `minimum` / `maximum` | Numeric range (number/integer only) |
| `pattern` | Regex pattern (string only): `'^[a-z]+$'` |

See `dags/typed_params_demo.js` for a full example.

---

## Deferrable Tasks

Deferrable tasks suspend mid-execution and free their worker slot. The scheduler polls a trigger condition; when it fires, the task resumes. This is equivalent to Airflow's `deferrable=True` operators and Triggerer process.

```js
export default dag({
  id: 'my_pipeline',
  schedule: null,
  tasks: {
    wait_for_job: {
      run: async (ctx) => {
        // Start a long-running external job
        const jobId = await startExternalJob(ctx.conf.params)
        await ctx.xcom.push('job_id', jobId)

        // Defer: free worker slot, poll every 10s
        await ctx.defer(
          // Trigger — runs in scheduler process, must be self-contained
          async (tctx) => {
            const jobId = await tctx.xcom.pull('wait_for_job', 'job_id')
            const status = await checkJobStatus(String(jobId))  // HTTP call or DB query
            return status === 'complete'
          },
          { interval: 10_000, timeout: 60 * 60 * 1000 }  // poll 10s, deadline 1h
        )
        // ctx.defer() never returns — task resumes as 'success' when trigger fires
      }
    },

    // Runs after wait_for_job resumes
    process: {
      dependsOn: ['wait_for_job'],
      run: async (ctx) => processResults(ctx),
    },
  }
})
```

**Key behaviors:**
- Worker slot is freed immediately when `ctx.defer()` is called
- Task state becomes `deferred` (non-terminal — run doesn't complete yet)
- `trigger()` is polled by the scheduler on each tick (~5s)
- `trigger() → true` → task succeeds; downstream tasks proceed
- Deadline exceeded → task fails; run fails
- `trigger()` throws → task fails

**Trigger function constraints:**
- Runs **in the scheduler process**, not a worker fork
- Must be **self-contained** — no closures over module-scope imports
- Use `tctx.xcom`, `tctx.conf`, or direct HTTP/DB calls to read state

See `dags/deferrable_demo.js` for a working example.

---

## Run Notifications (Email / Slack / Webhook)

Notify users when a DAG run completes. Three approaches:

### 0. Global hooks (all DAGs, zero config per-DAG)

Set environment variables — **every DAG** fires them automatically, no DAG file changes needed. Equivalent to Airflow's Listener plugins.

```bash
# docker-compose.yml or .env

# Slack — formatted message with DAG name, state, run ID, timestamp
GLOBAL_SLACK_WEBHOOK=https://hooks.slack.com/services/T.../B.../xxx

# Slack — success-only or failure-only
GLOBAL_SLACK_SUCCESS_WEBHOOK=https://hooks.slack.com/services/...
GLOBAL_SLACK_FAILURE_WEBHOOK=https://hooks.slack.com/services/...

# Generic JSON webhook — same payload as per-DAG onSuccess/onFailure
GLOBAL_SUCCESS_WEBHOOK=https://your-service.com/webhook/success
GLOBAL_FAILURE_WEBHOOK=https://your-service.com/webhook/failure
```

**Slack message format:**
```
✅ *my_pipeline* — success
  DAG: my_pipeline   State: success
  Run ID: 6a6c...    Ended: 2024-01-15 06:03Z
```

All global hooks are fire-and-forget with a 5s timeout. They fire **in addition to** any per-DAG `onSuccess`/`onFailure` webhooks.

---

### 1. Built-in webhooks (`onSuccess` / `onFailure`)

The fastest option — no extra tasks needed. airflow-nodejs POSTs a JSON payload to the URL when the run finishes.

```js
export default dag({
  id: 'my_pipeline',
  schedule: '0 6 * * *',

  // Called when ALL tasks succeed
  onSuccess: 'https://hooks.slack.com/services/T.../B.../xxx',

  // Called when any task fails
  onFailure: 'https://your-alerting-service.com/webhook/pipeline-failed',

  tasks: { ... }
})
```

**Payload sent to both URLs (POST, Content-Type: application/json):**
```json
{
  "dag_id": "my_pipeline",
  "run_id": "6a6c...",
  "state": "success",
  "logical_date": null,
  "conf": { "env": "prod" },
  "tags": ["daily"],
  "ended_at": "2024-01-15T06:03:22Z"
}
```

- Fire-and-forget with a 5s timeout — delivery failures are logged, not retried
- Works with any webhook receiver: Slack, PagerDuty, Teams, custom HTTP endpoints

---

### 2. Email via shell task (`triggerRule: 'all_done'`)

Add a final task that runs regardless of upstream outcome and sends email via `sendmail` or `curl`+SMTP API:

```js
export default dag({
  id: 'my_pipeline',
  schedule: '0 6 * * *',
  tasks: {
    extract:   { run: async () => extract() },
    transform: { dependsOn: ['extract'], run: async () => transform() },
    load:      { dependsOn: ['transform'], run: async () => load() },

    // Email on completion — runs whether pipeline succeeded or failed
    email_notify: {
      dependsOn: ['load'],
      triggerRule: 'all_done',
      shell: {
        interpreter: 'sh',
        // Uses curl + SendGrid API (replace with your SMTP relay or email API)
        command: [
          'STATUS=$(curl -sf http://localhost:3000/dag-runs/$RUN_ID | python3 -c "import json,sys; print(json.load(sys.stdin)[\'state\'])" 2>/dev/null || echo "unknown")',
          'ICON=$([ "$STATUS" = "success" ] && echo "✅" || echo "❌")',
          'curl -s --request POST \\',
          '  --url https://api.sendgrid.com/v3/mail/send \\',
          '  --header "Authorization: Bearer $SENDGRID_API_KEY" \\',
          '  --header "Content-Type: application/json" \\',
          '  --data "{',
          '    \\"personalizations\\": [{',
          '      \\"to\\": [{\\"email\\": \\"$NOTIFY_EMAIL\\"}]',
          '    }],',
          '    \\"from\\": {\\"email\\": \\"noreply@example.com\\"},',
          '    \\"subject\\": \\"$ICON Pipeline $STATUS — $DAG_ID\\",',
          '    \\"content\\": [{',
          '      \\"type\\": \\"text/plain\\",',
          '      \\"value\\": \\"DAG: $DAG_ID\\\\nRun: $RUN_ID\\\\nStatus: $STATUS\\"',
          '    }]',
          '  }"',
        ].join('\n'),
        env: {
          SENDGRID_API_KEY: 'SG.your-api-key-here',
          NOTIFY_EMAIL:     'team@example.com',
        },
      }
    },
  }
})
```

**Alternative: `sendmail` (if installed in the runtime image):**
```js
shell: {
  interpreter: 'sh',
  command: [
    'echo "Subject: Pipeline $DAG_ID done\\nRun: $RUN_ID" | sendmail team@example.com',
  ].join('\n'),
}
```

---

### 3. Slack notification via container task

```js
export default dag({
  id: 'my_pipeline',
  schedule: null,
  tasks: {
    work: { run: async () => doWork() },

    slack_notify: {
      dependsOn: ['work'],
      triggerRule: 'all_done',
      container: {
        image: 'curlimages/curl:latest',
        command: [
          'sh', '-c',
          [
            'STATUS=$(curl -sf http://host.docker.internal:3000/dag-runs/$RUN_ID',
            '  | python3 -c "import json,sys; print(json.load(sys.stdin)[\'state\'])" 2>/dev/null || echo unknown)',
            'ICON=$([ "$STATUS" = "success" ] && echo "✅" || echo "❌")',
            'curl -sf -X POST $SLACK_WEBHOOK',
            '  -H "Content-Type: application/json"',
            '  -d "{\\"text\\":\\"$ICON Pipeline *$DAG_ID* $STATUS\\\\nRun: $RUN_ID\\"}"',
          ].join(' '),
        ],
        env: {
          SLACK_WEBHOOK: 'https://hooks.slack.com/services/T.../B.../xxx',
        },
      }
    },
  }
})
```

---

### 4. Email via Python task (if Python image)

```js
export default dag({
  id: 'my_pipeline',
  schedule: null,
  tasks: {
    work: { run: async () => doWork() },

    email_notify: {
      dependsOn: ['work'],
      triggerRule: 'all_done',
      python: {
        interpreter: 'python3',
        code: [
          'import os, smtplib',
          'from email.mime.text import MIMEText',
          '',
          'dag_id  = os.environ["DAG_ID"]',
          'run_id  = os.environ["RUN_ID"]',
          'smtp_host = os.environ.get("SMTP_HOST", "smtp.gmail.com")',
          'smtp_port = int(os.environ.get("SMTP_PORT", "587"))',
          'smtp_user = os.environ["SMTP_USER"]',
          'smtp_pass = os.environ["SMTP_PASS"]',
          'to_addr   = os.environ["NOTIFY_EMAIL"]',
          '',
          'msg = MIMEText(f"DAG: {dag_id}\\nRun: {run_id}")',
          'msg["Subject"] = f"Pipeline complete: {dag_id}"',
          'msg["From"] = smtp_user',
          'msg["To"]   = to_addr',
          '',
          'with smtplib.SMTP(smtp_host, smtp_port) as s:',
          '    s.starttls()',
          '    s.login(smtp_user, smtp_pass)',
          '    s.send_message(msg)',
          'print(f"Email sent to {to_addr}")',
        ].join('\n'),
        env: {
          SMTP_HOST:    'smtp.gmail.com',
          SMTP_PORT:    '587',
          SMTP_USER:    'your@gmail.com',
          SMTP_PASS:    'app-password-here',  // use Gmail App Password
          NOTIFY_EMAIL: 'team@example.com',
        },
      }
    },
  }
})
```

---

### Comparison

### 5. Email with log attachment (`EmailNotifyOperator`) — **default notification**

The most useful notification: sends an email with the full task log attached as a **gzip-compressed file**. Large logs are split into ≤100 MB parts (configurable via `LOG_PART_MB`).

```js
import { getOperator } from 'airflow-nodejs/providers'
const EmailNotifyOperator = getOperator('notify-provider', 'EmailNotifyOperator')

export default dag({
  id: 'my_pipeline',
  schedule: '0 6 * * *',
  tasks: {
    work:  { run: async (ctx) => { ctx.log.info('processing...'); return 'done' } },

    // Runs after work — success OR failure — attaches log to email
    notify: {
      dependsOn: ['work'],
      triggerRule: 'all_done',
      ...EmailNotifyOperator({
        subject: 'Pipeline $dag_id report — run $run_id',
        body:    'See attached log for details.',
      }),
    },
  }
})
```

**Setup:**
```bash
# docker-compose.yml — add to app environment:
SMTP_HOST=smtp.gmail.com
SMTP_PORT=587
SMTP_USER=your@gmail.com
SMTP_PASS=your-app-password     # Gmail: Settings → Security → App passwords
NOTIFY_EMAIL=team@example.com
```

**What happens:**
1. After the task completes, `EmailNotifyOperator` finds the log file at `logs/<dag_id>/<run_id>/<task_id>.log`
2. Gzip-compresses it (`gzip -9`, typically 5–20× reduction)
3. If compressed size > 100 MB → splits into multiple emails (`part1`, `part2`, ...)
4. Sends each part via SMTP with the `.log.gz` file attached
5. Falls back to plain-text email if no log file found

**Decompress the attachment:**
```bash
gunzip my_pipeline_run123_work.log.gz
```

**Configuration env vars:**

| Variable | Default | Description |
|---|---|---|
| `SMTP_HOST` | — | SMTP server (e.g. `smtp.gmail.com`) |
| `SMTP_PORT` | `587` | Port (587=STARTTLS, 465=SSL) |
| `SMTP_USE_SSL` | `false` | Set `true` for port 465 |
| `SMTP_USER` | — | Sender email address |
| `SMTP_PASS` | — | Password / App Password |
| `NOTIFY_EMAIL` | — | Recipient address |
| `LOG_PART_MB` | `100` | Max attachment size per email (MB) |

Requires Python 3 — use the `airflow-nodejs:python` image variant.

See `dags/email_log_notification_demo.js` for a complete example.

---

### 6. SMS via Twilio (`SmsNotifyOperator`)

The `notify-provider` ships a ready-to-use `SmsNotifyOperator` powered by the Twilio REST API:

```js
import { getOperator } from 'airflow-nodejs/providers'
const SmsNotifyOperator = getOperator('notify-provider', 'SmsNotifyOperator')

export default dag({
  id: 'my_pipeline',
  schedule: '0 6 * * *',
  tasks: {
    work: { run: async () => doWork() },

    sms_alert: {
      dependsOn: ['work'],
      triggerRule: 'all_done',       // fires whether pipeline succeeded or failed
      ...SmsNotifyOperator({
        message: 'Pipeline $DAG_ID done — run $RUN_ID',
        // to: '+12025559999',        // override recipient per-task (optional)
      }),
    },
  }
})
```

**Setup — set these env vars** (in `docker-compose.yml` or `SECRETS_BACKEND=env`):

```bash
TWILIO_ACCOUNT_SID=ACxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
TWILIO_AUTH_TOKEN=your_auth_token
TWILIO_FROM=+12025551234    # your Twilio number
TWILIO_TO=+12025555678      # default recipient
```

Get credentials at [console.twilio.com](https://console.twilio.com) — free $15 trial, verified numbers only.

---

### 6. SMS via AWS SNS (`AwsSnsOperator`)

```js
const AwsSnsOperator = getOperator('notify-provider', 'AwsSnsOperator')

sms_alert: {
  dependsOn: ['work'],
  triggerRule: 'all_done',
  ...AwsSnsOperator({
    message: 'Pipeline $DAG_ID done — run $RUN_ID',
    phoneNumber: '+12025555678',              // direct SMS (E.164 format)
    // OR: topicArn: 'arn:aws:sns:us-east-1:123:alerts',  // fan-out topic
  }),
}
```

Requires `aws-cli` in the runtime image and `AWS_DEFAULT_REGION` + credentials (env or IAM role/IRSA).

---

### Comparison

| Approach | Config | Requires | Best for |
|---|---|---|---|
| `onSuccess`/`onFailure` | 1-line URL in DAG | HTTP endpoint | Webhooks, Slack, PagerDuty |
| Shell task + curl | Extra task | `triggerRule: 'all_done'` | SendGrid, Mailgun, custom APIs |
| Container task | Extra task | Docker socket | Any language / runtime |
| Python task | Extra task | Python image variant | SMTP relay, Python email libs |
| `EmailNotifyOperator` | Extra task | Python + SMTP creds | **Email with gzipped log attached** |
| `SmsNotifyOperator` | Extra task | Twilio credentials | SMS — Twilio |
| `AwsSnsOperator` | Extra task | AWS credentials + CLI | SMS / push — AWS SNS |

**Recommended for most teams:** use `EmailNotifyOperator` as the default (email + attached log), `GLOBAL_SLACK_WEBHOOK` for team-wide Slack visibility, and `SmsNotifyOperator` for critical on-call alerts.

See `dags/sms_notification_demo.js` for a complete example.

---

## Providers Ecosystem

Providers are reusable operator libraries — the Node.js equivalent of Airflow's community pip providers. Place provider files in `dags/providers/` and they're auto-discovered at startup.

**Provider file structure (`dags/providers/my-provider.js`):**

```js
export default {
  name: 'my-provider',
  version: '1.0.0',
  description: 'My custom operators',
  connectionTypes: ['my-service'],   // connection types this provider supports

  operators: {
    // Factory function → returns a TaskDefinition
    MyOperator: (opts = {}) => ({
      shell: {
        interpreter: 'sh',
        command: `echo "running with option: ${opts.message ?? 'default'}"`,
      }
    }),

    // Can also return python/container/run tasks
    MyPythonOperator: (opts = {}) => ({
      python: { code: `print("${opts.message ?? 'hello from provider'}")` }
    }),
  },
}
```

**Using operators in DAGs:**

```js
import { getOperator } from 'airflow-nodejs/providers'

const MyOperator = getOperator('my-provider', 'MyOperator')

export default dag({
  id: 'my_pipeline',
  schedule: null,
  tasks: {
    step: MyOperator({ message: 'hello' }),           // single operator
    step2: { dependsOn: ['step'], ...MyOperator({ message: 'world' }) },
  }
})
```

**Discovery API:**

```bash
GET /providers
# Returns npm packages (dependencies) AND local providers from dags/providers/
# {
#   "local_providers": [
#     { "package_name": "http-provider", "operator_names": ["HttpGetOperator", ...] }
#   ],
#   "npm_providers": [...],
#   "total_entries": N
# }

GET /providers/http-provider   # single provider details
```

**Built-in example providers** (in `dags/providers/`):

| Provider | Operators |
|---|---|
| `http-provider` | `HttpGetOperator`, `HealthCheckOperator`, `HttpPostOperator` |
| `notify-provider` | `LogNotifyOperator`, `SlackNotifyOperator` (webhook stub) |

See `dags/providers_demo.js` for a complete example using both providers.

---

## Dynamic Task Mapping

Fan out a task over an array — one instance per element. Two forms:

**Literal (static):** array known at authoring time.
```js
tasks: {
  process: {
    expand: ['us-east-1', 'us-west-2', 'eu-west-1'],
    run: async (ctx) => deploy(ctx.mapValue),   // ctx.mapIndex = 0/1/2
  }
}
```

**XCom-driven (dynamic):** list produced at runtime by an upstream task.
```js
tasks: {
  // Step 1: discover items
  discover: {
    run: async (ctx) => {
      const files = await listS3Files(ctx.conf.bucket)
      await ctx.xcom.push('files', files)   // push the list
    }
  },

  // Step 2: process each file — instances created after discover succeeds
  process: {
    dependsOn: ['discover'],
    expand: { from: 'discover', key: 'files' },  // ← XCom-driven
    run: async (ctx) => processFile(ctx.mapValue),
  },

  // Step 3: join — runs after ALL instances complete
  summarize: {
    dependsOn: ['process'],
    run: async (ctx) => {
      const results = await ctx.xcom.pull('process', 'return_value')  // array of all instances
      return { total: results.length }
    }
  }
}
```

**Behaviors:**
- Source pushes N items → N instances run in parallel; downstream waits for all
- Source pushes `[]` → mapped task auto-skipped; run terminates cleanly
- Source fails → mapped task `upstream_failed` via cascade; run terminates cleanly

See `dags/dynamic_mapping_demo.js` for a working example.

---

## Branching

Branch tasks return the task_id(s) to activate. All other direct dependents are automatically `skipped`. Downstream chains of skipped tasks cascade.

```js
export default dag({
  id: 'my_pipeline',
  schedule: null,
  tasks: {
    // Branch: return one or more task_ids to run
    route: {
      branch: async (ctx) => {
        const score = await ctx.xcom.pull('scorer', 'score') as number
        return score >= 0.9 ? 'fast_path' : 'slow_path'
      }
    },

    fast_path: { dependsOn: ['route'], run: async () => 'fast' },
    slow_path: { dependsOn: ['route'], run: async () => 'slow' },

    // Join: use triggerRule: 'none_failed' so it runs even when one branch is skipped
    join: {
      dependsOn: ['fast_path', 'slow_path'],
      triggerRule: 'none_failed',
      run: async () => 'done',
    },
  }
})
```

**Rules:**
- Return a `string` (single task_id) or `string[]` (multiple)
- Return `null` or `[]` to skip all downstream tasks
- Invalid task_ids are logged and ignored
- Branch decision stored as XCom key `_branch_decision` for debugging
- Cannot be combined with `run`, `poke`, `shell`, `python`, `java`, `container`, or `kubernetes`

See `dags/branching_demo.js` for a complete score-based routing example.

---

## Trigger Rules

Trigger rules control when a task runs based on the states of its upstream tasks (the `dependsOn` list). The default is `all_success`.

| Rule | Run when upstreams are… |
|---|---|
| `all_success` | **Default.** All succeeded |
| `all_failed` | All failed (or skipped) — useful for cleanup tasks |
| `all_done` | All finished (any terminal state) — always runs last |
| `one_success` | At least one succeeded |
| `one_failed` | At least one failed — useful for alerts |
| `none_failed` | None failed (all success or skipped) |

Tasks whose rule can **never** be satisfied are finished automatically so the run always reaches a terminal state: `upstream_failed` when an upstream failure is the reason, `skipped` otherwise (Airflow's outcomes). `one_failed` / `all_failed` count `upstream_failed` upstreams as failures; `one_done` does not count them as done.

```js
export default dag({
  id: 'my_dag',
  schedule: null,
  tasks: {
    work:    { run: async () => doWork() },

    // Cleanup runs only if work failed:
    cleanup: {
      dependsOn: ['work'],
      triggerRule: 'all_failed',
      run: async () => rollback(),
    },

    // Alert runs if work failed (use one_failed for mixed upstream sets):
    alert: {
      dependsOn: ['work'],
      triggerRule: 'one_failed',
      run: async () => sendAlert(),
    },

    // Summary always runs (all_done = any terminal outcome):
    summary: {
      dependsOn: ['work', 'cleanup', 'alert'],
      triggerRule: 'all_done',
      run: async () => report(),
    },
  }
})
```

See `dags/trigger_rules_demo.js` for a full working example.

---

## Timetables (Custom Schedules)

Timetables replace cron with arbitrary JavaScript scheduling logic. Use them for patterns cron can't express: weekdays only, business hours, N runs then stop, exponential backoff, or any date-math.

Set `schedule: null` and add a `timetable` function that returns:
- **`Date`** — when the next run should fire
- **`null`** — stop scheduling permanently

```js
export default dag({
  id: 'my_dag',
  schedule: null,                    // required when using timetable
  timetable: (lastRunAt, runCount) => {
    // lastRunAt: Date of most recent run, or null if no runs yet
    // runCount:  number of timetable-triggered runs so far

    // Example: every 30 minutes
    const base = lastRunAt ?? new Date()
    return new Date(base.getTime() + 30 * 60 * 1000)
  },
  tasks: { ... }
})
```

**Common patterns:**

```js
// Every 30 minutes
timetable: (last) => new Date((last ?? new Date()).getTime() + 30 * 60 * 1000)

// Weekdays only at 09:00 UTC
timetable: (last) => {
  const next = new Date(); next.setUTCHours(9, 0, 0, 0);
  if (next <= new Date()) next.setUTCDate(next.getUTCDate() + 1);
  while (next.getUTCDay() === 0 || next.getUTCDay() === 6)
    next.setUTCDate(next.getUTCDate() + 1);
  return next;
}

// Run exactly 5 times then stop
timetable: (last, count) => count >= 5 ? null :
  new Date((last ?? new Date()).getTime() + 60_000)

// Exponential backoff: 1min, 2min, 4min, 8min gaps
timetable: (last, count) => {
  if (count >= 5) return null
  const delay = count === 0 ? 0 : Math.pow(2, count - 1) * 60_000
  return new Date((last ?? new Date()).getTime() + delay)
}
```

**Notes:**
- Granularity floor: ~5s (scheduler poll interval)
- A throwing timetable is logged and treated as `null` — won't crash the scheduler
- Pause/resume works the same as cron-scheduled DAGs
- `trigger_type: 'timetable'` is stored on each run for filtering

See `dags/timetable_demo.js` for working examples (interval, weekdays, business hours, limited runs, exponential backoff).

---

## Task Logging with Severity Levels

`run:` tasks get a structured logger via `ctx.log` with four severity levels:

```js
run: async (ctx) => {
  ctx.log.debug('detailed trace — only visible when filtering debug')
  ctx.log.info('starting extraction')
  ctx.log.info({ records: 42, source: ctx.conf.source })  // objects auto-serialized
  ctx.log.warn('API rate limit at 80% — slowing down')
  ctx.log.error('database connection failed, will retry')
}
```

Lines are stored with a `level` field and prefixed with `[INFO]`, `[WARN]`, etc.:
```
[INFO] starting extraction
[INFO] {"records":42,"source":"warehouse"}
[WARN] API rate limit at 80% — slowing down
[ERROR] database connection failed, will retry
```

**Filter logs by severity:**
```bash
# Only warnings and errors
GET /dag-runs/:runId/tasks/:taskId/logs?level=warn

# Only stderr stream
GET /dag-runs/:runId/tasks/:taskId/logs?stream=stderr

# Combined: errors on stderr only
GET /dag-runs/:runId/tasks/:taskId/logs?level=error&stream=stderr
```

**Severity order:** `debug` < `info` < `warn` < `error`. Filtering `?level=warn` returns `warn` + `error` only.

For `shell:`, `python:`, `java:`, `container:` tasks: stdout lines are stored as `info`, stderr as `error`. Lines that match `[LEVEL]` prefixes are detected automatically.

---

## Shell Task Support

Shell tasks run a command string via a configurable interpreter instead of a JavaScript function.

### Usage

```js
export default dag({
  id: 'my_dag',
  schedule: null,
  tasks: {
    greet: {
      shell: {
        command: 'echo "Hello from $TASK_ID in run $RUN_ID"',
        interpreter: 'bash',   // optional — defaults to 'bash'
        cwd: '/tmp',           // optional working directory
        env: { FOO: 'bar' },   // optional extra env vars
        timeout: 10000,        // optional ms timeout
      }
    }
  }
});
```

### Injected environment variables

Every shell task receives these env vars automatically:

| Variable | Value |
|---|---|
| `DAG_ID` | The DAG id |
| `RUN_ID` | The current run id (MongoDB ObjectId hex) |
| `TASK_ID` | The task id within the DAG |

### Supported interpreters

All four shells are pre-installed in the official `airflow-nodejs:local` Docker image (90 MB, Debian slim base):

| Interpreter | Binary | Version | Notes |
|---|---|---|---|
| `sh` | `/usr/bin/sh` | POSIX sh (dash) | Always available; use for maximum portability |
| `bash` | `/usr/bin/bash` | 5.2.37 | **Default** — used when `interpreter` is omitted |
| `zsh` | `/usr/bin/zsh` | 5.9 | Z shell; supports arrays, globbing extensions |
| `tcsh` | `/usr/bin/tcsh` | 6.24.13 | C shell variant |

Any absolute path also works (e.g. `interpreter: '/usr/bin/python3'`).

### How runtime packages are installed (offline / Walmart network)

`docker build` runs on the **Walmart network where `apt-get` and Alpine `apk` are blocked**.
The workaround: download the required Debian `.deb` files on a public network first,
commit them into local git-ignored folders, then `COPY` + `dpkg -i` them during the build.

#### Folder layout

| Folder | Contents | Used by |
|---|---|---|
| `.docker-debs/` | zsh, tcsh + libs | `Dockerfile` (base) |
| `.docker-debs-python/` | Python 3.13 + libs | `Dockerfile` (`--build-arg VARIANT=python`) |
| `.docker-debs-java/` | OpenJDK JRE + libs | `Dockerfile` (`--build-arg VARIANT=java`) |

All three folders are **git-ignored** — they must exist locally before building.

#### Step-by-step: populate the folders (run on public WiFi)

```bash
# Step 1 — shells only (needed for ALL image variants)
./scripts/download-shell-debs.sh

# Step 2 — also download Python 3.13 (needed for :python and :java variants)
./scripts/download-shell-debs.sh --python

# Step 3 — also download OpenJDK JRE (needed for :java variant)
./scripts/download-shell-debs.sh --java              # JDK 21 LTS (default)
./scripts/download-shell-debs.sh --java --jdk 25     # JDK 25 (latest)

# Or download everything in one shot:
./scripts/download-shell-debs.sh --python --java             # JDK 21
./scripts/download-shell-debs.sh --python --java --jdk 25    # JDK 25

# Force a specific CPU architecture (default: auto-detect from host):
./scripts/download-shell-debs.sh --python --java arm64   # ARM (Apple Silicon, Graviton)
./scripts/download-shell-debs.sh --python --java amd64   # x86 servers
```

> **JDK version compatibility:** JDK 25 is fully backwards-compatible — the `:java25` image
> can run JARs compiled for any earlier Java version (8, 11, 17, 21, 25).
> Use `:java21` for LTS stability; use `:java25` if you need the latest features or want a
> single image that covers all Java versions. Both images have identical dependency closures —
> only the JRE binary itself differs.

#### What `download-shell-debs.sh` does internally

1. Fetches the Debian trixie package index from `snapshot.debian.org` (pinned snapshot date).
2. Looks up the `.deb` filename for each required package by exact name.
3. Downloads each `.deb` file into the appropriate local folder.
4. Prints a summary of downloaded files and sizes.

The pinned snapshot URL (`20260505T000000Z`) ensures reproducible builds — the same `.deb`
versions are always downloaded regardless of when the script runs.

#### Step-by-step: build an image after populating the folders

```bash
# Base image — shells only (sh, bash, zsh, tcsh)
./docker-build.sh                          # arm64 (auto-detected)
./docker-build.sh --platform linux/amd64   # x86

# Python variant — adds python3.13
./docker-build.sh --variant python
./docker-build.sh --variant python --platform linux/amd64

# Java variant — adds python3.13 + OpenJDK JRE
./docker-build.sh --variant java                           # JDK 21 LTS → airflow-nodejs:java21
./docker-build.sh --variant java --jdk 25                  # JDK 25     → airflow-nodejs:java25
./docker-build.sh --variant java --platform linux/amd64    # x86 + JDK 21
./docker-build.sh --variant java --jdk 25 --platform linux/amd64  # x86 + JDK 25
```

#### How to add a new package (e.g. `curl`, `git`, `ruby`)

1. Find the package name in the Debian trixie index:
   ```bash
   BASE="http://snapshot.debian.org/archive/debian/20260505T000000Z"
   curl -sL "$BASE/dists/trixie/main/binary-arm64/Packages.gz" | gunzip | grep "^Package: curl$" -A10
   ```

2. Add the package name to the appropriate `for pkg in ...` loop in `scripts/download-shell-debs.sh`.

3. Add the corresponding `COPY` + `dpkg -i` line in the relevant `Dockerfile*`.

4. Run the download script on public WiFi, then rebuild the image.

#### Re-running after a base image update

If `oven/bun:1.3-slim` is updated to a newer Debian version, some packages may already be
present in the new base (or their versions may change). Re-run the download script with
the new arch and rebuild to pick up compatible `.deb` versions.

### Exit codes and retries

- Exit `0` → task marked **success**
- Non-zero exit → task marked **failed** (last 5 lines of stderr included in error message)
- Retries and timeouts work the same as JS tasks — set `retries` and `timeout` on the task definition
- `ENOENT` (interpreter not found) surfaces as: `Shell interpreter 'zsh' not found — install it or use 'sh'`

### stdout / stderr

Both streams are captured line-by-line and written to task logs, visible in the UI **Log** panel.
