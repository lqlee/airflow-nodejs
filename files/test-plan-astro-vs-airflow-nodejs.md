# Test Plan — Astronomer Airflow (AFaaS) vs airflow-nodejs (WCNP)

Side-by-side validation of the two live deployments. Each case gives the Apache
Airflow (Astronomer) action, the airflow-nodejs equivalent, and the expected
outcome for both.

## Environments

| | Apache Airflow 2.x (Astronomer AFaaS) | airflow-nodejs (WCNP) |
|---|---|---|
| **Base URL** | `https://deployments.astro-nonprod1.us-west1.us.walmart.net/asteroidic-wave-5407/airflow` | `https://airflow-nodejs.dev.walmart.com` |
| **Release name** | `asteroidic-wave-5407` | n/a |
| **UI** | `<base>/home` | `<base>/` |
| **REST API** | `<base>/api/v1` (Airflow 2.x — **not** v2) | `<base>` (routes at root) |
| **Auth** | Deployment Service Account API Key, raw in `Authorization:` header (**no** `Bearer` prefix) | None — currently open access ⚠️ |
| **Metadata DB** | Astro-managed Postgres | MongoDB sidecar (ephemeral) ⚠️ |
| **Executor** | Celery / Kubernetes | Local fork (BullMQ when `REDIS_URL` set) |

> ⚠️ **Two caveats before testing.** airflow-nodejs currently runs with auth
> disabled and an ephemeral MongoDB sidecar — data is lost on every pod restart.
> Treat all airflow-nodejs results as non-durable.

## Setup

```bash
# ── Astronomer ────────────────────────────────────────────────────────────────
export ASTRO_BASE="https://deployments.astro-nonprod1.us-west1.us.walmart.net/asteroidic-wave-5407/airflow"
# Deployment Service Account API Key — generate at:
#   https://app.astro-nonprod1.us-west1.us.walmart.net/
#   → deployment asteroidic-wave-5407 → Service Accounts → New Deployment Service Account
# NOTE: shown once only; copy immediately. Header takes the raw key (NOT "Bearer").
export ASTRO_TOKEN="<deployment-service-account-api-key>"
alias acurl='curl -s -H "Authorization: $ASTRO_TOKEN" -H "Cache-Control: no-cache" -H "Content-Type: application/json"'

# ── airflow-nodejs ────────────────────────────────────────────────────────────
export ANJS_BASE="https://airflow-nodejs.dev.walmart.com"
alias ncurl='curl -s -H "Content-Type: application/json"'

# Smoke check — both should respond
acurl "$ASTRO_BASE/api/v1/version"
ncurl "$ANJS_BASE/health"
```

---

## 1. Variables

Entry point for this plan — `<base>/variable/list/`.

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 1.1 | `GET /api/v1/variables` | `GET /variables` | Both return a list; shape differs (`variables[]` vs `items[]`) |
| 1.2 | `POST /api/v1/variables` with `{key, value}` | `POST /variables` with `{key, value}` | 201/200; variable readable on next GET |
| 1.3 | Read in DAG: `Variable.get("k")` | Read in task: `ctx.variables.get('k')` | Same value returned at runtime |
| 1.4 | Update existing key | `PUT /variables/<key>` | Value replaced, not duplicated |
| 1.5 | Delete key | `DELETE /variables/<key>` | 204; subsequent GET is 404 |
| 1.6 | Secret masking (`_secret` suffix) | Masked in API response | Value shown as `***` in UI/API |

```bash
# 1.1 — list
acurl "$ASTRO_BASE/api/v1/variables" | jq '.variables[].key'
ncurl "$ANJS_BASE/variables" | jq '.'

# 1.2 — create
acurl -X POST "$ASTRO_BASE/api/v1/variables" \
  -d '{"key":"test_env","value":"dev","description":"parity test"}'
ncurl -X POST "$ANJS_BASE/variables" \
  -d '{"key":"test_env","value":"dev"}'

# 1.5 — delete
acurl -X DELETE "$ASTRO_BASE/api/v1/variables/test_env"
ncurl -X DELETE "$ANJS_BASE/variables/test_env"
```

---

## 2. Connections

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 2.1 | `GET /api/v1/connections` | `GET /connections` | Both list connections; passwords masked |
| 2.2 | Create conn with `conn_type=http` | `POST /connections` same shape | Created and retrievable |
| 2.3 | Use in task: `BaseHook.get_connection()` | `ctx.connections.get('id')` | Same host/login resolved |
| 2.4 | Env-var fallback `AIRFLOW_CONN_*` | `SECRETS_BACKEND=env` + `AIRFLOW_CONN_*` | Resolves when absent from DB |
| 2.5 | Delete connection | `DELETE /connections/<id>` | 204; GET returns 404 |

```bash
acurl "$ASTRO_BASE/api/v1/connections" | jq '.connections[].connection_id'
ncurl "$ANJS_BASE/connections" | jq '.'
```

---

## 3. DAG Discovery & Metadata

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 3.1 | `GET /api/v1/dags` | `GET /dags` | Both list all loaded DAGs |
| 3.2 | `GET /api/v1/dags/<id>` | `GET /dags/<id>` | Detail incl. schedule, tasks |
| 3.3 | Pause: `PATCH {is_paused: true}` | `POST /dags/<id>/pause` | No new scheduled runs created |
| 3.4 | Unpause | `POST /dags/<id>/unpause` | Scheduling resumes |
| 3.5 | `GET /api/v1/importErrors` | `GET /import-errors` | Broken DAG files listed with error text |
| 3.6 | DAG source view | `GET /dags/<id>/source` | Returns file contents |

```bash
acurl "$ASTRO_BASE/api/v1/dags" | jq '.dags[] | {dag_id, is_paused, schedule_interval}'
ncurl "$ANJS_BASE/dags" | jq '.items[] | {dag_id, schedule, paused}'

# 3.5 — import errors on both
acurl "$ASTRO_BASE/api/v1/importErrors" | jq '.import_errors'
ncurl "$ANJS_BASE/import-errors" | jq '.'
```

---

## 4. Triggering & Run Lifecycle

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 4.1 | `POST /dags/<id>/dagRuns` with `{conf}` | `POST /dags/<id>/trigger` with `{conf}` | Run created in `queued`/`running` |
| 4.2 | Poll run state | `GET /dag-runs/<runId>` | Reaches `success` |
| 4.3 | Read `conf` in task | `ctx.conf.<key>` | Same values available at runtime |
| 4.4 | List runs for a DAG | `GET /dags/<id>/runs` | Newest-first, paginated |
| 4.5 | Clear/retry a failed run | `POST /dag-runs/<runId>/clear` | Tasks re-queued |
| 4.6 | Mark run failed manually | `POST /dag-runs/<runId>/fail` | State transitions to `failed` |

```bash
# 4.1 — trigger with conf
acurl -X POST "$ASTRO_BASE/api/v1/dags/<dag_id>/dagRuns" \
  -d '{"logical_date":"2026-08-16T00:00:00Z","conf":{"rows":100}}'

RUN=$(ncurl -X POST "$ANJS_BASE/dags/hello_world/trigger" \
  -d '{"conf":{"rows":100}}' | jq -r '.dag_run_id')

# 4.2 — poll
ncurl "$ANJS_BASE/dag-runs/$RUN" | jq '.state'
```

---

## 5. Task Instances & Logs

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 5.1 | List TIs for a run | `GET /dag-runs/<runId>/tasks` | All tasks with states |
| 5.2 | Fetch task log | `GET /dag-runs/<runId>/tasks/<taskId>/logs` | Full stdout/stderr |
| 5.3 | Log severity filter | `?level=error` | Only ERROR lines returned |
| 5.4 | Stream filter | `?stream=stderr` | Only stderr lines |
| 5.5 | Retry a single task | `POST .../tasks/<taskId>/clear` | Task re-runs, `try_number` increments |
| 5.6 | Task duration recorded | `start_date`/`end_date` present | Non-null on completion |

```bash
ncurl "$ANJS_BASE/dag-runs/$RUN/tasks" | jq '.[] | {task_id, state, try_number}'
ncurl "$ANJS_BASE/dag-runs/$RUN/tasks/process/logs?level=error" | jq '.'
```

> **Note:** log severity + stream filtering (5.3/5.4) is an airflow-nodejs
> extension. Apache Airflow has no equivalent query param — its logs are a flat
> text stream.

---

## 6. XCom

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 6.1 | `ti.xcom_push(key, value)` | `ctx.xcom.push(key, value)` | Value stored against the task instance |
| 6.2 | `ti.xcom_pull(task_ids=...)` | `ctx.xcom.pull(taskId, key)` | Upstream value retrieved |
| 6.3 | Return value auto-push | Task return value auto-pushed | Available as `return_value` |
| 6.4 | `GET /.../xcomEntries` | `GET /dag-runs/<runId>/xcom` | Entries listed |

---

## 7. Scheduling Behaviour

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 7.1 | Cron `0 * * * *` | `schedule: '0 * * * *'` | Run created at top of hour |
| 7.2 | `schedule=None` | `schedule: null` | Manual trigger only |
| 7.3 | `catchup=False` | `catchup: false` | No backfill on unpause |
| 7.4 | `max_active_runs=1` | `maxActiveRuns: 1` | Second run queued, not parallel |
| 7.5 | Missed-interval recovery | Scheduler restart mid-window | Run appears after restart, not duplicated |

> ⚠️ **7.5 is the highest-risk case for airflow-nodejs on WCNP** — the MongoDB
> sidecar is ephemeral, so a pod restart wipes scheduler state entirely rather
> than recovering it. Expect this to fail until Cosmos DB is provisioned.

---

## 8. Trigger Rules & Branching

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 8.1 | `trigger_rule='all_success'` (default) | default | Runs only if all upstream succeed |
| 8.2 | `trigger_rule='all_done'` | `triggerRule: 'all_done'` | Runs regardless of upstream outcome |
| 8.3 | `trigger_rule='one_failed'` | `triggerRule: 'one_failed'` | Runs when any upstream fails |
| 8.4 | `BranchPythonOperator` | `branch: (ctx) => 'task_id'` | Only chosen branch runs; others skipped |
| 8.5 | Skipped-state propagation | downstream of skipped | Marked `skipped`, not `failed` |

---

## 9. Retries & Failure Handling

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 9.1 | `retries=2, retry_delay=30s` | `retries: 2, retryDelay: 30` | Two retries, 30s apart |
| 9.2 | Exponential backoff | `retryExponentialBackoff: true` | Delay doubles each attempt |
| 9.3 | `execution_timeout` | `timeout: <ms>` | Task killed, marked failed |
| 9.4 | `on_failure_callback` | failure webhook fires | Notification delivered |
| 9.5 | Zombie/orphan recovery | Kill pod mid-run | Run recovered or failed cleanly, not stuck `running` |

---

## 10. Concurrency & Pools

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 10.1 | Pool with 2 slots | `pool: {name, slots: 2}` | Third task waits |
| 10.2 | `priority_weight` | `priority: <n>` | Higher priority dequeued first |
| 10.3 | `max_active_tasks` | `maxActiveTasks` | Parallelism capped |

---

## 11. Health, Version & Observability

| # | Astronomer Airflow | airflow-nodejs | Expected |
|---|---|---|---|
| 11.1 | `GET /api/v1/health` | `GET /health` | `{"status":"ok"}`; scheduler heartbeat fresh |
| 11.2 | `GET /api/v1/version` | `GET /version` | Version string returned |
| 11.3 | `GET /api/v1/config` | `GET /config` | Effective config (secrets masked) |
| 11.4 | Metrics endpoint | `GET /metrics` | Prometheus-format metrics |

```bash
acurl "$ASTRO_BASE/api/v1/health" | jq '.'
ncurl "$ANJS_BASE/health" | jq '.'
```

---

## 12. Known Gaps — airflow-nodejs vs Apache Airflow

Cases expected to FAIL on airflow-nodejs. Record actual behaviour; do not treat
as regressions.

| Area | Apache Airflow | airflow-nodejs | Impact |
|---|---|---|---|
| Persistence | Durable Postgres | Ephemeral Mongo sidecar | **All history lost on restart** |
| Auth / RBAC | Full RBAC, SSO | Static API keys, currently disabled | **Open access on dev URL** |
| Horizontal scale | Multi-scheduler HA | Pinned `min: 1, max: 1` | Cannot scale; replicas would split-brain |
| Timetables | Custom timetable classes | Cron only | No data-interval-aware schedules |
| Datasets | Dataset-driven scheduling | Not implemented | No cross-DAG data triggers |
| Backfill | `airflow dags backfill` | Not implemented | No historical replay |
| SLA misses | SLA callbacks | Not implemented | No SLA alerting |
| Plugins | Full plugin system | Provider modules only | Limited extensibility |

See `test-plan-vs-apache-airflow.md` for the complete feature comparison
(~85% parity, 935 tests).

---

## 13. Results

| Section | Astro Pass | ANJS Pass | Notes |
|---|---|---|---|
| 1. Variables | / | / | |
| 2. Connections | / | / | |
| 3. DAG Discovery | / | / | |
| 4. Triggering | / | / | |
| 5. Task Instances & Logs | / | / | |
| 6. XCom | / | / | |
| 7. Scheduling | / | / | |
| 8. Trigger Rules | / | / | |
| 9. Retries | / | / | |
| 10. Concurrency | / | / | |
| 11. Health & Observability | / | / | |

**Tester:** ______________  **Date:** ______________

**Blocking issues found:**

1.
2.
3.
