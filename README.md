# airflow-nodejs

A production-grade reimplementation of Apache Airflow's core concepts in **Node.js + Fastify + MongoDB**, built to be lightweight, self-contained, and deployable as a single Docker image.

618 tests · 16 API route modules · multi-user RBAC · Docker-ready

---

## Features

| Category | What's included |
|----------|----------------|
| **Scheduling** | Cron schedules, manual triggers, backfill with lifecycle (pause/resume/cancel), data-aware scheduling (datasets) |
| **Task execution** | Local child-process fork, BullMQ distributed mode, retries, timeouts, sensors, dynamic task mapping, task groups, resource pools |
| **Observability** | SLA alerts, run statistics + duration histogram, event/audit log, task try history, dag warnings, import errors |
| **Data flow** | XCom push/pull (full CRUD), connections store (encrypted), variables store (encrypted), per-run conf/tags/notes |
| **Lifecycle** | Webhook callbacks on run completion, Human-in-the-Loop (HITL) approval gates, task clear-to-retry |
| **Operations** | Graceful shutdown with worker drain, config API, providers API, plugins API |
| **Auth** | Multi-user RBAC (viewer / editor / admin roles), scrypt-hashed DB-backed API keys, env-key fallback |
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

---

## Writing a Dag

Create a `.ts` (or `.js`) file in `dags/`:

```typescript
import { dag } from '../src/dag/types.js'

export default dag({
  id: 'my_pipeline',
  schedule: '0 9 * * *',   // cron, or null for manual-only
  onSuccess: 'https://hooks.example.com/success',  // webhook callback
  tasks: {
    extract: {
      pool: 'db_pool',      // limit concurrency via resource pool
      retries: 2,
      retryDelay: 5000,
      run: async (ctx) => {
        const conn = await ctx.connections.get('my_db')
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
GET  /dag-runs/:runId/tasks/:taskId/logs       stdout/stderr log lines
POST /dag-runs/:runId/tasks/:taskId/clear      reset to queued + re-run
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
```

---

## Architecture

```
Scheduler loop (5s poll)
  ├── Dag loader         scans dags/ every tick, registers in memory
  │     └── analyzeWarnings()   soft validation (missing run logic, cycles, etc.)
  ├── Cron scheduler     node-cron fires runs on schedule
  ├── Claim              atomic findOneAndUpdate (safe for concurrent schedulers)
  │     └── Gates: dependency, sensor poke_at, HITL approval, backfill pause
  └── Executor
        ├── local mode   fork child_process per task
        │     ├── Resource pool semaphore (per-pool slot limit)
        │     └── EXEC_PATH auto-detects dev (tsx) vs prod (node)
        └── BullMQ mode  enqueue to Redis → worker picks up

Worker process (isolated)
  ├── Injects: ctx.conf, ctx.connections, ctx.variables, ctx.xcom
  ├── Reads all state from MongoDB directly (never over IPC)
  └── Reports outcome via IPC message → executor records try history

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
  api_keys             scrypt-hashed tokens + roles
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URL` | `mongodb://localhost:27017` | MongoDB connection URL |
| `DB_NAME` | `airflow` | MongoDB database name |
| `ENCRYPTION_KEY` | _(required for secrets)_ | 64 hex chars — AES-256-GCM key for connections/variables |
| `API_KEYS` | _(unset)_ | Comma-separated static API keys (all have admin role) |
| `ADMIN_KEY` | _(unset)_ | Bootstrap admin key for DB-backed key management |
| `REDIS_URL` | _(unset)_ | Redis URL — enables BullMQ distributed mode |
| `WORKER_CONCURRENCY` | `4` | Concurrent tasks per BullMQ worker |
| `MAX_WORKERS` | `8` | Max concurrent tasks in local fork mode |
| `DRAIN_TIMEOUT_MS` | `20000` | Graceful shutdown worker drain timeout |
| `PORT` | `3000` | API + UI port |
| `HOST` | `0.0.0.0` | API bind address |
| `RATE_LIMIT_MAX` | `120` | Requests per minute per IP (global) |

---

## Scripts

```bash
npm run dev          # local mode with hot reload
npm run dev:bullmq   # BullMQ distributed mode with hot reload
npm run build        # compile TypeScript → dist/
npm start            # production: node dist/main.js
npm run worker       # standalone BullMQ worker: node dist/queue/consumer.js
npm test             # run all 618 tests (requires MongoDB at localhost:27017)
```

---

## Docker

### Build the image (56 MB)

```bash
npm run build        # compile TypeScript first
./docker-build.sh    # or: docker build -t airflow-nodejs .
```

### Run with local MongoDB

```bash
docker-compose up -d mongo

docker run -p 3000:3000 \
  -e MONGO_URL=mongodb://host.docker.internal:27017 \
  -e ENCRYPTION_KEY=$(openssl rand -hex 32) \
  -e ADMIN_KEY=my-bootstrap-key \
  -v $(pwd)/dags:/app/dags \
  airflow-nodejs:local
```

### Full stack (MongoDB + app)

Add the app to `docker-compose.yml`:

```yaml
services:
  app:
    image: airflow-nodejs:local
    ports: ["3000:3000"]
    environment:
      MONGO_URL: mongodb://mongo:27017
      ENCRYPTION_KEY: ${ENCRYPTION_KEY}
      ADMIN_KEY: ${ADMIN_KEY}
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

1. Stop scheduler (no new ticks)
2. Close HTTP server (drain in-flight requests)
3. Drain worker pool (wait for active forks + queue to reach 0)
4. Close BullMQ queue and MongoDB connection
5. Exit 0

Timeout: 20s (`DRAIN_TIMEOUT_MS`). Second signal forces exit 1.
