# airflow-nodejs

A production-grade reimplementation of Apache Airflow's core concepts in **Node.js + Fastify + MongoDB**, built to be lightweight, self-contained, and deployable as a single Docker image.

870 tests · 16 API route modules · multi-user RBAC · Docker-ready

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

1. Stop scheduler (no new ticks)
2. Close HTTP server (drain in-flight requests)
3. Drain worker pool (wait for active forks + queue to reach 0)
4. Close BullMQ queue and MongoDB connection
5. Exit 0

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
| **Grid view** | Per-task color grid across historical runs (run × task heatmap) | ❌ not implemented |
| **Task instance actions** | Re-run, clear, mark success/failed per task | ❌ click shows logs only |
| **Run clear / re-run** | Clear whole run or subset of tasks | ❌ cancel only |
| **Last run badge on DAG card** | Colored last-run state badge directly on each DAG card | ❌ shown only inside runs list |
| **DAG tags display** | Tags shown on DAG card, filterable | ❌ stored in DB, not shown in UI |

### 🟡 Medium value

| Feature | Airflow 3.x | Status |
|---|---|---|
| **Event log page** | Searchable audit log of all Airflow events | ❌ API exists (`/event-logs`), no UI page |
| **Dataset/Asset lineage view** | Visual dependency graph across datasets | ❌ API exists (`/datasets`), no UI |
| **DAG code viewer** | Show raw DAG source file in UI | ❌ not exposed |
| **Calendar view** | Run frequency heatmap calendar per DAG | ❌ not implemented |
| **Gantt chart** | Task duration Gantt chart per run | ❌ not implemented |

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
- Source fails → mapped task skipped via cascade; run terminates cleanly

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

Tasks whose rule can **never** be satisfied are automatically marked `skipped` so the run always reaches a terminal state.

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
