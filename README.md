# airflow-nodejs

A lightweight reimplementation of Apache Airflow's core concepts in Node.js + Fastify + MongoDB.

---

## Prerequisites

- **Node.js** v22+ — [nodejs.org](https://nodejs.org) or `nvm install 22`
- **npm** v10+ — comes with Node.js
- **Docker Desktop** — [docker.com](https://www.docker.com/products/docker-desktop/)

MongoDB and Redis run inside Docker — no local installs needed.

---

## Quick Start

```bash
git clone <repo-url>
cd airflow-nodejs
npm install
docker-compose up -d     # starts MongoDB + Redis
npm run dev              # starts scheduler + API + local task executor
```

Open **http://localhost:3000** for the web UI.

---

## Running Modes

### Local mode (default — no Redis needed)

Tasks execute in child processes on the same machine.

```bash
npm run dev
```

### Distributed mode (BullMQ via Redis)

Tasks are queued in Redis and picked up by worker processes — can run on separate machines.

```bash
npm run dev:bullmq
```

### Standalone worker (separate machine)

Run additional workers anywhere with access to Redis + MongoDB:

```bash
REDIS_URL=redis://<host>:6379 \
MONGO_URL=mongodb://<host>:27017 \
npm run worker
```

Scale concurrency per worker (default: 4):

```bash
WORKER_CONCURRENCY=8 REDIS_URL=redis://localhost:6379 npm run worker
```

---

## Authentication

Auth is **disabled by default** (open access). Enable it by setting `API_KEYS`:

```bash
API_KEYS=your-secret-key npm run dev
# Multiple keys (e.g. per team/service):
API_KEYS=key1,key2,key3 npm run dev
```

Protected endpoints require `Authorization: Bearer <key>`:

```bash
curl -H "Authorization: Bearer your-secret-key" http://localhost:3000/dags
```

Public endpoints (no auth required): `GET /health`, `GET /` (UI).

The web UI shows a login screen when auth is enabled. The key is stored in `localStorage` and sent automatically with every request. Click ⏻ to sign out.

---

## API

All endpoints except `/health` and `/` require auth when `API_KEYS` is set.

```
GET  /health                              server status + auth flag + worker pool stats
GET  /dags                                list all loaded Dags
GET  /dags/:dagId                         Dag detail + task graph
POST /dags/:dagId/trigger                 manually trigger a run
GET  /dags/:dagId/runs                    recent runs for a Dag
GET  /dag-runs/:runId                     run state + all task states
GET  /dag-runs/:runId/tasks/:taskId/logs  task stdout/stderr log lines
```

---

## Writing a Dag

Create a `.ts` file in `dags/`:

```typescript
import { dag } from '../src/dag/types.js'

export default dag({
  id: 'my_pipeline',
  schedule: '0 * * * *',  // cron — or null for manual-only
  tasks: {
    extract: {
      retries: 2,
      retryDelay: 5000,   // ms between retries
      run: async (ctx) => {
        const result = { rows: 42 }
        await ctx.xcom.push('result', result)
      }
    },
    transform: {
      dependsOn: ['extract'],
      run: async (ctx) => {
        const data = await ctx.xcom.pull('extract', 'result')
        // data === { rows: 42 }
      }
    },
    load: {
      dependsOn: ['transform'],
      run: async (ctx) => {
        console.log('loading...')
      }
    }
  }
})
```

Dags are hot-reloaded every 5 seconds — no restart needed.

---

## Architecture

```
Scheduler (poll loop)
  ├── Dag loader          scans dags/ every 5s, registers in memory
  ├── Cron scheduler      fires runs on schedule via node-cron
  ├── Claim               findOneAndUpdate (atomic, like FOR UPDATE SKIP LOCKED)
  └── Executor
        ├── local mode    fork child_process per task
        └── BullMQ mode   enqueue to Redis → worker picks up

Worker (local or remote)
  ├── Runs task function in isolated child process
  ├── XCom               push/pull via MongoDB (direct connection)
  └── Logs               stdout/stderr captured → task_logs collection

MongoDB collections:
  dag_runs          each Dag execution (queued → running → success/failed)
  task_instances    each task within a run + dependency tracking
  xcoms             cross-task data (push/pull by key)
  task_logs         stdout/stderr lines per task
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `MONGO_URL` | `mongodb://localhost:27017` | MongoDB connection URL |
| `DB_NAME` | `airflow` | MongoDB database name |
| `REDIS_URL` | _(unset)_ | Redis URL — enables BullMQ distributed mode |
| `WORKER_CONCURRENCY` | `4` | Concurrent tasks per BullMQ worker |
| `MAX_WORKERS` | `8` | Max concurrent tasks in local fork mode |
| `PORT` | `3000` | API + UI port |

---

## Scripts

```bash
npm run dev          # local mode with file watching
npm run dev:bullmq   # BullMQ mode with file watching
npm run worker       # standalone BullMQ worker
npm test             # run all tests
npm start            # production start (no watch)
```

---

## Docker

Start all services:
```bash
docker-compose up -d
```

Stop without deleting data:
```bash
docker-compose down
```

Stop and delete all data:
```bash
docker-compose down -v
```

Check Redis:
```bash
docker exec airflow-nodejs-redis-1 redis-cli ping
# PONG
```

Run test with Mongod:
```bash
mongod --dbpath /tmp/mongodb-test --logpath /tmp/mongod.log &
npm test
```
