# airflow-nodejs

A lightweight reimplementation of Apache Airflow's core concepts in Node.js + Fastify + MongoDB.

> **Status:** Work in progress — Phase 1 complete (DB + indexes).

---

## Prerequisites

| Tool | Version | Install |
|------|---------|---------|
| Node.js | v22+ | [nodejs.org](https://nodejs.org) or `nvm install 22` |
| npm | v10+ | Comes with Node.js |
| Docker Desktop | v24+ | [docker.com](https://www.docker.com/products/docker-desktop/) |

> **Note:** MongoDB runs inside Docker — no local MongoDB install needed.

---

## Quick Start

### 1. Clone and install dependencies

```bash
git clone <repo-url>
cd airflow-nodejs
npm install
```

### 2. Start MongoDB

```bash
docker-compose up -d
```

MongoDB will be available at `mongodb://localhost:27017/airflow`.

### 3. Verify Phase 1 (DB connection + indexes)

```bash
npx tsx src/db/verify.ts
```

Expected output:
```
✅ MongoDB connected to: airflow
✅ Indexes created
Collections: [ 'task_instances', 'dag_runs' ]
✅ Phase 1 complete
```

---

## Project Structure

```
airflow-nodejs/
├── docker-compose.yml       # MongoDB 7 container
├── package.json
├── tsconfig.json
└── src/
    └── db/
        ├── client.ts        # MongoDB connect/close/getDb helpers
        ├── indexes.ts       # Collection index definitions
        └── verify.ts        # Phase 1 verification script
```

---

## Architecture

This project reimplements Airflow's core scheduling loop using:

- **MongoDB** — metadata store (dags, dag_runs, task_instances)
- **Fastify** — REST API (trigger runs, inspect state)
- **node:child_process** — local task executor (one process per task)
- **Atomic claim** — `findOneAndUpdate` replaces PostgreSQL's `FOR UPDATE SKIP LOCKED`

### Collections

- `dags` — registered Dag definitions (upserted on load)
- `dag_runs` — each execution of a Dag (`queued → running → success/failed`)
- `task_instances` — each task within a run, with dependency tracking

---

## Build Phases

- [x] **Phase 1** — Docker Compose + MongoDB client + indexes
- [ ] **Phase 2** — Dag loader + in-memory registry
- [ ] **Phase 3** — Scheduler loop + dag_run/task_instance creation
- [ ] **Phase 4** — Atomic task claim + child_process executor
- [ ] **Phase 5** — Fastify API (trigger, inspect runs/tasks)

---

## Stopping MongoDB

```bash
docker-compose down
```

To also delete all data:

```bash
docker-compose down -v
```
