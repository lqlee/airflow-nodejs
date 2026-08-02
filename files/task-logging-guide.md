# Task Logging & Debugging Guide

*airflow-nodejs vs Apache Airflow 3.x comparison + improvement opportunities*

---

## 1. How Task Logging Works in airflow-nodejs

### Architecture

```
Task execution (any type)
        │
        ├── stdout lines ──→ appendLog(db, runId, dagId, taskId, 'stdout', line)
        └── stderr lines ──→ appendLog(db, runId, dagId, taskId, 'stderr', line)
                                        │
                                        ▼
                              MongoDB: task_logs collection
                              { dag_run_id, dag_id, task_id, ts, stream, line }
                                        │
                                        ▼
                              GET /dag-runs/:runId/tasks/:taskId/logs
                                        │
                                        ▼
                                  UI: LogPanel (polls every 2s)
```

### What Gets Captured

| Task type | Captured streams | Notes |
|---|---|---|
| `run:` (JS/TS) | stdout + stderr from worker fork | `console.log()` → stdout, `console.error()` → stderr |
| `shell:` | stdout + stderr from interpreter process | All output from bash/sh/zsh/tcsh command |
| `python:` | stdout + stderr from python3 process | `print()` → stdout |
| `java:` | stdout + stderr from java process | `System.out` → stdout, `System.err` → stderr |
| `container:` | stdout + stderr from `docker run` | Container output piped line-by-line |
| `kubernetes:` | stdout + stderr from `kubectl run --attach` | Pod output piped line-by-line |
| `branch:` | stdout + stderr from worker fork | Same as `run:` |
| `poke:` (sensor) | ❌ Not captured | Poke fn runs inline in scheduler; no log capture |
| Deferrable trigger fn | ❌ Not captured | Runs in scheduler process; no DB write |

### Storage Schema

```typescript
interface LogLine {
  dag_run_id: string      // which run
  dag_id:     string      // which DAG
  task_id:    string      // which task
  ts:         Date        // timestamp (UTC)
  stream:     'stdout' | 'stderr'
  line:       string      // one line of output
}
// Collection: task_logs, indexed on (dag_run_id, task_id, ts)
```

### Retrieve Logs

```bash
# All lines for a task (sorted by ts ascending)
GET /dag-runs/:runId/tasks/:taskId/logs

# Response:
[
  { "ts": "2024-01-15T06:03:01.123Z", "stream": "stdout", "line": "[extract] starting..." },
  { "ts": "2024-01-15T06:03:01.456Z", "stream": "stderr", "line": "Warning: deprecated API" }
]
```

---

## 2. How to Write Useful Log Messages

### `run:` JS/TS tasks

```js
tasks: {
  extract: {
    run: async (ctx) => {
      // Structured log helper (inline — no imports needed in worker)
      const log = (level, msg) =>
        console.log(JSON.stringify({ level, ts: new Date().toISOString(), msg,
          dag: ctx.dagId, run: ctx.runId.slice(-8), task: ctx.taskId }))

      log('info', `starting — conf=${JSON.stringify(ctx.conf)}`)

      try {
        const records = await fetchData(ctx.conf.source)
        log('info', `fetched ${records.length} records`)

        await ctx.xcom.push('count', records.length)
        log('info', 'pushed xcom count')

        return { count: records.length, status: 'ok' }
      } catch (err) {
        log('error', `failed: ${err.message}`)
        throw err   // re-throw — task marks failed with error message
      }
    }
  }
}
```

### `shell:` tasks

```js
tasks: {
  process: {
    shell: {
      interpreter: 'sh',
      command: [
        'set -e',   // exit on first error (best practice)
        'echo "[process] starting — dag=$DAG_ID run=$RUN_ID task=$TASK_ID"',
        'echo "[process] input: $(ls /data/input/ | wc -l) files"',
        '',
        '# Main work',
        'your_command --input /data/input/ --output /data/output/ 2>&1',
        '',
        '# Check result',
        'COUNT=$(ls /data/output/ | wc -l)',
        'echo "[process] done — produced $COUNT output files"',
      ].join('\n'),
    }
  }
}
```

### `python:` tasks

```js
tasks: {
  analyze: {
    python: {
      code: [
        'import os, json, datetime',
        '',
        'def log(level, msg, **kw):',
        '    entry = dict(level=level, ts=datetime.datetime.utcnow().isoformat(),',
        '                 dag=os.environ["DAG_ID"], run=os.environ["RUN_ID"][-8:],',
        '                 task=os.environ["TASK_ID"], msg=msg, **kw)',
        '    print(json.dumps(entry))',
        '',
        'log("info", "starting")',
        'data = [1, 2, 3]',
        'log("info", "processing", count=len(data))',
        'print(json.dumps({"result": sum(data)}))',
        'log("info", "done")',
      ].join('\n'),
    }
  }
}
```

---

## 3. Context Variables Available for Logging

### In `run:` / `branch:` / `poke:` functions

```js
ctx.dagId      // 'my_pipeline'
ctx.runId      // '6a6c...' (MongoDB ObjectId hex, 24 chars)
ctx.taskId     // 'extract'
ctx.mapIndex   // 0/1/2 for mapped tasks, null otherwise
ctx.mapValue   // the mapped input value, null otherwise
ctx.conf       // { env: 'prod', batch_size: 100, ... }
```

### In `shell:` / `python:` / `java:` / `container:` / `kubernetes:` tasks

```bash
$DAG_ID    # 'my_pipeline'
$RUN_ID    # '6a6c...'
$TASK_ID   # 'extract'
```

### Template variables (in command strings)

```js
shell: {
  command: 'echo "dag={{ dag_id }} date={{ ds }} env={{ conf.env }}"'
}
// Rendered before execution: "dag=my_pipeline date=2024-01-15 env=prod"
```

---

## 4. Error Information

When a task fails, the error is stored on the task instance (`GET /dag-runs/:runId/tasks/:taskId`):

| Task type | Error message format |
|---|---|
| `run:` | Thrown exception message: `"Cannot read properties of undefined"` |
| `shell:` | `Shell exited with code N: <last 5 stderr lines>` |
| `python:` | `Python exited with code N: <last 5 stderr lines>` |
| `java:` | `Java exited with code N: <last 5 stderr lines>` |
| `container:` | `Container exited with code N: <last 5 stderr lines>` |
| `kubernetes:` | `Kubernetes executor: pod exited with code N: <last 5 stderr lines>` |
| Binary not found | `kubectl binary 'kubectl' not found — is it installed?` |
| Timeout | `Task timed out after 30000ms` |
| Sensor timeout | `Sensor timed out after 3600000ms` |
| Deferred timeout | `Deferred task timed out after 60000ms` |

---

## 5. Retry History

Each retry attempt is recorded separately:

```bash
GET /dag-runs/:runId/tasks/:taskId/tries

# Response:
[
  { "try_number": 0, "state": "failed", "started_at": "...", "ended_at": "...", "error": "connection refused" },
  { "try_number": 1, "state": "failed", "started_at": "...", "ended_at": "...", "error": "connection refused" },
  { "try_number": 2, "state": "success", "started_at": "...", "ended_at": "..." }
]
```

---

## 6. Comparison with Apache Airflow 3.x

### What Airflow Does Better

| Feature | Apache Airflow 3.x | airflow-nodejs | Gap |
|---|---|---|---|
| **Log storage backends** | File, S3, GCS, Azure Blob, HDFS, ElasticSearch | MongoDB only | ❌ No remote log backends |
| **Log levels** | Python `logging` levels (DEBUG/INFO/WARNING/ERROR/CRITICAL) | stdout/stderr streams only | ❌ No severity levels in DB |
| **Log handlers** | Pluggable `FileTaskHandler` + community handlers | Single `appendLog()` function | ❌ Not pluggable |
| **Log filename template** | Configurable pattern per attempt/map_index | Not applicable (DB rows) | — |
| **Log retention/cleanup** | Configurable; `airflow db clean` | Manual MongoDB query | ❌ No automatic retention |
| **Log streaming to workers** | Real-time push while task runs | Polls `/logs` every 2s | ⚠️ Slight lag |
| **Metrics (StatsD)** | Built-in StatsD/OpenMetrics integration | No metrics endpoint | ❌ No built-in metrics |
| **Distributed tracing** | OpenTelemetry traces | No tracing | ❌ No distributed tracing |
| **FluentD / log aggregation** | Recommended for production | Not documented | ⚠️ Possible via stdout |
| **`self.log` on operators** | `logger.info()` / `logger.warning()` etc. | `console.log()` only | ⚠️ No level semantics |
| **Log search** | UI search within task log | No search | ❌ No full-text search |
| **External log links** | Custom "extra links" per task | No extra links | ❌ |

### Where airflow-nodejs Can Improve (Opportunities)

#### 1. **Log severity levels** (High value, low effort)
Store a `level` field on `LogLine`. Task functions call `ctx.log.info()` / `ctx.log.warn()` / `ctx.log.error()` which set the level. API and UI filter by level.

```typescript
// Enhanced LogLine
interface LogLine {
  level: 'debug' | 'info' | 'warn' | 'error'  // ← add this
  stream: 'stdout' | 'stderr'
  line: string
  // ...
}

// In task context
ctx.log = {
  info:  (msg) => appendLog(db, runId, dagId, taskId, 'stdout', msg, 'info'),
  warn:  (msg) => appendLog(db, runId, dagId, taskId, 'stderr', msg, 'warn'),
  error: (msg) => appendLog(db, runId, dagId, taskId, 'stderr', msg, 'error'),
}
```

#### 2. **Remote log backends** (High value, medium effort)
Pluggable log handlers — same pattern as secrets backends:

```bash
LOG_BACKEND=s3          # writes to S3 after task completes
LOG_BACKEND=gcs         # writes to GCS
LOG_BACKEND=mongodb     # current default
LOG_BACKEND=file        # local files (dev/offline)
```

#### 3. **Log retention policy** (Medium value, low effort)
Auto-purge old logs:

```bash
LOG_RETENTION_DAYS=30   # delete task_logs older than 30 days
```

Add to scheduler tick: prune `task_logs` where `ts < now - retention`.

#### 4. **Sensor/deferred task logging** (Medium value, medium effort)
Currently sensors and deferred trigger fns produce no log output. The trigger fn runs in the scheduler process — pipe its `console.log` to a per-task log buffer.

#### 5. **Structured JSON log API filter** (Low effort)
Add `?level=error` and `?stream=stderr` query params to the logs endpoint:

```bash
GET /dag-runs/:runId/tasks/:taskId/logs?stream=stderr
GET /dag-runs/:runId/tasks/:taskId/logs?level=error
```

#### 6. **Log search** (Medium effort)
MongoDB text index on `task_logs.line` + `GET /dag-runs/:runId/tasks/:taskId/logs?q=ERROR`:

```javascript
db.task_logs.createIndex({ line: 'text' })
```

#### 7. **OpenTelemetry traces** (High effort)
Instrument each task execution span with start/end timestamps, attributes (dag_id, run_id, task_id, state). Export to Jaeger/Zipkin/OTLP. Enables distributed tracing across the scheduler → worker → task chain.

---

## 7. Current Gaps Summary

| Gap | Effort | Impact | Notes |
|---|---|---|---|
| Log severity levels (DEBUG/INFO/WARN/ERROR) | Low | High | `ctx.log.info()` API |
| Remote log backends (S3/GCS) | Medium | High | After task completes |
| Log retention/auto-cleanup | Low | Medium | Scheduler tick prune |
| Sensor/deferred task log capture | Medium | Medium | In-scheduler stdout pipe |
| Log endpoint filters (`?level=`, `?stream=`) | Low | Medium | MongoDB query |
| Log search (`?q=`) | Low | Medium | MongoDB text index |
| StatsD / Prometheus metrics | High | High | Separate concern |
| OpenTelemetry tracing | High | High | Large scope |
| FluentD integration | Low (docs only) | Medium | Log to stdout, let FluentD collect |

---

## 8. Quick Debug Commands

```bash
BASE=http://localhost:3000

# Show all logs for a task
RUN=<your-run-id>
curl -s $BASE/dag-runs/$RUN/tasks/extract/logs | \
  python3 -c "import json,sys; [print(f\"{l['stream'][0].upper()} {l['ts'][11:19]} {l['line']}\") for l in json.load(sys.stdin)]"

# Show only stderr
curl -s $BASE/dag-runs/$RUN/tasks/extract/logs | \
  python3 -c "import json,sys; [print(l['line']) for l in json.load(sys.stdin) if l['stream']=='stderr']"

# Show retry history
curl -s $BASE/dag-runs/$RUN/tasks/extract/tries | \
  python3 -c "import json,sys; [print(f\"try {t['try_number']}: {t['state']} — {t.get('error','')[:60]}\") for t in json.load(sys.stdin)]"

# Show last failed task error
curl -s $BASE/dag-runs/$RUN/tasks | \
  python3 -c "import json,sys; [print(f\"{t['task_id']}: {t.get('error','')[:80]}\") for t in json.load(sys.stdin) if t['state']=='failed']"

# Show all xcoms for debugging data flow
curl -s $BASE/dag-runs/$RUN/xcoms | \
  python3 -c "import json,sys; [print(f\"{x['task_id']}.{x['key']} = {str(x['value'])[:60]}\") for x in json.load(sys.stdin)]"
```
