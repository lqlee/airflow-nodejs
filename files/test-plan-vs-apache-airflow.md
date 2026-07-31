# Test Plan — airflow-nodejs vs Apache Airflow 3.x

Validates that airflow-nodejs covers the classical Apache Airflow usage patterns.
Each test case includes: the Apache Airflow equivalent, the airflow-nodejs API/DAG
syntax to exercise, and the expected outcome.

**Setup:**
- airflow-nodejs running at http://localhost:3000 (`docker-compose up -d`)
- Apache Airflow 3.x running at http://localhost:8080 (optional, for side-by-side)
- Auth: leave `API_KEYS` unset for open access during testing

---

## 1. DAG Definition & Loading

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 1.1 | Define DAG with `@dag` decorator | Create `dags/test_basic.js` exporting `dag({id, schedule, tasks})` | DAG appears in `GET /dags` within 5s |
| 1.2 | DAG with `schedule_interval='@daily'` | `schedule: '0 0 * * *'` | `GET /dags/test_basic` shows cron expression |
| 1.3 | Manual-only DAG (`schedule=None`) | `schedule: null` | DAG listed; no automatic runs created |
| 1.4 | DAG reload on file change | Edit `dags/test_basic.js`, change a task | `GET /dags/test_basic` shows new version hash within 5s |
| 1.5 | Import error handling | Add invalid JS syntax to a dag file | `GET /import-errors` lists the file with error message |
| 1.6 | DAG ID uniqueness | Two files with same `id` | Second file's error appears in import-errors or warnings |

**Commands:**
```bash
# 1.1 — verify dag appears
curl -s http://localhost:3000/dags | jq '.items[].dag_id'

# 1.5 — check import errors
curl -s http://localhost:3000/import-errors | jq '.'
```

---

## 2. Task Dependencies

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 2.1 | `task_a >> task_b` (sequential) | `dependsOn: ['task_a']` on task_b | task_b starts only after task_a succeeds |
| 2.2 | Fan-out: `start >> [a, b, c]` | Three tasks all depending on `start` | a, b, c run in parallel after start |
| 2.3 | Fan-in: `[a, b] >> end` | `end` with `dependsOn: ['a', 'b']` | end waits for BOTH a and b |
| 2.4 | Diamond pattern | start→a, start→b, a→end, b→end | end runs after both branches complete |
| 2.5 | Cycle detection | task_a depends on task_b, task_b depends on task_a | Import error or dag warning reported |

**DAG to create (`dags/test_deps.js`):**
```js
export default dag({
  id: 'test_deps', schedule: null,
  tasks: {
    start:  { run: async () => 'started' },
    branch_a: { dependsOn: ['start'], shell: { command: 'echo a', interpreter: 'sh' } },
    branch_b: { dependsOn: ['start'], shell: { command: 'echo b', interpreter: 'sh' } },
    end:    { dependsOn: ['branch_a', 'branch_b'], run: async () => 'done' },
  }
})
```

```bash
curl -s -X POST http://localhost:3000/dags/test_deps/trigger -H 'Content-Type: application/json' -d '{}'
# Watch tasks reach success in order
curl -s http://localhost:3000/dag-runs/<runId>/tasks | jq '.[].state'
```

---

## 3. Scheduling

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 3.1 | Cron schedule fires automatically | DAG with `schedule: '* * * * *'` (every minute) | Run created within 60s without manual trigger |
| 3.2 | Pause DAG stops scheduling | `POST /dags/:id/pause` | No new runs created while paused |
| 3.3 | Resume DAG resumes scheduling | `POST /dags/:id/resume` | Runs resume on next cron tick |
| 3.4 | Manual trigger with conf | `POST /dags/:id/trigger` with `{"conf":{"env":"prod"}}` | `ctx.conf.env === 'prod'` inside task |
| 3.5 | Manual trigger with tags | Trigger with `{"tags":["smoke-test"]}` | Run visible in `GET /dags/:id/runs?tag=smoke-test` |
| 3.6 | Backfill historical range | `POST /dags/:id/backfill` with `start`/`end` dates | Multiple runs created for each date in range |

**Commands:**
```bash
# 3.1 — create a 1-minute dag
cat > dags/test_cron.js << 'EOF'
import { dag } from 'airflow-nodejs/dag/types';
export default dag({ id: 'test_cron', schedule: '* * * * *',
  tasks: { tick: { run: async () => new Date().toISOString() } }
})
EOF

# 3.2 — pause
curl -s -X POST http://localhost:3000/dags/test_cron/pause

# 3.6 — backfill
curl -s -X POST http://localhost:3000/dags/test_cron/backfill \
  -H 'Content-Type: application/json' \
  -d '{"start":"2025-01-01","end":"2025-01-03"}'
```

---

## 4. Task Types — Operators Equivalents

### 4a. PythonOperator / @task

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 4a.1 | `@task` function returning value | `run: async (ctx) => ({ result: 42 })` | Task succeeds; return value stored as XCom |
| 4a.2 | Task failure (raise exception) | `run: async () => { throw new Error('boom') }` | Task state = failed, error = 'boom' |
| 4a.3 | Python inline code | `python: { code: 'print("hello")' }` | Task succeeds; "hello" in task logs |
| 4a.4 | Python script file | `python: { script: './dags/scripts/job.py' }` | Script runs, exit 0 = success |

### 4b. BashOperator

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 4b.1 | `BashOperator(bash_command='echo hi')` | `shell: { command: 'echo hi', interpreter: 'bash' }` | "hi" in task logs, state = success |
| 4b.2 | Non-zero exit = failure | `shell: { command: 'exit 1' }` | state = failed, error mentions exit code 1 |
| 4b.3 | Environment variables | `shell: { command: 'echo $MY_VAR', env: { MY_VAR: 'hello' } }` | "hello" in task logs |
| 4b.4 | DAG_ID/RUN_ID/TASK_ID injected | `shell: { command: 'echo $DAG_ID $TASK_ID' }` | dag id and task id appear in logs |
| 4b.5 | sh interpreter | `shell: { interpreter: 'sh', command: 'echo posix' }` | succeeds with "posix" in logs |
| 4b.6 | zsh interpreter | `shell: { interpreter: 'zsh', command: 'echo zsh' }` | succeeds (if zsh installed) |

### 4c. DockerOperator / KubernetesPodOperator

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 4c.1 | `DockerOperator(image='alpine', command='echo hi')` | `container: { image: 'alpine:latest', command: ['echo', 'hi'] }` | "hi" in logs, state = success |
| 4c.2 | Container env vars | `container: { image: 'alpine:latest', env: { FOO: 'bar' }, command: ['sh','-c','echo $FOO'] }` | "bar" in logs |
| 4c.3 | Container memory limit | `container: { image: 'alpine:latest', memory: '256m' }` | Runs successfully within limit |
| 4c.4 | Container CPU limit | `container: { image: 'alpine:latest', cpus: '0.5' }` | Runs successfully |
| 4c.5 | `KubernetesPodOperator` | `kubernetes: { image: 'alpine:latest', namespace: 'default' }` | Pod created, runs, deleted; state = success |
| 4c.6 | K8s resource limits | `kubernetes: { image: 'alpine:latest', memory: '256Mi', cpu: '250m' }` | --limits/--requests flags set correctly |

**Commands:**
```bash
# 4c.1 — container task (requires Docker socket)
cat > dags/test_container.js << 'EOF'
import { dag } from 'airflow-nodejs/dag/types';
export default dag({ id: 'test_container', schedule: null,
  tasks: { hello: { container: { image: 'alpine:latest', command: ['echo', 'hello-container'] } } }
})
EOF
curl -s -X POST http://localhost:3000/dags/test_container/trigger -d '{}'
```

### 4d. Sensors

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 4d.1 | `FileSensor` (poll until condition) | `poke: async () => Math.random() > 0.7` | Task eventually succeeds after N pokes |
| 4d.2 | Sensor timeout | `poke: async () => false, sensorTimeout: 5000` | Task fails after 5s with timeout message |
| 4d.3 | Custom poke interval | `poke: async () => false, pokeInterval: 2000` | Re-queued every ~2s |
| 4d.4 | Sensor success | `poke: async () => true` | Succeeds on first poke |

---

## 5. XCom (Cross-Task Communication)

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 5.1 | `task_instance.xcom_push(key, value)` | `ctx.xcom.push('result', {rows: 42})` | `GET /dag-runs/:runId/xcoms/task_id/result` returns `{rows:42}` |
| 5.2 | `task_instance.xcom_pull(task_ids, key)` | `ctx.xcom.pull('upstream_task', 'result')` | Returns pushed value |
| 5.3 | XCom between tasks in same run | task_a pushes, task_b pulls | task_b receives task_a's value |
| 5.4 | XCom from mapped task | mapped task pushes, downstream pulls | Returns array of all instances' values |
| 5.5 | XCom via API | `POST /dag-runs/:runId/xcoms` | Value readable by subsequent tasks |

**DAG to create (`dags/test_xcom.js`):**
```js
export default dag({
  id: 'test_xcom', schedule: null,
  tasks: {
    producer: {
      run: async (ctx) => { await ctx.xcom.push('count', 99); return 'pushed' }
    },
    consumer: {
      dependsOn: ['producer'],
      run: async (ctx) => {
        const count = await ctx.xcom.pull('producer', 'count')
        if (count !== 99) throw new Error(`expected 99, got ${count}`)
        return 'verified'
      }
    }
  }
})
```

---

## 6. Variables & Connections

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 6.1 | `Variable.get('my_key')` | `ctx.variables.get('my_key')` after `POST /variables` | Returns stored value |
| 6.2 | Secret variable (masked) | `POST /variables` with `is_secret: true` | `GET /variables` shows `***` for value |
| 6.3 | `BaseHook.get_connection('my_db')` | `ctx.connections.get('my_db')` after `POST /connections` | Returns conn object with host/port/password |
| 6.4 | Connection password encrypted | Store connection with password | `GET /connections/:id` does NOT expose plaintext password |
| 6.5 | Variable unavailable | `ctx.variables.get('nonexistent')` | Returns `null` (no exception) |

**Commands:**
```bash
# 6.1 — create variable, read in task
curl -s -X POST http://localhost:3000/variables \
  -H 'Content-Type: application/json' \
  -d '{"key":"my_key","value":"hello_world"}'

# 6.3 — create connection
curl -s -X POST http://localhost:3000/connections \
  -H 'Content-Type: application/json' \
  -d '{"conn_id":"my_db","conn_type":"postgres","host":"db.example.com","port":5432,"login":"user","password":"secret"}'
```

---

## 7. Dynamic Task Mapping

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 7.1 | `task.expand(item=items)` | `expand: ['a','b','c']` | 3 task instances created at run time |
| 7.2 | `map_index` available | `ctx.mapIndex` | 0, 1, 2 for each instance |
| 7.3 | `map_value` available | `ctx.mapValue` | 'a', 'b', 'c' for each instance |
| 7.4 | Downstream waits for all | task after mapped task | Downstream starts only after ALL instances succeed |
| 7.5 | One mapped instance fails | One instance throws | Run fails; downstream task doesn't run |

**DAG to create (`dags/test_mapping.js`):**
```js
export default dag({
  id: 'test_mapping', schedule: null,
  tasks: {
    process: {
      expand: ['alpha', 'beta', 'gamma'],
      run: async (ctx) => {
        return `processed ${ctx.mapValue} at index ${ctx.mapIndex}`
      }
    },
    summarize: {
      dependsOn: ['process'],
      run: async (ctx) => {
        const results = await ctx.xcom.pull('process', 'return_value')
        return `all done: ${JSON.stringify(results)}`
      }
    }
  }
})
```

---

## 8. Retries & Timeouts

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 8.1 | `retries=3` | `retries: 3` on task | Task retried 3 times before permanent fail |
| 8.2 | `retry_delay=timedelta(seconds=1)` | `retryDelay: 1000` | ~1s pause between retries visible in try history |
| 8.3 | `execution_timeout=timedelta(seconds=5)` | `timeout: 5000` | Task killed after 5s; state = failed with timeout message |
| 8.4 | Try history | After retries | `GET /dag-runs/:runId/tasks/:taskId/tries` shows all attempts |
| 8.5 | Shell task timeout | Shell with infinite loop + `timeout: 2000` | Killed at 2s, state = failed |

**Commands:**
```bash
# 8.4 — check try history after retries
curl -s http://localhost:3000/dag-runs/<runId>/tasks/flaky_task/tries | jq '.'
```

---

## 9. Resource Pools

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 9.1 | `Pool(pool='db_conn', slots=2)` | `POST /pools` with `{name:'db_pool',slots:2}` | Pool created with 2 slots |
| 9.2 | Task uses pool | `pool: 'db_pool'` on task | Task acquires slot from pool |
| 9.3 | Pool concurrency limit | 3 tasks with same pool (2 slots) | Only 2 run simultaneously; 3rd waits |
| 9.4 | Pool slot release on completion | Tasks complete | Slots freed; queued task starts |

**Commands:**
```bash
curl -s -X POST http://localhost:3000/pools \
  -H 'Content-Type: application/json' \
  -d '{"name":"db_pool","slots":2,"description":"Database connections"}'
```

---

## 10. Dataset / Asset Scheduling

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 10.1 | DAG produces dataset (`outlets`) | `outlets: ['s3://bucket/users/']` | After run success, dataset event recorded |
| 10.2 | DAG consumes dataset (`schedule=[dataset]`) | `datasets: ['s3://bucket/users/']` | Consumer DAG triggered after producer completes |
| 10.3 | AND semantics | Consumer with 2 datasets | Triggered only after BOTH datasets have new events |
| 10.4 | Dataset not yet updated | Consumer dag, no outlet event | Consumer run NOT created |

**Commands:**
```bash
# Trigger producer, verify consumer auto-starts
curl -s -X POST http://localhost:3000/dags/producer_dag/trigger -d '{}'
# Wait, then check consumer
curl -s http://localhost:3000/dags/consumer_dag/runs | jq '.'
```

---

## 11. Webhooks (Callbacks)

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 11.1 | `on_success_callback` | `onSuccess: 'http://localhost:9999/hook'` | POST received at hook URL on run success |
| 11.2 | `on_failure_callback` | `onFailure: 'http://localhost:9999/hook'` | POST received when run fails |
| 11.3 | Webhook payload | Receive POST body | Contains `dag_id`, `run_id`, `state`, `ended_at` |
| 11.4 | Webhook failure is non-fatal | Hook URL unreachable | Run still completes; error logged only |

**Test webhook receiver:**
```bash
# Start a simple receiver
python3 -m http.server 9999 &
# Trigger dag with onSuccess webhook; verify POST arrives
```

---

## 12. Human-in-the-Loop (HITL)

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 12.1 | Manual approval gate | `requiresApproval: true` on task | Task parks at queued; `GET /hitl` shows it pending |
| 12.2 | Approve → task runs | `POST /hitl/:runId/:taskId` with `{"action":"approve"}` | Task transitions to success/running |
| 12.3 | Reject → run fails | `POST /hitl/:runId/:taskId` with `{"action":"reject"}` | Task fails; run fails |
| 12.4 | HITL prompt shown | `hitlPrompt: 'Review before deploying'` | `GET /hitl/:runId/:taskId` shows prompt text |
| 12.5 | Approve with note | `{"action":"approve","note":"Looks good"}` | Note stored on task instance |

---

## 13. Observability

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 13.1 | Task logs | After task runs | `GET /dag-runs/:runId/tasks/:taskId/logs` returns stdout/stderr lines |
| 13.2 | SLA breach | DAG with `sla: 100` (100ms), slow task | `GET /sla-alerts` shows breach within seconds |
| 13.3 | SLA acknowledge | `POST /sla-alerts/:alertId/ack` | Alert disappears from `?unacked=true` filter |
| 13.4 | Run statistics | After several runs | `GET /dags/:id/stats` returns duration histogram |
| 13.5 | Audit log | Various actions (trigger, pause, approve) | `GET /event-logs` records each action |
| 13.6 | DAG version history | Edit dag file twice | `GET /dags/:id/versions` shows 2+ entries |
| 13.7 | Source snapshot | `GET /dags/:id/source?version=<hash>` | Returns the dag source at that version |

---

## 14. Run Lifecycle

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 14.1 | Cancel running run | `POST /dag-runs/:runId/cancel` | Run state = cancelled; in-flight tasks finish |
| 14.2 | Clear task to retry | `POST /dag-runs/:runId/tasks/:taskId/clear` | Task state reset to queued; re-runs |
| 14.3 | Add note to run | `POST /dag-runs/:runId/note` with `{"note":"investigated"}` | Note visible in `GET /dag-runs/:runId` |
| 14.4 | Task group dependencies | Tasks in groups with group `dependsOn` | Group B tasks start only after all Group A tasks succeed |

---

## 15. Authentication & RBAC

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 15.1 | API key required | `API_KEYS=mykey` set; request without key | 401 Unauthorized |
| 15.2 | Valid API key accepted | Request with `Authorization: Bearer mykey` | 200 OK |
| 15.3 | Viewer can read, not write | Create viewer key; try `POST /dags/:id/trigger` | 403 Forbidden |
| 15.4 | Editor can trigger | Create editor key; trigger dag | 200 OK, run created |
| 15.5 | Admin can manage keys | Admin key; `POST /api-keys` | New key created |
| 15.6 | Key revocation | `DELETE /api-keys/:keyId` | Revoked key returns 401 on next request |

**Commands:**
```bash
# Start with ADMIN_KEY set
ADMIN_KEY=bootstrap docker-compose up -d

# Create editor key
curl -s -X POST http://localhost:3000/api-keys \
  -H 'Authorization: Bearer bootstrap' \
  -H 'Content-Type: application/json' \
  -d '{"name":"ci-editor","role":"editor"}'

# Test viewer blocked from triggering
curl -s -X POST http://localhost:3000/dags/test_cron/trigger \
  -H 'Authorization: Bearer <viewer-key>' -d '{}'
# → expect {"statusCode":403,"error":"Forbidden"}
```

---

## 16. Backfill

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 16.1 | Create backfill | `POST /dags/:id/backfill` with date range | Runs created for each date |
| 16.2 | Pause backfill | `POST /backfills/:id/pause` | No new runs from backfill while paused |
| 16.3 | Resume backfill | `POST /backfills/:id/resume` | Backfill continues from where paused |
| 16.4 | Cancel backfill | `POST /backfills/:id/cancel` | Remaining runs not created |
| 16.5 | Backfill ID on runs | `GET /dag-runs/:runId` | `backfill_id` field set for backfill runs |

---

## 17. Task Groups

| # | Airflow Classic | airflow-nodejs Test | Expected |
|---|---|---|---|
| 17.1 | `TaskGroup('extract')` | `group: 'extract'` on tasks + `groups: {extract:{label:'Extract'}}` | Tasks show group in `GET /dags/:id` |
| 17.2 | Group-level dependency | `groups: {load:{dependsOn:['transform']}}` | All load tasks wait for all transform tasks |
| 17.3 | Group label | `groups: {extract:{label:'Data Extraction'}}` | Label visible in dag detail API |

---

## Summary Coverage Matrix

| Apache Airflow Feature | airflow-nodejs | Test Section |
|---|---|---|
| DAG definition & auto-reload | ✅ Full | §1 |
| Task dependencies (>>, fan-out, fan-in) | ✅ Full | §2 |
| Cron scheduling | ✅ Full | §3 |
| Pause / resume DAG | ✅ Full | §3 |
| Manual trigger with conf | ✅ Full | §3 |
| Backfill | ✅ Full | §16 |
| PythonOperator / @task | ✅ Via `run:` | §4a |
| BashOperator | ✅ Via `shell:` | §4b |
| DockerOperator | ✅ Via `container:` | §4c |
| KubernetesPodOperator | ✅ Via `kubernetes:` | §4c |
| Sensors (poll + timeout) | ✅ Via `poke:` | §4d |
| XCom push/pull | ✅ Full | §5 |
| Variables (encrypted secrets) | ✅ Full | §6 |
| Connections (encrypted) | ✅ Full | §6 |
| Dynamic task mapping | ✅ Literal expand | §7 |
| Retries + retry delay | ✅ Full | §8 |
| Task timeout | ✅ Full | §8 |
| Resource pools (concurrency slots) | ✅ Full | §9 |
| Dataset / asset scheduling | ✅ Full | §10 |
| on_success / on_failure callbacks | ✅ Via webhooks | §11 |
| Human approval gate | ✅ Via HITL | §12 |
| Task logs | ✅ Full | §13 |
| SLA alerts | ✅ Full | §13 |
| Audit log | ✅ Full | §13 |
| Run cancel / task clear | ✅ Full | §14 |
| Task groups | ✅ Full | §17 |
| RBAC (viewer/editor/admin) | ✅ Full | §15 |
| Timetables (custom schedules) | ❌ Not implemented | — |
| Trigger rules (one_failed, etc.) | ❌ Not implemented | — |
| Branching (@task.branch) | ❌ Not implemented | — |
| Jinja2 templating | ❌ Not implemented | — |
| XCom-driven dynamic mapping | ❌ Planned | — |
| Providers ecosystem | ❌ Not implemented | — |

---

## Quick Smoke Test (5 minutes)

Run these in order to validate the most critical paths:

```bash
BASE=http://localhost:3000

# 1. Server health
curl -s $BASE/health | jq '.status'

# 2. DAGs loaded
curl -s $BASE/dags | jq '.items | length'

# 3. Trigger hello_world dag
RUN=$(curl -s -X POST $BASE/dags/hello_world/trigger \
  -H 'Content-Type: application/json' -d '{}' | jq -r '.run_id')
echo "Run: $RUN"

# 4. Poll until complete (max 30s)
for i in $(seq 1 30); do
  STATE=$(curl -s $BASE/dag-runs/$RUN | jq -r '.state')
  echo "$i: $STATE"
  [ "$STATE" = "success" ] && break
  sleep 1
done

# 5. Check task logs
curl -s $BASE/dag-runs/$RUN/tasks/extract/logs | jq '.[].line' | head -5

# 6. Check XCom
curl -s $BASE/dag-runs/$RUN/xcoms | jq '.'

# 7. Verify run stats
curl -s $BASE/dags/hello_world/stats | jq '.runs_total, .success_rate'
```

All 7 steps passing = core pipeline is healthy.
