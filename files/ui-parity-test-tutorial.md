# UI Parity Test — Step-by-Step Tutorial

Manual walkthrough for testing the airflow-nodejs UI at **https://airflow-nodejs.dev.walmart.com**.

Each scenario walks you through the exact clicks, what you should see at each step,
and how to verify the result.

**Before you start:**
- Open **https://airflow-nodejs.dev.walmart.com** in Chrome
- No login required (auth is currently disabled on dev)
- Keep DevTools → Network tab open to spot any red 4xx/5xx responses

---

## Scenario 1 — Hello World (Node.js Inline Tasks)

**What it tests:** The core pipeline: sequential tasks, XCom push/pull, task dependencies.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `https://airflow-nodejs.dev.walmart.com` | Dashboard loads; DAG cards visible |
| 2 | Find the **`hello_world`** card | Card shows schedule `null` (manual only) |
| 3 | Click the **`hello_world`** card title | DAG detail page opens; three tasks shown: `extract`, `transform`, `load` |
| 4 | Click **▶ Run** (or **Trigger**) button | Modal or confirmation appears |
| 5 | Click **Confirm** (if a modal appeared) | Page shows a new run entry; state = `queued` or `running` |
| 6 | Wait ~10 seconds; refresh or watch the run update | Run state changes to `success` |
| 7 | Click the run row to open run detail | Three task boxes shown: extract ✅, transform ✅, load ✅ |
| 8 | Click the **`extract`** task box | Task detail panel opens |
| 9 | Click **Logs** | Log lines appear: `extracting data...` and `pushed xcom: {rows: 42}` |
| 10 | Click **`transform`** task → Logs | Log shows `pulled from extract: {rows: 42}` and `pushed xcom: {rows: 84}` |
| 11 | Click **`load`** task → Logs | Log shows `loading 84 rows` |

**Pass criteria:** All 3 tasks green, logs show the XCom chain working.

---

## Scenario 2 — Shell Job (Bash/sh Tasks)

**What it tests:** Shell subprocess execution, stdout capture, exit code handling.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Return to Dashboard | DAG list visible |
| 2 | Find the **`shell_demo`** card | Card visible |
| 3 | Click card → click **Run** → **Confirm** | New run starts |
| 4 | Wait ~15 seconds | Run state = `success` |
| 5 | Open run detail → click first task → **Logs** | Shell output visible: `echo` output, date, hostname |
| 6 | Check that `stderr` tab (if available) is empty or shows only warnings | No unexpected errors |

**Pass criteria:** Run succeeds; shell stdout appears in logs.

---

## Scenario 3 — Node.js Job (Custom Run Function)

**What it tests:** `run: async (ctx) => {...}` pattern, context variables, XCom.

> `hello_world` already covers Node.js inline tasks. Use `parallel_demo` for a more complex Node.js flow.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Find the **`parallel_demo`** card | Card visible |
| 2 | Click card → click **Run** → **Confirm** | Run starts |
| 3 | Watch the task graph on the detail page | Multiple tasks start simultaneously (fan-out) |
| 4 | Wait ~20 seconds | All tasks complete; run state = `success` |
| 5 | Click any parallel task → **Logs** | Task-specific log output visible |
| 6 | Verify the `join` (or final) task ran last | `join` task started only after all parallel tasks finished |

**Pass criteria:** Fan-out tasks run in parallel; join runs after; all green.

---

## Scenario 4 — Python Job

**What it tests:** Python subprocess execution via `shell.interpreter: python3`.

> ⚠️ **Requires the Python image variant.** On the base WCNP image, Python may not be installed.
> If tasks fail, check Logs for `python3: not found` — this is expected on the base image.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Find the **`python_demo`** card | Card visible |
| 2 | Click card → click **Run** → **Confirm** | Run starts |
| 3 | Wait ~30 seconds | Run state = `success` (or `failed` if Python not installed) |
| 4 | Click first task → **Logs** | Python print output visible (e.g. `Hello from Python 3.x`) |
| 5 | If state = `failed` | Open logs; look for `python3: not found` → expected, skip this scenario |

**Pass criteria:** Either `success` with Python output, or graceful `failed` with `not found` error logged.

---

## Scenario 5 — Java Job

**What it tests:** Java jar execution via `shell.interpreter: java`.

> ⚠️ **Requires the Java image variant.** On the base WCNP image, Java is not installed.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Find the **`java_demo`** card | Card visible |
| 2 | Click card → click **Run** → **Confirm** | Run starts |
| 3 | Wait ~30 seconds | Run state = `success` (or `failed` if Java not installed) |
| 4 | Click first task → **Logs** | `Hello from Java` or similar output visible |
| 5 | If state = `failed` | Open logs; look for `java: not found` → expected on base image |

**Pass criteria:** Either `success` with Java output, or graceful `failed` with `not found` error logged.

---

## Scenario 6 — Branching (Branch Operator)

**What it tests:** The branch operator — one path runs, others are skipped.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Find the **`branching_demo`** card | Card visible |
| 2 | Click card → view task graph | Graph shows a diamond: one task branching to 2+ paths |
| 3 | Click **Run** → **Confirm** | Run starts |
| 4 | Wait ~15 seconds | Run state = `success` |
| 5 | Open run detail | One branch tasks shows ✅ `success`; others show ⊘ `skipped` |
| 6 | Click the skipped task | Task state = `skipped`; no logs (it never ran) |
| 7 | Click the successful branch task → **Logs** | Task output visible |

**Pass criteria:** Exactly one branch ran; others are `skipped` (not `failed`).

---

## Scenario 7 — Pause and Resume a DAG

**What it tests:** DAG pause/unpause — no new runs fire while paused.

| Step | Action | Expected |
|------|--------|----------|
| 1 | On Dashboard, find any DAG with a schedule (e.g. `daily_etl`) | Card shows cron schedule |
| 2 | Click the **Pause** button on the card | Card border turns amber; schedule icon shows paused |
| 3 | Wait past the next scheduled interval | No new run appears for that DAG |
| 4 | Click **Resume** | Card returns to normal colour |
| 5 | Optionally: click **Run** to trigger manually | New run starts successfully |

**Pass criteria:** Pause suppresses automatic runs; Resume restores scheduling.

---

## Scenario 8 — Task Retry

**What it tests:** Automatic retry on failure, retry count displayed in UI.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Open `hello_world` DAG detail | Task list visible |
| 2 | Note the `retries: 2` on the `extract` task (in DAG source) | — |
| 3 | Trigger a run normally | Run succeeds on first try |
| 4 | (Optional) Edit `dags/hello_world.js` locally to throw on first attempt | For deeper retry testing |
| 5 | In run detail, click `extract` task | `Try number` field shows `1` (or `2` if retried) |

**Pass criteria:** `try_number` visible; retries increment correctly on failure.

---

## Scenario 9 — View & Filter Task Logs

**What it tests:** Log streaming, severity filtering, stream filtering.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Trigger `hello_world` and wait for success | Run completes |
| 2 | Open run detail → click `extract` task → **Logs** | All log lines shown |
| 3 | In the URL or log filter UI, add `?level=info` | Only INFO-level lines shown |
| 4 | Change filter to `?level=error` | Log area is empty (no errors in a successful run) |
| 5 | Add `?stream=stdout` | Only stdout lines shown |
| 6 | Add `?stream=stderr` | Only stderr lines (if any) |

**API equivalent (paste in a new tab):**
```
https://airflow-nodejs.dev.walmart.com/dag-runs/<RUN_ID>/tasks/extract/logs?level=info
https://airflow-nodejs.dev.walmart.com/dag-runs/<RUN_ID>/tasks/extract/logs?stream=stdout
```

**Pass criteria:** Filters reduce log output correctly; no errors on filter change.

---

## Scenario 10 — Cancel a Running Job

**What it tests:** The cancel / stop button kills an in-progress run.

| Step | Action | Expected |
|------|--------|----------|
| 1 | Find `parallel_demo` (takes ~20s to complete) | Card visible |
| 2 | Click **Run** → **Confirm** | Run starts; state = `running` |
| 3 | Immediately click **Cancel** (or **Stop**) on the run | Confirmation prompt appears |
| 4 | Confirm the cancel | Run state transitions to `cancelled` or `failed` |
| 5 | Check task states | Tasks that hadn't started show `skipped` or `upstream_failed` |

**Pass criteria:** Run does not reach `success`; state = `cancelled`/`failed` quickly.

---

## Result Log

Fill in after each scenario:

| Scenario | Result | Notes |
|---|---|---|
| 1. Hello World (Node.js) | ☐ Pass  ☐ Fail  ☐ Skip | |
| 2. Shell Job (bash) | ☐ Pass  ☐ Fail  ☐ Skip | |
| 3. Node.js Job (parallel) | ☐ Pass  ☐ Fail  ☐ Skip | |
| 4. Python Job | ☐ Pass  ☐ Fail  ☐ Skip | |
| 5. Java Job | ☐ Pass  ☐ Fail  ☐ Skip | |
| 6. Branching | ☐ Pass  ☐ Fail  ☐ Skip | |
| 7. Pause / Resume | ☐ Pass  ☐ Fail  ☐ Skip | |
| 8. Task Retry | ☐ Pass  ☐ Fail  ☐ Skip | |
| 9. Log Filtering | ☐ Pass  ☐ Fail  ☐ Skip | |
| 10. Cancel Job | ☐ Pass  ☐ Fail  ☐ Skip | |

**Tester:** ________________  **Date:** ________________  **Image variant:** ________________

**Known limitations on dev:**
- MongoDB is ephemeral (data lost on pod restart)
- Python/Java scenarios require non-base image variants
- DAGs must be loaded; WCNP pod may have an empty `dags/` directory
- Auth is disabled — anyone on the Walmart network can access the URL
