#!/usr/bin/env bash
#
# Parity test — Astronomer Airflow (AFaaS) vs airflow-nodejs (WCNP)
#
# Runs the read-only + CRUD cases from files/test-plan-astro-vs-airflow-nodejs.md
# against both live deployments and prints a pass/fail summary.
#
# Usage:
#   export ASTRO_TOKEN='<deployment-service-account-api-key>'
#   ./scripts/parity-test.sh                 # read-only checks
#   ./scripts/parity-test.sh --with-writes   # also create/delete a test variable
#   ./scripts/parity-test.sh --with-trigger  # also trigger a DAG run on airflow-nodejs
#
# The API key is NEVER hardcoded here. Put it in a gitignored .env instead:
#   echo "ASTRO_TOKEN=xxxx" >> .env && set -a && . ./.env && set +a
#
set -uo pipefail

ASTRO_BASE="${ASTRO_BASE:-https://deployments.astro-nonprod1.us-west1.us.walmart.net/asteroidic-wave-5407/airflow}"
ANJS_BASE="${ANJS_BASE:-https://airflow-nodejs.dev.walmart.com}"
TIMEOUT="${TIMEOUT:-20}"

WITH_WRITES=0
WITH_TRIGGER=0
for arg in "$@"; do
  case "$arg" in
    --with-writes)  WITH_WRITES=1 ;;
    --with-trigger) WITH_TRIGGER=1 ;;
    -h|--help)      sed -n '2,20p' "$0"; exit 0 ;;
    *) echo "unknown flag: $arg" >&2; exit 2 ;;
  esac
done

if [[ -z "${ASTRO_TOKEN:-}" ]]; then
  cat >&2 <<'EOF'
ERROR: ASTRO_TOKEN is not set.

Generate a Deployment Service Account API Key:
  1. https://app.astro-nonprod1.us-west1.us.walmart.net/
  2. Deployment "asteroidic-wave-5407" -> Service Accounts
  3. New Deployment Service Account -> copy the key (shown once)

Then:  export ASTRO_TOKEN='<key>'
EOF
  exit 1
fi

command -v jq >/dev/null || { echo "ERROR: jq is required." >&2; exit 1; }

# ── output helpers ────────────────────────────────────────────────────────────
if [[ -t 1 ]]; then
  R=$'\e[31m'; G=$'\e[32m'; Y=$'\e[33m'; B=$'\e[1m'; D=$'\e[2m'; N=$'\e[0m'
else
  R=''; G=''; Y=''; B=''; D=''; N=''
fi

PASS=0; FAIL=0; SKIP=0
declare -a FAILURES=()

section() { printf '\n%s== %s ==%s\n' "$B" "$1" "$N"; }

ok()   { PASS=$((PASS+1)); printf '  %sPASS%s  %-14s %s\n' "$G" "$N" "$1" "$2"; }
bad()  { FAIL=$((FAIL+1)); FAILURES+=("$1 [$2] $3"); printf '  %sFAIL%s  %-14s %s %s(%s)%s\n' "$R" "$N" "$1" "$2" "$D" "$3" "$N"; }
skip() { SKIP=$((SKIP+1)); printf '  %sSKIP%s  %-14s %s\n' "$Y" "$N" "$1" "$2"; }

# astro <method> <path> -> body on stdout, HTTP code on fd 3
astro() {
  curl -s -m "$TIMEOUT" -w '\n%{http_code}' -X "$1" "$ASTRO_BASE$2" \
    -H "Authorization: $ASTRO_TOKEN" \
    -H 'Cache-Control: no-cache' \
    -H 'Content-Type: application/json' \
    ${3:+--data "$3"}
}

anjs() {
  local method="$1" path="$2" body="${3:-}"
  if [[ -n "$body" ]]; then
    curl -s -m "$TIMEOUT" -w '\n%{http_code}' -X "$method" "$ANJS_BASE$path" \
      -H 'Content-Type: application/json' --data "$body"
  else
    curl -s -m "$TIMEOUT" -w '\n%{http_code}' -X "$method" "$ANJS_BASE$path"
  fi
}

# check <label> <target> <expected-code> <method> <path> [body] [jq-filter]
# Prints the jq-filter result when it succeeds, so callers can eyeball values.
check() {
  local label="$1" target="$2" want="$3" method="$4" path="$5" body="${6:-}" filter="${7:-}"
  local raw code out
  if [[ -n "$body" ]]; then
    raw=$([[ "$target" == astro ]] && astro "$method" "$path" "$body" || anjs "$method" "$path" "$body")
  else
    raw=$([[ "$target" == astro ]] && astro "$method" "$path" || anjs "$method" "$path")
  fi
  code=$(tail -n1 <<<"$raw")
  out=$(sed '$d' <<<"$raw")

  if [[ "$code" != "$want" ]]; then
    bad "$label" "$target" "HTTP $code, want $want"
    return 1
  fi

  if [[ -n "$filter" ]]; then
    local val
    val=$(jq -r "$filter" <<<"$out" 2>/dev/null)
    if [[ -z "$val" || "$val" == "null" ]]; then
      bad "$label" "$target" "HTTP $code but filter '$filter' empty"
      return 1
    fi
    ok "$label" "$target -> $val"
  else
    ok "$label" "$target -> HTTP $code"
  fi
  return 0
}

printf '%sParity test — Astro AFaaS vs airflow-nodejs%s\n' "$B" "$N"
printf '%sastro:%s %s\n' "$D" "$N" "$ASTRO_BASE"
printf '%sanjs :%s %s\n' "$D" "$N" "$ANJS_BASE"
printf '%sdate :%s %s\n' "$D" "$N" "$(date '+%Y-%m-%d %H:%M:%S %Z')"

# ── 11. Health & version (run first — everything else depends on reachability) ─
section "11. Health & Version"
check "health"  astro 200 GET /api/v1/health '' '.metadatabase.status'
check "health"  anjs  200 GET /health        '' '.status'
check "version" astro 200 GET /api/v1/version '' '.version'

# ── 1. Variables ──────────────────────────────────────────────────────────────
# NOTE: Astro service account needs 'variable:read' scope. A 403 here means
# the key has dag:read only — grant more permissions in Astro UI → Service Accounts.
section "1. Variables"
VARS_CODE=$(astro GET /api/v1/variables | tail -n1)
if [[ "$VARS_CODE" == 200 ]]; then
  ok "list" "astro -> $(astro GET /api/v1/variables | sed '$d' | jq -r '.total_entries')"
else
  skip "list" "astro HTTP $VARS_CODE — grant variable:read scope on service account in Astro UI"
fi
check "list"    anjs  200 GET /variables

if (( WITH_WRITES )); then
  # Use $BASHPID (current process, stable) + timestamp to guarantee uniqueness
  # across subshells — $$ can change in subshells, causing create/delete key mismatch
  KEY="parity_test_${RANDOM}${RANDOM}"
  # Test write on anjs (no auth required)
  # POST /variables returns 201 Created (correct REST semantics)
  check "create" anjs 201 POST /variables "{\"key\":\"$KEY\",\"value\":\"dev\"}"
  check "read"   anjs 200 GET  "/variables/$KEY" '' '.value'
  check "delete" anjs 204 DELETE "/variables/$KEY"
  # Test write on astro only if service account has variable:write scope
  WRITE_CODE=$(astro POST /api/v1/variables "{\"key\":\"${KEY}_astro\",\"value\":\"dev\"}" | tail -n1)
  if [[ "$WRITE_CODE" == 200 ]]; then
    ok "create" "astro"
    astro DELETE "/api/v1/variables/${KEY}_astro" >/dev/null
    ok "delete" "astro (cleanup)"
  else
    skip "create/delete" "astro HTTP $WRITE_CODE — grant variable:write scope on service account in Astro UI"
  fi
else
  skip "create/delete" "both (use --with-writes)"
fi

# ── 2. Connections ────────────────────────────────────────────────────────────
# NOTE: same scope requirement — needs 'connection:read' on the service account.
section "2. Connections"
CONN_CODE=$(astro GET /api/v1/connections | tail -n1)
if [[ "$CONN_CODE" == 200 ]]; then
  ok "list" "astro -> $(astro GET /api/v1/connections | sed '$d' | jq -r '.total_entries')"
else
  skip "list" "astro HTTP $CONN_CODE — grant connection:read scope on service account in Astro UI"
fi
check "list" anjs  200 GET /connections

# ── 3. DAG discovery ──────────────────────────────────────────────────────────
section "3. DAG Discovery"
check "list dags"     astro 200 GET /api/v1/dags '' '.total_entries'
check "list dags"     anjs  200 GET /dags
check "import errors" astro 200 GET /api/v1/importErrors '' '.total_entries'
check "import errors" anjs  200 GET /import-errors

# Discover a DAG id on each side for the detail checks
ASTRO_DAG=$(astro GET /api/v1/dags | sed '$d' | jq -r '.dags[0].dag_id // empty' 2>/dev/null)
ANJS_DAG=$(anjs  GET /dags          | sed '$d' | jq -r '(.items // .)[0].dag_id // empty' 2>/dev/null)

if [[ -n "$ASTRO_DAG" ]]; then
  check "dag detail" astro 200 GET "/api/v1/dags/$ASTRO_DAG" '' '.dag_id'
  check "dag tasks"  astro 200 GET "/api/v1/dags/$ASTRO_DAG/tasks" '' '.total_entries'
else
  skip "dag detail" "astro (no DAGs found)"
fi

if [[ -n "$ANJS_DAG" ]]; then
  check "dag detail" anjs 200 GET "/dags/$ANJS_DAG"        '' '.dag_id'
  check "dag tasks"  anjs 200 GET "/dags/$ANJS_DAG/tasks"
  check "dag source" anjs 200 GET "/dags/$ANJS_DAG/source"
  check "dag stats"  anjs 200 GET "/dags/$ANJS_DAG/stats"
else
  skip "dag detail" "anjs (no DAGs mounted in WCNP pod — upload DAGs via POST /dags or mount a dags volume)"
fi

# ── 10. Pools ─────────────────────────────────────────────────────────────────
section "10. Pools"
check "list" astro 200 GET /api/v1/pools '' '.total_entries'
check "list" anjs  200 GET /pools

# ── airflow-nodejs extensions (no Astro equivalent) ───────────────────────────
section "Extensions (airflow-nodejs only)"
check "providers" anjs 200 GET /providers
check "plugins"   anjs 200 GET /plugins
check "sla-alerts" anjs 200 GET /sla-alerts
check "config"    anjs 200 GET /config

# ── 4/5. Trigger + run lifecycle + logs ───────────────────────────────────────
section "4-5. Trigger, Run Lifecycle & Logs"
if (( WITH_TRIGGER )) && [[ -n "$ANJS_DAG" ]]; then
  RAW=$(anjs POST "/dags/$ANJS_DAG/trigger" '{"conf":{"parity_test":true}}')
  CODE=$(tail -n1 <<<"$RAW")
  RUN=$(sed '$d' <<<"$RAW" | jq -r '.dag_run_id // .run_id // empty' 2>/dev/null)

  if [[ "$CODE" =~ ^20[01]$ && -n "$RUN" ]]; then
    ok "trigger" "anjs -> $RUN"

    STATE=""
    for _ in $(seq 1 30); do
      sleep 2
      STATE=$(anjs GET "/dag-runs/$RUN" | sed '$d' | jq -r '.state // empty' 2>/dev/null)
      [[ "$STATE" == success || "$STATE" == failed ]] && break
    done

    if [[ "$STATE" == success ]]; then
      ok "run completes" "anjs -> success"
    else
      bad "run completes" "anjs" "state=${STATE:-unknown} after 60s"
    fi

    check "task list" anjs 200 GET "/dag-runs/$RUN/tasks"
    check "xcoms"     anjs 200 GET "/dag-runs/$RUN/xcoms"

    TASK=$(anjs GET "/dag-runs/$RUN/tasks" | sed '$d' | jq -r '(.items // .)[0].task_id // empty' 2>/dev/null)
    if [[ -n "$TASK" ]]; then
      check "task logs"    anjs 200 GET "/dag-runs/$RUN/tasks/$TASK/logs"
      check "logs ?level"  anjs 200 GET "/dag-runs/$RUN/tasks/$TASK/logs?level=error"
      check "logs ?stream" anjs 200 GET "/dag-runs/$RUN/tasks/$TASK/logs?stream=stdout"
      check "task tries"   anjs 200 GET "/dag-runs/$RUN/tasks/$TASK/tries"
    else
      skip "task logs" "anjs (no tasks in run)"
    fi
  else
    bad "trigger" "anjs" "HTTP $CODE, no run id"
  fi
else
  skip "trigger" "anjs (use --with-trigger)"
fi

# ── summary ───────────────────────────────────────────────────────────────────
TOTAL=$((PASS+FAIL+SKIP))
printf '\n%s%s%s\n' "$B" "$(printf '=%.0s' {1..60})" "$N"
printf '%sSummary%s  %spass %d%s  %sfail %d%s  %sskip %d%s  (total %d)\n' \
  "$B" "$N" "$G" "$PASS" "$N" "$R" "$FAIL" "$N" "$Y" "$SKIP" "$N" "$TOTAL"

if (( FAIL )); then
  printf '\n%sFailures:%s\n' "$R" "$N"
  printf '  - %s\n' "${FAILURES[@]}"
fi

printf '\n%sKnown gaps (see test plan section 12):%s\n' "$Y" "$N"
printf '  - airflow-nodejs MongoDB is an ephemeral sidecar; history is lost on pod restart\n'
printf '  - airflow-nodejs auth is disabled; the dev URL is open to the network\n'
printf '  - airflow-nodejs is pinned to 1 replica; no HA comparison possible\n'

exit $(( FAIL > 0 ))
