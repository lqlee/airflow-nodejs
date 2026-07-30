import { dag } from 'airflow-nodejs/dag/types';

/**
 * Dev Session — provision a long-running interactive workspace for a user.
 *
 * Runs a 3-step pipeline: setup → run_workload → teardown
 * The workload step runs for up to SESSION_HOURS (default 8h).
 *
 * ── Trigger examples ────────────────────────────────────────────────────────
 *
 *   Default (holds session open for 8 hours):
 *     POST /dags/dev_session/trigger   body: {}
 *
 *   Custom command:
 *     body: { "conf": { "USER_CMD": "python3 /tmp/my_script.py" } }
 *
 *   Short test (1 minute):
 *     body: { "conf": { "SESSION_HOURS": "0.017", "USER_CMD": "echo done" } }
 *
 * ── Session workspace ────────────────────────────────────────────────────────
 *   /tmp/session-<RUN_ID>/ — created at setup, removed at teardown
 *
 * ── Logs ────────────────────────────────────────────────────────────────────
 *   All stdout/stderr captured to task logs — visible in the UI Log panel.
 */

const SETUP_CMD = [
  'echo "=============================================="',
  'echo "  DEV SESSION STARTED"',
  'echo "  DAG    : $DAG_ID"',
  'echo "  Run ID : $RUN_ID"',
  'echo "  Started: $(date)"',
  'echo "=============================================="',
  'WORKSPACE="/tmp/session-$RUN_ID"',
  'mkdir -p "$WORKSPACE"',
  'echo "  Workspace: $WORKSPACE"',
].join('\n');

const WORKLOAD_CMD = [
  'WORKSPACE="/tmp/session-$RUN_ID"',
  'HOURS="${SESSION_HOURS:-8}"',
  'SECS=$(python3 -c "import sys; print(int(float(sys.argv[1])*3600))" "$HOURS" 2>/dev/null || echo "28800")',
  'echo "=== Session $RUN_ID — workload phase ==="',
  'echo "  Duration : ${HOURS}h"',
  'echo "  Workspace: $WORKSPACE"',
  'echo ""',
  'if [ -n "$USER_CMD" ]; then',
  '  echo "=== Running user command ==="',
  '  echo "  CMD: $USER_CMD"',
  '  cd "$WORKSPACE"',
  '  eval "$USER_CMD"',
  '  EXIT_CODE=$?',
  '  echo "=== Command finished (exit $EXIT_CODE) ==="',
  'else',
  '  echo "=== No USER_CMD — holding session open for ${HOURS}h ==="',
  '  echo "  Re-trigger with: { \\"conf\\":{  \\"USER_CMD\\": \\"your command\\" } }"',
  '  ELAPSED=0',
  '  INTERVAL=1800',
  '  while [ "$ELAPSED" -lt "$SECS" ]; do',
  '    REMAINING=$((SECS - ELAPSED))',
  '    RH=$((REMAINING / 3600))',
  '    RM=$(( (REMAINING % 3600) / 60 ))',
  '    echo "  [$(date +%H:%M:%S)] Session alive — ${RH}h ${RM}m remaining"',
  '    SLEEP=$(( REMAINING < INTERVAL ? REMAINING : INTERVAL ))',
  '    sleep "$SLEEP"',
  '    ELAPSED=$((ELAPSED + SLEEP))',
  '  done',
  '  echo "=== Session duration elapsed ==="',
  'fi',
].join('\n');

const TEARDOWN_CMD = [
  'WORKSPACE="/tmp/session-$RUN_ID"',
  'echo "=============================================="',
  'echo "  DEV SESSION ENDED"',
  'echo "  Run ID : $RUN_ID"',
  'echo "  Ended  : $(date)"',
  'echo "=============================================="',
  '[ -d "$WORKSPACE" ] && rm -rf "$WORKSPACE" && echo "  Workspace cleaned up."',
].join('\n');

export default dag({
  id: 'dev_session',
  schedule: null,

  tasks: {
    setup: {
      shell: { interpreter: 'bash', command: SETUP_CMD },
    },

    run_workload: {
      dependsOn: ['setup'],
      timeout: (8 * 60 + 5) * 60 * 1000,  // 8h 5m
      shell: {
        interpreter: 'bash',
        command: WORKLOAD_CMD,
        env: {
          SESSION_HOURS: '8',
          USER_CMD: '',
        },
      },
    },

    teardown: {
      dependsOn: ['run_workload'],
      shell: { interpreter: 'bash', command: TEARDOWN_CMD },
    },
  },
});
