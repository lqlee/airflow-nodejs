import { dag } from 'airflow-nodejs/dag/types';

/**
 * Dev Session (Container) — run a long-lived user session inside a Docker container.
 *
 * The server needs Docker socket mounted:
 *   docker run -v /var/run/docker.sock:/var/run/docker.sock \
 *              --group-add $(stat -c %g /var/run/docker.sock) \
 *              -p 8888:8888 \        ← forward Jupyter port to host
 *              airflow-nodejs:local
 *
 * ── Trigger examples ────────────────────────────────────────────────────────
 *
 *   Default Python session (8h, heartbeat only):
 *     POST /dags/dev_session_container/trigger   body: {}
 *
 *   Run a Python script:
 *     { "conf": { "USER_CMD": "python3 /workspace/my_analysis.py" } }
 *
 *   Short test (7 seconds):
 *     { "conf": { "SESSION_HOURS": "0.002", "USER_CMD": "echo done" } }
 *
 * ── Jupyter notebook variant (separate dag below) ────────────────────────────
 *   See the commented task at the bottom — use a Jupyter image + ports mapping.
 *
 * ── Logs ────────────────────────────────────────────────────────────────────
 *   All container stdout/stderr captured to task logs — visible in the UI.
 */

// ── Python dev session ────────────────────────────────────────────────────────

const PYTHON_SESSION_CODE = [
  'import os, sys, time, datetime',
  '',
  'dag_id = os.environ["DAG_ID"]',
  'run_id = os.environ["RUN_ID"]',
  'task_id = os.environ["TASK_ID"]',
  'user_cmd = os.environ.get("USER_CMD", "").strip()',
  'session_hours = float(os.environ.get("SESSION_HOURS", "8"))',
  'session_secs = int(session_hours * 3600)',
  '',
  'print("=" * 50)',
  'print("  Container Dev Session (Python)")',
  'print(f"  Python : {sys.version.split()[0]}")',
  'print(f"  DAG    : {dag_id}")',
  'print(f"  Run ID : {run_id}")',
  'print(f"  Started: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}")',
  'print("=" * 50)',
  'print()',
  '',
  'if user_cmd:',
  '    print(f"=== Running: {user_cmd} ===")',
  '    import subprocess',
  '    result = subprocess.run(user_cmd, shell=True, text=True)',
  '    print(f"\\n=== Command exited with code {result.returncode} ===")',
  'else:',
  '    print(f"=== Holding session open for {session_hours}h ===")',
  '    print(\'    Trigger with conf: { "USER_CMD": "your code" }\')',
  '    print()',
  '    elapsed = 0',
  '    interval = 1800',
  '    while elapsed < session_secs:',
  '        remaining = session_secs - elapsed',
  '        h, m = divmod(remaining, 3600)',
  '        print(f"  [{datetime.datetime.now():%H:%M:%S}] Alive — {h}h {m//60}m remaining")',
  '        sys.stdout.flush()',
  '        sleep_for = min(interval, remaining)',
  '        time.sleep(sleep_for)',
  '        elapsed += sleep_for',
  '    print("\\n=== Session duration elapsed ===")',
].join('\n');

export default dag({
  id: 'dev_session_container',
  schedule: null,

  // Uncomment to pre-load custom images from .tar files:
  // requiredImages: ['./images/my-custom-env.tar'],

  tasks: {

    // ── Step 1: Log session start ───────────────────────────────────────────
    setup: {
      shell: {
        interpreter: 'sh',
        command: [
          'echo "======================================"',
          'echo "  Container Dev Session Starting"',
          'echo "  Run ID: $RUN_ID"',
          'echo "  Started: $(date)"',
          'echo "======================================"',
        ].join('\n'),
      }
    },

    // ── Step 2: Run workload inside a container ─────────────────────────────
    // Default: Python 3.13. Override image at trigger time via conf.
    run_in_container: {
      dependsOn: ['setup'],
      timeout: (8 * 60 + 10) * 60 * 1000,  // 8h 10m
      container: {
        image: 'airflow-nodejs:python',
        command: ['python3', '-c', PYTHON_SESSION_CODE],
        env: {
          SESSION_HOURS: '8',   // override at trigger time
          USER_CMD: '',          // empty = hold session open
        },
        // Port mappings: expose ports from container to host
        // Useful for web servers, debuggers, APIs started inside the container.
        // ports: ['8080:8080'],
      }
    },

    // ── Jupyter notebook example (uncomment to use) ─────────────────────────
    // Starts a Jupyter notebook server — accessible at http://localhost:8888
    // Requires: docker pull jupyter/scipy-notebook:latest  (or use requiredImages)
    //
    // jupyter_session: {
    //   dependsOn: ['setup'],
    //   timeout: (8 * 60 + 10) * 60 * 1000,
    //   container: {
    //     image: 'jupyter/scipy-notebook:latest',
    //     command: [
    //       'jupyter', 'notebook',
    //       '--ip=0.0.0.0',
    //       '--port=8888',
    //       '--no-browser',
    //       '--NotebookApp.token=',
    //       '--NotebookApp.password=',
    //     ],
    //     ports: ['8888:8888'],                  // → http://localhost:8888
    //     volumes: ['/host/notebooks:/home/jovyan/work'],
    //   }
    // },

    // ── Step 3: Report completion ──────────────────────────────────────────
    teardown: {
      dependsOn: ['run_in_container'],
      shell: {
        interpreter: 'sh',
        command: [
          'echo "======================================"',
          'echo "  Container Dev Session Ended"',
          'echo "  Run ID: $RUN_ID"',
          'echo "  Ended: $(date)"',
          'echo "======================================"',
        ].join('\n'),
      }
    },

  }
});
