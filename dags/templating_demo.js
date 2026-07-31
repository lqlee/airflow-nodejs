import { dag } from 'airflow-nodejs/dag/types';

/**
 * Templating Demo — use {{ variable }} syntax in shell/python/java/container tasks.
 *
 * Template variables available:
 *   {{ dag_id }}       — DAG id
 *   {{ run_id }}       — run id
 *   {{ task_id }}      — task id
 *   {{ ds }}           — execution date as YYYY-MM-DD
 *   {{ ts }}           — ISO-8601 timestamp
 *   {{ ts_nodash }}    — timestamp without dashes (20240101T120000Z)
 *   {{ logical_date }} — logical_date ISO string, or '' for manual runs
 *   {{ conf.key }}     — trigger-time conf value
 *   {{ conf.a.b }}     — nested conf value
 *
 * Undefined paths render as '' (empty string).
 *
 * Trigger with conf:
 *   POST /dags/templating_demo/trigger
 *   body: { "conf": { "env": "prod", "bucket": "my-bucket", "date_override": "2024-01-01" } }
 */
export default dag({
  id: 'templating_demo',
  schedule: null,

  tasks: {

    // ── Shell task with templated command ────────────────────────────────────
    shell_demo: {
      shell: {
        interpreter: 'sh',
        command: [
          'echo "=== Template Demo ==="',
          'echo "  dag_id  : {{ dag_id }}"',
          'echo "  run_id  : {{ run_id }}"',
          'echo "  task_id : {{ task_id }}"',
          'echo "  ds      : {{ ds }}"',
          'echo "  ts      : {{ ts }}"',
          'echo "  env     : {{ conf.env }}"',
          'echo "  bucket  : {{ conf.bucket }}"',
          'echo "  undef   : [{{ conf.undefined_key }}]"',   // renders as ''
        ].join('\n'),
      }
    },

    // ── Shell task with templated env vars ───────────────────────────────────
    shell_env_demo: {
      dependsOn: ['shell_demo'],
      shell: {
        interpreter: 'sh',
        command: 'echo "MY_ENV=$MY_ENV MY_DATE=$MY_DATE MY_DAG=$MY_DAG"',
        env: {
          MY_ENV:  '{{ conf.env }}',
          MY_DATE: '{{ ds }}',
          MY_DAG:  '{{ dag_id }}',
        },
      }
    },

    // ── Python task with templated code ─────────────────────────────────────
    python_demo: {
      dependsOn: ['shell_env_demo'],
      python: {
        code: [
          'import os',
          'print("Python task with templates:")',
          'print(f"  ds     = {{ ds }}")',         // rendered before python sees it
          'print(f"  env    = {{ conf.env }}")',
          'print(f"  dag_id = {{ dag_id }}")',
        ].join('\n'),
      }
    },

  }
})
