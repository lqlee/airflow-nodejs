import { dag } from 'airflow-nodejs/dag/types';

/**
 * Demonstrates shell tasks — each task runs a shell command instead of JS.
 *
 * Default interpreter: 'sh' (always available on Alpine/any POSIX system).
 * For bash-specific syntax, set interpreter: 'bash' (must be installed).
 * Other shells: 'zsh', 'tcsh', 'fish', or any absolute path like '/usr/bin/python3'.
 */
export default dag({
  id: 'shell_demo',
  schedule: null,  // manual trigger only
  tasks: {
    system_info: {
      shell: {
        // Context env vars: DAG_ID, RUN_ID, TASK_ID are injected automatically
        command: 'echo "=== System ===" && uname -a && echo "=== Date ===" && date && echo "DAG=$DAG_ID RUN=$RUN_ID TASK=$TASK_ID"',
        // interpreter defaults to 'sh' — works on Alpine without extra packages
      }
    },
    disk_check: {
      dependsOn: ['system_info'],
      shell: {
        command: 'df -h / && echo "Disk check OK"',
      }
    },
    env_vars: {
      dependsOn: ['system_info'],
      shell: {
        command: 'echo "Custom env: GREETING=$GREETING"',
        env: { GREETING: 'hello from shell task' },
      }
    },
    summary: {
      dependsOn: ['disk_check', 'env_vars'],
      shell: {
        command: 'echo "All shell tasks completed for run: $RUN_ID"',
      }
    }
  }
});
