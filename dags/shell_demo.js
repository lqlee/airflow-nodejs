import { dag } from 'airflow-nodejs/dag/types';

/**
 * Demonstrates shell tasks — each task runs a shell command instead of JS.
 *
 * Default interpreter: 'bash' (pre-installed in the airflow-nodejs Docker image).
 * Other options: 'sh' (always available), 'zsh', 'tcsh', or any absolute path.
 *
 * Context env vars injected automatically: DAG_ID, RUN_ID, TASK_ID
 */
export default dag({
  id: 'shell_demo',
  schedule: null,  // manual trigger only
  tasks: {
    system_info: {
      shell: {
        // bash array syntax and [[ ]] — requires bash (default)
        // Use single quotes to prevent JS template literal substitution.
        // $DAG_ID etc. are injected at runtime by the executor as env vars.
        command: String.raw`
          echo "=== System ==="
          uname -a
          echo "=== Bash version ==="
          bash --version | head -1
          echo "=== Context ==="
          echo "DAG=$DAG_ID  RUN=$RUN_ID  TASK=$TASK_ID"
        `,
      }
    },
    disk_check: {
      dependsOn: ['system_info'],
      shell: {
        command: `
          df -h /
          # bash arithmetic
          FREE=$(df / | awk 'NR==2{print $4}')
          echo "Free blocks: $FREE"
          [[ $FREE -gt 0 ]] && echo "Disk OK" || echo "WARNING: disk full"
        `,
      }
    },
    custom_env: {
      dependsOn: ['system_info'],
      shell: {
        command: 'echo "Greeting: $GREETING from run $RUN_ID"',
        env: { GREETING: 'hello from shell task' },
      }
    },
    sh_fallback: {
      dependsOn: ['system_info'],
      shell: {
        // Use sh explicitly — always available even without bash
        command: 'echo "POSIX sh works too: $(uname -s)"',
        interpreter: 'sh',
      }
    },
    summary: {
      dependsOn: ['disk_check', 'custom_env', 'sh_fallback'],
      shell: {
        command: 'echo "All shell tasks completed ✓  run=$RUN_ID"',
      }
    }
  }
});
