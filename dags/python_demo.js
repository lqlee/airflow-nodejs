import { dag } from 'airflow-nodejs/dag/types';

/**
 * Demonstrates python tasks — each task runs Python code instead of JS.
 *
 * Requires the python variant image:
 *   ./docker-build.sh --variant python
 *   docker run ... airflow-nodejs:python
 *
 * Context env vars available via os.environ: DAG_ID, RUN_ID, TASK_ID
 */
export default dag({
  id: 'python_demo',
  schedule: null,  // manual trigger only
  tasks: {
    inline_code: {
      python: {
        // Inline python code — quick one-liners or multi-line scripts
        code: `
import sys, os, platform
print(f"Python {sys.version}")
print(f"Platform: {platform.machine()}")
print(f"DAG={os.environ['DAG_ID']}  RUN={os.environ['RUN_ID']}  TASK={os.environ['TASK_ID']}")
`,
      }
    },
    data_processing: {
      dependsOn: ['inline_code'],
      python: {
        code: `
import json, os

# Simulate data processing
data = [{"id": i, "value": i * 2} for i in range(5)]
result = {"count": len(data), "sum": sum(d["value"] for d in data), "run_id": os.environ["RUN_ID"]}
print(json.dumps(result, indent=2))
print("Processing complete")
`,
      }
    },
    with_env: {
      dependsOn: ['inline_code'],
      python: {
        code: 'import os; print(f"REGION={os.environ[\"REGION\"]}  ENV={os.environ[\"ENV\"]}")',
        env: { REGION: 'us-central1', ENV: 'dev' },
      }
    },
    summary: {
      dependsOn: ['data_processing', 'with_env'],
      python: {
        code: 'import os; print(f"All python tasks done for run {os.environ[\'RUN_ID\']}")',
      }
    }
  }
});
