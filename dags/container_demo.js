import { dag } from 'airflow-nodejs/dag/types';

/**
 * Demonstrates container tasks — each task runs inside its own Docker container.
 *
 * Requirements:
 *   1. Server must have Docker socket mounted:
 *      docker run -v /var/run/docker.sock:/var/run/docker.sock \
 *                 --group-add $(stat -c %g /var/run/docker.sock) \
 *                 airflow-nodejs:local
 *
 *   2. Images must be available locally. Options:
 *      a) Pre-pulled image (needs network or internal registry)
 *      b) User-supplied .tar file via requiredImages (fully offline):
 *         docker save python:3.13-slim -o dags/images/python-3.13-slim.tar
 *         Then: requiredImages: ['./images/python-3.13-slim.tar']
 *
 * Context env vars inside container: DAG_ID, RUN_ID, TASK_ID
 * XCom is NOT available in container tasks — pass data via volumes or env vars.
 *
 * To supply images offline (no registry access needed):
 *   # On a machine with internet access:
 *   mkdir -p dags/images
 *   docker save <image> -o dags/images/<name>.tar
 *   # Drop the .tar alongside the dag file — server loads it automatically
 */
export default dag({
  id: 'container_demo',
  schedule: null,

  // Optional: declare .tar files to load at dag load time (fully offline workflow).
  // Remove if images are already available locally or via registry.
  // requiredImages: [
  //   './images/python-3.13-slim.tar',
  //   './images/redis-7-alpine.tar',
  // ],

  tasks: {

    // Run inside the python variant of the server image
    python_container: {
      container: {
        image: 'airflow-nodejs:python',
        command: ['python3', '-c', [
          'import os, sys, platform',
          'print(f"Python {sys.version}")',
          'print(f"DAG_ID={os.environ[\'DAG_ID\']}")',
          'print(f"RUN_ID={os.environ[\'RUN_ID\']}")',
          'print(f"TASK_ID={os.environ[\'TASK_ID\']}")',
        ].join('\n')],
      }
    },

    // Redis container — run a command inside it
    redis_check: {
      dependsOn: ['python_container'],
      container: {
        image: 'generic.ci.artifacts.walmart.com/hub-docker-release-remote/redis:7-alpine',
        command: ['sh', '-c', 'redis-cli --version && echo "Redis container task: $TASK_ID"'],
      }
    },

    // Java container — use the java variant image
    java_container: {
      dependsOn: ['python_container'],
      container: {
        image: 'airflow-nodejs:java21',
        command: ['java', '-version'],
        env: { MY_ENV: 'demo' },
      }
    },

    summary: {
      dependsOn: ['redis_check', 'java_container'],
      container: {
        image: 'generic.ci.artifacts.walmart.com/hub-docker-release-remote/mongo:7',
        command: ['sh', '-c', 'echo "All container tasks done. Run=$RUN_ID"'],
      }
    },

  }
});
