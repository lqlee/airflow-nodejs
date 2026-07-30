import { dag } from 'airflow-nodejs/dag/types';

/**
 * Kubernetes Demo — run tasks as ephemeral Pods on a K8s cluster.
 *
 * Prerequisites:
 *   - kubectl on PATH, configured with a valid kubeconfig
 *   - Any cluster: minikube, kind, EKS, GKE, AKS, k3d, Rancher Desktop, etc.
 *
 * Quick start with minikube:
 *   minikube start
 *   kubectl config use-context minikube
 *   # Then trigger this dag
 *
 * Quick start with kind:
 *   kind create cluster --name airflow
 *   kubectl config use-context kind-airflow
 *
 * Each task runs `kubectl run --restart=Never --rm --attach`, which:
 *   - Creates an ephemeral Pod
 *   - Streams stdout/stderr to task logs
 *   - Deletes the Pod when it exits
 *   - Propagates the exit code (0 = success, non-zero = failure)
 *
 * Trigger:
 *   POST /dags/kubernetes_demo/trigger   body: {}
 */

export default dag({
  id: 'kubernetes_demo',
  schedule: null,

  tasks: {

    // ── Simple echo task ───────────────────────────────────────────────────
    hello: {
      kubernetes: {
        image: 'alpine:latest',
        command: ['sh', '-c', 'echo "Hello from Kubernetes! DAG_ID=$DAG_ID TASK_ID=$TASK_ID"'],
        namespace: 'default',
      },
    },

    // ── Python task in a container ─────────────────────────────────────────
    python_step: {
      dependsOn: ['hello'],
      kubernetes: {
        image: 'python:3.13-slim',
        command: [
          'python3', '-c',
          [
            'import os, sys',
            'print(f"Python {sys.version.split()[0]} on Kubernetes")',
            'print(f"DAG: {os.environ[\'DAG_ID\']}")',
            'print(f"Task: {os.environ[\'TASK_ID\']}")',
          ].join('; '),
        ],
        namespace: 'default',
        // Resource limits in Kubernetes format
        memory: '256Mi',
        cpu: '250m',
      },
    },

    // ── Custom env vars ────────────────────────────────────────────────────
    env_check: {
      dependsOn: ['hello'],
      kubernetes: {
        image: 'alpine:latest',
        command: ['sh', '-c', 'echo "REGION=$REGION ENV=$DEPLOY_ENV"'],
        env: {
          REGION: 'us-east-1',
          DEPLOY_ENV: 'staging',
        },
      },
    },

    // ── Resource-limited batch job ─────────────────────────────────────────
    batch_job: {
      dependsOn: ['python_step', 'env_check'],
      kubernetes: {
        image: 'python:3.13-slim',
        command: [
          'python3', '-c',
          [
            'import time, os',
            'print("Batch job starting...")',
            'time.sleep(1)',
            'print(f"Run: {os.environ[\'RUN_ID\']}")',
            'print("Batch job complete")',
          ].join('; '),
        ],
        namespace: 'default',
        memory: '512Mi',
        cpu: '500m',
        timeout: 5 * 60 * 1000,  // 5 min timeout
      },
    },

    // ── Optional: EKS/GKE/AKS example with IRSA/Workload Identity ──────────
    // Uncomment and fill in your cluster-specific values:
    //
    // cloud_job: {
    //   dependsOn: ['batch_job'],
    //   kubernetes: {
    //     image: 'amazon/aws-cli:latest',           // or gcr.io/google.com/cloudsdktool/cloud-sdk
    //     command: ['aws', 's3', 'ls', 's3://my-bucket/'],
    //     namespace: 'airflow',
    //     serviceAccount: 'airflow-sa',             // bound to AWS IAM role via IRSA
    //     memory: '128Mi',
    //     cpu: '100m',
    //     context: 'arn:aws:eks:us-east-1:123456789:cluster/my-cluster',
    //     kubeconfig: '/opt/airflow/.kube/config',
    //   },
    // },

  },
});
