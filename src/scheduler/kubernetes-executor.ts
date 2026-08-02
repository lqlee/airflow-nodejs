/**
 * Kubernetes Executor — runs each task as an ephemeral Kubernetes Pod.
 *
 * Equivalent to Airflow's KubernetesExecutor. When enabled, every task that
 * would normally fork a child process is instead dispatched as a `kubectl run`
 * pod. The pod runs the same worker image and worker CLI, connecting back to
 * the same MongoDB instance.
 *
 * Enable via environment variables:
 *   KUBERNETES_EXECUTOR=true
 *   KUBERNETES_NAMESPACE=airflow          (default: 'default')
 *   KUBERNETES_IMAGE=airflow-nodejs:local (image that contains dist/)
 *   KUBERNETES_KUBECONFIG=/path/to/kubeconfig (optional)
 *   KUBERNETES_CONTEXT=my-cluster         (optional)
 *   KUBERNETES_SERVICE_ACCOUNT=airflow-sa (optional; for IRSA/Workload Identity)
 *   KUBERNETES_CPU_REQUEST=100m           (optional)
 *   KUBERNETES_MEMORY_REQUEST=256Mi       (optional)
 *
 * The pod runs:
 *   node dist/scheduler/worker-cli.js --dag-run-id <id> --task-id <id>
 *
 * The worker-cli reads the task function from DB (by dag_id+task_id),
 * executes it, and exits. The pod is ephemeral (--restart=Never --rm).
 *
 * Requirements:
 *   - kubectl on PATH, configured for the target cluster
 *   - The scheduler has `pods/create` + `pods/delete` RBAC permissions
 *   - MongoDB reachable from within the cluster (MONGO_URL must be cluster-internal)
 *   - AIRFLOW_IMAGE must be pullable from within the cluster
 *
 * Limitations (same as Airflow KubernetesExecutor):
 *   - Sensors are not supported (require rescheduling; use local mode for sensors)
 *   - Deferred tasks are not supported in this mode
 *   - BullMQ and Kubernetes executor are mutually exclusive
 *
 * ⚠️  This executor requires cluster access with pods/create permission.
 *     Unit tests verify argv construction; live execution requires a working cluster.
 */

import { spawn } from 'node:child_process'
import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { buildPodName, scheduleRetry } from './executor.js'
import { acquire, release } from './pool.js'
import { acquirePool, releasePool } from '../pools/index.js'
import { appendLog } from '../logs/index.js'
import { createInterface } from 'node:readline'
import { recordTry } from './tries.js'

export const USE_KUBERNETES_EXECUTOR = Boolean(process.env.KUBERNETES_EXECUTOR)

export interface KubernetesExecutorConfig {
  namespace:       string
  image:           string
  kubeconfig?:     string
  context?:        string
  serviceAccount?: string
  cpuRequest?:     string
  memoryRequest?:  string
}

export function getKubernetesExecutorConfig(): KubernetesExecutorConfig {
  return {
    namespace:      process.env.KUBERNETES_NAMESPACE      ?? 'default',
    image:          process.env.KUBERNETES_IMAGE          ?? 'airflow-nodejs:local',
    kubeconfig:     process.env.KUBERNETES_KUBECONFIG,
    context:        process.env.KUBERNETES_CONTEXT,
    serviceAccount: process.env.KUBERNETES_SERVICE_ACCOUNT,
    cpuRequest:     process.env.KUBERNETES_CPU_REQUEST,
    memoryRequest:  process.env.KUBERNETES_MEMORY_REQUEST,
  }
}

/**
 * Build the kubectl argv for dispatching a task as a pod.
 * Exported for unit testing — does not execute.
 */
export function buildKubectlArgs(
  ti: TaskInstance,
  cfg: KubernetesExecutorConfig,
): string[] {
  const podName = buildPodName(
    undefined,
    ti.dag_id,
    ti.dag_run_id,
    ti.task_id,
  )

  const args: string[] = [
    'run', podName,
    '--image', cfg.image,
    '--namespace', cfg.namespace,
    '--restart', 'Never',
    '--rm',
    '--attach',
    '--quiet',
  ]

  if (cfg.kubeconfig)     args.push('--kubeconfig', cfg.kubeconfig)
  if (cfg.context)        args.push('--context',    cfg.context)
  if (cfg.serviceAccount) args.push('--serviceaccount', cfg.serviceAccount)

  // Resource requests (Guaranteed QoS — request = limit)
  const limits: string[] = []
  const requests: string[] = []
  if (cfg.cpuRequest)    { limits.push(`cpu=${cfg.cpuRequest}`);    requests.push(`cpu=${cfg.cpuRequest}`) }
  if (cfg.memoryRequest) { limits.push(`memory=${cfg.memoryRequest}`); requests.push(`memory=${cfg.memoryRequest}`) }
  if (limits.length)   args.push(`--limits=${limits.join(',')}`)
  if (requests.length) args.push(`--requests=${requests.join(',')}`)

  // Pass through env vars the worker needs to connect back to MongoDB
  const envVars: Record<string, string | undefined> = {
    MONGO_URL:        process.env.MONGO_URL,
    DB_NAME:          process.env.DB_NAME,
    ENCRYPTION_KEY:   process.env.ENCRYPTION_KEY,
    // Task identity — worker-cli reads these
    K8S_EXEC_DAG_ID:      ti.dag_id,
    K8S_EXEC_RUN_ID:      ti.dag_run_id,
    K8S_EXEC_TASK_ID:     ti.task_id,
    K8S_EXEC_MAP_INDEX:   ti.map_index != null ? String(ti.map_index) : '',
  }
  for (const [k, v] of Object.entries(envVars)) {
    if (v !== undefined) args.push('--env', `${k}=${v}`)
  }

  // Pod command: run the worker-cli
  args.push('--', 'node', 'dist/scheduler/worker-cli.js')

  return args
}

/**
 * Dispatch a task as a Kubernetes pod.
 * Blocks until the pod exits (kubectl --attach).
 * Stdout/stderr are piped to task logs.
 */
export async function executeTaskOnKubernetes(
  db: Db,
  ti: TaskInstance,
): Promise<void> {
  if (ti.is_sensor) {
    await markFailed(db, ti, 'Sensor tasks are not supported by the Kubernetes executor — use local mode')
    return
  }

  const cfg = getKubernetesExecutorConfig()
  const args = buildKubectlArgs(ti, cfg)
  const podName = args[1]  // second arg is the pod name

  await acquire()
  if (ti.pool) await acquirePool(db, ti.pool)

  console.log(`[k8s-executor] dispatching ${ti.dag_id}.${ti.task_id} → pod ${podName}`)

  return new Promise((done) => {
    const child = spawn('kubectl', args)

    let timedOut = false
    let errored = false
    let killTimer: ReturnType<typeof setTimeout> | null = null

    if (ti.timeout_ms > 0) {
      killTimer = setTimeout(() => {
        timedOut = true
        spawn('kubectl', ['delete', 'pod', podName, '--namespace', cfg.namespace, '--force', '--grace-period=0'], { stdio: 'ignore' })
        child.kill('SIGTERM')
        const msg = `Kubernetes executor: task timed out after ${ti.timeout_ms}ms`
        console.error(`[k8s-executor] ⏱ ${ti.dag_id}.${ti.task_id}: ${msg}`)
        release()
        if (ti.pool) releasePool(ti.pool)
        void recordTry(db, ti, 'failed', new Date(), msg)
        void markFailed(db, ti, msg).then(() => done())
      }, ti.timeout_ms)
    }

    const clearKillTimer = () => { if (killTimer) { clearTimeout(killTimer); killTimer = null } }

    const stderrLines: string[] = []

    createInterface({ input: child.stdout }).on('line', (line) => {
      process.stdout.write(`${line}\n`)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stdout', line)
    })
    createInterface({ input: child.stderr }).on('line', (line) => {
      process.stderr.write(`${line}\n`)
      stderrLines.push(line)
      void appendLog(db, ti.dag_run_id, ti.dag_id, ti.task_id, 'stderr', line)
    })

    child.on('error', async (err) => {
      if (timedOut) return
      errored = true
      clearKillTimer()
      release()
      if (ti.pool) releasePool(ti.pool)
      const msg = (err as NodeJS.ErrnoException).code === 'ENOENT'
        ? `kubectl not found — is it installed and on PATH?`
        : err.message
      void recordTry(db, ti, 'failed', new Date(), msg)
      await markFailed(db, ti, msg)
      done()
    })

    child.on('close', async (code) => {
      if (timedOut || errored) return
      clearKillTimer()
      release()
      if (ti.pool) releasePool(ti.pool)
      const endedAt = new Date()

      if (code === 0) {
        void recordTry(db, ti, 'success', endedAt)
        await markSuccess(db, ti)
        console.log(`[k8s-executor] ✓ ${ti.dag_id}.${ti.task_id}`)
      } else {
        const stderr = stderrLines.slice(-5).join('\n')
        const error = `Kubernetes executor: pod exited with code ${code}${stderr ? `: ${stderr}` : ''}`
        if (ti.try_number < ti.max_retries) {
          void recordTry(db, ti, 'failed', endedAt, error)
          await scheduleRetry(db, ti, error)
          console.warn(`[k8s-executor] ↩ ${ti.dag_id}.${ti.task_id} retrying`)
        } else {
          void recordTry(db, ti, 'failed', endedAt, error)
          await markFailed(db, ti, error)
          console.error(`[k8s-executor] ✗ ${ti.dag_id}.${ti.task_id}: ${error}`)
        }
      }
      done()
    })
  })
}

// ── Internal helpers ──────────────────────────────────────────────────────────

function tiFilter(ti: TaskInstance) {
  return { dag_run_id: ti.dag_run_id, task_id: ti.task_id, map_index: ti.map_index ?? null }
}

async function markSuccess(db: Db, ti: TaskInstance): Promise<void> {
  await db.collection('task_instances').updateOne(tiFilter(ti), { $set: { state: 'success', ended_at: new Date() } })
}

async function markFailed(db: Db, ti: TaskInstance, error: string): Promise<void> {
  await db.collection('task_instances').updateOne(tiFilter(ti), { $set: { state: 'failed', ended_at: new Date(), error } })
}
