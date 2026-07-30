/**
 * Unit tests for Kubernetes task execution.
 *
 * Uses vi.mock to intercept spawn calls — no real cluster or kubeconfig needed.
 * The argv-capture tests verify the exact kubectl flags built by executeKubernetesTask.
 *
 * A separate e2e suite (not included here) would run against minikube/kind
 * and verify actual Pod exit-code propagation.
 *
 * What each test answers:
 *  - Is the correct kubectl sub-command and flags built?
 *  - Are DAG_ID / RUN_ID / TASK_ID injected as --env flags?
 *  - Are resource limits translated to --limits= / --requests= correctly?
 *  - Are kubeconfig / context / serviceAccount flags forwarded?
 *  - Does `buildPodName` produce RFC-1123-valid names?
 *  - Does `kubectl` not found → task fails with informative error?
 *  - Is `--` separator inserted before the command override?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest'
import { EventEmitter } from 'node:events'
import { Readable } from 'node:stream'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import { buildPodName } from '../executor.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'

let client: MongoClient
let db: Db

// ── captured argv storage ─────────────────────────────────────────────────────

/** Set by each test before running the dag; read after to assert argv. */
let capturedKubectlArgs: string[] = []
let mockExitCode = 0
let mockEmitEnoent = false

// ── ESM-compatible mock of node:child_process ─────────────────────────────────

vi.mock('node:child_process', async (importOriginal) => {
  const original = await importOriginal<typeof import('node:child_process')>()

  const fakeFork = (...args: Parameters<typeof original.fork>) => original.fork(...args)

  const fakeSpawn = (cmd: string, args: string[], _opts?: object) => {
    const fake = new EventEmitter() as any
    fake.stdout = new Readable({ read() { this.push(null) } })
    fake.stderr = new Readable({ read() { this.push(null) } })
    fake.kill = () => {}

    if (cmd === 'kubectl') {
      capturedKubectlArgs = [...args]

      if (mockEmitEnoent) {
        setImmediate(() => {
          const err = Object.assign(new Error(`spawn kubectl ENOENT`), { code: 'ENOENT' })
          fake.emit('error', err)
        })
      } else {
        setImmediate(() => fake.emit('close', mockExitCode))
      }
    } else {
      // Other binaries (e.g. bash, docker) — pass through to real spawn
      return original.spawn(cmd, args as any, _opts as any)
    }
    return fake
  }

  return { ...original, spawn: fakeSpawn, fork: fakeFork }
})

// ── helpers ───────────────────────────────────────────────────────────────────

async function runDag(dag: DagDefinition, maxTicks = 15): Promise<void> {
  register(dag)
  const runId = await createRun(db, dag)
  for (let i = 0; i < maxTicks; i++) await advanceRun(db, runId)
}

async function taskState(dagId: string, taskId: string) {
  return db.collection('task_instances').findOne(
    { dag_id: dagId, task_id: taskId },
    { projection: { state: 1, error: 1, _id: 0 } }
  )
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_kubernetes_tasks')
  clearRegistry()
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('task_logs').deleteMany({})
  capturedKubectlArgs = []
  mockExitCode = 0
  mockEmitEnoent = false
  clearRegistry()
})

// ══════════════════════════════════════════════════════════════════════════════
// buildPodName — RFC-1123 compliance (pure unit tests, no DB needed)
// ══════════════════════════════════════════════════════════════════════════════

describe('buildPodName', () => {
  it('produces lowercase output', () => {
    const name = buildPodName(undefined, 'MyDag', 'abc123', 'MyTask')
    expect(name).toBe(name.toLowerCase())
  })

  it('replaces underscores and dots with hyphens', () => {
    const name = buildPodName(undefined, 'my_dag.id', 'abc123', 'my_task')
    expect(name).not.toMatch(/[_.]/)
  })

  it('result is ≤63 chars', () => {
    const name = buildPodName(
      undefined,
      'a-very-long-dag-id-that-exceeds-limits-easily',
      'f'.repeat(32),
      'a-very-long-task-id-too'
    )
    expect(name.length).toBeLessThanOrEqual(63)
  })

  it('does not start or end with a hyphen', () => {
    const name = buildPodName('---prefix---', 'dag', 'run123', 'task')
    expect(name).not.toMatch(/^-/)
    expect(name).not.toMatch(/-$/)
  })

  it('uses custom prefix when provided', () => {
    const name = buildPodName('my-job', 'dag', 'abc999', 'task')
    expect(name).toMatch(/^my-job/)
  })

  it('only contains alphanumeric and hyphen characters', () => {
    const name = buildPodName(undefined, 'dag/with:special!chars', 'run_id-123', 'task@test')
    expect(name).toMatch(/^[a-z0-9-]+$/)
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// kubectl argv — verified via hoisted vi.mock on node:child_process
// ══════════════════════════════════════════════════════════════════════════════

describe('kubernetes task argv', () => {
  it('passes run + --restart Never + --rm + --attach', async () => {
    const dag: DagDefinition = {
      id: 'k8s_basic',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest' } } },
    }
    await runDag(dag)
    expect(capturedKubectlArgs[0]).toBe('run')
    expect(capturedKubectlArgs).toContain('--restart')
    expect(capturedKubectlArgs[capturedKubectlArgs.indexOf('--restart') + 1]).toBe('Never')
    expect(capturedKubectlArgs).toContain('--rm')
    expect(capturedKubectlArgs).toContain('--attach')
  })

  it('image is passed as --image <value>', async () => {
    const dag: DagDefinition = {
      id: 'k8s_image',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'python:3.13-slim' } } },
    }
    await runDag(dag)
    expect(capturedKubectlArgs).toContain('--image')
    expect(capturedKubectlArgs[capturedKubectlArgs.indexOf('--image') + 1]).toBe('python:3.13-slim')
  })

  it('default namespace is "default"', async () => {
    const dag: DagDefinition = {
      id: 'k8s_ns_default',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest' } } },
    }
    await runDag(dag)
    expect(capturedKubectlArgs).toContain('--namespace')
    expect(capturedKubectlArgs[capturedKubectlArgs.indexOf('--namespace') + 1]).toBe('default')
  })

  it('custom namespace is forwarded', async () => {
    const dag: DagDefinition = {
      id: 'k8s_ns_custom',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest', namespace: 'production' } } },
    }
    await runDag(dag)
    expect(capturedKubectlArgs[capturedKubectlArgs.indexOf('--namespace') + 1]).toBe('production')
  })

  it('DAG_ID, RUN_ID, TASK_ID are injected via --env flags', async () => {
    const dag: DagDefinition = {
      id: 'k8s_env_inject',
      schedule: null,
      tasks: { check: { kubernetes: { image: 'alpine:latest' } } },
    }
    await runDag(dag)
    const envValues: string[] = []
    for (let i = 0; i < capturedKubectlArgs.length; i++) {
      if (capturedKubectlArgs[i] === '--env') envValues.push(capturedKubectlArgs[i + 1])
    }
    expect(envValues.some(v => v.startsWith('DAG_ID=k8s_env_inject'))).toBe(true)
    expect(envValues.some(v => v.startsWith('RUN_ID='))).toBe(true)
    expect(envValues.some(v => v.startsWith('TASK_ID=check'))).toBe(true)
  })

  it('custom env vars are forwarded via --env', async () => {
    const dag: DagDefinition = {
      id: 'k8s_env_custom',
      schedule: null,
      tasks: {
        step: {
          kubernetes: {
            image: 'alpine:latest',
            env: { MY_VAR: 'hello', REGION: 'us-west-2' },
          }
        }
      },
    }
    await runDag(dag)
    const envValues: string[] = []
    for (let i = 0; i < capturedKubectlArgs.length; i++) {
      if (capturedKubectlArgs[i] === '--env') envValues.push(capturedKubectlArgs[i + 1])
    }
    expect(envValues).toContain('MY_VAR=hello')
    expect(envValues).toContain('REGION=us-west-2')
  })

  it('memory limit → --limits=memory=<v> and --requests=memory=<v>', async () => {
    const dag: DagDefinition = {
      id: 'k8s_mem',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest', memory: '512Mi' } } },
    }
    await runDag(dag)
    expect(capturedKubectlArgs.some(a => a.startsWith('--limits=') && a.includes('memory=512Mi'))).toBe(true)
    expect(capturedKubectlArgs.some(a => a.startsWith('--requests=') && a.includes('memory=512Mi'))).toBe(true)
  })

  it('cpu limit → --limits=cpu=<v> and --requests=cpu=<v>', async () => {
    const dag: DagDefinition = {
      id: 'k8s_cpu',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest', cpu: '500m' } } },
    }
    await runDag(dag)
    expect(capturedKubectlArgs.some(a => a.startsWith('--limits=') && a.includes('cpu=500m'))).toBe(true)
    expect(capturedKubectlArgs.some(a => a.startsWith('--requests=') && a.includes('cpu=500m'))).toBe(true)
  })

  it('memory + cpu both set → combined in --limits and --requests flags', async () => {
    const dag: DagDefinition = {
      id: 'k8s_both_limits',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest', memory: '1Gi', cpu: '1' } } },
    }
    await runDag(dag)
    const limitsArg = capturedKubectlArgs.find(a => a.startsWith('--limits='))
    const requestsArg = capturedKubectlArgs.find(a => a.startsWith('--requests='))
    expect(limitsArg).toContain('memory=1Gi')
    expect(limitsArg).toContain('cpu=1')
    expect(requestsArg).toContain('memory=1Gi')
    expect(requestsArg).toContain('cpu=1')
  })

  it('no resource limits → no --limits or --requests flags', async () => {
    const dag: DagDefinition = {
      id: 'k8s_no_limits',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest' } } },
    }
    await runDag(dag)
    expect(capturedKubectlArgs.some(a => a.startsWith('--limits='))).toBe(false)
    expect(capturedKubectlArgs.some(a => a.startsWith('--requests='))).toBe(false)
  })

  it('command override is placed after "--" separator', async () => {
    const dag: DagDefinition = {
      id: 'k8s_cmd',
      schedule: null,
      tasks: {
        step: {
          kubernetes: {
            image: 'python:3.13-slim',
            command: ['python3', '-c', 'print("hello")'],
          }
        }
      },
    }
    await runDag(dag)
    const dashDash = capturedKubectlArgs.indexOf('--')
    expect(dashDash).toBeGreaterThan(-1)
    expect(capturedKubectlArgs.slice(dashDash + 1)).toEqual(['python3', '-c', 'print("hello")'])
  })

  it('no command override → no "--" standalone arg', async () => {
    const dag: DagDefinition = {
      id: 'k8s_no_cmd',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest' } } },
    }
    await runDag(dag)
    expect(capturedKubectlArgs.includes('--')).toBe(false)
  })

  it('kubeconfig path is forwarded via --kubeconfig', async () => {
    const dag: DagDefinition = {
      id: 'k8s_kubeconfig',
      schedule: null,
      tasks: {
        step: {
          kubernetes: { image: 'alpine:latest', kubeconfig: '/home/user/.kube/my-cluster' }
        }
      },
    }
    await runDag(dag)
    expect(capturedKubectlArgs).toContain('--kubeconfig')
    expect(capturedKubectlArgs[capturedKubectlArgs.indexOf('--kubeconfig') + 1]).toBe('/home/user/.kube/my-cluster')
  })

  it('context is forwarded via --context', async () => {
    const dag: DagDefinition = {
      id: 'k8s_context',
      schedule: null,
      tasks: {
        step: { kubernetes: { image: 'alpine:latest', context: 'my-eks-cluster' } },
      },
    }
    await runDag(dag)
    expect(capturedKubectlArgs).toContain('--context')
    expect(capturedKubectlArgs[capturedKubectlArgs.indexOf('--context') + 1]).toBe('my-eks-cluster')
  })

  it('serviceAccount is forwarded via --serviceaccount', async () => {
    const dag: DagDefinition = {
      id: 'k8s_sa',
      schedule: null,
      tasks: {
        step: { kubernetes: { image: 'alpine:latest', serviceAccount: 'my-sa' } },
      },
    }
    await runDag(dag)
    expect(capturedKubectlArgs).toContain('--serviceaccount')
    expect(capturedKubectlArgs[capturedKubectlArgs.indexOf('--serviceaccount') + 1]).toBe('my-sa')
  })

  it('kubectl not found (ENOENT) → task fails with informative error', async () => {
    mockEmitEnoent = true
    const dag: DagDefinition = {
      id: 'k8s_no_kubectl',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest' } } },
    }
    await runDag(dag)
    const ti = await taskState('k8s_no_kubectl', 'step')
    expect(ti?.state).toBe('failed')
    expect(ti?.error).toMatch(/kubectl/i)
  })

  it('non-zero exit code → task fails', async () => {
    mockExitCode = 1
    const dag: DagDefinition = {
      id: 'k8s_fail',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest' } } },
    }
    await runDag(dag)
    const ti = await taskState('k8s_fail', 'step')
    expect(ti?.state).toBe('failed')
  })

  it('exit code 0 → task succeeds', async () => {
    mockExitCode = 0
    const dag: DagDefinition = {
      id: 'k8s_success',
      schedule: null,
      tasks: { step: { kubernetes: { image: 'alpine:latest' } } },
    }
    await runDag(dag)
    const ti = await taskState('k8s_success', 'step')
    expect(ti?.state).toBe('success')
  })
})
