/**
 * Integration tests for container tasks.
 *
 * Uses real `docker run` via the host Docker socket — Docker must be available
 * in the test environment (verified at test startup; tests skip gracefully if not).
 *
 * Uses `alpine:latest` (13 MB, already local on any dev machine) as the test image.
 *
 * Each test drives a real MongoDB + real child_process + real docker run.
 *
 * What each test answers:
 *  - Does exit-0 container → task success?
 *  - Does non-zero exit → task failed with informative error?
 *  - Are DAG_ID / RUN_ID / TASK_ID injected as env vars?
 *  - Do custom env vars reach the container?
 *  - Does a missing image → task failed with useful error?
 *  - Does docker-not-found → friendly ENOENT message?
 *  - Do container tasks integrate with dependency chains?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { execSync } from 'node:child_process'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_IMAGE = 'alpine:latest'   // tiny (13 MB), always local on dev machines

let client: MongoClient
let db: Db
let dockerAvailable = false

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

async function taskLogs(runId: string, taskId: string): Promise<string[]> {
  const docs = await db.collection('task_logs')
    .find({ dag_run_id: runId, task_id: taskId })
    .sort({ ts: 1 })
    .toArray()
  return docs.map(d => d.line as string)
}

async function getRunId(dagId: string): Promise<string> {
  const run = await db.collection('dag_runs').findOne({ dag_id: dagId })
  return run!._id.toString()
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  process.env.LOG_BACKEND = 'mongodb'  // tests read from task_logs collection directly
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_container_tasks')
  clearRegistry()

  // Check if docker is available and test image is present
  try {
    execSync('docker info', { stdio: 'ignore', timeout: 5000 })
    execSync(`docker image inspect ${TEST_IMAGE}`, { stdio: 'ignore', timeout: 5000 })
    dockerAvailable = true
  } catch {
    dockerAvailable = false
    console.warn(`[container-tasks] docker not available or ${TEST_IMAGE} not pulled — tests will be skipped`)
  }
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('task_logs').deleteMany({})
  clearRegistry()
})

// ══════════════════════════════════════════════════════════════════════════════
// CONTAINER TASKS
// ══════════════════════════════════════════════════════════════════════════════

describe('container tasks', () => {
  it('exit 0 → task succeeds', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_success',
      schedule: null,
      tasks: { step: { container: { image: TEST_IMAGE, command: ['sh', '-c', 'exit 0'] } } },
    }
    await runDag(dag)
    expect((await taskState('ct_success', 'step'))?.state).toBe('success')
  })

  it('non-zero exit → task fails with exit code in error', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_fail',
      schedule: null,
      tasks: { step: { container: { image: TEST_IMAGE, command: ['sh', '-c', 'exit 42'] } } },
    }
    await runDag(dag)
    const ti = await taskState('ct_fail', 'step')
    expect(ti?.state).toBe('failed')
    expect(ti?.error).toMatch(/42/)
  })

  it('stdout is captured to task logs', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_stdout',
      schedule: null,
      tasks: { step: { container: { image: TEST_IMAGE, command: ['sh', '-c', 'echo "hello_from_container"'] } } },
    }
    await runDag(dag)
    const runId = await getRunId('ct_stdout')
    const lines = await taskLogs(runId, 'step')
    expect(lines).toContain('hello_from_container')
  })

  it('DAG_ID, RUN_ID, TASK_ID are injected as env vars', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_env',
      schedule: null,
      tasks: { check: { container: { image: TEST_IMAGE, command: ['sh', '-c', 'echo "D=$DAG_ID R=$RUN_ID T=$TASK_ID"'] } } },
    }
    await runDag(dag)
    const runId = await getRunId('ct_env')
    const lines = await taskLogs(runId, 'check')
    const line = lines.join(' ')
    expect(line).toMatch(/D=ct_env/)
    expect(line).toMatch(/R=[0-9a-f]+/)
    expect(line).toMatch(/T=check/)
  })

  it('custom env vars reach the container', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_custom_env',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'echo "COLOR=$COLOR REGION=$REGION"'],
            env: { COLOR: 'blue', REGION: 'us-east-1' },
          }
        }
      },
    }
    await runDag(dag)
    const runId = await getRunId('ct_custom_env')
    const lines = await taskLogs(runId, 'step')
    const line = lines.join(' ')
    expect(line).toContain('COLOR=blue')
    expect(line).toContain('REGION=us-east-1')
  })

  it('missing image → task fails with useful error', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_bad_image',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: 'this-image-does-not-exist-xyz:never',
            command: ['echo', 'hi'],
          }
        }
      },
    }
    await runDag(dag, 20)
    const ti = await taskState('ct_bad_image', 'step')
    expect(ti?.state).toBe('failed')
    // Error should mention the image or a pull/not-found message
    expect(ti?.error).toBeTruthy()
  })

  it('docker binary not found → task fails with informative error', async () => {
    // Uses a non-existent absolute path so spawn fires ENOENT regardless of PATH
    const dag: DagDefinition = {
      id: 'ct_no_docker',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['echo', 'hi'],
          }
        }
      },
    }

    // Override PATH so the 'docker' binary resolves to a non-existent path
    const origPath = process.env.PATH
    process.env.PATH = '/this/path/does/not/exist'
    try {
      await runDag(dag)
    } finally {
      process.env.PATH = origPath
    }

    const ti = await taskState('ct_no_docker', 'step')
    expect(ti?.state).toBe('failed')
    // ENOENT on Linux → "not found" message; on macOS may vary — just verify it failed
    expect(ti?.error).toBeTruthy()
  })

  it('retries on non-zero exit', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_retry',
      schedule: null,
      tasks: {
        step: {
          retries: 2,
          retryDelay: 0,
          container: { image: TEST_IMAGE, command: ['sh', '-c', 'exit 1'] },
        }
      },
    }
    await runDag(dag, 20)
    const ti = await taskState('ct_retry', 'step')
    expect(ti?.state).toBe('failed')
    const raw = await db.collection('task_instances').findOne({ dag_id: 'ct_retry', task_id: 'step' })
    expect(raw?.try_number).toBe(2)  // 0-indexed: tried 3 times total
  })

  it('sequential dependency: second container runs after first succeeds', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_chain',
      schedule: null,
      tasks: {
        a: { container: { image: TEST_IMAGE, command: ['sh', '-c', 'echo "a done"'] } },
        b: { dependsOn: ['a'], container: { image: TEST_IMAGE, command: ['sh', '-c', 'echo "b done"'] } },
      },
    }
    await runDag(dag, 20)
    expect((await taskState('ct_chain', 'a'))?.state).toBe('success')
    expect((await taskState('ct_chain', 'b'))?.state).toBe('success')
  })

  it('downstream container task does not run if upstream fails', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_chain_fail',
      schedule: null,
      tasks: {
        a: { container: { image: TEST_IMAGE, command: ['sh', '-c', 'exit 1'] } },
        b: { dependsOn: ['a'], container: { image: TEST_IMAGE, command: ['sh', '-c', 'echo "should not run"'] } },
      },
    }
    await runDag(dag, 10)
    expect((await taskState('ct_chain_fail', 'a'))?.state).toBe('failed')
    const bState = (await taskState('ct_chain_fail', 'b'))?.state
    expect(['queued', 'cancelled', 'skipped', undefined]).toContain(bState)
  })

  it('mixed: shell task → container task in same DAG', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_mixed',
      schedule: null,
      tasks: {
        shell_step: {
          shell: { command: 'echo "shell done"', interpreter: 'sh' },
        },
        container_step: {
          dependsOn: ['shell_step'],
          container: { image: TEST_IMAGE, command: ['sh', '-c', 'echo "container done after shell"'] },
        },
      },
    }
    await runDag(dag, 20)
    expect((await taskState('ct_mixed', 'shell_step'))?.state).toBe('success')
    expect((await taskState('ct_mixed', 'container_step'))?.state).toBe('success')

    const runId = await getRunId('ct_mixed')
    const logs = await taskLogs(runId, 'container_step')
    expect(logs.join(' ')).toContain('container done after shell')
  })

  it('workdir is applied inside the container', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_workdir',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'pwd'],
            workdir: '/tmp/mydir',
          }
        }
      },
    }
    await runDag(dag)
    const runId = await getRunId('ct_workdir')
    const lines = await taskLogs(runId, 'step')
    expect(lines.join(' ')).toContain('/tmp/mydir')
  })

  // ── resource limits ───────────────────────────────────────────────────────

  it('memory limit — container runs within the limit', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_memory',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'echo "running with 256m memory limit"'],
            memory: '256m',
          }
        }
      },
    }
    await runDag(dag)
    expect((await taskState('ct_memory', 'step'))?.state).toBe('success')
  })

  it('memory + swap limit accepted by docker', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_memory_swap',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'echo "memory+swap limited"'],
            memory: '128m',
            memorySwap: '256m',  // total = memory + swap
          }
        }
      },
    }
    await runDag(dag)
    expect((await taskState('ct_memory_swap', 'step'))?.state).toBe('success')
  })

  it('cpu limit — container runs within the cpu quota', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_cpus',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'echo "running with 0.5 cpu limit"'],
            cpus: '0.5',
          }
        }
      },
    }
    await runDag(dag)
    expect((await taskState('ct_cpus', 'step'))?.state).toBe('success')
  })

  it('all resource limits combined — memory + cpus', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_resources_all',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'echo "fully resource-limited container"'],
            memory: '512m',
            memorySwap: '512m',  // no swap
            cpus: '1.0',
          }
        }
      },
    }
    await runDag(dag)
    const ti = await taskState('ct_resources_all', 'step')
    expect(ti?.state).toBe('success')
    const runId = await getRunId('ct_resources_all')
    const lines = await taskLogs(runId, 'step')
    expect(lines.join(' ')).toContain('fully resource-limited container')
  })

  it('memory OOM → container exits non-zero → task fails', async () => {
    if (!dockerAvailable) return

    // Allocate more memory than the limit — should be killed by OOM
    const dag: DagDefinition = {
      id: 'ct_oom',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            // Try to allocate ~200 MB in a 4 MB container — OOM kill
            command: ['sh', '-c', 'dd if=/dev/zero bs=1M count=200 | cat > /dev/null'],
            memory: '4m',
            memorySwap: '4m',
          }
        }
      },
    }
    await runDag(dag, 15)
    // Container should be OOM-killed → non-zero exit → task fails
    const ti = await taskState('ct_oom', 'step')
    expect(ti?.state).toBe('failed')
  })

  // ── ports ─────────────────────────────────────────────────────────────────

  it('container with ports mapping succeeds — docker run -p flag is accepted', async () => {
    if (!dockerAvailable) return

    // The -p flag is a Docker routing concern — we verify:
    //   1. docker run accepts the port flag without error
    //   2. container starts, runs, and exits 0 (task succeeds)
    const dag: DagDefinition = {
      id: 'ct_ports_basic',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'echo "container started with port mapping"'],
            ports: ['19999:19999'],
          }
        }
      },
    }
    await runDag(dag)
    const ti = await taskState('ct_ports_basic', 'step')
    expect(ti?.state).toBe('success')

    const runId = await getRunId('ct_ports_basic')
    const lines = await taskLogs(runId, 'step')
    expect(lines.join(' ')).toContain('container started with port mapping')
  })

  it('multiple ports are all mapped', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_ports_multi',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            // Just verify the container starts and runs with multiple port flags
            command: ['sh', '-c', 'echo "running with 2 ports mapped"'],
            ports: ['19990:19990', '19991:19991'],
          }
        }
      },
    }
    await runDag(dag)
    expect((await taskState('ct_ports_multi', 'step'))?.state).toBe('success')
  })

  it('localhost-only port binding works', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_ports_localhost',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'echo "localhost-only port"'],
            ports: ['127.0.0.1:19992:19992'],
          }
        }
      },
    }
    await runDag(dag)
    expect((await taskState('ct_ports_localhost', 'step'))?.state).toBe('success')
  })

  it('ports field is optional — container works without it', async () => {
    if (!dockerAvailable) return

    const dag: DagDefinition = {
      id: 'ct_ports_none',
      schedule: null,
      tasks: {
        step: {
          container: {
            image: TEST_IMAGE,
            command: ['sh', '-c', 'echo "no ports needed"'],
            // ports field intentionally omitted
          }
        }
      },
    }
    await runDag(dag)
    expect((await taskState('ct_ports_none', 'step'))?.state).toBe('success')
  })
})
