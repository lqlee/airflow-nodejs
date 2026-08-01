/**
 * Tests for the providers ecosystem.
 *
 * Two levels:
 *  1. Unit: registry + loader — register/list/get/clear, load from fixture dir
 *  2. Integration: DAG using an operator from a provider runs end-to-end
 *
 * What each test answers:
 *  - Does registerProvider/listProviders/getProvider work?
 *  - Does loadProviders discover .js files and register them?
 *  - Does a file with no valid export emit a warning (not crash)?
 *  - Does getOperator return the factory function?
 *  - Does an operator factory return a valid TaskDefinition?
 *  - Does a DAG using an operator from a provider execute and succeed?
 *  - Does the provider appear in the registry after loading?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest'
import { mkdtemp, writeFile, mkdir, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { MongoClient, type Db } from 'mongodb'
import {
  registerProvider,
  clearProviders,
  listProviders,
  getProvider,
  getOperator,
} from '../registry.js'
import { loadProviders } from '../loader.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'

let client: MongoClient
let db: Db

// ── helpers ───────────────────────────────────────────────────────────────────

async function runDag(dag: DagDefinition, maxTicks = 15): Promise<string> {
  register(dag)
  const runId = await createRun(db, dag)
  for (let i = 0; i < maxTicks; i++) await advanceRun(db, runId)
  return runId
}

async function taskState(runId: string, taskId: string) {
  return db.collection('task_instances').findOne(
    { dag_run_id: runId, task_id: taskId },
    { projection: { state: 1, error: 1, _id: 0 } }
  )
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_providers')
  clearRegistry()
  clearProviders()
})

afterAll(async () => {
  await new Promise(r => setTimeout(r, 200))
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('task_logs').deleteMany({})
  clearRegistry()
  clearProviders()
  vi.restoreAllMocks()
})

// ══════════════════════════════════════════════════════════════════════════════
// Registry unit tests
// ══════════════════════════════════════════════════════════════════════════════

describe('provider registry', () => {
  it('registers and lists a provider', () => {
    registerProvider({
      name: 'test-provider',
      version: '1.0.0',
      description: 'A test provider',
      operators: {
        EchoOperator: (opts = {}) => ({
          shell: { command: `echo "${(opts as any).message ?? 'hello'}"`, interpreter: 'sh' }
        }),
      },
      connectionTypes: ['test-conn'],
    })

    const providers = listProviders()
    expect(providers).toHaveLength(1)
    expect(providers[0].name).toBe('test-provider')
    expect(providers[0].version).toBe('1.0.0')
    expect(providers[0].operator_names).toEqual(['EchoOperator'])
    expect(providers[0].connection_types).toEqual(['test-conn'])
    expect(providers[0].source).toBe('dags/providers')
  })

  it('getProvider returns full definition including operator functions', () => {
    registerProvider({
      name: 'fn-provider',
      version: '0.1.0',
      description: '',
      operators: {
        MyOp: (opts = {}) => ({ shell: { command: 'echo ok', interpreter: 'sh' } }),
      },
      connectionTypes: [],
    })

    const def = getProvider('fn-provider')
    expect(def).toBeDefined()
    expect(typeof def!.operators.MyOp).toBe('function')
  })

  it('getOperator returns the factory function', () => {
    registerProvider({
      name: 'op-provider',
      version: '1.0.0',
      description: '',
      operators: {
        TestOp: () => ({ shell: { command: 'echo test', interpreter: 'sh' } }),
      },
      connectionTypes: [],
    })

    const factory = getOperator('op-provider', 'TestOp')
    expect(typeof factory).toBe('function')
  })

  it('getOperator returns undefined for unknown provider', () => {
    expect(getOperator('nonexistent', 'SomeOp')).toBeUndefined()
  })

  it('getOperator returns undefined for unknown operator on known provider', () => {
    registerProvider({
      name: 'partial-provider',
      version: '1.0.0',
      description: '',
      operators: { KnownOp: () => ({ shell: { command: 'echo x', interpreter: 'sh' } }) },
      connectionTypes: [],
    })
    expect(getOperator('partial-provider', 'NonExistentOp')).toBeUndefined()
  })

  it('operator factory returns valid TaskDefinition (shell form)', () => {
    registerProvider({
      name: 'shell-provider',
      version: '1.0.0',
      description: '',
      operators: {
        EchoOp: (opts = {}) => ({
          shell: {
            interpreter: 'sh',
            command: `echo "${(opts as any).msg ?? 'default'}"`,
          }
        }),
      },
      connectionTypes: [],
    })

    const factory = getOperator('shell-provider', 'EchoOp')!
    const taskDef = factory({ msg: 'hello' })
    expect(taskDef.shell).toBeDefined()
    expect(taskDef.shell!.command).toContain('hello')
  })

  it('clearProviders empties the registry', () => {
    registerProvider({
      name: 'to-clear',
      version: '1.0.0',
      description: '',
      operators: { Op: () => ({ shell: { command: 'echo x', interpreter: 'sh' } }) },
      connectionTypes: [],
    })
    expect(listProviders()).toHaveLength(1)
    clearProviders()
    expect(listProviders()).toHaveLength(0)
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// Loader tests
// ══════════════════════════════════════════════════════════════════════════════

describe('provider loader', () => {
  it('loads a valid provider file from dags/providers/', async () => {
    const tmpDir = await mkdtemp(join(tmpdir(), 'airflow-test-providers-'))
    const providersDir = join(tmpDir, 'providers')
    await mkdir(providersDir)

    await writeFile(join(providersDir, 'test-provider.js'), `
      export default {
        name: 'loaded-provider',
        version: '2.0.0',
        description: 'Loaded from file',
        operators: {
          FileOp: (opts = {}) => ({
            shell: { command: 'echo "from file"', interpreter: 'sh' }
          }),
        },
        connectionTypes: ['file-conn'],
      }
    `)

    await loadProviders(tmpDir)

    const providers = listProviders()
    expect(providers).toHaveLength(1)
    expect(providers[0].name).toBe('loaded-provider')
    expect(providers[0].operator_names).toContain('FileOp')
    expect(providers[0].connection_types).toContain('file-conn')

    await rm(tmpDir, { recursive: true })
  })

  it('gracefully handles missing dags/providers/ directory', async () => {
    const tmpDir = await mkdtemp(join(tmpdir(), 'airflow-test-noproviders-'))
    // No providers/ subdir — should not throw
    await expect(loadProviders(tmpDir)).resolves.not.toThrow()
    expect(listProviders()).toHaveLength(0)
    await rm(tmpDir, { recursive: true })
  })

  it('skips files with no valid default export', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    const tmpDir = await mkdtemp(join(tmpdir(), 'airflow-test-badprovider-'))
    const providersDir = join(tmpDir, 'providers')
    await mkdir(providersDir)

    // File with no default export
    await writeFile(join(providersDir, 'bad-provider.js'), `
      export const something = 'not a provider'
    `)

    await loadProviders(tmpDir)
    expect(listProviders()).toHaveLength(0)
    expect(warnSpy).toHaveBeenCalledWith(expect.stringContaining('no valid default export'))

    await rm(tmpDir, { recursive: true })
  })

  it('loads multiple providers from the same directory', async () => {
    const tmpDir = await mkdtemp(join(tmpdir(), 'airflow-test-multiproviders-'))
    const providersDir = join(tmpDir, 'providers')
    await mkdir(providersDir)

    for (const name of ['alpha-provider', 'beta-provider']) {
      await writeFile(join(providersDir, `${name}.js`), `
        export default {
          name: '${name}',
          version: '1.0.0',
          description: '${name} description',
          operators: { Op${name.charAt(0).toUpperCase()}: () => ({ shell: { command: 'echo ${name}', interpreter: 'sh' } }) },
          connectionTypes: [],
        }
      `)
    }

    await loadProviders(tmpDir)
    const names = listProviders().map(p => p.name).sort()
    expect(names).toEqual(['alpha-provider', 'beta-provider'])

    await rm(tmpDir, { recursive: true })
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// Integration: DAG using provider operator executes successfully
// ══════════════════════════════════════════════════════════════════════════════

describe('provider operator integration', () => {
  it('operator factory task executes and succeeds in a real DAG run', async () => {
    // Register a provider with a simple echo operator
    registerProvider({
      name: 'test-exec-provider',
      version: '1.0.0',
      description: 'Integration test provider',
      operators: {
        EchoOp: (opts = {}) => ({
          shell: {
            interpreter: 'sh',
            command: `echo "provider_operator_ran: ${(opts as any).label ?? 'default'}"`,
          }
        }),
      },
      connectionTypes: [],
    })

    const EchoOp = getOperator('test-exec-provider', 'EchoOp')!

    const dag: DagDefinition = {
      id: 'prov_integration',
      schedule: null,
      tasks: {
        // Use operator factory to build the task definition
        step1: EchoOp({ label: 'hello-from-provider' }),
        step2: {
          dependsOn: ['step1'],
          ...EchoOp({ label: 'step2-done' }),
        },
      },
    }

    const runId = await runDag(dag)

    const s1 = await taskState(runId, 'step1')
    const s2 = await taskState(runId, 'step2')
    expect(s1?.state).toBe('success')
    expect(s2?.state).toBe('success')

    // Verify output reached task logs
    const logs = await db.collection('task_logs')
      .find({ dag_run_id: runId, task_id: 'step1' })
      .toArray()
    expect(logs.some(l => (l.line as string).includes('provider_operator_ran'))).toBe(true)
  })

  it('provider appears in listProviders after registration', () => {
    registerProvider({
      name: 'visible-provider',
      version: '3.1.4',
      description: 'Should appear in /providers',
      operators: {
        VisibleOp: () => ({ run: async () => 'visible' }),
      },
      connectionTypes: ['visible-conn'],
    })

    const record = listProviders().find(p => p.name === 'visible-provider')
    expect(record).toBeDefined()
    expect(record!.version).toBe('3.1.4')
    expect(record!.operator_names).toContain('VisibleOp')
    expect(record!.connection_types).toContain('visible-conn')
  })
})
