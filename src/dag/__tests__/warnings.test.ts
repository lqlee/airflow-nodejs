/**
 * Dag Warnings tests.
 *
 * analyzeWarnings() is pure — tested without DB or filesystem.
 * API tests use setDagWarnings() to seed the in-memory registry directly.
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { analyzeWarnings } from '../warnings.js'
import { setDagWarnings, getDagWarnings } from '../import-errors.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../registry.js'
import type { DagDefinition } from '../types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_dag_warnings')
  clearRegistry()
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

afterEach(() => {
  setDagWarnings([])
  clearRegistry()
})

// ── analyzeWarnings — pure unit ───────────────────────────────────────────

describe('analyzeWarnings — no_tasks', () => {
  it('warns when dag has zero tasks', () => {
    const dag: DagDefinition = { id: 'empty_dag', schedule: null, tasks: {} }
    const ws = analyzeWarnings(dag)
    expect(ws).toHaveLength(1)
    expect(ws[0].warning_type).toBe('no_tasks')
    expect(ws[0].dag_id).toBe('empty_dag')
  })
})

describe('analyzeWarnings — no_run_logic', () => {
  it('warns for task with no run, poke, or expand', () => {
    const dag: DagDefinition = {
      id: 'noop_dag', schedule: null,
      tasks: { gate: {} },  // no run/poke/expand
    }
    const ws = analyzeWarnings(dag)
    const w = ws.find(w => w.warning_type === 'no_run_logic')
    expect(w).toBeDefined()
    expect(w?.task_ids).toContain('gate')
  })

  it('does NOT warn for task with run body', () => {
    const dag: DagDefinition = {
      id: 'ok_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    const ws = analyzeWarnings(dag)
    expect(ws.filter(w => w.warning_type === 'no_run_logic')).toHaveLength(0)
  })

  it('does NOT warn for sensor (poke) task', () => {
    const dag: DagDefinition = {
      id: 'sensor_dag', schedule: null,
      tasks: { wait: { poke: async () => true } },
    }
    const ws = analyzeWarnings(dag)
    expect(ws.filter(w => w.warning_type === 'no_run_logic')).toHaveLength(0)
  })

  it('does NOT warn for expand-only task', () => {
    const dag: DagDefinition = {
      id: 'expand_dag', schedule: null,
      tasks: { fan: { expand: ['a', 'b'], run: async () => {} } },
    }
    const ws = analyzeWarnings(dag)
    expect(ws.filter(w => w.warning_type === 'no_run_logic')).toHaveLength(0)
  })
})

describe('analyzeWarnings — unknown_dependency', () => {
  it('warns when depends_on references non-existent task', () => {
    const dag: DagDefinition = {
      id: 'dep_dag', schedule: null,
      tasks: {
        step: { dependsOn: ['ghost_task'], run: async () => {} },
      },
    }
    const ws = analyzeWarnings(dag)
    const w = ws.find(w => w.warning_type === 'unknown_dependency')
    expect(w).toBeDefined()
    expect(w?.task_ids).toContain('step')
    expect(w?.message).toContain('ghost_task')
  })

  it('does NOT warn for valid dependency', () => {
    const dag: DagDefinition = {
      id: 'valid_dep_dag', schedule: null,
      tasks: {
        a: { run: async () => {} },
        b: { dependsOn: ['a'], run: async () => {} },
      },
    }
    const ws = analyzeWarnings(dag)
    expect(ws.filter(w => w.warning_type === 'unknown_dependency')).toHaveLength(0)
  })
})

describe('analyzeWarnings — unknown_group', () => {
  it('warns when task.group references undefined group', () => {
    const dag: DagDefinition = {
      id: 'group_dag', schedule: null,
      groups: { existing_group: {} },
      tasks: {
        step: { group: 'nonexistent_group', run: async () => {} },
      },
    }
    const ws = analyzeWarnings(dag)
    const w = ws.find(w => w.warning_type === 'unknown_group')
    expect(w).toBeDefined()
    expect(w?.task_ids).toContain('step')
  })

  it('does NOT warn when group exists', () => {
    const dag: DagDefinition = {
      id: 'good_group_dag', schedule: null,
      groups: { etl: {} },
      tasks: { step: { group: 'etl', run: async () => {} } },
    }
    const ws = analyzeWarnings(dag)
    expect(ws.filter(w => w.warning_type === 'unknown_group')).toHaveLength(0)
  })
})

describe('analyzeWarnings — sensor_no_timeout', () => {
  it('warns for sensor task without sensorTimeout', () => {
    const dag: DagDefinition = {
      id: 'sensor_warn_dag', schedule: null,
      tasks: { wait: { poke: async () => false } },  // no sensorTimeout
    }
    const ws = analyzeWarnings(dag)
    const w = ws.find(w => w.warning_type === 'sensor_no_timeout')
    expect(w).toBeDefined()
    expect(w?.task_ids).toContain('wait')
  })

  it('does NOT warn when sensorTimeout is set', () => {
    const dag: DagDefinition = {
      id: 'sensor_ok_dag', schedule: null,
      tasks: { wait: { poke: async () => true, sensorTimeout: 60000 } },
    }
    const ws = analyzeWarnings(dag)
    expect(ws.filter(w => w.warning_type === 'sensor_no_timeout')).toHaveLength(0)
  })
})

describe('analyzeWarnings — circular_dependency', () => {
  it('detects a simple A→B→A cycle', () => {
    const dag: DagDefinition = {
      id: 'cycle_dag', schedule: null,
      tasks: {
        a: { dependsOn: ['b'], run: async () => {} },
        b: { dependsOn: ['a'], run: async () => {} },
      },
    }
    const ws = analyzeWarnings(dag)
    const w = ws.find(w => w.warning_type === 'circular_dependency')
    expect(w).toBeDefined()
  })

  it('does NOT warn for linear A→B→C chain', () => {
    const dag: DagDefinition = {
      id: 'linear_dag', schedule: null,
      tasks: {
        a: { run: async () => {} },
        b: { dependsOn: ['a'], run: async () => {} },
        c: { dependsOn: ['b'], run: async () => {} },
      },
    }
    const ws = analyzeWarnings(dag)
    expect(ws.filter(w => w.warning_type === 'circular_dependency')).toHaveLength(0)
  })
})

describe('analyzeWarnings — clean dag produces no warnings', () => {
  it('well-formed dag with multiple tasks has zero warnings', () => {
    const dag: DagDefinition = {
      id: 'clean_dag', schedule: '0 9 * * *',
      tasks: {
        extract: { run: async () => {} },
        transform: { dependsOn: ['extract'], run: async () => {} },
        load: { dependsOn: ['transform'], run: async () => {} },
      },
    }
    expect(analyzeWarnings(dag)).toHaveLength(0)
  })
})

// ── API routes ────────────────────────────────────────────────────────────

describe('GET /dag-warnings', () => {
  it('returns empty when no warnings', async () => {
    const res = await app.inject({ method: 'GET', url: '/dag-warnings' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.warnings).toEqual([])
    expect(body.total_entries).toBe(0)
  })

  it('returns seeded warnings with all fields', async () => {
    setDagWarnings([{
      dag_id: 'warn_dag',
      warning_type: 'no_run_logic',
      message: 'Task x has no run logic',
      task_ids: ['x'],
      detected_at: new Date(),
    }])

    const res = await app.inject({ method: 'GET', url: '/dag-warnings' })
    expect(res.statusCode).toBe(200)
    const { warnings, total_entries } = res.json()
    expect(total_entries).toBe(1)
    expect(warnings[0].dag_id).toBe('warn_dag')
    expect(warnings[0].warning_type).toBe('no_run_logic')
    expect(warnings[0].task_ids).toContain('x')
    expect(warnings[0].detected_at).toBeDefined()
  })

  it('?warning_type= filters by type', async () => {
    setDagWarnings([
      { dag_id: 'd1', warning_type: 'no_run_logic', message: 'm1', task_ids: [], detected_at: new Date() },
      { dag_id: 'd2', warning_type: 'unknown_dependency', message: 'm2', task_ids: [], detected_at: new Date() },
    ])

    const res = await app.inject({ method: 'GET', url: '/dag-warnings?warning_type=no_run_logic' })
    const { warnings } = res.json()
    expect(warnings).toHaveLength(1)
    expect(warnings[0].dag_id).toBe('d1')
  })
})

describe('GET /dag-warnings/:dagId', () => {
  it('returns 404 for unregistered dag', async () => {
    const res = await app.inject({ method: 'GET', url: '/dag-warnings/nonexistent' })
    expect(res.statusCode).toBe(404)
  })

  it('returns empty for registered dag with no warnings', async () => {
    const dag: DagDefinition = {
      id: 'clean_registered', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    const res = await app.inject({ method: 'GET', url: '/dag-warnings/clean_registered' })
    expect(res.statusCode).toBe(200)
    expect(res.json().warnings).toEqual([])
  })

  it('returns only warnings for the requested dag', async () => {
    const dag: DagDefinition = {
      id: 'warned_dag', schedule: null,
      tasks: { step: { run: async () => {} } },
    }
    register(dag)
    setDagWarnings([
      { dag_id: 'warned_dag', warning_type: 'no_run_logic', message: 'm', task_ids: [], detected_at: new Date() },
      { dag_id: 'other_dag', warning_type: 'no_tasks', message: 'm2', task_ids: [], detected_at: new Date() },
    ])

    const res = await app.inject({ method: 'GET', url: '/dag-warnings/warned_dag' })
    const { warnings, total_entries } = res.json()
    expect(total_entries).toBe(1)
    expect(warnings[0].dag_id ?? 'warned_dag').toBe('warned_dag')
  })
})
