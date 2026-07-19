import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../../scheduler/runs.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../registry.js'
import { expandGroups } from '../taskgroups.js'
import type { DagDefinition } from '../types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const noop = async () => {}

const groupedDag: DagDefinition = expandGroups({
  id: 'grouped_pipeline',
  schedule: null,
  groups: {
    extract: {},
    transform: { dependsOn: ['extract'] },
  },
  tasks: {
    fetch_data: { run: noop, group: 'extract' },
    validate: { run: noop, group: 'transform' },
    enrich: { run: noop, group: 'transform', dependsOn: ['validate'] },
  },
})

const flatDag: DagDefinition = {
  id: 'flat_pipeline',
  schedule: null,
  tasks: {
    step1: { run: noop },
    step2: { run: noop, dependsOn: ['step1'] },
  },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_taskgroup')
  clearRegistry()
  register(groupedDag)
  register(flatDag)
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
})

describe('TaskGroup — createRun stamps group_id', () => {
  it('stamps group_id from task.group onto task_instance', async () => {
    const runId = await createRun(db, groupedDag)
    const instances = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    const fetchData = instances.find(t => t.task_id === 'fetch_data')
    const validate = instances.find(t => t.task_id === 'validate')
    expect(fetchData?.group_id).toBe('extract')
    expect(validate?.group_id).toBe('transform')
  })

  it('group_id is null for tasks without a group', async () => {
    const runId = await createRun(db, flatDag)
    const instances = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
    for (const inst of instances) {
      expect(inst.group_id).toBeNull()
    }
  })
})

describe('TaskGroup — group→group deps expand correctly', () => {
  it('validate (root of transform) depends on fetch_data (leaf of extract)', () => {
    // expandGroups ran at test setup — check the expanded dag's tasks
    expect(groupedDag.tasks['validate'].dependsOn).toContain('fetch_data')
  })

  it('enrich (non-root of transform) does not get cross-group edge', () => {
    expect(groupedDag.tasks['enrich'].dependsOn).not.toContain('fetch_data')
    expect(groupedDag.tasks['enrich'].dependsOn).toContain('validate')
  })

  it('task_instances inherit expanded depends_on', async () => {
    const runId = await createRun(db, groupedDag)
    const validate = await db
      .collection('task_instances')
      .findOne({ dag_run_id: runId, task_id: 'validate' })
    expect(validate?.depends_on).toContain('fetch_data')
  })
})

describe('TaskGroup — API', () => {
  it('GET /dag-runs/:runId tasks include group_id', async () => {
    const runId = await createRun(db, groupedDag)
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    const fetchTask = body.tasks.find((t: { task_id: string }) => t.task_id === 'fetch_data')
    const validateTask = body.tasks.find((t: { task_id: string }) => t.task_id === 'validate')
    expect(fetchTask?.group_id).toBe('extract')
    expect(validateTask?.group_id).toBe('transform')
  })

  it('GET /dag-runs/:runId tasks with no group have group_id: null', async () => {
    const runId = await createRun(db, flatDag)
    const res = await app.inject({ method: 'GET', url: `/dag-runs/${runId}` })
    const body = res.json()
    for (const t of body.tasks) {
      expect(t.group_id).toBeNull()
    }
  })

  it('GET /dags/:dagId exposes groups array', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags/grouped_pipeline' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(Array.isArray(body.groups)).toBe(true)
    const extractGroup = body.groups.find((g: { group_id: string }) => g.group_id === 'extract')
    const transformGroup = body.groups.find((g: { group_id: string }) => g.group_id === 'transform')
    expect(extractGroup).toBeDefined()
    expect(transformGroup?.depends_on).toContain('extract')
  })

  it('GET /dags/:dagId tasks include group_id field', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags/grouped_pipeline' })
    const body = res.json()
    const fetchTask = body.tasks.find((t: { task_id: string }) => t.task_id === 'fetch_data')
    expect(fetchTask?.group_id).toBe('extract')
  })

  it('GET /dags/:dagId returns empty groups array for flat dags', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags/flat_pipeline' })
    const body = res.json()
    expect(body.groups).toEqual([])
  })
})
