import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import {
  listVariables,
  getVariable,
  getVariableRuntime,
  setVariable,
  deleteVariable,
} from '../index.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { buildServer } from '../../api/server.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_KEY = 'd'.repeat(64)

let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.ENCRYPTION_KEY = TEST_KEY
  process.env.DB_NAME = 'airflow_test_variables'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_variables')
  clearRegistry()
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  delete process.env.ENCRYPTION_KEY
  delete process.env.DB_NAME
})

afterEach(async () => {
  await db.collection('variables').deleteMany({})
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('xcoms').deleteMany({})
  clearRegistry()
})

describe('setVariable + getVariableRuntime', () => {
  it('stores and retrieves plaintext variable', async () => {
    await setVariable(db, { key: 'batch_size', value: '500' })
    expect(await getVariableRuntime(db, 'batch_size')).toBe('500')
  })

  it('stores and decrypts secret variable', async () => {
    await setVariable(db, { key: 'db_password', value: 'hunter2', is_secret: true })
    expect(await getVariableRuntime(db, 'db_password')).toBe('hunter2')
  })

  it('GET summary masks secret value as null', async () => {
    await setVariable(db, { key: 'secret_token', value: 'tok_xyz', is_secret: true })
    const summary = await getVariable(db, 'secret_token')
    expect(summary?.value).toBeNull()
    expect(summary?.is_secret).toBe(true)
  })

  it('GET summary returns plaintext value for non-secret', async () => {
    await setVariable(db, { key: 'region', value: 'us-east-1' })
    const summary = await getVariable(db, 'region')
    expect(summary?.value).toBe('us-east-1')
    expect(summary?.is_secret).toBe(false)
  })

  it('upsert overwrites existing value', async () => {
    await setVariable(db, { key: 'counter', value: '1' })
    await setVariable(db, { key: 'counter', value: '42' })
    expect(await getVariableRuntime(db, 'counter')).toBe('42')
  })

  it('returns null for unknown key', async () => {
    expect(await getVariableRuntime(db, 'nonexistent')).toBeNull()
    expect(await getVariable(db, 'nonexistent')).toBeNull()
  })
})

describe('listVariables', () => {
  it('returns variables sorted by key, secrets masked', async () => {
    await setVariable(db, { key: 'z_var', value: 'zval' })
    await setVariable(db, { key: 'a_secret', value: 'aval', is_secret: true })
    const vars = await listVariables(db)
    expect(vars[0].key).toBe('a_secret')
    expect(vars[0].value).toBeNull()  // masked
    expect(vars[1].key).toBe('z_var')
    expect(vars[1].value).toBe('zval')
  })
})

describe('deleteVariable', () => {
  it('removes variable and returns true', async () => {
    await setVariable(db, { key: 'to_delete', value: 'x' })
    expect(await deleteVariable(db, 'to_delete')).toBe(true)
    expect(await getVariable(db, 'to_delete')).toBeNull()
  })

  it('returns false for unknown key', async () => {
    expect(await deleteVariable(db, 'ghost')).toBe(false)
  })
})

describe('Variables API', () => {
  it('POST /variables creates variable', async () => {
    const res = await app.inject({
      method: 'POST', url: '/variables',
      payload: { key: 'api_batch', value: '100' },
    })
    expect(res.statusCode).toBe(201)
    const body = res.json()
    expect(body.key).toBe('api_batch')
    expect(body.value).toBe('100')
  })

  it('POST /variables with is_secret=true masks value in response', async () => {
    const res = await app.inject({
      method: 'POST', url: '/variables',
      payload: { key: 'api_secret', value: 'shh', is_secret: true },
    })
    expect(res.statusCode).toBe(201)
    expect(res.json().value).toBeNull()
    expect(res.json().is_secret).toBe(true)
  })

  it('GET /variables returns list with secrets masked', async () => {
    await setVariable(db, { key: 'visible', value: 'yes' })
    await setVariable(db, { key: 'hidden', value: 'no', is_secret: true })
    const res = await app.inject({ method: 'GET', url: '/variables' })
    const vars = res.json()
    const hidden = vars.find((v: { key: string }) => v.key === 'hidden')
    expect(hidden?.value).toBeNull()
  })

  it('GET /variables/:key returns 404 for unknown key', async () => {
    const res = await app.inject({ method: 'GET', url: '/variables/nope' })
    expect(res.statusCode).toBe(404)
  })

  it('DELETE /variables/:key removes variable', async () => {
    await setVariable(db, { key: 'del_me', value: 'bye' })
    const res = await app.inject({ method: 'DELETE', url: '/variables/del_me' })
    expect(res.statusCode).toBe(204)
    expect(await getVariable(db, 'del_me')).toBeNull()
  })

  it('POST /variables returns 400 when key missing', async () => {
    const res = await app.inject({
      method: 'POST', url: '/variables',
      payload: { value: 'orphan' },
    })
    expect(res.statusCode).toBe(400)
  })
})

// ── Task reads a variable (forked worker, full e2e) ───────────────────────────

describe('task reads a variable via ctx.variables.get()', () => {
  it('task reads plaintext variable and pushes value to XCom', async () => {
    await setVariable(db, { key: 'page_size', value: '250' })

    const dag: DagDefinition = {
      id: 'var_read_dag', schedule: null,
      tasks: {
        reader: {
          run: async (ctx) => {
            const pageSize = await ctx.variables.get('page_size')
            await ctx.xcom.push('read_value', pageSize)
          },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const xcom = await db.collection('xcoms')
      .findOne({ dag_run_id: runId, task_id: 'reader', key: 'read_value' })
    expect(xcom?.value).toBe('250')
  })

  it('task reads a secret variable — decrypted value injected, never plaintext over IPC', async () => {
    await setVariable(db, { key: 'db_password', value: 'sup3r-s3cr3t', is_secret: true })

    const dag: DagDefinition = {
      id: 'secret_var_dag', schedule: null,
      tasks: {
        reader: {
          run: async (ctx) => {
            const pwd = await ctx.variables.get('db_password')
            // Push the decrypted value so the test can verify it arrived correctly
            await ctx.xcom.push('decrypted', pwd)
          },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const xcom = await db.collection('xcoms')
      .findOne({ dag_run_id: runId, task_id: 'reader', key: 'decrypted' })
    // Worker decrypted the secret; plaintext value is correct
    expect(xcom?.value).toBe('sup3r-s3cr3t')

    // API must still mask the value (never expose it)
    const apiRes = await app.inject({ method: 'GET', url: '/variables/db_password' })
    expect(apiRes.json().value).toBeNull()
    expect(apiRes.json().is_secret).toBe(true)
  })

  it('ctx.variables.get() returns null for a missing key — task handles gracefully', async () => {
    const dag: DagDefinition = {
      id: 'missing_var_dag', schedule: null,
      tasks: {
        reader: {
          run: async (ctx) => {
            const val = await ctx.variables.get('nonexistent_key')
            await ctx.xcom.push('got', val)
          },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const xcom = await db.collection('xcoms')
      .findOne({ dag_run_id: runId, task_id: 'reader', key: 'got' })
    expect(xcom?.value).toBeNull()
  })
})

// ── Task reads then updates a variable (read → process → write-back) ─────────

describe('task reads variable, then caller updates it after run completes', () => {
  /**
   * Airflow pattern: tasks read variables to get config; after the run
   * succeeds, an external caller (operator/CI) writes back an updated value
   * (e.g. updating a "last_processed_offset" cursor).
   * Tasks themselves cannot write variables directly via ctx — they use XCom
   * to surface results, and the caller uses POST /variables to persist the update.
   */
  it('pipeline: read initial offset → process → update offset via API', async () => {
    // Set up: initial offset variable
    await setVariable(db, { key: 'last_offset', value: '0' })

    // Task reads the current offset and emits the next one
    const dag: DagDefinition = {
      id: 'offset_dag', schedule: null,
      tasks: {
        process: {
          run: async (ctx) => {
            const currentOffset = await ctx.variables.get('last_offset')
            const nextOffset = String(parseInt(currentOffset ?? '0', 10) + 100)
            // Emit next offset via XCom — caller will persist it
            await ctx.xcom.push('next_offset', nextOffset)
          },
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    // Verify the task read the current offset and computed the next one
    const xcom = await db.collection('xcoms')
      .findOne({ dag_run_id: runId, task_id: 'process', key: 'next_offset' })
    expect(xcom?.value).toBe('100')

    // Caller (operator/CI) reads the XCom result and writes back to variables
    const nextOffset = xcom?.value as string
    await app.inject({
      method: 'POST', url: '/variables',
      payload: { key: 'last_offset', value: nextOffset },
    })

    // Verify the variable was updated
    const updated = await getVariableRuntime(db, 'last_offset')
    expect(updated).toBe('100')

    // Run it again — new run reads the updated offset
    const dag2: DagDefinition = { ...dag, id: 'offset_dag2' }
    register(dag2)
    const runId2 = await createRun(db, dag2)
    await advanceRun(db, runId2)

    const xcom2 = await db.collection('xcoms')
      .findOne({ dag_run_id: runId2, task_id: 'process', key: 'next_offset' })
    // Second run started from offset 100, so next is 200
    expect(xcom2?.value).toBe('200')
  })

  it('multi-task dag: first task reads config, second task writes back result', async () => {
    await setVariable(db, { key: 'multiplier', value: '3' })

    const dag: DagDefinition = {
      id: 'multi_var_dag', schedule: null,
      tasks: {
        fetch: {
          run: async (ctx) => {
            const mult = await ctx.variables.get('multiplier')
            await ctx.xcom.push('multiplier', parseInt(mult ?? '1', 10))
          },
        },
        compute: {
          run: async (ctx) => {
            const mult = await ctx.xcom.pull('fetch', 'multiplier') as number
            const result = 7 * mult
            await ctx.xcom.push('result', result)
          },
          dependsOn: ['fetch'],
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    // Verify compute task used the variable value
    const resultXcom = await db.collection('xcoms')
      .findOne({ dag_run_id: runId, task_id: 'compute', key: 'result' })
    expect(resultXcom?.value).toBe(21) // 7 * 3

    // Post-run: update multiplier via API based on result
    await app.inject({
      method: 'POST', url: '/variables',
      payload: { key: 'multiplier', value: String(resultXcom?.value) },
    })

    expect(await getVariableRuntime(db, 'multiplier')).toBe('21')
  })
})
