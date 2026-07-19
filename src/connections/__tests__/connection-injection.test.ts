/**
 * End-to-end injection test: verifies that a forked worker can read
 * Connections and Variables via ctx.connections.get() / ctx.variables.get()
 * and that ENCRYPTION_KEY propagates to the child process.
 *
 * This is the only test that exercises the full path:
 *   upsertConnection → encrypt → DB → worker fork → decrypt → task reads plaintext
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { upsertConnection } from '../index.js'
import { setVariable } from '../../variables/index.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_KEY = 'e'.repeat(64)

let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.ENCRYPTION_KEY = TEST_KEY
  // Worker uses DB_NAME env var — must match the test DB
  process.env.DB_NAME = 'airflow_test_injection'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_injection')
  clearRegistry()
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
  delete process.env.ENCRYPTION_KEY
  delete process.env.DB_NAME
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('xcoms').deleteMany({})
  await db.collection('connections').deleteMany({})
  await db.collection('variables').deleteMany({})
})

describe('Connection + Variable injection in forked worker', () => {
  it('task can read decrypted password via ctx.connections.get()', async () => {
    // Store an encrypted connection
    await upsertConnection(db, {
      conn_id: 'test_db',
      conn_type: 'postgres',
      host: 'localhost',
      login: 'admin',
      password: 'hunter2',
    })

    // Dag that reads the connection and pushes password to XCom
    const dag: DagDefinition = {
      id: 'conn_injection_dag',
      schedule: null,
      tasks: {
        read_conn: {
          run: async (ctx) => {
            const conn = await ctx.connections.get('test_db')
            await ctx.xcom.push('conn_password', conn?.password ?? null)
          },
        },
      },
    }
    register(dag)

    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const xcom = await db.collection('xcoms').findOne({
      dag_run_id: runId,
      task_id: 'read_conn',
      key: 'conn_password',
    })
    // The worker decrypted 'hunter2' and pushed it via XCom
    expect(xcom?.value).toBe('hunter2')
  })

  it('task can read decrypted secret variable via ctx.variables.get()', async () => {
    await setVariable(db, { key: 'api_token', value: 'tok_secret_123', is_secret: true })

    const dag: DagDefinition = {
      id: 'var_injection_dag',
      schedule: null,
      tasks: {
        read_var: {
          run: async (ctx) => {
            const val = await ctx.variables.get('api_token')
            await ctx.xcom.push('token_value', val)
          },
        },
      },
    }
    register(dag)

    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const xcom = await db.collection('xcoms').findOne({
      dag_run_id: runId,
      task_id: 'read_var',
      key: 'token_value',
    })
    expect(xcom?.value).toBe('tok_secret_123')
  })

  it('ctx.connections.get() returns null for unknown conn_id', async () => {
    const dag: DagDefinition = {
      id: 'missing_conn_dag',
      schedule: null,
      tasks: {
        check_missing: {
          run: async (ctx) => {
            const conn = await ctx.connections.get('does_not_exist')
            await ctx.xcom.push('result', conn)
          },
        },
      },
    }
    register(dag)

    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const xcom = await db.collection('xcoms').findOne({
      dag_run_id: runId,
      task_id: 'check_missing',
      key: 'result',
    })
    expect(xcom?.value).toBeNull()
  })

  it('task run succeeds and connection + xcom both work in same run', async () => {
    await upsertConnection(db, {
      conn_id: 'multi_conn',
      conn_type: 'http',
      host: 'api.example.com',
      password: 's3cr3t',
    })
    await setVariable(db, { key: 'batch', value: '50' })

    const dag: DagDefinition = {
      id: 'multi_injection_dag',
      schedule: null,
      tasks: {
        combined: {
          run: async (ctx) => {
            const conn = await ctx.connections.get('multi_conn')
            const batchSize = await ctx.variables.get('batch')
            await ctx.xcom.push('result', {
              password: conn?.password,
              batch: batchSize,
              host: conn?.host,
            })
          },
        },
      },
    }
    register(dag)

    const runId = await createRun(db, dag)
    await advanceRun(db, runId)

    const xcom = await db.collection('xcoms').findOne({
      dag_run_id: runId,
      task_id: 'combined',
      key: 'result',
    })
    expect(xcom?.value).toMatchObject({
      password: 's3cr3t',
      batch: '50',
      host: 'api.example.com',
    })
  })
})
