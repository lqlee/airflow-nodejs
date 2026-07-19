import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import {
  listVariables,
  getVariable,
  getVariableRuntime,
  setVariable,
  deleteVariable,
} from '../index.js'
import { buildServer } from '../../api/server.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_KEY = 'd'.repeat(64)

let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.ENCRYPTION_KEY = TEST_KEY
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_variables')
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  delete process.env.ENCRYPTION_KEY
})

afterEach(async () => {
  await db.collection('variables').deleteMany({})
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
