/**
 * Tests for DAG typed params validation.
 *
 * What each test answers:
 *  - Required param missing → validation error?
 *  - Default values merged when param not supplied?
 *  - Type mismatch (string vs number) → validation error?
 *  - Enum violation → validation error?
 *  - Numeric range (minimum/maximum) → validation error?
 *  - Integer check (float vs integer) → validation error?
 *  - String pattern violation → validation error?
 *  - No params defined → pass through unchanged?
 *  - Caller value overrides default?
 *  - Multiple errors reported at once?
 *  - Integration: POST /dags/:id/trigger with bad params → 400?
 *  - Integration: POST with valid params → 201 + defaults merged in run conf?
 */

import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { validateParams } from '../params.js'
import type { DagDefinition } from '../types.js'
import { MongoClient, type Db } from 'mongodb'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../registry.js'
import type { FastifyInstance } from 'fastify'

// ══════════════════════════════════════════════════════════════════════════════
// Pure unit tests — validateParams
// ══════════════════════════════════════════════════════════════════════════════

describe('validateParams — pure unit tests', () => {
  const dag = (params: DagDefinition['params']): DagDefinition => ({
    id: 'test', schedule: null, params, tasks: {},
  })

  it('no params defined → pass through conf unchanged', () => {
    const result = validateParams(dag(undefined), { x: 1 })
    expect(result.ok).toBe(true)
    expect(result.mergedConf).toEqual({ x: 1 })
  })

  it('empty params → pass through conf unchanged', () => {
    const result = validateParams(dag({}), { x: 1 })
    expect(result.ok).toBe(true)
    expect(result.mergedConf).toEqual({ x: 1 })
  })

  it('required param missing → error', () => {
    const result = validateParams(
      dag({ name: { type: 'string' } }),  // no default → required
      {}
    )
    expect(result.ok).toBe(false)
    expect(result.errors[0].param).toBe('name')
    expect(result.errors[0].message).toMatch(/required/i)
  })

  it('default filled in when param not supplied', () => {
    const result = validateParams(
      dag({ env: { type: 'string', default: 'dev' } }),
      {}
    )
    expect(result.ok).toBe(true)
    expect(result.mergedConf.env).toBe('dev')
  })

  it('caller value overrides default', () => {
    const result = validateParams(
      dag({ env: { type: 'string', default: 'dev' } }),
      { env: 'prod' }
    )
    expect(result.ok).toBe(true)
    expect(result.mergedConf.env).toBe('prod')
  })

  it('type string: number supplied → error', () => {
    const result = validateParams(dag({ name: { type: 'string', default: 'x' } }), { name: 42 })
    expect(result.ok).toBe(false)
    expect(result.errors[0].message).toMatch(/string/)
  })

  it('type number: string supplied → error', () => {
    const result = validateParams(dag({ count: { type: 'number', default: 1 } }), { count: 'ten' })
    expect(result.ok).toBe(false)
    expect(result.errors[0].message).toMatch(/number/)
  })

  it('type integer: float supplied → error', () => {
    const result = validateParams(dag({ n: { type: 'integer', default: 1 } }), { n: 3.14 })
    expect(result.ok).toBe(false)
    expect(result.errors[0].message).toMatch(/integer/)
  })

  it('type boolean: string supplied → error', () => {
    const result = validateParams(dag({ flag: { type: 'boolean', default: false } }), { flag: 'true' })
    expect(result.ok).toBe(false)
    expect(result.errors[0].message).toMatch(/boolean/)
  })

  it('enum: value not in list → error', () => {
    const result = validateParams(
      dag({ env: { type: 'string', enum: ['dev', 'prod'], default: 'dev' } }),
      { env: 'staging' }
    )
    expect(result.ok).toBe(false)
    expect(result.errors[0].message).toMatch(/one of/)
    expect(result.errors[0].message).toContain('dev')
    expect(result.errors[0].message).toContain('prod')
  })

  it('enum: value in list → ok', () => {
    const result = validateParams(
      dag({ env: { type: 'string', enum: ['dev', 'prod'], default: 'dev' } }),
      { env: 'prod' }
    )
    expect(result.ok).toBe(true)
  })

  it('minimum: value too small → error', () => {
    const result = validateParams(dag({ n: { type: 'integer', minimum: 1, default: 1 } }), { n: 0 })
    expect(result.ok).toBe(false)
    expect(result.errors[0].message).toMatch(/>=\s*1/)
  })

  it('maximum: value too large → error', () => {
    const result = validateParams(dag({ n: { type: 'integer', maximum: 100, default: 10 } }), { n: 101 })
    expect(result.ok).toBe(false)
    expect(result.errors[0].message).toMatch(/<=\s*100/)
  })

  it('minimum and maximum: value in range → ok', () => {
    const result = validateParams(dag({ n: { type: 'integer', minimum: 1, maximum: 100, default: 10 } }), { n: 50 })
    expect(result.ok).toBe(true)
  })

  it('pattern: string matches → ok', () => {
    const result = validateParams(
      dag({ id: { type: 'string', pattern: '^[a-z]+$', default: 'abc' } }),
      { id: 'hello' }
    )
    expect(result.ok).toBe(true)
  })

  it('pattern: string does not match → error', () => {
    const result = validateParams(
      dag({ id: { type: 'string', pattern: '^[a-z]+$', default: 'abc' } }),
      { id: 'Hello123' }
    )
    expect(result.ok).toBe(false)
    expect(result.errors[0].message).toMatch(/pattern/)
  })

  it('multiple errors reported simultaneously', () => {
    const result = validateParams(
      dag({
        name: { type: 'string' },    // required, missing
        count: { type: 'integer', default: 1 },  // wrong type supplied
      }),
      { count: 'ten' }  // name missing, count wrong type
    )
    expect(result.ok).toBe(false)
    expect(result.errors.length).toBe(2)
    expect(result.errors.some(e => e.param === 'name')).toBe(true)
    expect(result.errors.some(e => e.param === 'count')).toBe(true)
  })

  it('unknown params (not in schema) pass through unchanged', () => {
    const result = validateParams(
      dag({ env: { type: 'string', default: 'dev' } }),
      { env: 'prod', extra: 'value', count: 42 }
    )
    expect(result.ok).toBe(true)
    expect(result.mergedConf.extra).toBe('value')
    expect(result.mergedConf.count).toBe(42)
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// Integration tests — POST /dags/:id/trigger param validation
// ══════════════════════════════════════════════════════════════════════════════

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_params')
  clearRegistry()
  app = buildServer(db)
  await app.ready()

  register({
    id: 'param_dag',
    schedule: null,
    params: {
      env:   { type: 'string', enum: ['dev', 'staging', 'prod'], default: 'dev', description: 'Deployment environment' },
      count: { type: 'integer', minimum: 1, maximum: 100, default: 10 },
      name:  { type: 'string', description: 'Required — no default' },
    },
    tasks: { step: { run: async (ctx) => ctx.conf } },
  })
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

describe('trigger route param validation', () => {
  it('valid params + required name supplied → 201', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/param_dag/trigger',
      payload: { conf: { name: 'alice', env: 'prod', count: 5 } },
    })
    expect(res.statusCode).toBe(201)
    const body = res.json()
    expect(body.run_id).toBeTruthy()
  })

  it('defaults merged into run conf when params omitted', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/param_dag/trigger',
      payload: { conf: { name: 'bob' } },  // env and count use defaults
    })
    expect(res.statusCode).toBe(201)
    // Verify defaults in DB
    const run = await db.collection('dag_runs').findOne({ 'conf.name': 'bob' })
    expect(run?.conf.env).toBe('dev')    // default
    expect(run?.conf.count).toBe(10)    // default
    expect(run?.conf.name).toBe('bob')  // supplied
  })

  it('required param missing → 400 with param_errors', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/param_dag/trigger',
      payload: { conf: { env: 'dev' } },  // name is required
    })
    expect(res.statusCode).toBe(400)
    const body = res.json()
    expect(body.error).toMatch(/param validation/i)
    expect(body.param_errors).toHaveLength(1)
    expect(body.param_errors[0].param).toBe('name')
  })

  it('type mismatch → 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/param_dag/trigger',
      payload: { conf: { name: 'test', count: 'five' } },  // count must be integer
    })
    expect(res.statusCode).toBe(400)
    const body = res.json()
    expect(body.param_errors.some((e: any) => e.param === 'count')).toBe(true)
  })

  it('enum violation → 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/param_dag/trigger',
      payload: { conf: { name: 'test', env: 'production' } },  // must be dev/staging/prod
    })
    expect(res.statusCode).toBe(400)
    const body = res.json()
    expect(body.param_errors.some((e: any) => e.param === 'env')).toBe(true)
  })

  it('range violation → 400', async () => {
    const res = await app.inject({
      method: 'POST',
      url: '/dags/param_dag/trigger',
      payload: { conf: { name: 'test', count: 999 } },  // max is 100
    })
    expect(res.statusCode).toBe(400)
    const body = res.json()
    expect(body.param_errors.some((e: any) => e.param === 'count')).toBe(true)
  })

  it('no params schema → any conf accepted', async () => {
    register({
      id: 'no_params_dag',
      schedule: null,
      tasks: { step: { run: async () => 'ok' } },
    })
    const res = await app.inject({
      method: 'POST',
      url: '/dags/no_params_dag/trigger',
      payload: { conf: { anything: 'goes', x: 42 } },
    })
    expect(res.statusCode).toBe(201)
  })
})
