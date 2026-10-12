import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { buildServer } from '../server.js'
import { buildOpenApiDocument, toOpenApiPath } from '../openapi.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_openapi')
  app = buildServer(db)
  await app.ready()
})
afterAll(async () => { await app.close(); await db.dropDatabase(); await client.close() })

describe('toOpenApiPath', () => {
  it('converts :params to {params}', () => {
    expect(toOpenApiPath('/dags/:dagId/runs/:runId/tasks/:taskId')).toEqual({
      path: '/dags/{dagId}/runs/{runId}/tasks/{taskId}', params: ['dagId', 'runId', 'taskId'],
    })
  })
  it('leaves plain paths alone', () => {
    expect(toOpenApiPath('/health')).toEqual({ path: '/health', params: [] })
  })
})

describe('buildOpenApiDocument', () => {
  const routes = [
    { method: 'GET', url: '/dags/:dagId' },
    { method: 'HEAD', url: '/dags/:dagId' },
    { method: 'POST', url: '/dags/:dagId/trigger' },
    { method: 'GET', url: '/api-keys' },
    { method: 'GET', url: '/*' },
    { method: 'GET', url: '/health' },
    { method: 'GET', url: '/custom', requiredRole: 'admin' as const },
  ]
  const doc = buildOpenApiDocument(routes, { title: 't', version: '1', authEnabled: true }) as any

  it('drops HEAD and wildcard routes', () => {
    expect(Object.keys(doc.paths)).toEqual(['/api-keys', '/custom', '/dags/{dagId}', '/dags/{dagId}/trigger', '/health'])
    expect(doc.paths['/dags/{dagId}'].head).toBeUndefined()
  })
  it('declares path parameters and a request body for POST', () => {
    expect(doc.paths['/dags/{dagId}'].get.parameters).toEqual([{ name: 'dagId', in: 'path', required: true, schema: { type: 'string' } }])
    expect(doc.paths['/dags/{dagId}/trigger'].post.requestBody).toBeDefined()
    expect(doc.paths['/dags/{dagId}'].get.requestBody).toBeUndefined()
  })
  it('records the minimum role per operation', () => {
    expect(doc.paths['/dags/{dagId}'].get['x-required-role']).toBe('viewer')
    expect(doc.paths['/dags/{dagId}/trigger'].post['x-required-role']).toBe('editor')
    expect(doc.paths['/api-keys'].get['x-required-role']).toBe('admin')
    expect(doc.paths['/custom'].get['x-required-role']).toBe('admin')   // route override
  })
  it('public routes carry no security requirement; others use bearerAuth', () => {
    expect(doc.paths['/health'].get.security).toEqual([])
    expect(doc.paths['/dags/{dagId}'].get.security).toEqual([{ bearerAuth: [] }])
    expect(doc.components.securitySchemes.bearerAuth).toMatchObject({ type: 'http', scheme: 'bearer' })
  })
  it('omits 401/403 and security when auth is disabled', () => {
    const open = buildOpenApiDocument(routes, { title: 't', version: '1', authEnabled: false }) as any
    expect(open.paths['/dags/{dagId}'].get.security).toEqual([])
    expect(Object.keys(open.paths['/dags/{dagId}'].get.responses)).toEqual(['200'])
  })
  it('operationIds are unique', () => {
    const ids = Object.values(doc.paths).flatMap((p: any) => Object.values(p).map((o: any) => o.operationId))
    expect(new Set(ids).size).toBe(ids.length)
  })
})

describe('GET /openapi.json', () => {
  it('is served, uncached, and lists the real API surface', async () => {
    const res = await app.inject({ method: 'GET', url: '/openapi.json' })
    expect(res.statusCode).toBe(200)
    expect(res.headers['cache-control']).toBe('no-store')
    const doc = res.json()
    expect(doc.openapi).toBe('3.1.0')
    for (const p of ['/dags', '/dags/{dagId}', '/dags/{dagId}/trigger', '/dags/{dagId}/tasks', '/metrics', '/health', '/api-keys', '/openapi.json']) {
      expect(doc.paths[p], p).toBeDefined()
    }
  })

  it('routes from across the route modules appear in the document', async () => {
    const doc = (await app.inject({ method: 'GET', url: '/openapi.json' })).json()
    const listed = new Set(Object.entries(doc.paths).flatMap(([p, ops]) => Object.keys(ops as object).map(m => `${m.toUpperCase()} ${p}`)))
    // fastify prints a route tree, but the ground truth is what answers requests: probe a spread
    for (const [m, p] of [['GET', '/dags/{dagId}/runs'], ['POST', '/dags/{dagId}/trigger'], ['GET', '/pools'], ['GET', '/variables'], ['GET', '/event-logs']]) {
      expect(listed.has(`${m} ${p}`), `${m} ${p}`).toBe(true)
    }
  })
})
