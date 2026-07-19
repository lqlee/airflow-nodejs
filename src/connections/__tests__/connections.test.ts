import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import {
  listConnections,
  getConnection,
  getConnectionRuntime,
  upsertConnection,
  deleteConnection,
} from '../index.js'
import { buildServer } from '../../api/server.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_KEY = 'c'.repeat(64)

let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  process.env.ENCRYPTION_KEY = TEST_KEY
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_connections')
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
  await db.collection('connections').deleteMany({})
})

describe('upsertConnection + getConnectionRuntime', () => {
  it('stores and decrypts password correctly', async () => {
    await upsertConnection(db, {
      conn_id: 'my_pg',
      conn_type: 'postgres',
      host: 'localhost',
      port: 5432,
      login: 'admin',
      password: 'hunter2',
    })
    const runtime = await getConnectionRuntime(db, 'my_pg')
    expect(runtime?.password).toBe('hunter2')
    expect(runtime?.login).toBe('admin')
    expect(runtime?.host).toBe('localhost')
  })

  it('GET summary NEVER returns the password', async () => {
    await upsertConnection(db, { conn_id: 'secret_conn', conn_type: 'http', password: 'topsecret' })
    const summary = await getConnection(db, 'secret_conn')
    expect(summary).not.toHaveProperty('password')
    expect(summary?.has_password).toBe(true)
  })

  it('stores and decrypts extra JSON', async () => {
    const extra = JSON.stringify({ sslmode: 'require', keepalives: 1 })
    await upsertConnection(db, { conn_id: 'pg_ssl', conn_type: 'postgres', extra })
    const runtime = await getConnectionRuntime(db, 'pg_ssl')
    expect(runtime?.extra?.sslmode).toBe('require')
  })

  it('upsert updates only provided fields, preserves existing password', async () => {
    await upsertConnection(db, { conn_id: 'upd_conn', conn_type: 'postgres', password: 'original' })
    // Update host without touching password
    await upsertConnection(db, { conn_id: 'upd_conn', conn_type: 'postgres', host: 'new-host' })
    const runtime = await getConnectionRuntime(db, 'upd_conn')
    expect(runtime?.host).toBe('new-host')
    expect(runtime?.password).toBe('original')
  })

  it('returns null for unknown connection', async () => {
    expect(await getConnectionRuntime(db, 'nonexistent')).toBeNull()
    expect(await getConnection(db, 'nonexistent')).toBeNull()
  })
})

describe('listConnections', () => {
  it('returns all connections sorted by conn_id', async () => {
    await upsertConnection(db, { conn_id: 'z_conn', conn_type: 'http' })
    await upsertConnection(db, { conn_id: 'a_conn', conn_type: 'postgres' })
    const conns = await listConnections(db)
    expect(conns[0].conn_id).toBe('a_conn')
    expect(conns[1].conn_id).toBe('z_conn')
  })

  it('no connection doc exposes password or ciphertext', async () => {
    await upsertConnection(db, { conn_id: 'safe', conn_type: 's3', password: 'aws-secret-key' })
    const conns = await listConnections(db)
    const c = conns.find(x => x.conn_id === 'safe')
    expect(c).not.toHaveProperty('password')
    expect(c).not.toHaveProperty('password_enc')
    expect(c?.has_password).toBe(true)
  })
})

describe('deleteConnection', () => {
  it('removes connection and returns true', async () => {
    await upsertConnection(db, { conn_id: 'to_delete', conn_type: 'http' })
    expect(await deleteConnection(db, 'to_delete')).toBe(true)
    expect(await getConnection(db, 'to_delete')).toBeNull()
  })

  it('returns false for non-existent connection', async () => {
    expect(await deleteConnection(db, 'ghost')).toBe(false)
  })
})

describe('Connections API', () => {
  it('POST /connections creates connection and returns summary without password', async () => {
    const res = await app.inject({
      method: 'POST', url: '/connections',
      payload: { conn_id: 'api_pg', conn_type: 'postgres', host: 'db.example.com', password: 'secret' },
    })
    expect(res.statusCode).toBe(201)
    const body = res.json()
    expect(body.conn_id).toBe('api_pg')
    expect(body).not.toHaveProperty('password')
    expect(body.has_password).toBe(true)
  })

  it('GET /connections returns list without secrets', async () => {
    await upsertConnection(db, { conn_id: 'list_test', conn_type: 'http' })
    const res = await app.inject({ method: 'GET', url: '/connections' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(Array.isArray(body)).toBe(true)
    for (const c of body) {
      expect(c).not.toHaveProperty('password')
      expect(c).not.toHaveProperty('password_enc')
    }
  })

  it('GET /connections/:connId returns 404 for unknown id', async () => {
    const res = await app.inject({ method: 'GET', url: '/connections/does_not_exist' })
    expect(res.statusCode).toBe(404)
  })

  it('DELETE /connections/:connId removes the connection', async () => {
    await upsertConnection(db, { conn_id: 'del_api', conn_type: 'http' })
    const res = await app.inject({ method: 'DELETE', url: '/connections/del_api' })
    expect(res.statusCode).toBe(204)
    expect(await getConnection(db, 'del_api')).toBeNull()
  })

  it('POST /connections returns 400 when conn_id missing', async () => {
    const res = await app.inject({
      method: 'POST', url: '/connections',
      payload: { conn_type: 'http' },
    })
    expect(res.statusCode).toBe(400)
  })
})
