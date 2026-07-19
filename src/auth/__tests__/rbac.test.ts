/**
 * RBAC integration tests.
 *
 * AUTH_ENABLED is evaluated at module import time, so ADMIN_KEY must be set
 * before any import runs. We use vi.hoisted() to set the env var before the
 * static imports are evaluated (vitest hoists vi.mock/vi.hoisted above imports).
 */

import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest'

// CRITICAL: set ADMIN_KEY before any module that reads it is imported.
// vi.hoisted runs before static imports — this is the correct pattern.
const { ADMIN_KEY_VALUE } = vi.hoisted(() => {
  const ADMIN_KEY_VALUE = 'test-admin-bootstrap-key'
  process.env.ADMIN_KEY = ADMIN_KEY_VALUE
  process.env.DB_NAME = 'airflow_test_rbac'
  return { ADMIN_KEY_VALUE }
})

import { MongoClient, type Db } from 'mongodb'
import { createApiKey } from '../keys.js'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'

let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_rbac')
  clearRegistry()
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  delete process.env.ADMIN_KEY
  delete process.env.DB_NAME
})

afterEach(async () => {
  await db.collection('api_keys').deleteMany({})
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  clearRegistry()
})

// ── Helper ─────────────────────────────────────────────────────────────────

const auth = (key: string) => ({ authorization: `Bearer ${key}` })

function registerTestDag(id = 'rbac_dag'): DagDefinition {
  const dag: DagDefinition = {
    id,
    schedule: null,
    tasks: { step: { run: async () => { /* ok */ } } },
  }
  register(dag)
  return dag
}

// ── Key creation + role field ───────────────────────────────────────────────

describe('createApiKey — role field', () => {
  it('defaults to viewer when role omitted', async () => {
    await createApiKey(db, 'ci-bot')
    const key = await db.collection('api_keys').findOne({ name: 'ci-bot' })
    expect(key?.role).toBe('viewer')
  })

  it('stores editor role', async () => {
    await createApiKey(db, 'deployer', 'editor')
    const key = await db.collection('api_keys').findOne({ name: 'deployer' })
    expect(key?.role).toBe('editor')
  })

  it('stores admin role', async () => {
    await createApiKey(db, 'super', 'admin')
    const key = await db.collection('api_keys').findOne({ name: 'super' })
    expect(key?.role).toBe('admin')
  })
})

// ── POST /api-keys — admin-only ─────────────────────────────────────────────

describe('POST /api-keys', () => {
  it('admin (ADMIN_KEY) can create a key', async () => {
    const res = await app.inject({
      method: 'POST', url: '/api-keys',
      headers: auth(ADMIN_KEY_VALUE),
      payload: { name: 'new-key', role: 'viewer' },
    })
    expect(res.statusCode).toBe(201)
    expect(res.json().role).toBe('viewer')
    expect(res.json().key).toMatch(/^an_/)
  })

  it('editor cannot create a key — gets 403', async () => {
    const { raw } = await createApiKey(db, 'editor-key', 'editor')
    const res = await app.inject({
      method: 'POST', url: '/api-keys',
      headers: auth(raw),
      payload: { name: 'escalation-attempt' },
    })
    expect(res.statusCode).toBe(403)
    expect(res.json().error).toContain("requires the 'admin' role")
  })

  it('viewer cannot create a key — gets 403', async () => {
    const { raw } = await createApiKey(db, 'viewer-key', 'viewer')
    const res = await app.inject({
      method: 'POST', url: '/api-keys',
      headers: auth(raw),
      payload: { name: 'escalation' },
    })
    expect(res.statusCode).toBe(403)
  })

  it('returns 400 for invalid role', async () => {
    const res = await app.inject({
      method: 'POST', url: '/api-keys',
      headers: auth(ADMIN_KEY_VALUE),
      payload: { name: 'bad', role: 'superuser' },
    })
    expect(res.statusCode).toBe(400)
    expect(res.json().error).toContain('"role" must be one of')
  })

  it('no token → 401', async () => {
    const res = await app.inject({
      method: 'POST', url: '/api-keys',
      payload: { name: 'anon' },
    })
    expect(res.statusCode).toBe(401)
  })
})

// ── GET /api-keys — admin only (all key-mgmt operations require admin) ─────

describe('GET /api-keys', () => {
  it('admin can list keys', async () => {
    await createApiKey(db, 'listed-key', 'viewer')
    const res = await app.inject({
      method: 'GET', url: '/api-keys',
      headers: auth(ADMIN_KEY_VALUE),
    })
    expect(res.statusCode).toBe(200)
    const keys = res.json() as Array<{ name: string; role: string }>
    expect(keys.some(k => k.name === 'listed-key')).toBe(true)
    expect(keys.every(k => k.role !== undefined)).toBe(true)
  })

  it('viewer cannot list keys — gets 403 (key mgmt is admin-only)', async () => {
    const { raw } = await createApiKey(db, 'read-only-viewer', 'viewer')
    const res = await app.inject({
      method: 'GET', url: '/api-keys',
      headers: auth(raw),
    })
    expect(res.statusCode).toBe(403)
    expect(res.json().error).toContain("requires the 'admin' role")
  })

  it('editor cannot list keys — gets 403', async () => {
    const { raw } = await createApiKey(db, 'ed-viewer', 'editor')
    const res = await app.inject({
      method: 'GET', url: '/api-keys',
      headers: auth(raw),
    })
    expect(res.statusCode).toBe(403)
  })

  it('includes role field for all keys', async () => {
    await createApiKey(db, 'v', 'viewer')
    await createApiKey(db, 'e', 'editor')
    await createApiKey(db, 'a', 'admin')
    const res = await app.inject({
      method: 'GET', url: '/api-keys',
      headers: auth(ADMIN_KEY_VALUE),
    })
    const roles = res.json().map((k: { role: string }) => k.role).sort()
    expect(roles).toEqual(['admin', 'editor', 'viewer'])
  })
})

// ── GET /dags — viewer allowed ──────────────────────────────────────────────

describe('GET /dags — viewer access', () => {
  it('viewer can list dags', async () => {
    registerTestDag('rbac_list_dag')
    const { raw } = await createApiKey(db, 'read-only', 'viewer')
    const res = await app.inject({ method: 'GET', url: '/dags', headers: auth(raw) })
    expect(res.statusCode).toBe(200)
    const dags = res.json() as Array<{ id: string }>
    expect(dags.some(d => d.id === 'rbac_list_dag')).toBe(true)
  })

  it('no token → 401', async () => {
    const res = await app.inject({ method: 'GET', url: '/dags' })
    expect(res.statusCode).toBe(401)
  })
})

// ── POST /dags/:dagId/trigger — editor required ────────────────────────────

describe('POST trigger — editor/admin allowed, viewer blocked', () => {
  it('editor can trigger a dag run', async () => {
    registerTestDag('trigger_dag_editor')
    const { raw } = await createApiKey(db, 'editor-trigger', 'editor')
    const res = await app.inject({
      method: 'POST',
      url: '/dags/trigger_dag_editor/trigger',
      headers: auth(raw),
      payload: {},
    })
    expect(res.statusCode).toBe(201)
    expect(res.json().state).toMatch(/^(queued|running|success|failed)$/)
  })

  it('admin can trigger a dag run', async () => {
    registerTestDag('trigger_dag_admin')
    const res = await app.inject({
      method: 'POST',
      url: '/dags/trigger_dag_admin/trigger',
      headers: auth(ADMIN_KEY_VALUE),
      payload: {},
    })
    expect(res.statusCode).toBe(201)
  })

  it('viewer cannot trigger — gets 403', async () => {
    registerTestDag('trigger_dag_viewer')
    const { raw } = await createApiKey(db, 'viewer-trigger', 'viewer')
    const res = await app.inject({
      method: 'POST',
      url: '/dags/trigger_dag_viewer/trigger',
      headers: auth(raw),
      payload: {},
    })
    expect(res.statusCode).toBe(403)
    expect(res.json().error).toContain("requires the 'editor' role")
    expect(res.json().error).toContain("you have 'viewer'")
  })
})

// ── DELETE /api-keys/:id — admin only ──────────────────────────────────────

describe('DELETE /api-keys/:keyId — admin only', () => {
  it('admin can revoke a key', async () => {
    const { id } = await createApiKey(db, 'to-revoke', 'viewer')
    const res = await app.inject({
      method: 'DELETE',
      url: `/api-keys/${id}`,
      headers: auth(ADMIN_KEY_VALUE),
    })
    expect(res.statusCode).toBe(200)
    expect(res.json().revoked).toBe(true)
  })

  it('editor cannot revoke — gets 403', async () => {
    const { id } = await createApiKey(db, 'another', 'viewer')
    const { raw: editorRaw } = await createApiKey(db, 'ed', 'editor')
    const res = await app.inject({
      method: 'DELETE',
      url: `/api-keys/${id}`,
      headers: auth(editorRaw),
    })
    expect(res.statusCode).toBe(403)
  })
})

// ── Revoked key no longer works ─────────────────────────────────────────────

describe('revoked key is rejected', () => {
  it('revoked admin key gets 401 on subsequent requests', async () => {
    const { raw, id } = await createApiKey(db, 'temp-admin', 'admin')

    // Key works before revocation
    const before = await app.inject({ method: 'GET', url: '/dags', headers: auth(raw) })
    expect(before.statusCode).toBe(200)

    // Revoke via ADMIN_KEY
    await app.inject({
      method: 'DELETE',
      url: `/api-keys/${id}`,
      headers: auth(ADMIN_KEY_VALUE),
    })

    // Key no longer valid
    const after = await app.inject({ method: 'GET', url: '/dags', headers: auth(raw) })
    expect(after.statusCode).toBe(401)
  })
})

// ── validateApiKey returns { name, role } ──────────────────────────────────

describe('validateApiKey — returns { name, role }', () => {
  it('returns role for a valid key', async () => {
    const { validateApiKey } = await import('../keys.js')
    const { raw } = await createApiKey(db, 'role-check', 'editor')
    const result = await validateApiKey(db, raw)
    expect(result).not.toBeNull()
    expect(result?.role).toBe('editor')
    expect(result?.name).toBe('role-check')
  })

  it('returns null for an unknown key', async () => {
    const { validateApiKey } = await import('../keys.js')
    expect(await validateApiKey(db, 'an_nonexistent')).toBeNull()
  })

  it('pre-migration key without role field defaults to viewer (fail-closed)', async () => {
    const { validateApiKey } = await import('../keys.js')
    const { scrypt, randomBytes } = await import('node:crypto')
    const { promisify } = await import('node:util')
    const scryptAsync = promisify(scrypt)

    // Insert a key document that has no role field (pre-migration)
    const legacyRaw = 'an_legacy_raw_test_key'
    const salt = randomBytes(16)
    const hash = (await scryptAsync(legacyRaw, salt, 32)) as Buffer
    const keyHash = `${salt.toString('hex')}:${hash.toString('hex')}`

    await db.collection('api_keys').insertOne({
      name: 'no-role-legacy',
      key_hash: keyHash,
      created_at: new Date(),
      last_used_at: null,
      revoked: false,
      // intentionally no 'role' field
    })

    const result = await validateApiKey(db, legacyRaw)
    expect(result).not.toBeNull()
    expect(result?.role).toBe('viewer')  // fail-closed: missing role → viewer
  })
})
