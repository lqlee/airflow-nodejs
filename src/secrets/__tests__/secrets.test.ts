/**
 * Tests for the secrets backend system.
 *
 * What each test answers:
 *  - NullBackend (default): returns null for everything?
 *  - EnvBackend: reads AIRFLOW_CONN_{ID}/AIRFLOW_VAR_{KEY} from process.env?
 *  - EnvBackend: parses JSON connection correctly?
 *  - EnvBackend: handles malformed JSON gracefully?
 *  - FileBackend: reads connections and variables from JSON file?
 *  - FileBackend: handles missing file gracefully (returns null)?
 *  - FileBackend: handles malformed JSON gracefully?
 *  - getSecretsBackend() selects backend from SECRETS_BACKEND env?
 *  - Fallback: variable NOT in DB → resolved from backend?
 *  - Fallback: variable IN DB → backend not consulted (DB wins)?
 *  - Fallback: connection NOT in DB → resolved from backend?
 */

import { describe, it, expect, beforeEach, afterEach, beforeAll, afterAll } from 'vitest'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { MongoClient, type Db } from 'mongodb'
import {
  FileSecretsBackend,
  setSecretsBackend,
  resetSecretsBackend,
  getSecretsBackend,
} from '../index.js'
import { getVariableRuntime } from '../../variables/index.js'
import { getConnectionRuntime } from '../../connections/index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_secrets')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

beforeEach(() => { resetSecretsBackend() })
afterEach(() => { resetSecretsBackend() })

// ══════════════════════════════════════════════════════════════════════════════
// NullBackend (default)
// ══════════════════════════════════════════════════════════════════════════════

describe('NullSecretsBackend (SECRETS_BACKEND=none)', () => {
  it('returns null for connections', async () => {
    const backend = getSecretsBackend()
    expect(backend.name).toBe('none')
    expect(await backend.getConnection('any')).toBeNull()
  })

  it('returns null for variables', async () => {
    const backend = getSecretsBackend()
    expect(await backend.getVariable('any')).toBeNull()
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// EnvSecretsBackend
// ══════════════════════════════════════════════════════════════════════════════

describe('EnvSecretsBackend (SECRETS_BACKEND=env)', () => {
  beforeEach(() => {
    process.env.SECRETS_BACKEND = 'env'
    resetSecretsBackend()
  })
  afterEach(() => {
    delete process.env.SECRETS_BACKEND
    delete process.env.AIRFLOW_VAR_MY_KEY
    delete process.env.AIRFLOW_CONN_MY_DB
  })

  it('reads variable from AIRFLOW_VAR_<KEY_UPPER>', async () => {
    process.env.AIRFLOW_VAR_MY_KEY = 'hello-secret'
    const backend = getSecretsBackend()
    expect(await backend.getVariable('my_key')).toBe('hello-secret')
  })

  it('returns null for missing variable', async () => {
    const backend = getSecretsBackend()
    expect(await backend.getVariable('does_not_exist')).toBeNull()
  })

  it('reads connection from AIRFLOW_CONN_<CONN_ID_UPPER>', async () => {
    process.env.AIRFLOW_CONN_MY_DB = JSON.stringify({
      conn_type: 'postgres',
      host: 'db.example.com',
      port: 5432,
      login: 'admin',
      password: 'secret123',
      schema: 'public',
    })
    const backend = getSecretsBackend()
    const conn = await backend.getConnection('my_db')
    expect(conn).not.toBeNull()
    expect(conn!.conn_type).toBe('postgres')
    expect(conn!.host).toBe('db.example.com')
    expect(conn!.port).toBe(5432)
    expect(conn!.login).toBe('admin')
    expect(conn!.password).toBe('secret123')
    expect(conn!.schema).toBe('public')
  })

  it('accepts json: prefix in AIRFLOW_CONN', async () => {
    process.env.AIRFLOW_CONN_MY_DB = 'json:{"conn_type":"http","host":"api.example.com"}'
    const conn = await getSecretsBackend().getConnection('my_db')
    expect(conn?.conn_type).toBe('http')
    expect(conn?.host).toBe('api.example.com')
  })

  it('returns null for malformed connection JSON', async () => {
    process.env.AIRFLOW_CONN_MY_DB = 'not-valid-json'
    const conn = await getSecretsBackend().getConnection('my_db')
    expect(conn).toBeNull()
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// FileSecretsBackend
// ══════════════════════════════════════════════════════════════════════════════

describe('FileSecretsBackend', () => {
  let tmpDir: string

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), 'airflow-secrets-test-'))
  })
  afterEach(async () => {
    await rm(tmpDir, { recursive: true })
  })

  it('reads variables from JSON file', async () => {
    const filePath = join(tmpDir, 'secrets.json')
    await writeFile(filePath, JSON.stringify({
      variables: { api_key: 'my-secret-key', region: 'us-east-1' }
    }))
    const backend = new FileSecretsBackend(filePath)
    expect(await backend.getVariable('api_key')).toBe('my-secret-key')
    expect(await backend.getVariable('region')).toBe('us-east-1')
    expect(await backend.getVariable('missing')).toBeNull()
  })

  it('reads connections from JSON file', async () => {
    const filePath = join(tmpDir, 'secrets.json')
    await writeFile(filePath, JSON.stringify({
      connections: {
        prod_db: {
          conn_type: 'postgres',
          host: 'prod.db.example.com',
          port: 5432,
          login: 'prod_user',
          password: 'prod_pass',
          schema: 'app',
        }
      }
    }))
    const backend = new FileSecretsBackend(filePath)
    const conn = await backend.getConnection('prod_db')
    expect(conn).not.toBeNull()
    expect(conn!.conn_type).toBe('postgres')
    expect(conn!.host).toBe('prod.db.example.com')
    expect(conn!.password).toBe('prod_pass')
  })

  it('returns null for missing connection', async () => {
    const filePath = join(tmpDir, 'secrets.json')
    await writeFile(filePath, JSON.stringify({ connections: {} }))
    const backend = new FileSecretsBackend(filePath)
    expect(await backend.getConnection('nonexistent')).toBeNull()
  })

  it('handles missing file gracefully', async () => {
    const backend = new FileSecretsBackend('/nonexistent/path/secrets.json')
    expect(await backend.getVariable('key')).toBeNull()
    expect(await backend.getConnection('conn')).toBeNull()
  })

  it('handles malformed JSON file gracefully', async () => {
    const filePath = join(tmpDir, 'secrets.json')
    await writeFile(filePath, 'not valid json {{{')
    const backend = new FileSecretsBackend(filePath)
    expect(await backend.getVariable('key')).toBeNull()
  })

  it('invalidate() forces re-read on next access', async () => {
    const filePath = join(tmpDir, 'secrets.json')
    await writeFile(filePath, JSON.stringify({ variables: { key: 'v1' } }))
    const backend = new FileSecretsBackend(filePath)
    expect(await backend.getVariable('key')).toBe('v1')

    // Update file
    await writeFile(filePath, JSON.stringify({ variables: { key: 'v2' } }))
    backend.invalidate()
    expect(await backend.getVariable('key')).toBe('v2')
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// Fallback chain: DB → secrets backend
// ══════════════════════════════════════════════════════════════════════════════

describe('secrets fallback chain', () => {
  let tmpDir: string

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), 'airflow-secrets-fallback-'))
    await db.collection('variables').deleteMany({})
    await db.collection('connections').deleteMany({})
    resetSecretsBackend()
  })
  afterEach(async () => {
    await rm(tmpDir, { recursive: true })
    await db.collection('variables').deleteMany({})
    await db.collection('connections').deleteMany({})
    resetSecretsBackend()
  })

  it('variable NOT in DB → resolved from file backend', async () => {
    const filePath = join(tmpDir, 'secrets.json')
    await writeFile(filePath, JSON.stringify({ variables: { backend_var: 'from-file' } }))
    setSecretsBackend(new FileSecretsBackend(filePath))

    const value = await getVariableRuntime(db, 'backend_var')
    expect(value).toBe('from-file')
  })

  it('variable IN DB → DB wins, backend NOT consulted', async () => {
    // Insert variable into DB
    await db.collection('variables').insertOne({
      key: 'shared_var',
      value: 'from-db',
      is_secret: false,
      created_at: new Date(),
      updated_at: new Date(),
    })

    // File backend has a different value for same key
    const filePath = join(tmpDir, 'secrets.json')
    await writeFile(filePath, JSON.stringify({ variables: { shared_var: 'from-file' } }))
    setSecretsBackend(new FileSecretsBackend(filePath))

    const value = await getVariableRuntime(db, 'shared_var')
    expect(value).toBe('from-db')  // DB wins
  })

  it('connection NOT in DB → resolved from env backend', async () => {
    process.env.AIRFLOW_CONN_TEST_CONN = JSON.stringify({
      conn_type: 'http', host: 'api.test.com'
    })
    process.env.SECRETS_BACKEND = 'env'
    resetSecretsBackend()

    try {
      const conn = await getConnectionRuntime(db, 'test_conn')
      expect(conn).not.toBeNull()
      expect(conn!.host).toBe('api.test.com')
    } finally {
      delete process.env.AIRFLOW_CONN_TEST_CONN
      delete process.env.SECRETS_BACKEND
    }
  })

  it('variable not in DB and not in backend → null', async () => {
    setSecretsBackend(new FileSecretsBackend('/nonexistent/file.json'))
    const value = await getVariableRuntime(db, 'totally_missing')
    expect(value).toBeNull()
  })
})
