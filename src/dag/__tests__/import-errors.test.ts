import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import {
  setImportErrors,
  getImportErrors,
  hasImportErrors,
} from '../import-errors.js'
import { buildServer } from '../../api/server.js'
import type { FastifyInstance } from 'fastify'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_import_errors')
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

afterEach(() => {
  // Reset to clean state after each test
  setImportErrors([])
})

// ── in-memory registry ────────────────────────────────────────────────────────

describe('import-errors registry', () => {
  it('starts empty', () => {
    expect(getImportErrors()).toEqual([])
    expect(hasImportErrors()).toBe(false)
  })

  it('setImportErrors replaces the list atomically', () => {
    setImportErrors([
      { filename: 'bad_dag.ts', error: 'SyntaxError: Unexpected token', imported_at: new Date() },
    ])
    expect(getImportErrors()).toHaveLength(1)
    expect(hasImportErrors()).toBe(true)

    // Replace with new list — old entry gone
    setImportErrors([
      { filename: 'other.ts', error: 'TypeError: x is not a function', imported_at: new Date() },
    ])
    const errors = getImportErrors()
    expect(errors).toHaveLength(1)
    expect(errors[0].filename).toBe('other.ts')
  })

  it('clearing errors makes hasImportErrors return false', () => {
    setImportErrors([{ filename: 'f.ts', error: 'oops', imported_at: new Date() }])
    expect(hasImportErrors()).toBe(true)
    setImportErrors([])
    expect(hasImportErrors()).toBe(false)
  })

  it('getImportErrors returns a snapshot (not the internal reference)', () => {
    setImportErrors([{ filename: 'a.ts', error: 'e', imported_at: new Date() }])
    const snap1 = getImportErrors()
    setImportErrors([])
    const snap2 = getImportErrors()
    // snap1 should still reflect the old state
    expect(snap1).toHaveLength(1)
    expect(snap2).toHaveLength(0)
  })
})

// ── loader integration ────────────────────────────────────────────────────────

describe('loader — populates import errors', () => {
  it('loadDags with no dags/ dir clears errors', async () => {
    // Seed an error first
    setImportErrors([{ filename: 'stale.ts', error: 'old', imported_at: new Date() }])

    // loadDags with missing dir should clear errors
    const { loadDags } = await import('../loader.js')
    // This will warn about missing dir and clear errors
    await loadDags()
    // After load (even with missing dir), errors are reset
    expect(getImportErrors()).toHaveLength(0)
  })
})

// ── API ───────────────────────────────────────────────────────────────────────

describe('GET /import-errors API', () => {
  it('returns empty array when no errors', async () => {
    setImportErrors([])
    const res = await app.inject({ method: 'GET', url: '/import-errors' })
    expect(res.statusCode).toBe(200)
    expect(res.json()).toEqual([])
  })

  it('returns current error list with all fields', async () => {
    const now = new Date()
    setImportErrors([
      { filename: 'broken_dag.ts', error: 'SyntaxError: Unexpected token }', imported_at: now },
      { filename: 'missing_export.ts', error: 'no valid default export', imported_at: now },
    ])

    const res = await app.inject({ method: 'GET', url: '/import-errors' })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body).toHaveLength(2)

    const filenames = body.map((e: { filename: string }) => e.filename)
    expect(filenames).toContain('broken_dag.ts')
    expect(filenames).toContain('missing_export.ts')

    const broken = body.find((e: { filename: string }) => e.filename === 'broken_dag.ts')
    expect(broken.error).toBe('SyntaxError: Unexpected token }')
    expect(broken.imported_at).toBeDefined()
  })

  it('reflects updated errors after setImportErrors', async () => {
    setImportErrors([{ filename: 'dag_a.ts', error: 'err A', imported_at: new Date() }])
    const res1 = await app.inject({ method: 'GET', url: '/import-errors' })
    expect(res1.json()).toHaveLength(1)

    // Simulate a reload where dag_a.ts was fixed
    setImportErrors([])
    const res2 = await app.inject({ method: 'GET', url: '/import-errors' })
    expect(res2.json()).toHaveLength(0)
  })
})
