/**
 * API tests for Docker image management routes.
 *
 *   GET    /images          — list uploaded .tar files
 *   POST   /images/upload   — upload a .tar, save to dags/images/, docker load
 *   DELETE /images/:name    — remove a .tar file
 *
 * These are route-level tests using Fastify inject (no real HTTP).
 * docker load is fire-and-forget — we don't assert on it here (tested in images.test.ts).
 * We verify: correct status codes, response shapes, file CRUD, and validation.
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { mkdir, rm, writeFile, readdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import type { FastifyInstance } from 'fastify'
import { buildServer } from '../server.js'
import { clearRegistry } from '../../dag/registry.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

// Use a temp dir as the dags/ directory so we don't pollute the real one
let testDagsDir: string
let testImagesDir: string

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_images_api')
  clearRegistry()

  // Create isolated temp dirs
  testDagsDir   = join(tmpdir(), `airflow-images-test-${Date.now()}`)
  testImagesDir = join(testDagsDir, 'images')
  await mkdir(testImagesDir, { recursive: true })

  // Override DAGS_DIR for the images route (via process.cwd() mock isn't feasible;
  // instead we pre-create the images dir and let the route use it)
  process.env.TEST_IMAGES_DIR = testImagesDir

  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  await rm(testDagsDir, { recursive: true, force: true })
  delete process.env.TEST_IMAGES_DIR
})

afterEach(async () => {
  // Clean up uploaded files between tests
  try {
    const files = await readdir(testImagesDir)
    await Promise.all(files.map(f => rm(join(testImagesDir, f), { force: true })))
  } catch { /* dir may not exist */ }
})

// ── helpers ───────────────────────────────────────────────────────────────────

const AUTH = { Authorization: 'Bearer airflow' }

/** Create a minimal fake .tar content (not a real docker image — just file bytes) */
function fakeTar(label = 'test'): Buffer {
  return Buffer.from(`fake-docker-image-tar:${label}`)
}

function multipartBody(filename: string, content: Buffer): { body: Buffer; boundary: string } {
  const boundary = `----FormBoundary${Date.now()}`
  const body = Buffer.concat([
    Buffer.from(`--${boundary}\r\nContent-Disposition: form-data; name="file"; filename="${filename}"\r\nContent-Type: application/x-tar\r\n\r\n`),
    content,
    Buffer.from(`\r\n--${boundary}--\r\n`),
  ])
  return { body, boundary }
}

// ══════════════════════════════════════════════════════════════════════════════
// GET /images
// ══════════════════════════════════════════════════════════════════════════════

describe('GET /images', () => {
  it('returns empty array when no images uploaded', async () => {
    const res = await app.inject({ method: 'GET', url: '/images', headers: AUTH })
    expect(res.statusCode).toBe(200)
    expect(JSON.parse(res.body)).toEqual([])
  })

  it('lists uploaded .tar files with metadata', async () => {
    // Pre-populate the images dir
    await writeFile(join(testImagesDir, 'python-slim.tar'), fakeTar('python'))
    await writeFile(join(testImagesDir, 'ruby-slim.tar'),   fakeTar('ruby'))

    const res = await app.inject({ method: 'GET', url: '/images', headers: AUTH })
    expect(res.statusCode).toBe(200)

    const body = JSON.parse(res.body)
    const names = body.map((f: any) => f.name).sort()
    expect(names).toEqual(['python-slim.tar', 'ruby-slim.tar'])

    // Each entry has required fields
    for (const entry of body) {
      expect(entry).toHaveProperty('name')
      expect(entry).toHaveProperty('path')
      expect(entry).toHaveProperty('size_bytes')
      expect(entry.path).toMatch(/^\.\/images\//)
      expect(entry.name).toMatch(/\.tar$/)
    }
  })

  it('does not list non-.tar files in images dir', async () => {
    await writeFile(join(testImagesDir, 'notes.txt'), 'not a tar')
    await writeFile(join(testImagesDir, 'valid.tar'), fakeTar())

    const res = await app.inject({ method: 'GET', url: '/images', headers: AUTH })
    const names = JSON.parse(res.body).map((f: any) => f.name)
    expect(names).toContain('valid.tar')
    expect(names).not.toContain('notes.txt')
  })

  it('path field matches the requiredImages format', async () => {
    await writeFile(join(testImagesDir, 'myimage.tar'), fakeTar())
    const res = await app.inject({ method: 'GET', url: '/images', headers: AUTH })
    const [entry] = JSON.parse(res.body)
    expect(entry.path).toBe('./images/myimage.tar')
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// POST /images/upload
// ══════════════════════════════════════════════════════════════════════════════

describe('POST /images/upload', () => {
  it('rejects non-.tar files with 400', async () => {
    const { body, boundary } = multipartBody('script.sh', Buffer.from('#!/bin/sh'))
    const res = await app.inject({
      method: 'POST', url: '/images/upload',
      headers: { ...AUTH, 'Content-Type': `multipart/form-data; boundary=${boundary}` },
      payload: body,
    })
    expect(res.statusCode).toBe(400)
    expect(JSON.parse(res.body).error).toMatch(/\.tar/)
  })

  it('rejects requests with no file part', async () => {
    const res = await app.inject({
      method: 'POST', url: '/images/upload',
      headers: { ...AUTH, 'Content-Type': 'multipart/form-data; boundary=empty' },
      payload: Buffer.from('--empty--\r\n'),
    })
    expect(res.statusCode).toBe(400)
  })

  it('uploads a .tar file and returns 201 with metadata', async () => {
    const { body, boundary } = multipartBody('myimage.tar', fakeTar('upload-test'))
    const res = await app.inject({
      method: 'POST', url: '/images/upload',
      headers: { ...AUTH, 'Content-Type': `multipart/form-data; boundary=${boundary}` },
      payload: body,
    })
    expect(res.statusCode).toBe(201)
    const data = JSON.parse(res.body)
    expect(data.name).toBe('myimage.tar')
    expect(data.path).toBe('./images/myimage.tar')
    expect(data.size_bytes).toBeGreaterThan(0)
    expect(data.message).toMatch(/[Uu]ploaded/)
  })

  it('saved file appears in GET /images listing', async () => {
    const { body, boundary } = multipartBody('listed.tar', fakeTar())
    await app.inject({
      method: 'POST', url: '/images/upload',
      headers: { ...AUTH, 'Content-Type': `multipart/form-data; boundary=${boundary}` },
      payload: body,
    })

    const listRes = await app.inject({ method: 'GET', url: '/images', headers: AUTH })
    const names = JSON.parse(listRes.body).map((f: any) => f.name)
    expect(names).toContain('listed.tar')
  })

  it('sanitizes filename — strips path components', async () => {
    // Filename with path traversal attempt
    const { body, boundary } = multipartBody('../../../etc/evil.tar', fakeTar())
    const res = await app.inject({
      method: 'POST', url: '/images/upload',
      headers: { ...AUTH, 'Content-Type': `multipart/form-data; boundary=${boundary}` },
      payload: body,
    })
    if (res.statusCode === 201) {
      // Saved name should not contain path separators
      const data = JSON.parse(res.body)
      expect(data.name).not.toContain('/')
      expect(data.name).not.toContain('..')
    }
    // 400 is also acceptable (rejected outright)
    expect([201, 400]).toContain(res.statusCode)
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// DELETE /images/:name
// ══════════════════════════════════════════════════════════════════════════════

describe('DELETE /images/:name', () => {
  it('returns 404 for non-existent file', async () => {
    const res = await app.inject({
      method: 'DELETE', url: '/images/does-not-exist.tar', headers: AUTH,
    })
    expect(res.statusCode).toBe(404)
  })

  it('deletes an existing .tar file', async () => {
    await writeFile(join(testImagesDir, 'to-delete.tar'), fakeTar())

    const res = await app.inject({
      method: 'DELETE', url: '/images/to-delete.tar', headers: AUTH,
    })
    expect(res.statusCode).toBe(200)
    expect(JSON.parse(res.body).deleted).toBe('to-delete.tar')

    // Confirm gone from listing
    const listRes = await app.inject({ method: 'GET', url: '/images', headers: AUTH })
    const names = JSON.parse(listRes.body).map((f: any) => f.name)
    expect(names).not.toContain('to-delete.tar')
  })

  it('rejects path traversal attempts with 400', async () => {
    const res = await app.inject({
      method: 'DELETE', url: '/images/..%2F..%2Fetc%2Fpasswd', headers: AUTH,
    })
    expect(res.statusCode).toBe(400)
  })

  it('rejects non-.tar names with 400', async () => {
    const res = await app.inject({
      method: 'DELETE', url: '/images/notatar.sh', headers: AUTH,
    })
    expect(res.statusCode).toBe(400)
  })
})
