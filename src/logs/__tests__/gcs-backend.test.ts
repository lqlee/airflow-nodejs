/**
 * LOG_BACKEND=gcs: lines are staged in the local file and appended to one object per task instance
 * when the task's process exits. Cloud Storage is an in-memory fake behind fetch (the JSON API calls
 * gcs.ts makes), so no emulator is needed.
 */
import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { existsSync } from 'node:fs'
import { EventEmitter } from 'node:events'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import type { ChildProcess } from 'node:child_process'
import type { Db } from 'mongodb'
import { appendLog, getTaskLogs, getLogFilePath, uploadTaskLog, uploadTaskLogOnExit } from '../index.js'
import { drainPool } from '../../scheduler/pool.js'

const db = {} as Db   // the gcs and file backends never touch the database
const objects = new Map<string, string>()
let failUploads = false

/** The Cloud Storage JSON API calls gcs.ts makes: media download and media upload. */
async function fakeGcs(input: string | URL | Request, init?: RequestInit): Promise<Response> {
  const url = new URL(String(input))
  const upload = /^\/upload\/storage\/v1\/b\/([^/]+)\/o$/.exec(url.pathname)
  if (upload && init?.method === 'POST') {
    if (failUploads) return new Response('backend error', { status: 503 })
    objects.set(`${decodeURIComponent(upload[1])}/${url.searchParams.get('name')}`, String(init.body))
    return new Response('{}', { status: 200 })
  }
  const download = /^\/storage\/v1\/b\/([^/]+)\/o\/(.+)$/.exec(url.pathname)
  if (download && url.searchParams.get('alt') === 'media') {
    const body = objects.get(`${decodeURIComponent(download[1])}/${decodeURIComponent(download[2])}`)
    return body === undefined ? new Response('not found', { status: 404 }) : new Response(body, { status: 200 })
  }
  return new Response('unexpected request', { status: 400 })
}

let logDir: string
beforeEach(async () => {
  logDir = await mkdtemp(join(tmpdir(), 'gcs-logs-'))
  objects.clear()
  failUploads = false
  vi.stubGlobal('fetch', vi.fn(fakeGcs))
  vi.stubEnv('LOG_BACKEND', 'gcs')
  vi.stubEnv('LOG_DIR', logDir)
  vi.stubEnv('LOG_GCS_BUCKET', 'logs-bucket')
  vi.stubEnv('LOG_GCS_PREFIX', 'airflow/task-logs')
  vi.stubEnv('STORAGE_EMULATOR_HOST', 'http://fake-gcs')
})
afterEach(async () => {
  vi.unstubAllGlobals()
  vi.unstubAllEnvs()
  await rm(logDir, { recursive: true, force: true })
})

const lines = async (taskId = 't') => (await getTaskLogs(db, 'run1', taskId, {}, 'my_dag')).map(l => `${l.stream}:${l.level}:${l.line}`)
const OBJECT = 'logs-bucket/airflow/task-logs/my_dag/run1/t.log'

describe('LOG_BACKEND=gcs', () => {
  it('serves staged lines while the task runs, then moves them to the object and removes the local file', async () => {
    await appendLog(db, 'run1', 'my_dag', 't', 'stdout', 'starting')
    await appendLog(db, 'run1', 'my_dag', 't', 'stderr', '[WARN] slow', 'warn')
    expect(await lines()).toEqual(['stdout:info:starting', 'stderr:warn:[WARN] slow'])
    expect(objects.size).toBe(0)

    await uploadTaskLog('my_dag', 'run1', 't')
    expect(existsSync(getLogFilePath('my_dag', 'run1', 't'))).toBe(false)
    expect(objects.get(OBJECT)!.trim().split('\n')).toHaveLength(2)
    expect(await lines()).toEqual(['stdout:info:starting', 'stderr:warn:[WARN] slow'])
  })

  it('appends each try to the object, so a retry on another replica keeps the earlier tries', async () => {
    await appendLog(db, 'run1', 'my_dag', 't', 'stdout', 'try 1')
    await uploadTaskLog('my_dag', 'run1', 't')
    // the retry runs elsewhere: this replica has no local file left, only the object
    await appendLog(db, 'run1', 'my_dag', 't', 'stdout', 'try 2')
    expect(await lines()).toEqual(['stdout:info:try 1', 'stdout:info:try 2'])   // object + staged
    await uploadTaskLog('my_dag', 'run1', 't')
    expect(await lines()).toEqual(['stdout:info:try 1', 'stdout:info:try 2'])
    expect(objects.get(OBJECT)!.trim().split('\n')).toHaveLength(2)
  })

  it('keeps the local file when the upload fails, so nothing is lost', async () => {
    await appendLog(db, 'run1', 'my_dag', 't', 'stdout', 'kept')
    failUploads = true
    await expect(uploadTaskLog('my_dag', 'run1', 't')).rejects.toThrow(/write gs:\/\/logs-bucket\/airflow\/task-logs\/my_dag\/run1\/t\.log: HTTP 503/)
    expect(await lines()).toEqual(['stdout:info:kept'])
    failUploads = false
    await uploadTaskLog('my_dag', 'run1', 't')
    expect(objects.has(OBJECT)).toBe(true)
  })

  it('uploads when the task process closes, including lines logged right before it exited', async () => {
    const child = new EventEmitter() as ChildProcess
    uploadTaskLogOnExit(child, { dag_id: 'my_dag', dag_run_id: 'run1', task_id: 't' })
    void appendLog(db, 'run1', 'my_dag', 't', 'stdout', 'last words')   // still being written
    child.emit('close', 0)
    expect((await drainPool({ timeoutMs: 5_000, pollMs: 5 })).drained).toBe(true)
    expect(objects.get(OBJECT)).toContain('last words')
  })

  it('needs a bucket', async () => {
    vi.stubEnv('LOG_GCS_BUCKET', '')
    await appendLog(db, 'run1', 'my_dag', 't', 'stdout', 'x')
    await expect(uploadTaskLog('my_dag', 'run1', 't')).rejects.toThrow('LOG_BACKEND=gcs needs LOG_GCS_BUCKET')
  })
})

describe('file backend ordering', () => {
  it('keeps lines in the order they were logged, even when appends overlap', async () => {
    vi.stubEnv('LOG_BACKEND', 'file')
    await Promise.all(Array.from({ length: 50 }, (_, i) => appendLog(db, 'run1', 'my_dag', 'order', 'stdout', `line ${i}`)))
    expect((await getTaskLogs(db, 'run1', 'order', {}, 'my_dag')).map(l => l.line))
      .toEqual(Array.from({ length: 50 }, (_, i) => `line ${i}`))
  })

  it('does nothing on upload when the backend is not gcs', async () => {
    vi.stubEnv('LOG_BACKEND', 'file')
    await appendLog(db, 'run1', 'my_dag', 't', 'stdout', 'local only')
    await uploadTaskLog('my_dag', 'run1', 't')
    expect(objects.size).toBe(0)
    expect(existsSync(getLogFilePath('my_dag', 'run1', 't'))).toBe(true)
  })
})
