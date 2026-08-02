/**
 * Tests for log severity levels and log backends (file + mongodb).
 *
 * What each test answers:
 *  - Does appendLog default level to 'info' for stdout, 'error' for stderr?
 *  - Does appendLog store the explicit level when provided?
 *  - Does getTaskLogs return all levels when no filter?
 *  - Does ?level=warn return only warn + error (not info/debug)?
 *  - Does ?level=error return only error?
 *  - Does ?stream=stderr filter to stderr only?
 *  - Does combined ?level=warn&stream=stderr filter correctly?
 *  - File backend: writes a .log file, reads it back with correct levels?
 *  - File backend: ?level filter works on file-based logs?
 *  - Does a DAG task using ctx.log.warn() write [WARN] prefix to stdout?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach, beforeEach } from 'vitest'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { MongoClient, type Db } from 'mongodb'
import { appendLog, getTaskLogs } from '../index.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_DB = 'airflow_test_log_levels'

let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.DB_NAME = TEST_DB
  // Default to mongodb for existing tests to avoid needing dag_id param
  process.env.LOG_BACKEND = 'mongodb'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(TEST_DB)
  clearRegistry()
})

afterAll(async () => {
  await new Promise(r => setTimeout(r, 300))
  delete process.env.DB_NAME
  delete process.env.LOG_BACKEND
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('task_logs').deleteMany({})
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  clearRegistry()
})

// ── helpers ───────────────────────────────────────────────────────────────────

async function seedLogs(runId: string, taskId: string) {
  await appendLog(db, runId, 'dag', taskId, 'stdout', 'debug message', 'debug')
  await appendLog(db, runId, 'dag', taskId, 'stdout', 'info message',  'info')
  await appendLog(db, runId, 'dag', taskId, 'stderr', 'warn message',  'warn')
  await appendLog(db, runId, 'dag', taskId, 'stderr', 'error message', 'error')
}

// ══════════════════════════════════════════════════════════════════════════════
// appendLog defaults
// ══════════════════════════════════════════════════════════════════════════════

describe('appendLog level defaults', () => {
  it('stdout with no level → stored as info', async () => {
    await appendLog(db, 'run1', 'dag', 'task', 'stdout', 'hello')
    const logs = await db.collection('task_logs').find({ dag_run_id: 'run1' }).toArray()
    expect(logs[0].level).toBe('info')
  })

  it('stderr with no level → stored as error', async () => {
    await appendLog(db, 'run2', 'dag', 'task', 'stderr', 'oh no')
    const logs = await db.collection('task_logs').find({ dag_run_id: 'run2' }).toArray()
    expect(logs[0].level).toBe('error')
  })

  it('explicit level overrides default', async () => {
    await appendLog(db, 'run3', 'dag', 'task', 'stderr', 'heads up', 'warn')
    const logs = await db.collection('task_logs').find({ dag_run_id: 'run3' }).toArray()
    expect(logs[0].level).toBe('warn')
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// getTaskLogs filtering
// ══════════════════════════════════════════════════════════════════════════════

describe('getTaskLogs level filtering', () => {
  it('no filter → returns all 4 levels', async () => {
    await seedLogs('r1', 't1')
    const logs = await getTaskLogs(db, 'r1', 't1')
    expect(logs.length).toBe(4)
  })

  it('level=debug → returns all (debug is lowest)', async () => {
    await seedLogs('r2', 't2')
    const logs = await getTaskLogs(db, 'r2', 't2', { level: 'debug' })
    expect(logs.length).toBe(4)
    const levels = logs.map(l => l.level).sort()
    expect(levels).toEqual(['debug', 'error', 'info', 'warn'])
  })

  it('level=info → returns info + warn + error (excludes debug)', async () => {
    await seedLogs('r3', 't3')
    const logs = await getTaskLogs(db, 'r3', 't3', { level: 'info' })
    expect(logs.length).toBe(3)
    expect(logs.every(l => l.level !== 'debug')).toBe(true)
  })

  it('level=warn → returns only warn + error', async () => {
    await seedLogs('r4', 't4')
    const logs = await getTaskLogs(db, 'r4', 't4', { level: 'warn' })
    expect(logs.length).toBe(2)
    expect(logs.every(l => l.level === 'warn' || l.level === 'error')).toBe(true)
  })

  it('level=error → returns only error lines', async () => {
    await seedLogs('r5', 't5')
    const logs = await getTaskLogs(db, 'r5', 't5', { level: 'error' })
    expect(logs.length).toBe(1)
    expect(logs[0].level).toBe('error')
    expect(logs[0].line).toBe('error message')
  })

  it('stream=stdout → returns only stdout lines', async () => {
    await seedLogs('r6', 't6')
    const logs = await getTaskLogs(db, 'r6', 't6', { stream: 'stdout' })
    expect(logs.length).toBe(2)
    expect(logs.every(l => l.stream === 'stdout')).toBe(true)
  })

  it('stream=stderr → returns only stderr lines', async () => {
    await seedLogs('r7', 't7')
    const logs = await getTaskLogs(db, 'r7', 't7', { stream: 'stderr' })
    expect(logs.length).toBe(2)
    expect(logs.every(l => l.stream === 'stderr')).toBe(true)
  })

  it('level=warn + stream=stderr → returns warn+error on stderr only', async () => {
    await seedLogs('r8', 't8')
    const logs = await getTaskLogs(db, 'r8', 't8', { level: 'warn', stream: 'stderr' })
    expect(logs.length).toBe(2)
    expect(logs.every(l => l.stream === 'stderr')).toBe(true)
    expect(logs.every(l => l.level === 'warn' || l.level === 'error')).toBe(true)
  })

  it('logs are sorted by timestamp ascending', async () => {
    await seedLogs('r9', 't9')
    const logs = await getTaskLogs(db, 'r9', 't9')
    for (let i = 1; i < logs.length; i++) {
      expect(new Date(logs[i].ts).getTime()).toBeGreaterThanOrEqual(new Date(logs[i-1].ts).getTime())
    }
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// Integration: ctx.log in a real task
// ══════════════════════════════════════════════════════════════════════════════

describe('ctx.log integration', () => {
  it('ctx.log.info() stores info level in task_logs', async () => {
    const dag: DagDefinition = {
      id: 'log_levels_dag',
      schedule: null,
      tasks: {
        step: {
          run: async (ctx) => {
            ctx.log.debug('debug message from task')
            ctx.log.info('info message from task')
            ctx.log.warn('warn message from task')
            ctx.log.error('error message from task')
            return 'done'
          }
        }
      }
    }
    register(dag)
    const runId = await createRun(db, dag)
    for (let i = 0; i < 15; i++) await advanceRun(db, runId)

    // Wait for log writes (fire-and-forget in worker)
    await new Promise(r => setTimeout(r, 500))

    const allLogs = await db.collection('task_logs')
      .find({ dag_run_id: runId, task_id: 'step' })
      .toArray()

    const levels = allLogs.map(l => l.level)

    expect(levels).toContain('debug')
    expect(levels).toContain('info')
    expect(levels).toContain('warn')
    expect(levels).toContain('error')
  })

  it('ctx.log.warn() line is prefixed with [WARN]', async () => {
    const dag: DagDefinition = {
      id: 'log_prefix_dag',
      schedule: null,
      tasks: {
        step: {
          run: async (ctx) => {
            ctx.log.warn('rate limit approaching')
            return 'ok'
          }
        }
      }
    }
    register(dag)
    const runId = await createRun(db, dag)
    for (let i = 0; i < 15; i++) await advanceRun(db, runId)
    await new Promise(r => setTimeout(r, 500))

    const warnLogs = await db.collection('task_logs')
      .find({ dag_run_id: runId, task_id: 'step', level: 'warn' })
      .toArray()
    // At least one warn log from ctx.log.warn()
    expect(warnLogs.some(l => (l.line as string).includes('[WARN]'))).toBe(true)
    expect(warnLogs.some(l => (l.line as string).includes('rate limit'))).toBe(true)
  })

  it('ctx.log.info() accepts objects (JSON stringified)', async () => {
    const dag: DagDefinition = {
      id: 'log_object_dag',
      schedule: null,
      tasks: {
        step: {
          run: async (ctx) => {
            ctx.log.info({ records: 42, source: 'db' })
            return 'ok'
          }
        }
      }
    }
    register(dag)
    const runId = await createRun(db, dag)
    for (let i = 0; i < 15; i++) await advanceRun(db, runId)
    await new Promise(r => setTimeout(r, 500))

    const infoLogs = await db.collection('task_logs')
      .find({ dag_run_id: runId, task_id: 'step', level: 'info' })
      .toArray()
    expect(infoLogs.some(l => (l.line as string).includes('"records":42'))).toBe(true)
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// File backend tests
// ══════════════════════════════════════════════════════════════════════════════

describe('file log backend', () => {
  let tmpDir: string

  beforeEach(async () => {
    tmpDir = await mkdtemp(join(tmpdir(), 'airflow-logs-test-'))
    process.env.LOG_BACKEND = 'file'
    process.env.LOG_DIR = tmpDir
  })

  afterEach(async () => {
    process.env.LOG_BACKEND = 'mongodb'  // restore for other tests
    delete process.env.LOG_DIR
    await rm(tmpDir, { recursive: true })
  })

  it('writes log file and reads it back', async () => {
    await appendLog(db, 'run123', 'my_dag', 'step1', 'stdout', 'hello world', 'info')
    const logs = await getTaskLogs(db, 'run123', 'step1', {}, 'my_dag')
    expect(logs.length).toBe(1)
    expect(logs[0].line).toBe('hello world')
    expect(logs[0].level).toBe('info')
    expect(logs[0].stream).toBe('stdout')
  })

  it('stores multiple lines in order', async () => {
    await appendLog(db, 'run456', 'dag2', 'taskA', 'stdout', 'line 1', 'debug')
    await appendLog(db, 'run456', 'dag2', 'taskA', 'stdout', 'line 2', 'info')
    await appendLog(db, 'run456', 'dag2', 'taskA', 'stderr', 'line 3', 'error')
    const logs = await getTaskLogs(db, 'run456', 'taskA', {}, 'dag2')
    expect(logs.length).toBe(3)
    expect(logs.map(l => l.line)).toEqual(['line 1', 'line 2', 'line 3'])
  })

  it('level filter works on file logs', async () => {
    await appendLog(db, 'run789', 'dag3', 'taskB', 'stdout', 'debug msg',  'debug')
    await appendLog(db, 'run789', 'dag3', 'taskB', 'stdout', 'info msg',   'info')
    await appendLog(db, 'run789', 'dag3', 'taskB', 'stderr', 'warn msg',   'warn')
    await appendLog(db, 'run789', 'dag3', 'taskB', 'stderr', 'error msg',  'error')

    const warnPlus = await getTaskLogs(db, 'run789', 'taskB', { level: 'warn' }, 'dag3')
    expect(warnPlus.length).toBe(2)
    expect(warnPlus.every(l => l.level === 'warn' || l.level === 'error')).toBe(true)

    const errOnly = await getTaskLogs(db, 'run789', 'taskB', { level: 'error' }, 'dag3')
    expect(errOnly.length).toBe(1)
    expect(errOnly[0].level).toBe('error')
  })

  it('stream filter works on file logs', async () => {
    await appendLog(db, 'runABC', 'dag4', 'taskC', 'stdout', 'out line', 'info')
    await appendLog(db, 'runABC', 'dag4', 'taskC', 'stderr', 'err line', 'error')

    const stdoutOnly = await getTaskLogs(db, 'runABC', 'taskC', { stream: 'stdout' }, 'dag4')
    expect(stdoutOnly.length).toBe(1)
    expect(stdoutOnly[0].stream).toBe('stdout')

    const stderrOnly = await getTaskLogs(db, 'runABC', 'taskC', { stream: 'stderr' }, 'dag4')
    expect(stderrOnly.length).toBe(1)
    expect(stderrOnly[0].stream).toBe('stderr')
  })

  it('returns empty array for non-existent task log', async () => {
    const logs = await getTaskLogs(db, 'nonexistent', 'taskX', {}, 'dagX')
    expect(logs).toEqual([])
  })

  it('defaults level: stdout→info, stderr→error', async () => {
    await appendLog(db, 'runDEF', 'dag5', 'taskD', 'stdout', 'auto info')
    await appendLog(db, 'runDEF', 'dag5', 'taskD', 'stderr', 'auto error')
    const logs = await getTaskLogs(db, 'runDEF', 'taskD', {}, 'dag5')
    expect(logs.find(l => l.stream === 'stdout')?.level).toBe('info')
    expect(logs.find(l => l.stream === 'stderr')?.level).toBe('error')
  })
})
