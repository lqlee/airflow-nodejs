import { describe, it, expect, beforeAll, afterAll, afterEach, beforeEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { mkdtemp, mkdir, writeFile, rm, stat } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join, dirname } from 'node:path'
import { purgeOldRuns, maybePurge, resetPurgeState, retentionDays } from '../cleanup.js'
import { parseCleanupArgs } from '../cleanup-cli.js'
import { getLogFilePath } from '../../logs/index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let logDir: string
const CHILDREN = ['task_instances', 'task_instance_tries', 'xcoms', 'task_logs', 'sla_alerts', 'event_logs']

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_cleanup')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
beforeEach(async () => {
  logDir = await mkdtemp(join(tmpdir(), 'anode-cleanup-'))
  process.env.LOG_DIR = logDir
  process.env.LOG_BACKEND = 'file'
})
afterEach(async () => {
  for (const c of ['dag_runs', ...CHILDREN]) await db.collection(c).deleteMany({})
  await rm(logDir, { recursive: true, force: true })
  delete process.env.LOG_DIR; delete process.env.LOG_BACKEND; delete process.env.RETENTION_DAYS
  resetPurgeState()
})

const NOW = new Date('2026-06-01T00:00:00Z')
const daysAgo = (n: number) => new Date(NOW.getTime() - n * 86_400_000)
const CUTOFF = daysAgo(30)

/** A run with one row in every child collection and a log file on disk. */
async function seedRun(state: string, createdAt: Date, dagId = 'd'): Promise<string> {
  const { insertedId } = await db.collection('dag_runs').insertOne({ dag_id: dagId, state, created_at: createdAt })
  const runId = insertedId.toString()
  for (const c of CHILDREN) await db.collection(c).insertOne({ dag_run_id: runId, dag_id: dagId, task_id: 't', created_at: createdAt })
  const file = getLogFilePath(dagId, runId, 't')
  await mkdir(dirname(file), { recursive: true })
  await writeFile(file, '{}\n')
  return runId
}
const exists = (p: string) => stat(p).then(() => true, () => false)
const remaining = async (runId: string) =>
  Object.fromEntries(await Promise.all(CHILDREN.map(async c => [c, await db.collection(c).countDocuments({ dag_run_id: runId })])))

describe('purgeOldRuns', () => {
  it('deletes finished runs older than the cutoff with all their rows and log files', async () => {
    const oldOk = await seedRun('success', daysAgo(40))
    const oldFailed = await seedRun('failed', daysAgo(31))

    const res = await purgeOldRuns(db, { olderThan: CUTOFF, batchSize: 1 })   // batch 1 → exercises paging

    expect(res.runs).toBe(2)
    expect(res.rows.dag_runs).toBe(2)
    expect(res.rows.task_instances).toBe(2)
    expect(res.logDirs).toBe(2)
    expect(await db.collection('dag_runs').countDocuments()).toBe(0)
    for (const id of [oldOk, oldFailed]) {
      expect(Object.values(await remaining(id)).every(n => n === 0)).toBe(true)
      expect(await exists(dirname(getLogFilePath('d', id, 't')))).toBe(false)
    }
  })

  it('keeps recent runs and runs that are still active, however old', async () => {
    const recent = await seedRun('success', daysAgo(5))
    const oldRunning = await seedRun('running', daysAgo(90))
    const oldQueued = await seedRun('queued', daysAgo(90))

    const res = await purgeOldRuns(db, { olderThan: CUTOFF })

    expect(res.runs).toBe(0)
    expect(await db.collection('dag_runs').countDocuments()).toBe(3)
    for (const id of [recent, oldRunning, oldQueued]) {
      expect(Object.values(await remaining(id)).every(n => n === 1)).toBe(true)
      expect(await exists(getLogFilePath('d', id, 't'))).toBe(true)
    }
  })

  it('dry run reports what would go without deleting anything', async () => {
    await seedRun('success', daysAgo(40))
    await seedRun('cancelled', daysAgo(50))
    await seedRun('success', daysAgo(1))

    const res = await purgeOldRuns(db, { olderThan: CUTOFF, dryRun: true, batchSize: 1 })

    expect(res).toMatchObject({ dryRun: true, runs: 2, logDirs: 2 })
    expect(res.rows.task_instances).toBe(2)
    expect(await db.collection('dag_runs').countDocuments()).toBe(3)
    expect(await db.collection('task_instances').countDocuments()).toBe(3)
  })

  it('also ages out audit events that are not tied to a run', async () => {
    await db.collection('event_logs').insertMany([
      { event_type: 'dag_paused', dag_id: 'd', created_at: daysAgo(60) },
      { event_type: 'dag_paused', dag_id: 'd', created_at: daysAgo(2) },
    ])
    const res = await purgeOldRuns(db, { olderThan: CUTOFF })
    expect(res.rows.event_logs).toBe(1)
    expect(await db.collection('event_logs').countDocuments()).toBe(1)
  })
})

describe('maybePurge', () => {
  it('does nothing unless RETENTION_DAYS is set', async () => {
    await seedRun('success', daysAgo(400))
    expect(await maybePurge(db, NOW)).toBeNull()
    expect(await db.collection('dag_runs').countDocuments()).toBe(1)
  })

  it('purges with the configured retention, at most once per interval', async () => {
    process.env.RETENTION_DAYS = '30'
    await seedRun('success', daysAgo(40))
    expect((await maybePurge(db, NOW))!.runs).toBe(1)

    await seedRun('success', daysAgo(40))
    expect(await maybePurge(db, new Date(NOW.getTime() + 60_000))).toBeNull()        // throttled
    expect((await maybePurge(db, new Date(NOW.getTime() + 3_600_000)))!.runs).toBe(1) // next hour
  })

  it('retentionDays ignores invalid values', () => {
    expect(retentionDays({ RETENTION_DAYS: '-3' })).toBe(0)
    expect(retentionDays({ RETENTION_DAYS: 'abc' })).toBe(0)
    expect(retentionDays({ RETENTION_DAYS: '14' })).toBe(14)
  })
})

describe('parseCleanupArgs', () => {
  it('parses days, dry-run and batch size', () => {
    expect(parseCleanupArgs(['--days', '30', '--dry-run', '--batch-size', '50'])).toEqual({ days: 30, dryRun: true, batchSize: 50 })
    expect(parseCleanupArgs(['--days', '7'])).toEqual({ days: 7, dryRun: false, batchSize: 100 })
  })
  it('rejects a missing or non-positive --days and a bad batch size', () => {
    expect(() => parseCleanupArgs([])).toThrow(/--days/)
    expect(() => parseCleanupArgs(['--days', '0'])).toThrow(/--days/)
    expect(() => parseCleanupArgs(['--days', '5', '--batch-size', '0'])).toThrow(/--batch-size/)
  })
})
