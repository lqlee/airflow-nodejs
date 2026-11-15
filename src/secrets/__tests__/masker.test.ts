/** Secret masking: what gets registered, how it is redacted, and that a real task's log shows `***`. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { registerSecret, registerSensitiveFields, redact, isSensitiveName, clearSecrets, maskedCount } from '../masker.js'
import { upsertConnection, getConnectionRuntime } from '../../connections/index.js'
import { setVariable, getVariableRuntime } from '../../variables/index.js'
import { appendLog, getTaskLogs } from '../../logs/index.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { ensureIndexes } from '../../db/indexes.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB = 'airflow_test_masker'
let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.LOG_BACKEND = 'mongodb'
  process.env.DB_NAME = DB                    // forked workers connect here
  process.env.ENCRYPTION_KEYS = '3'.repeat(64)
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(DB)
  await ensureIndexes(db)
})
afterAll(async () => {
  for (const k of ['LOG_BACKEND', 'DB_NAME', 'ENCRYPTION_KEYS']) delete process.env[k]
  await db.dropDatabase()
  await client.close()
})
afterEach(async () => {
  clearSecrets()
  clearRegistry()
  delete process.env.MASK_MIN_LENGTH
  delete process.env.SENSITIVE_FIELD_NAMES
  for (const c of ['connections', 'variables', 'dag_runs', 'task_instances', 'task_instance_tries', 'task_logs', 'event_logs']) {
    await db.collection(c).deleteMany({})
  }
})

describe('redact', () => {
  it('masks the value and its JSON-escaped, URL-encoded and per-line forms', () => {
    registerSecret('p@ss "word"/x')
    registerSecret('-----BEGIN KEY-----\nAAAABBBBCCCC\n-----END KEY-----')
    expect(redact('raw p@ss "word"/x')).toBe('raw ***')
    expect(redact(JSON.stringify({ pw: 'p@ss "word"/x' }))).toBe('{"pw":"***"}')
    expect(redact('https://u:p%40ss%20%22word%22%2Fx@host')).toBe('https://u:***@host')
    expect(redact('key line: AAAABBBBCCCC')).toBe('key line: ***')
  })

  it('masks the longest match first and leaves short values alone', () => {
    registerSecret('secret-abc')
    registerSecret('secret-abc-def')
    registerSecret('abc')                     // shorter than MASK_MIN_LENGTH (5)
    expect(redact('secret-abc-def / secret-abc / abc')).toBe('*** / *** / abc')
    process.env.MASK_MIN_LENGTH = '3'
    registerSecret('xyz')
    expect(redact('xyz')).toBe('***')
  })

  it('masks values under sensitive keys of a connection extra, at any depth', () => {
    registerSensitiveFields({ region: 'us-east1', auth: { access_token: 'tok-123456', scopes: ['read-all'] }, keys: [{ private_key: 'pk-abcdef' }] })
    expect(redact('us-east1 tok-123456 read-all pk-abcdef')).toBe('us-east1 *** read-all ***')
  })

  it('sensitive names include extra SENSITIVE_FIELD_NAMES', () => {
    expect(isSensitiveName('DB_PASSWORD')).toBe(true)
    expect(isSensitiveName('region')).toBe(false)
    process.env.SENSITIVE_FIELD_NAMES = 'region'
    expect(isSensitiveName('aws_region')).toBe(true)
  })
})

describe('what the runtime helpers register', () => {
  it('a connection password and sensitive extras; a secret or sensitively-named variable; not a plain one', async () => {
    await upsertConnection(db, { conn_id: 'pg', conn_type: 'postgres', login: 'loginname', password: 'pg-password', extra: '{"api_token":"tok-999999","sslmode":"require"}' })
    await setVariable(db, { key: 'opaque', value: 'is-secret-value', is_secret: true })
    await setVariable(db, { key: 'stripe_api_key', value: 'sk-live-1234' })
    await setVariable(db, { key: 'region', value: 'us-central1' })

    await getConnectionRuntime(db, 'pg')
    for (const k of ['opaque', 'stripe_api_key', 'region']) await getVariableRuntime(db, k)
    expect(redact('loginname pg-password tok-999999 require is-secret-value sk-live-1234 us-central1'))
      .toBe('loginname *** *** require *** *** us-central1')
  })

  it('appendLog masks what this process knows before the line is stored', async () => {
    registerSecret('hunter2-hunter2')
    await appendLog(db, 'r1', 'd', 't', 'stdout', 'password is hunter2-hunter2')
    expect((await getTaskLogs(db, 'r1', 't')).map(l => l.line)).toEqual(['password is ***'])
    expect(maskedCount()).toBeGreaterThan(0)
  })
})

describe('a task printing a secret it read', () => {
  it('its log and its failure message show *** — the masking happens in the worker process', async () => {
    await upsertConnection(db, { conn_id: 'warehouse', conn_type: 'postgres', password: 'wh-Pa55word!' })
    await setVariable(db, { key: 'signing', value: 'sig-0123456789', is_secret: true })
    const dag: DagDefinition = {
      id: 'mask_dag', schedule: null,
      tasks: {
        leak: {
          run: new Function(`return async (ctx) => {
            const c = await ctx.connections.get('warehouse')
            const s = await ctx.variables.get('signing')
            console.log('connecting with ' + c.password)
            ctx.log.warn({ signing: s })
            throw new Error('auth failed for ' + c.password)
          }`)() as () => Promise<void>,
        },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)
    for (let i = 0; i < 6; i++) await advanceRun(db, runId)

    await expect.poll(async () => (await getTaskLogs(db, runId, 'leak')).map(l => l.line), { timeout: 10_000 })
      .toEqual(expect.arrayContaining(['connecting with ***', '[WARN] {"signing":"***"}']))
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'leak' })
    expect(ti).toMatchObject({ state: 'failed', error: 'auth failed for ***' })
    const all = JSON.stringify(await getTaskLogs(db, runId, 'leak'))
    expect(all).not.toContain('wh-Pa55word!')
    expect(all).not.toContain('sig-0123456789')
  }, 30_000)
})
