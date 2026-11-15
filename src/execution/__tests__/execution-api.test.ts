/**
 * Execution API: workers reach XCom, connections, variables, run context, dataset events and logs with a
 * token scoped to one running try — over HTTP (Kubernetes pods) or IPC (forked workers, no DB credentials).
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'
import { buildServer } from '../../api/server.js'
import { signTaskToken, verifyTaskToken, tokenExpiry, type TaskScope } from '../token.js'
import { httpExecutionClient } from '../client.js'
import { buildApiTaskContext } from '../task-context.js'
import { upsertConnection } from '../../connections/index.js'
import { setVariable } from '../../variables/index.js'
import { getTaskLogs } from '../../logs/index.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { clearSecrets, redact } from '../../secrets/masker.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance
let baseUrl: string
let runId: string
let scope: TaskScope

const savedEnv = { ...process.env }

beforeAll(async () => {
  process.env.ENCRYPTION_KEYS = '4'.repeat(64)
  process.env.LOG_BACKEND = 'mongodb'
  process.env.MONGO_URL = MONGO_URL
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_execution_api')
  app = buildServer(db, { rateLimitMax: 0 })
  await app.listen({ port: 0, host: '127.0.0.1' })
  const addr = app.server.address() as { port: number }
  baseUrl = `http://127.0.0.1:${addr.port}`
})
afterAll(async () => {
  process.env = savedEnv
  await app.close()
  await db.dropDatabase()
  await client.close()
})
beforeEach(async () => {
  clearSecrets(); clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'xcoms', 'connections', 'variables', 'dataset_events', 'task_logs']) await db.collection(c).deleteMany({})
  const run = await db.collection('dag_runs').insertOne({
    _id: new ObjectId(), dag_id: 'exe', state: 'running', conf: { region: 'eu' }, created_at: new Date('2026-03-01T00:00:00Z'),
    logical_date: new Date('2026-03-01T00:00:00Z'), data_interval_start: null, data_interval_end: null,
  } as never)
  runId = run.insertedId.toString()
  await db.collection('task_instances').insertMany([
    { dag_run_id: runId, dag_id: 'exe', task_id: 'up', map_index: null, state: 'success', try_number: 1 },
    { dag_run_id: runId, dag_id: 'exe', task_id: 'me', map_index: null, state: 'running', try_number: 2, map_value: null },
  ])
  scope = { dagId: 'exe', runId, taskId: 'me', mapIndex: null, tryNumber: 2 }
})

const call = (method: 'GET' | 'POST', url: string, token: string | null, payload?: unknown) =>
  app.inject({ method, url, headers: token ? { authorization: `Bearer ${token}` } : {}, payload: payload as never })

describe('task tokens', () => {
  it('round-trip their scope and reject tampering, expiry and the wrong key', () => {
    const token = signTaskToken(scope, 60_000)
    expect(verifyTaskToken(token)).toEqual(scope)
    expect(tokenExpiry(token)! - Date.now()).toBeGreaterThan(55_000)

    const [h, p, s] = token.split('.')
    const forged = Buffer.from(JSON.stringify({ ...JSON.parse(Buffer.from(p, 'base64url').toString()), task_id: 'other' })).toString('base64url')
    expect(verifyTaskToken(`${h}.${forged}.${s}`)).toBeNull()
    expect(verifyTaskToken(signTaskToken(scope, 1_000, Date.now() - 5_000))).toBeNull()

    process.env.EXECUTION_API_SECRET = 'another-deployment-secret-0123456789'
    try { expect(verifyTaskToken(token)).toBeNull() } finally { delete process.env.EXECUTION_API_SECRET }
  })
})

describe('HTTP execution API', () => {
  it('needs a task token — user credentials and garbage are refused', async () => {
    expect((await call('GET', '/execution/v1/context', null)).statusCode).toBe(401)
    expect((await call('GET', '/execution/v1/context', 'not-a-token')).statusCode).toBe(401)
  })

  it('is refused once the try stops running, or for another try', async () => {
    const token = signTaskToken(scope)
    expect((await call('GET', '/execution/v1/context', token)).statusCode).toBe(200)
    expect((await call('GET', '/execution/v1/context', signTaskToken({ ...scope, tryNumber: 1 }))).statusCode).toBe(409)
    await db.collection('task_instances').updateOne({ task_id: 'me' }, { $set: { state: 'success' } })
    const res = await call('GET', '/execution/v1/context', token)
    expect(res.statusCode).toBe(409)
    expect((await call('POST', '/execution/v1/token/refresh', token)).statusCode).toBe(409)
  })

  it('serves the task its context, XCom, connections, variables, datasets and logs — within its scope', async () => {
    await upsertConnection(db, { conn_id: 'wh', conn_type: 'postgres', host: 'db.internal', password: 'wh-Secret-123' })
    await setVariable(db, { key: 'signing', value: 'sig-abcdef-1', is_secret: true })
    await setVariable(db, { key: 'region', value: 'us-1' })
    await db.collection('xcoms').insertOne({ dag_run_id: runId, task_id: 'up', map_index: null, key: 'rows', value: 7 })

    const api = httpExecutionClient(baseUrl, signTaskToken(scope))
    try {
      const { ctx, recordDatasets } = await buildApiTaskContext(api, { dagId: 'exe', runId, taskId: 'me', mapIndex: null })
      expect(ctx.conf).toEqual({ region: 'eu' })
      expect(ctx.logicalDate).toEqual(new Date('2026-03-01T00:00:00Z'))
      expect(await ctx.xcom.pull('up', 'rows')).toBe(7)
      expect(await ctx.xcom.pull('up', 'missing')).toBeNull()
      await ctx.xcom.push('out', { n: 8 })
      expect(await db.collection('xcoms').findOne({ dag_run_id: runId, key: 'out' })).toMatchObject({ task_id: 'me', dag_id: 'exe', value: { n: 8 } })

      expect(await ctx.connections.get('wh')).toMatchObject({ host: 'db.internal', password: 'wh-Secret-123' })
      expect(await ctx.connections.get('nope')).toBeNull()
      expect(await ctx.variables.get('signing')).toBe('sig-abcdef-1')
      expect(await ctx.variables.get('region')).toBe('us-1')
      // Secret values the worker read are masked in the worker's own output
      expect(redact('wh-Secret-123 sig-abcdef-1 us-1')).toBe('*** *** us-1')

      ctx.emitDataset('s3://bucket/out', { extra: { rows: 8 } })
      await recordDatasets()
      expect(await db.collection('dataset_events').findOne({ uri: 's3://bucket/out' })).toMatchObject({ produced_by: 'exe', task_id: 'me', run_id: runId })

      await api.call('logs.append', [[{ line: '[WARN] careful' }, { stream: 'stderr', line: 'oops' }]])
      expect((await getTaskLogs(db, runId, 'me')).map(l => [l.line, l.level])).toEqual([['[WARN] careful', 'warn'], ['oops', 'error']])
    } finally {
      api.close()
    }
  })

  it('rejects bad input with 400', async () => {
    const token = signTaskToken(scope)
    expect((await call('POST', '/execution/v1/datasets', token, { events: 'x' })).statusCode).toBe(400)
    expect((await call('POST', '/execution/v1/datasets', token, { events: [{ uri: '' }] })).statusCode).toBe(400)
    expect((await call('POST', '/execution/v1/logs', token, { lines: Array(1001).fill({ line: 'x' }) })).statusCode).toBe(400)
  })

  it('task.source returns the run-function source; refresh issues a token for the same try', async () => {
    register({ id: 'exe', schedule: null, tasks: { me: { run: async () => 'hi' } } } as DagDefinition)
    const token = signTaskToken(scope)
    const src = (await call('GET', '/execution/v1/task', token)).json()
    expect(src.file).toBeNull()
    expect(await (new Function(`return (${src.run})`)() as () => Promise<string>)()).toBe('hi')

    const fresh = (await call('POST', '/execution/v1/token/refresh', token)).json().token as string
    expect(verifyTaskToken(fresh)).toEqual(scope)
  })

  it('the HTTP client refreshes its token before it runs out', async () => {
    const api = httpExecutionClient(baseUrl, signTaskToken(scope, 9_000))
    try {
      await new Promise(r => setTimeout(r, 6_500))   // less than a third of the lifetime left (2.5s margin)
      expect(await api.call('xcom.pull', ['up', 'none'])).toBeNull()
      await new Promise(r => setTimeout(r, 4_000))   // the original token has expired by now
      expect(await api.call('xcom.pull', ['up', 'none'])).toBeNull()
    } finally {
      api.close()
    }
  }, 25_000)
})

describe('Kubernetes pod entry point (worker-cli)', () => {
  const runCli = async (env: Record<string, string>) => {
    const { spawn } = await import('node:child_process')
    const { createRequire } = await import('node:module')
    const { pathToFileURL } = await import('node:url')
    const tsx = pathToFileURL(createRequire(import.meta.url).resolve('tsx')).href
    const child = spawn(process.execPath, ['--import', tsx, 'src/scheduler/worker-cli.ts'], {
      env: { PATH: process.env.PATH ?? '', ...env }, stdio: ['ignore', 'pipe', 'pipe'],
    })
    let output = ''
    child.stdout.on('data', d => { output += d })
    child.stderr.on('data', d => { output += d })
    const code = await new Promise<number | null>(r => child.on('close', r))
    return { code, output }
  }

  it('imports the task from the Dag file in the image (closures work) and uses only the API', async () => {
    const { mkdtemp, writeFile } = await import('node:fs/promises')
    const { tmpdir } = await import('node:os')
    const { join } = await import('node:path')
    const { hydrateDag, serializeDag } = await import('../../dag/serialize.js')
    const dir = await mkdtemp(join(tmpdir(), 'anode-pod-'))
    await writeFile(join(dir, 'package.json'), '{"type":"module"}')
    const file = join(dir, 'exe.js')
    await writeFile(file, `
      const double = (n) => n * 2
      export default { id: 'exe', schedule: null, tasks: { me: { run: async (ctx) => {
        await ctx.xcom.push('pod', { doubled: double(await ctx.xcom.pull('up', 'rows')), region: ctx.conf.region, mongo: process.env.MONGO_URL ?? null })
      } } } }`)
    await db.collection('xcoms').insertOne({ dag_run_id: runId, task_id: 'up', map_index: null, key: 'rows', value: 21 })
    const mod = await import(`${file}?t=${Date.now()}`)
    register(hydrateDag(serializeDag(mod.default), file))   // what the API process holds: the serialized Dag

    const env = {
      K8S_EXEC_DAG_ID: 'exe', K8S_EXEC_RUN_ID: runId, K8S_EXEC_TASK_ID: 'me', K8S_EXEC_MAP_INDEX: '',
      EXECUTION_API_URL: baseUrl, EXECUTION_API_TOKEN: signTaskToken(scope), DAG_DIR: dir,
    }
    const { code, output } = await runCli(env)
    expect(output).toContain('✓ exe.me')
    expect(code).toBe(0)
    expect((await db.collection('xcoms').findOne({ dag_run_id: runId, task_id: 'me', key: 'pod' }))?.value)
      .toEqual({ doubled: 42, region: 'eu', mongo: null })
  }, 30_000)

  it('exits 1 when the token does not match a running try, 2 without API settings', async () => {
    const base = { K8S_EXEC_DAG_ID: 'exe', K8S_EXEC_RUN_ID: runId, K8S_EXEC_TASK_ID: 'me' }
    expect((await runCli(base)).code).toBe(2)
    const stale = await runCli({ ...base, EXECUTION_API_URL: baseUrl, EXECUTION_API_TOKEN: signTaskToken({ ...scope, tryNumber: 9 }) })
    expect(stale.code).toBe(1)
    expect(stale.output).toMatch(/not running this try/)
  }, 30_000)
})

describe('forked workers', () => {
  it('get no database credentials or keys, yet read and write through the execution API', async () => {
    await setVariable(db, { key: 'greeting', value: 'hello' })
    const dag: DagDefinition = {
      id: 'exe_fork', schedule: null,
      tasks: {
        a: { run: async (ctx) => { await ctx.xcom.push('n', 41) } },
        b: {
          dependsOn: ['a'],
          run: new Function(`return async (ctx) => {
            ctx.emitDataset('mem://exe_fork')
            await ctx.xcom.push('seen', {
              n: await ctx.xcom.pull('a', 'n'),
              greeting: await ctx.variables.get('greeting'),
              conf: ctx.conf,
              mongo: process.env.MONGO_URL ?? null,
              keys: process.env.ENCRYPTION_KEYS ?? null,
            })
          }`)() as () => Promise<void>,
        },
      },
    }
    register(dag)
    const id = await createRun(db, dag, { conf: { x: 1 } } as never)
    await expect.poll(async () => {
      await advanceRun(db, id)
      return (await db.collection('dag_runs').findOne({ _id: new ObjectId(id) }))?.state
    }, { timeout: 20_000, interval: 300 }).toBe('success')

    const seen = await db.collection('xcoms').findOne({ dag_run_id: id, task_id: 'b', key: 'seen' })
    expect(seen?.value).toEqual({ n: 41, greeting: 'hello', conf: { x: 1 }, mongo: null, keys: null })
    expect(await db.collection('dataset_events').countDocuments({ uri: 'mem://exe_fork', run_id: id })).toBe(1)
  }, 30_000)
})
