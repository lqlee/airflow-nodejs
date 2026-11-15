/**
 * `anode` against a running API server: Dag listing, pause, trigger (--wait), run states, usage
 * errors, and `tasks test` reading XComs/variables of a real run through the REST API.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import type { FastifyInstance } from 'fastify'
import type { AddressInfo } from 'node:net'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { buildServer } from '../../api/server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { xcomPush } from '../../xcom/index.js'
import { setVariable } from '../../variables/index.js'
import { runCli } from '../run.js'
import { createApiClient, ApiError } from '../client.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const DB = 'airflow_test_cli'
let client: MongoClient
let db: Db
let app: FastifyInstance
let url: string
let out: string[]
let err: string[]
const io = { out: (s: string) => { out.push(s) }, err: (s: string) => { err.push(s) } }
const cli = (...argv: string[]) => runCli(argv, { io, env: { ANODE_API_URL: url }, pollMs: 50 })
const json = () => JSON.parse(out.join('\n'))

beforeAll(async () => {
  process.env.DB_NAME = DB
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(DB)
  app = buildServer(db, { rateLimitMax: 0 })
  await app.listen({ port: 0, host: '127.0.0.1' })
  url = `http://127.0.0.1:${(app.server.address() as AddressInfo).port}`
})
afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
  delete process.env.DB_NAME
})
beforeEach(() => {
  out = []; err = []
  register({ id: 'cli_etl', schedule: '0 6 * * *', tags: ['etl', 'daily'], owner: 'data', tasks: {
    a: { run: async (ctx) => { if ((ctx.conf as { fail?: boolean }).fail) throw new Error('told to fail') } },
    b: { dependsOn: ['a'], run: async () => {} },
  } })
  register({ id: 'cli_other', schedule: null, tags: ['adhoc'], tasks: { only: { run: async () => {} } } })
})
afterEach(async () => {
  clearRegistry()
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'xcoms', 'variables', 'dag_paused', 'event_logs']) {
    await db.collection(c).deleteMany({})
  }
})

describe('anode dags', () => {
  it('list prints a table, -o json the API objects, --tag filters', async () => {
    expect(await cli('dags', 'list')).toBe(0)
    const [header, ...rows] = out[0].split('\n')
    expect(header).toMatch(/^id\s+schedule\s+is_paused\s+owner\s+tags$/)
    expect(rows.find(r => r.startsWith('cli_etl'))).toMatch(/^cli_etl\s+0 6 \* \* \*\s+false\s+data\s+etl,daily$/)

    out = []
    expect(await cli('dags', 'list', '--tag', 'etl', '--tag', 'daily', '-o', 'json')).toBe(0)
    expect(json().map((d: { id: string }) => d.id)).toEqual(['cli_etl'])
  })

  it('pause and unpause change what the API reports', async () => {
    expect(await cli('dags', 'pause', 'cli_etl')).toBe(0)
    out = []
    await cli('dags', 'show', 'cli_etl', '-o', 'json')
    expect(json().is_paused).toBe(true)

    expect(await cli('dags', 'unpause', 'cli_etl')).toBe(0)
    out = []
    await cli('dags', 'show', 'cli_etl', '-o', 'json')
    expect(json().is_paused).toBe(false)
  })

  it('trigger --wait returns 0 when the run succeeds and 1 with the task error when it fails', async () => {
    expect(await cli('dags', 'trigger', 'cli_etl', '--tag', 'from-cli', '--wait', '--timeout', '60')).toBe(0)
    expect(out.at(-1)).toMatch(/state\s+success/)
    expect(out.at(-1)).toMatch(/tasks\s+success=2/)

    out = []
    expect(await cli('dags', 'trigger', 'cli_etl', '--conf', '{"fail":true}', '--wait', '-o', 'json')).toBe(1)
    const run = json()
    expect(run.state).toBe('failed')
    expect(run.tasks.find((t: { task_id: string }) => t.task_id === 'a').error).toMatch(/told to fail/)

    out = []
    expect(await cli('dags', 'list-runs', 'cli_etl', '-o', 'json')).toBe(0)
    const runs = json()
    expect(runs.map((r: { state: string }) => r.state)).toEqual(['failed', 'success'])
    expect(runs[1].tags).toEqual(['from-cli'])

    out = []
    expect(await cli('tasks', 'states-for-dag-run', runs[0].run_id)).toBe(0)
    expect(out[0]).toMatch(/\na\s+failed\s+0\s.*told to fail\nb\s+upstream_failed\s/)
  })

  it('reports API errors with exit code 1 and usage errors with exit code 2', async () => {
    expect(await cli('dags', 'trigger', 'ghost')).toBe(1)
    expect(err.at(-1)).toBe("error: Dag 'ghost' not found")

    expect(await cli('dags', 'trigger', 'cli_etl', '--conf', '{oops')).toBe(2)
    expect(await cli('dags', 'trigger', 'cli_etl', '--timeout', '5')).toBe(2)
    expect(err.at(-1)).toMatch(/^--timeout needs --wait/)
    expect(await cli('dags', 'list', '--wait')).toBe(2)
    expect(err.at(-1)).toMatch(/^'dags list' does not take --wait/)
    expect(await cli('dags', 'show')).toBe(2)
    expect(await cli('dags', 'explode')).toBe(2)
    expect(await cli('dags', 'list', '-o', 'yaml')).toBe(2)
    expect(await cli('--help')).toBe(0)
    expect(out.at(-1)).toMatch(/tasks test <dag_id> <task_id>/)
    expect(await cli('dags', 'pause', 'cli_etl')).toBe(0)   // the failures above changed nothing
  })

  it('fails clearly when the API is unreachable', async () => {
    expect(await runCli(['dags', 'list'], { io, env: { ANODE_API_URL: 'http://127.0.0.1:9' } })).toBe(1)
    expect(err.at(-1)).toMatch(/^error: cannot reach http:\/\/127\.0\.0\.1:9 .*ANODE_API_URL/)
  })
})

describe('anode tasks test with --run-id', () => {
  let dir: string
  beforeEach(async () => {
    dir = await mkdtemp(join(tmpdir(), 'anode-cli-api-'))
    await writeFile(join(dir, 'package.json'), '{"type":"module"}')
    await writeFile(join(dir, 'report.js'), `export default { id: 'cli_report', schedule: null, tasks: {
      extract: { expand: [1, 2], run: async () => {} },
      report: { run: async (ctx) => {
        await ctx.xcom.push('summary', {
          rows: await ctx.xcom.pull('extract', 'rows'),
          missing: await ctx.xcom.pull('extract', 'nope'),
          region: await ctx.variables.get('region'),
          key: await ctx.variables.get('api_key'),
        })
      } },
    } }`)
  })
  afterEach(() => rm(dir, { recursive: true, force: true }))

  it("pulls the real run's XComs and non-secret variables over REST; secret ones come back null with a hint", async () => {
    process.env.ENCRYPTION_KEYS ??= 'b'.repeat(64)
    const runId = new ObjectId().toString()
    await xcomPush(db, runId, 'cli_report', 'extract', 0, 'rows', 10)
    await xcomPush(db, runId, 'cli_report', 'extract', 1, 'rows', 20)
    await setVariable(db, { key: 'region', value: 'eu' })
    await setVariable(db, { key: 'api_key', value: 'k-123456789', is_secret: true })

    const code = await runCli(['tasks', 'test', 'cli_report', 'report', '--run-id', runId, '-o', 'json'],
      { io, env: { ANODE_API_URL: url, DAG_DIR: dir } })

    expect(code).toBe(0)
    expect(JSON.parse(out.at(-1)!).xcoms.summary).toEqual({ rows: [10, 20], missing: null, region: 'eu', key: null })
    expect(err).toContain("[warn] variable 'api_key' is secret — the REST API does not return it; set AIRFLOW_VAR_API_KEY with SECRETS_BACKEND=env")
    expect(await db.collection('xcoms').countDocuments({ task_id: 'report' })).toBe(0)
  })
})

describe('API client', () => {
  it('sends the token as a Bearer header and turns a 401 into a hint', async () => {
    let seen: Headers | undefined
    const api = createApiClient({
      baseUrl: 'http://api.test/', token: 'k1',
      fetchImpl: (async (_u: URL, init: RequestInit) => {
        seen = new Headers(init.headers)
        return new Response(JSON.stringify({ error: 'Unauthorized' }), { status: 401 })
      }) as typeof fetch,
    })
    const e = await api.get('/dags').catch(x => x)
    expect(seen?.get('authorization')).toBe('Bearer k1')
    expect(e).toBeInstanceOf(ApiError)
    expect(e.status).toBe(401)
    expect(e.message).toBe('Unauthorized — set --token or ANODE_API_TOKEN')
  })

  it('rejects non-http URLs', () => {
    expect(() => createApiClient({ baseUrl: 'file:///etc/passwd' })).toThrow(/must be http/)
  })
})
