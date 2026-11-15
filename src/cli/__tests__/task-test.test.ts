/**
 * `anode tasks test`: runs one task function locally from the Dag file, records nothing, and
 * resolves variables/connections from the local secrets backend.
 */
import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { mkdtemp, writeFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { testTask, type TaskTestOptions } from '../task-test.js'
import { runCli } from '../run.js'
import { resetSecretsBackend } from '../../secrets/index.js'
import { clearSecrets } from '../../secrets/masker.js'

let dir: string
let out: string[]
let err: string[]
const savedEnv = { ...process.env }
const io = { out: (s: string) => { out.push(s) }, err: (s: string) => { err.push(s) } }

const DAG = `
export default {
  id: 'cli_demo',
  schedule: '0 6 * * *',
  scheduleMode: 'interval',
  params: { region: { type: 'string', default: 'us' } },
  tasks: {
    extract: {
      run: async (ctx) => {
        await ctx.xcom.push('meta', { region: ctx.params.region, ds: ctx.ds, start: ctx.dataIntervalStart?.toISOString() ?? null })
        await ctx.xcom.push('token', await ctx.variables.get('api_token'))
        const conn = await ctx.connections.get('warehouse')
        await ctx.xcom.push('host', conn?.host ?? null)
        ctx.emitDataset('gs://bucket/orders')
        ctx.log.info('extracted')
      },
    },
    boom: { run: async () => { throw new Error('extract failed: bad row') } },
    fanout: { expand: ['a', 'b', 'c'], run: async (ctx) => { await ctx.xcom.push('item', ctx.mapValue) } },
    dynamic: { expand: { from: 'extract', key: 'files' }, run: async (ctx) => { await ctx.xcom.push('item', ctx.mapValue) } },
    waiting: {
      run: async (ctx) => {
        ctx.emitDataset('gs://bucket/discarded')
        await ctx.defer(async (t) => { globalThis.__cliChecks = (globalThis.__cliChecks ?? 0) + 1; return globalThis.__cliChecks >= 2 }, { interval: 100 })
      },
    },
    sensor: { poke: async (ctx) => ctx.conf.ready === true, pokeInterval: 1000, sensorTimeout: 500 },
    pick: { branch: async (ctx) => ctx.conf.region === 'eu' ? ['eu_load'] : 'us_load' },
    slow: { timeout: 100, run: async () => new Promise(r => setTimeout(r, 2000)) },
    bash: { shell: { command: 'echo hi' } },
  },
}
`

const run = (taskId: string, extra: Partial<TaskTestOptions> = {}) =>
  testTask({ dagId: 'cli_demo', taskId, dagsDir: dir, api: null, io, ...extra })

beforeEach(async () => {
  out = []; err = []
  dir = await mkdtemp(join(tmpdir(), 'anode-cli-'))
  await writeFile(join(dir, 'package.json'), '{"type":"module"}')
  await writeFile(join(dir, 'broken.js'), 'export default {'   )   // an unrelated broken file must not block the search
  await writeFile(join(dir, 'cli_demo.js'), DAG)
  process.env.SECRETS_BACKEND = 'env'
  resetSecretsBackend()
  delete (globalThis as Record<string, unknown>).__cliChecks
})
afterEach(async () => {
  process.env = { ...savedEnv }
  resetSecretsBackend()
  clearSecrets()
  await rm(dir, { recursive: true, force: true })
})

describe('tasks test', () => {
  it('runs the task with param defaults, the logical date interval and local secrets; records only in memory', async () => {
    process.env.AIRFLOW_VAR_API_TOKEN = 's3cr3t-token'
    process.env.AIRFLOW_CONN_WAREHOUSE = 'postgres://etl:pw@db.internal:5432/dw'

    const r = await run('extract', { logicalDate: new Date('2026-03-02T06:00:00Z') })

    expect(r.ok).toBe(true)
    expect(r.xcoms).toEqual({
      meta: { region: 'us', ds: '2026-03-02', start: '2026-03-02T06:00:00.000Z' },
      token: 's3cr3t-token',
      host: 'db.internal',
    })
    expect(r.datasets).toEqual([{ uri: 'gs://bucket/orders' }])
    expect(out).toContain('[INFO] extracted')
    expect(out.at(-1)).toMatch(/✓ cli_demo\.extract succeeded/)
  })

  it('passes --conf through param validation', async () => {
    const ok = await run('pick', { conf: { region: 'eu' } })
    expect(ok.ok).toBe(true)
    expect(out).toContain('[branch] → eu_load')

    const bad = await run('pick', { conf: { region: 42 } })
    expect(bad.ok).toBe(false)
    expect(bad.error).toMatch(/param validation failed[\s\S]*region/)
  })

  it('reports a failing task with its error', async () => {
    const r = await run('boom')
    expect(r).toMatchObject({ ok: false, error: 'extract failed: bad row' })
    expect(err.at(-1)).toBe('[test] ✗ cli_demo.boom failed: extract failed: bad row')
  })

  it('mapped tasks need --map-index (literal) or --map-value (from XCom)', async () => {
    expect((await run('fanout')).error).toMatch(/mapped over 3 value\(s\) — pass --map-index 0\.\.2/)
    expect((await run('fanout', { mapIndex: 3 })).error).toMatch(/--map-index must be 0\.\.2/)
    expect((await run('fanout', { mapIndex: 1 })).xcoms).toEqual({ item: 'b' })

    expect((await run('dynamic')).error).toMatch(/maps over XCom 'extract\.files' — pass --map-value/)
    expect((await run('dynamic', { mapValue: 'f.csv' })).xcoms).toEqual({ item: 'f.csv' })
    expect((await run('boom', { mapIndex: 0 })).error).toMatch(/not a mapped task/)
  })

  it('polls a deferred trigger until it fires and drops datasets emitted before defer()', async () => {
    const r = await run('waiting')
    expect(r.ok).toBe(true)
    expect(r.datasets).toEqual([])
    expect((globalThis as Record<string, unknown>).__cliChecks).toBe(2)
    expect(out.some(l => l.startsWith('[defer] trigger check #1 not ready'))).toBe(true)
  })

  it('sensors succeed when the poke is ready and time out otherwise', async () => {
    expect((await run('sensor', { conf: { ready: true } })).ok).toBe(true)
    expect((await run('sensor', { conf: { ready: false } })).error).toBe('sensor timed out after 1 poke(s)')
  })

  it("enforces the task's timeout", async () => {
    expect((await run('slow')).error).toBe('task timed out after 100ms')
  })

  it('refuses tasks that only the scheduler can run, unknown tasks and unknown Dags', async () => {
    expect((await run('bash')).error).toMatch(/'bash' is a shell task/)
    expect((await run('nope')).error).toMatch(/has no task 'nope' \(tasks: extract, boom/)
    const missing = await testTask({ dagId: 'ghost', taskId: 'x', dagsDir: dir, api: null, io })
    expect(missing.error).toMatch(/Dag 'ghost' not found[\s\S]*broken\.js/)
  })

  it('masks secrets the task read in what it prints', async () => {
    process.env.AIRFLOW_VAR_API_TOKEN = 'hunter2-very-secret'
    await writeFile(join(dir, 'leak.js'), `export default { id: 'leak', schedule: null, tasks: {
      t: { run: async (ctx) => { throw new Error('token was ' + await ctx.variables.get('api_token')) } } } }`)
    const r = await testTask({ dagId: 'leak', taskId: 't', dagsDir: dir, api: null, io })
    expect(r.error).toBe('token was ***')
  })

  it('anode tasks test exits 0 on success and 1 on failure', async () => {
    const env = { ...process.env, DAG_DIR: dir, ANODE_API_URL: 'http://127.0.0.1:9' }
    expect(await runCli(['tasks', 'test', 'cli_demo', 'fanout', '--map-index', '2'], { io, env })).toBe(0)
    expect(out).toContain('[xcom] push item = "c"')
    expect(await runCli(['tasks', 'test', 'cli_demo', 'boom'], { io, env })).toBe(1)
    expect(await runCli(['tasks', 'test', 'cli_demo', 'fanout', '--map-index=-1'], { io, env })).toBe(2)
    expect(err.at(-1)).toMatch(/^--map-index must be an integer ≥ 0\nusage: anode tasks test <dag_id> <task_id>/)
    expect(await runCli(['tasks', 'test', 'cli_demo', 'pick', '--conf', '[1]'], { io, env })).toBe(2)
    expect(err.at(-1)).toMatch(/^--conf must be a JSON object\n/)
  })
})
