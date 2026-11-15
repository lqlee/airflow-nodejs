/**
 * The sandbox runs Dag-author functions (callbacks, timetables, deferred triggers) for the scheduler in
 * a separate process — re-importing the Dag file so closures work, falling back to the parsed source.
 */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { mkdtemp, writeFile, readFile, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { invokeInSandbox, runTriggerInSandbox, stopSandbox } from '../client.js'
import { hydrateDag, serializeDag } from '../../dag/serialize.js'
import { notifyAll } from '../../scheduler/callbacks.js'
import { isControlPlaneEnv, userCodeEnv } from '../env.js'
import type { DagDefinition } from '../../dag/types.js'

let dir: string
let out: string
const savedEnv = { ...process.env }

const DAG_SRC = (outFile: string) => `
import { appendFileSync } from 'node:fs'
const OUT = ${JSON.stringify(outFile)}
const record = (what) => appendFileSync(OUT, JSON.stringify({ what, pid: process.pid, mongo: process.env.MONGO_URL ?? null }) + '\\n')

export default {
  id: 'sbx',
  schedule: null,
  timetable: (last, n) => n >= 2 ? null : new Date(Date.UTC(2030, 0, 1 + n)),
  onSuccess: [{ notify(info) { record('dag-success:' + info.runId) } }],
  tasks: {
    t: { run: async () => 1, onFailure: (info) => record('task-failure:' + info.error) },
  },
}
`

async function loadSerialized(): Promise<DagDefinition> {
  const fileloc = join(dir, 'sbx.js')
  const mod = await import(`${fileloc}?t=${Date.now()}`)
  return hydrateDag(serializeDag(mod.default), fileloc)
}

const records = async () => (await readFile(out, 'utf8').catch(() => '')).trim().split('\n').filter(Boolean).map(l => JSON.parse(l))

beforeAll(async () => {
  dir = await mkdtemp(join(tmpdir(), 'anode-sbx-'))
  out = join(dir, 'out.jsonl')
  await writeFile(join(dir, 'package.json'), '{"type":"module"}')
  await writeFile(join(dir, 'sbx.js'), DAG_SRC(out))
  process.env.DAG_DIR = dir
  process.env.MONGO_URL = 'mongodb://secret-host:27017'
})
afterEach(async () => { await rm(out, { force: true }) })
afterAll(() => { stopSandbox(); process.env = savedEnv })

describe('sandbox', () => {
  it('runs serialized callbacks in another process, with the file’s closures and no DB credentials', async () => {
    const dag = await loadSerialized()
    await notifyAll(dag.onSuccess, { runId: 'r1' } as never, 'test')
    await notifyAll(dag.tasks.t.onFailure, { error: 'boom' } as never, 'test')
    const rows = await records()
    expect(rows.map(r => r.what)).toEqual(['dag-success:r1', 'task-failure:boom'])
    for (const r of rows) {
      expect(r.pid).not.toBe(process.pid)
      expect(r.mongo).toBeNull()
    }
  }, 30_000)

  it('a timetable returns Dates across the process boundary', async () => {
    const dag = await loadSerialized()
    expect(await dag.timetable!(null, 0)).toEqual(new Date(Date.UTC(2030, 0, 1)))
    expect(await dag.timetable!(null, 2)).toBeNull()
  }, 30_000)

  it('falls back to the parsed source when the file no longer defines the function', async () => {
    const ref = { dagId: 'gone', fileloc: join(dir, 'missing.js'), path: ['timetable'], source: '(last, n) => n * 7' }
    expect(await invokeInSandbox(ref, [null, 6])).toBe(42)
  }, 30_000)

  it('reports the function’s error', async () => {
    const ref = { dagId: 'x', fileloc: null, path: ['f'], source: '() => { throw new Error("nope") }' }
    await expect(invokeInSandbox(ref, [])).rejects.toThrow('nope')
  }, 30_000)

  it('evaluates deferred triggers with xcom calls served by the scheduler', async () => {
    const pushed: unknown[] = []
    const ready = await runTriggerInSandbox(
      `async (ctx) => { const v = await ctx.xcom.pull('up', 'n'); await ctx.xcom.push('seen', v + ctx.conf.add); return v > 1 }`,
      { dagId: 'd', runId: 'r', taskId: 't', conf: { add: 5 } },
      { xcomPull: async (task, key) => (task === 'up' && key === 'n' ? 2 : null), xcomPush: async (k, v) => { pushed.push([k, v]) } },
    )
    expect(ready).toBe(true)
    expect(pushed).toEqual([['seen', 7]])
  }, 30_000)

  it('a call that blocks the sandbox times out, and the sandbox is replaced', async () => {
    const hang = { dagId: 'x', fileloc: null, path: ['f'], source: '() => { for (;;) {} }' }
    await expect(invokeInSandbox(hang, [], 500)).rejects.toThrow(/timed out after 500ms/)
    // It misses the follow-up ping (2s) and is killed; the next call gets a fresh process
    await new Promise(r => setTimeout(r, 2_500))
    const ok = { dagId: 'x', fileloc: null, path: ['f'], source: '() => "alive"' }
    expect(await invokeInSandbox(ok, [], 15_000)).toBe('alive')
  }, 30_000)
})

describe('userCodeEnv', () => {
  it.each(['MONGO_URL', 'SPANNER_PROJECT_ID', 'ENCRYPTION_KEY', 'ENCRYPTION_KEYS', 'ADMIN_KEY', 'API_KEYS', 'OIDC_CLIENT_SECRET', 'VAULT_TOKEN', 'AKEYLESS_ACCESS_KEY', 'REDIS_URL'])(
    'drops %s', (name) => expect(isControlPlaneEnv(name)).toBe(true))

  it('keeps everything else', () => {
    expect(userCodeEnv({ PATH: '/bin', SLACK_WEBHOOK: 'x', MONGO_URL: 'm', DAG_DIR: '/d' })).toEqual({ PATH: '/bin', SLACK_WEBHOOK: 'x', DAG_DIR: '/d' })
  })
})
