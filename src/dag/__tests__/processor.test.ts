/**
 * Dag processor: Dag files are parsed in a child process into serialized_dags; the scheduler side syncs
 * from the database and never imports the files.
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { mkdtemp, writeFile, rm, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { runDagProcessor, maybeRunDagProcessor, resetDagProcessor, stopDagProcessor, SERIALIZED_DAGS, PROCESSOR_STATE } from '../processor.js'
import { syncDagsFromDb, resetDagSync } from '../sync.js'
import { clearRegistry, getDag, listDags } from '../registry.js'
import { getImportErrors, getDagWarnings } from '../import-errors.js'
import { isSerializedDag } from '../serialize.js'
import { listProviders } from '../../providers/registry.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let dir: string
const savedEnv = { ...process.env }

const dagFile = (id: string, extra = '') => `
export default {
  id: '${id}',
  schedule: null,
  description: 'mongo=' + String(process.env.MONGO_URL) + ' keys=' + String(process.env.ENCRYPTION_KEYS),
  tasks: {
    extract: { run: async (ctx) => 1 },
    load: { dependsOn: ['extract'], run: async (ctx) => 2, onFailure: { notify(info) { return info } } },
  },
  ${extra}
}
globalThis.__processorTestImported = true
`

const write = (name: string, body: string) => writeFile(join(dir, name), body)

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_dag_processor')
  process.env.MONGO_URL = MONGO_URL
  process.env.ENCRYPTION_KEYS = 'a'.repeat(64)
  process.env.DAG_PARSE_TIMEOUT_MS = '4000'
})
afterAll(async () => {
  stopDagProcessor()
  process.env = savedEnv
  await db.dropDatabase()
  await client.close()
})
beforeEach(async () => {
  await db.dropDatabase()
  dir = await mkdtemp(join(tmpdir(), 'anode-dags-'))
  await write('package.json', '{"type":"module"}')
  process.env.DAG_DIR = dir
  clearRegistry(); resetDagSync(); resetDagProcessor()
  delete (globalThis as Record<string, unknown>).__processorTestImported
})

describe('runDagProcessor', () => {
  it('stores serialized Dags without importing user code into this process', async () => {
    await write('a.js', dagFile('proc_a', `timetable: (last, n) => n > 2 ? null : new Date(0)`))
    const result = await runDagProcessor(db)
    expect(result).toMatchObject({ dags: 1, changed: ['proc_a'], deleted: [], errors: [] })

    const doc = await db.collection(SERIALIZED_DAGS).findOne({ dag_id: 'proc_a' })
    expect(doc).toMatchObject({ _id: 'proc_a', fileloc: join(dir, 'a.js'), task_ids: ['extract', 'load'] })
    expect(doc!.version).toMatch(/^[0-9a-f]{12}$/)
    expect(JSON.parse(doc!.data).tasks.extract.run.__fn).toContain('async (ctx) => 1')

    // The file ran in the parse child, not here
    expect((globalThis as Record<string, unknown>).__processorTestImported).toBeUndefined()

    await syncDagsFromDb(db)
    const dag = getDag('proc_a')!
    expect(isSerializedDag(dag)).toBe(true)
    expect(typeof dag.tasks.extract.run).toBe('function')
    expect(typeof dag.timetable).toBe('function')
    expect(dag.version).toBe(doc!.version)
  }, 30_000)

  it('the parse child gets no database credentials or encryption keys', async () => {
    await write('a.js', dagFile('proc_env'))
    await runDagProcessor(db)
    await syncDagsFromDb(db)
    expect(getDag('proc_env')!.description).toBe('mongo=undefined keys=undefined')
  }, 30_000)

  it('records import errors for broken, duplicate, hanging and crashing files — and still parses the rest', async () => {
    await write('a.js', dagFile('proc_a'))
    await write('b_dup.js', dagFile('proc_a'))
    await write('c_broken.js', 'export default {{{')
    await write('d_hang.js', 'await new Promise(() => {})\nexport default {}')
    await write('e_crash.js', 'process.exit(3)')
    await write('f_noexport.js', 'export const x = 1')
    await write('g.js', dagFile('proc_g'))

    const result = await runDagProcessor(db)
    expect(result.dags).toBe(2)
    const byFile = Object.fromEntries(result.errors.map(e => [e.filename, e.error]))
    expect(Object.keys(byFile).sort()).toEqual(['b_dup.js', 'c_broken.js', 'd_hang.js', 'e_crash.js', 'f_noexport.js'])
    expect(byFile['b_dup.js']).toBe("Dag id 'proc_a' is already defined in a.js")
    expect(byFile['d_hang.js']).toMatch(/timed out after 4000ms/)
    expect(byFile['e_crash.js']).toMatch(/parse process exited \(code 3\)/)
    expect(byFile['f_noexport.js']).toMatch(/no valid default export/)

    await syncDagsFromDb(db)
    expect(listDags().map(d => d.id).sort()).toEqual(['proc_a', 'proc_g'])
    expect(getImportErrors().map(e => e.filename).sort()).toEqual(Object.keys(byFile).sort())
  }, 60_000)

  it('updates changed Dags only, keeps the last good Dag of a broken file, drops Dags whose file is gone', async () => {
    await write('a.js', dagFile('proc_a'))
    await write('b.js', dagFile('proc_b'))
    expect((await runDagProcessor(db)).changed.sort()).toEqual(['proc_a', 'proc_b'])
    expect((await runDagProcessor(db)).changed).toEqual([])            // nothing changed

    await write('a.js', dagFile('proc_a', `tags: ['v2']`))
    await write('b.js', 'export default {{{')                        // broken: proc_b is kept
    let result = await runDagProcessor(db)
    expect(result.changed).toEqual(['proc_a'])
    expect(result.deleted).toEqual([])
    await syncDagsFromDb(db)
    expect(getDag('proc_a')!.tags).toEqual(['v2'])
    expect(getDag('proc_b')).toBeDefined()
    expect(getImportErrors().map(e => e.filename)).toEqual(['b.js'])

    await rm(join(dir, 'b.js'))
    result = await runDagProcessor(db)
    expect(result.deleted).toEqual(['proc_b'])
    await syncDagsFromDb(db)
    expect(getDag('proc_b')).toBeUndefined()
    expect(getImportErrors()).toEqual([])
    // A new version is recorded once per (dag, version)
    expect(await db.collection('dag_versions').countDocuments({ dag_id: 'proc_a' })).toBe(2)
  }, 60_000)

  it('stores warnings and provider records in the state row for the API to read', async () => {
    await write('a.js', dagFile('proc_w', `tasks: { lonely: { dependsOn: ['missing'] } }`))
    const { mkdir } = await import('node:fs/promises')
    await mkdir(join(dir, 'providers'))
    await write('providers/p.js', `export default { name: 'proc-provider', version: '1.2.3', description: 'd', operators: { Echo: () => ({}) }, connectionTypes: ['x'] }`)
    await runDagProcessor(db)
    const state = await db.collection(PROCESSOR_STATE).findOne({ _id: 'state' as never })
    expect(state).toMatchObject({ files: 1 })

    await syncDagsFromDb(db)
    expect(getDagWarnings('proc_w').map(w => w.warning_type)).toContain('unknown_dependency')
    expect(listProviders().map(p => p.name)).toContain('proc-provider')
  }, 30_000)

  it('maybeRunDagProcessor parses only when the folder changed', async () => {
    await write('a.js', dagFile('proc_a'))
    expect(await maybeRunDagProcessor(db)).not.toBeNull()
    expect(await maybeRunDagProcessor(db)).toBeNull()
    await new Promise(r => setTimeout(r, 20))
    await write('a.js', dagFile('proc_a', `tags: ['changed']`))
    expect((await maybeRunDagProcessor(db))?.changed).toEqual(['proc_a'])
  }, 30_000)

  it('a missing Dag folder removes every Dag', async () => {
    await write('a.js', dagFile('proc_a'))
    await runDagProcessor(db)
    process.env.DAG_DIR = join(dir, 'nope')
    expect((await runDagProcessor(db)).deleted).toEqual(['proc_a'])
  }, 30_000)

  it('parses the repository dags/ folder cleanly', async () => {
    process.env.DAG_DIR = join(process.cwd(), 'dags')
    const result = await runDagProcessor(db)
    expect(result.errors).toEqual([])
    expect(result.dags).toBeGreaterThan(10)
    await syncDagsFromDb(db)
    expect(Object.keys(getDag('hello_world')!.tasks)).toEqual(['extract', 'transform', 'load'])
    // The source a worker receives still evaluates to the task function
    const src = String(getDag('hello_world')!.tasks.extract.run)
    expect(typeof new Function(`return (${src})`)()).toBe('function')
    expect(await readFile(join(process.cwd(), 'dags/hello_world.ts'), 'utf8')).toContain('hello_world')
  }, 60_000)
})
