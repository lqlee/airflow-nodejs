import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_short_circuit'   // forked workers write XCom to this DB
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_short_circuit')
  clearRegistry()
})
afterAll(async () => { await db.dropDatabase(); await client.close(); delete process.env.DB_NAME })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'xcoms', 'event_logs']) await db.collection(c).deleteMany({})
  clearRegistry()
})

async function run(dag: DagDefinition): Promise<{ states: Record<string, string>; runState: string }> {
  register(dag)
  const runId = await createRun(db, dag)
  for (let i = 0; i < 2; i++) await advanceRun(db, runId)
  const tis = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
  const r = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
  return { states: Object.fromEntries(tis.map(t => [t.task_id, t.state])), runState: r!.state }
}

const chain = (cond: () => Promise<unknown>): DagDefinition => ({
  id: 'sc_dag', schedule: null,
  tasks: {
    before: { run: async () => {} },
    gate: { dependsOn: ['before'], shortCircuit: cond as never },
    child: { dependsOn: ['gate'], run: async () => {} },
    grandchild: { dependsOn: ['child'], run: async () => {} },
    cleanup: { dependsOn: ['grandchild'], triggerRule: 'all_done', run: async () => {} },   // would normally always run
    sibling: { dependsOn: ['before'], run: async () => {} },
  },
})

describe('shortCircuit', () => {
  it('falsy result skips ALL downstream tasks (even all_done), leaves upstream and siblings alone', async () => {
    const { states, runState } = await run(chain(async () => false))
    expect(states).toEqual({
      before: 'success', gate: 'success', sibling: 'success',
      child: 'skipped', grandchild: 'skipped', cleanup: 'skipped',
    })
    expect(runState).toBe('success')   // skipped is not failure
  })

  it('truthy result lets downstream run normally', async () => {
    const { states, runState } = await run(chain(async () => true))
    expect(Object.values(states).every(s => s === 'success')).toBe(true)
    expect(runState).toBe('success')
  })

  it('treats any falsy value (0 / null / undefined) as short-circuit', async () => {
    for (const [i, value] of [0, null, undefined].entries()) {
      const dag = chain(new Function(`return async () => ${String(value)}`)() as never)
      dag.id = `sc_falsy_${i}`
      expect((await run(dag)).states.child).toBe('skipped')
    }
  }, 30000)   // forks a worker per case

  it('records the decision as XCom _short_circuit', async () => {
    await run(chain(async () => false))
    const x = await db.collection('xcoms').findOne({ task_id: 'gate', key: '_short_circuit' })
    expect(x!.value).toBe(false)
  })

  it('can read upstream XCom to decide', async () => {
    const dag: DagDefinition = {
      id: 'sc_xcom', schedule: null,
      tasks: {
        count: { run: async (ctx) => { await ctx.xcom.push('rows', 0) } },
        gate: { dependsOn: ['count'], shortCircuit: async (ctx) => (await ctx.xcom.pull('count', 'rows') as number) > 0 },
        load: { dependsOn: ['gate'], run: async () => {} },
      },
    }
    const { states } = await run(dag)
    expect(states).toEqual({ count: 'success', gate: 'success', load: 'skipped' })
  })

  it('a throwing condition fails the task and the run (not a silent skip)', async () => {
    const { states, runState } = await run(chain(async () => { throw new Error('boom') }))
    expect(states.gate).toBe('failed')
    expect(runState).toBe('failed')
  })

  it('stamps is_short_circuit only on the short-circuit task', async () => {
    register(chain(async () => true))
    const runId = await createRun(db, chain(async () => true))
    const flags = Object.fromEntries((await db.collection('task_instances').find({ dag_run_id: runId }).toArray())
      .map(t => [t.task_id, t.is_short_circuit]))
    expect(flags.gate).toBe(true)
    expect(flags.child).toBe(false)
  })
})
