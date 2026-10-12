/** setup / teardown tasks: all_done_setup_success, run-state handling, skip exemptions. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createRun } from '../runs.js'
import { advanceRun, cancelRun } from '../index.js'
import { isSatisfied, isUnsatisfiable } from '../claim.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { analyzeWarnings } from '../../dag/warnings.js'
import type { DagDefinition, TaskDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_setup_teardown'   // forked workers (shortCircuit/branch) write XCom here
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_setup_teardown')
  clearRegistry()
})
afterAll(async () => { await db.dropDatabase(); await client.close(); delete process.env.DB_NAME })
afterEach(async () => {
  for (const c of ['dag_runs', 'task_instances', 'task_instance_tries', 'xcoms', 'event_logs', 'task_logs']) await db.collection(c).deleteMany({})
  clearRegistry()
})

const ok = async () => {}
const boom = async () => { throw new Error('boom') }

async function run(id: string, tasks: Record<string, TaskDefinition>, ticks = 12) {
  const dag: DagDefinition = { id, schedule: null, tasks }
  register(dag)
  const runId = await createRun(db, dag)
  for (let i = 0; i < ticks; i++) await advanceRun(db, runId)
  const tis = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
  const r = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
  return { states: Object.fromEntries(tis.map(t => [t.task_id, t.state])), runState: r!.state as string, tis }
}

describe('isSatisfied — all_done_setup_success', () => {
  const R = 'all_done_setup_success'
  it('waits for every upstream to finish', () => {
    expect(isSatisfied(R, ['success', 'pending'], [true, false])).toBe(false)
  })
  it('runs when work failed but a setup succeeded', () => {
    expect(isSatisfied(R, ['success', 'failed'], [true, false])).toBe(true)
  })
  it('needs at least one successful setup — not all', () => {
    expect(isSatisfied(R, ['success', 'failed', 'success'], [true, true, false])).toBe(true)
    expect(isSatisfied(R, ['failed', 'failed', 'success'], [true, true, false])).toBe(false)
    expect(isSatisfied(R, ['skipped', 'success'], [true, false])).toBe(false)
  })
  it('with no setup upstreams it behaves like all_done', () => {
    expect(isSatisfied(R, ['failed', 'skipped'], [false, false])).toBe(true)
    expect(isSatisfied(R, ['failed', 'skipped'])).toBe(true)
  })
  it('is unsatisfiable (→ skipped) once every setup failed, and not while still pending', () => {
    expect(isUnsatisfiable(R, ['failed', 'success'], [true, false])).toBe(true)
    expect(isUnsatisfiable(R, ['pending', 'success'], [true, false])).toBe(false)
  })
})

describe('setup / teardown runs', () => {
  it('happy path: setup → work → teardown all succeed; teardown defaults to all_done_setup_success', async () => {
    const { states, runState, tis } = await run('st_happy', {
      setup: { setup: true, run: ok },
      work: { dependsOn: ['setup'], run: ok },
      teardown: { teardown: true, dependsOn: ['setup', 'work'], run: ok },
    })
    expect(states).toEqual({ setup: 'success', work: 'success', teardown: 'success' })
    expect(runState).toBe('success')
    expect(tis.find(t => t.task_id === 'teardown')).toMatchObject({ trigger_rule: 'all_done_setup_success', is_teardown: true })
    expect(tis.find(t => t.task_id === 'setup')).toMatchObject({ trigger_rule: 'all_success', is_setup: true })
  })

  it('work fails: teardown still runs; the run fails because of the work task', async () => {
    const { states, runState } = await run('st_work_fails', {
      setup: { setup: true, run: ok },
      work: { dependsOn: ['setup'], run: boom },
      teardown: { teardown: true, dependsOn: ['setup', 'work'], run: ok },
    })
    expect(states).toEqual({ setup: 'success', work: 'failed', teardown: 'success' })
    expect(runState).toBe('failed')
  })

  it('setup fails: work and teardown are skipped (nothing to clean up); the run fails', async () => {
    const { states, runState } = await run('st_setup_fails', {
      setup: { setup: true, run: boom },
      work: { dependsOn: ['setup'], run: ok },
      teardown: { teardown: true, dependsOn: ['setup', 'work'], run: ok },
    })
    expect(states).toEqual({ setup: 'failed', work: 'skipped', teardown: 'skipped' })
    expect(runState).toBe('failed')
  })

  it('teardown fails: run still succeeds…', async () => {
    const { states, runState } = await run('st_td_fails', {
      setup: { setup: true, run: ok },
      work: { dependsOn: ['setup'], run: ok },
      teardown: { teardown: true, dependsOn: ['setup', 'work'], run: boom },
    })
    expect(states.teardown).toBe('failed')
    expect(runState).toBe('success')
  })

  it('…unless onFailureFailDagrun is set', async () => {
    const { states, runState } = await run('st_td_fails_strict', {
      setup: { setup: true, run: ok },
      work: { dependsOn: ['setup'], run: ok },
      teardown: { teardown: true, onFailureFailDagrun: true, dependsOn: ['setup', 'work'], run: boom },
    })
    expect(states.teardown).toBe('failed')
    expect(runState).toBe('failed')
  })

  it('a non-teardown task failing still fails the run even next to a failed teardown', async () => {
    const { runState } = await run('st_mixed_fail', {
      work: { run: boom },
      teardown: { teardown: true, dependsOn: ['work'], run: boom },
    })
    expect(runState).toBe('failed')
  })

  it('teardown without any setup behaves like all_done', async () => {
    const { states } = await run('st_no_setup', {
      work: { run: boom },
      teardown: { teardown: true, dependsOn: ['work'], run: ok },
    })
    expect(states).toEqual({ work: 'failed', teardown: 'success' })
  })

  it('an explicit triggerRule on a teardown wins over the default', async () => {
    const { states, tis } = await run('st_explicit_rule', {
      work: { run: boom },
      teardown: { teardown: true, triggerRule: 'all_success', dependsOn: ['work'], run: ok },
    })
    expect(states.teardown).toBe('skipped')
    expect(tis.find(t => t.task_id === 'teardown')?.trigger_rule).toBe('all_success')
  })

  it('onFailureFailDagrun on a non-teardown task is ignored', async () => {
    const { tis } = await run('st_flag_ignored', { t: { onFailureFailDagrun: true, run: ok } }, 3)
    expect(tis[0].on_failure_fail_dagrun).toBe(false)
  })
})

describe('teardowns survive skip decisions', () => {
  it('ShortCircuit(false) skips downstream work but the teardown still runs', async () => {
    const { states, runState } = await run('st_short_circuit', {
      setup: { setup: true, run: ok },
      gate: { dependsOn: ['setup'], shortCircuit: (async () => false) as never },
      work: { dependsOn: ['gate'], run: ok },
      after: { dependsOn: ['work'], run: ok },
      teardown: { teardown: true, dependsOn: ['setup', 'gate', 'work'], run: ok },
    }, 6)
    expect(states).toEqual({ setup: 'success', gate: 'success', work: 'skipped', after: 'skipped', teardown: 'success' })
    expect(runState).toBe('success')
  })

  it('an unselected branch does not skip a teardown that depends on the branch task', async () => {
    const { states, runState } = await run('st_branch', {
      setup: { setup: true, run: ok },
      decide: { dependsOn: ['setup'], branch: (async () => 'path_a') as never },
      path_a: { dependsOn: ['decide'], run: ok },
      path_b: { dependsOn: ['decide'], run: ok },
      teardown: { teardown: true, dependsOn: ['setup', 'decide', 'path_a'], run: ok },
    }, 6)
    expect(states.path_b).toBe('skipped')
    expect(states.teardown).toBe('success')
    expect(runState).toBe('success')
  })
})

describe('setup / teardown — more shapes', () => {
  it('two setups, one fails: teardown still runs (at least one setup succeeded) and the run fails', async () => {
    const { states, runState } = await run('st_two_setups', {
      s1: { setup: true, run: ok },
      s2: { setup: true, run: boom },
      work: { dependsOn: ['s1', 's2'], run: ok },
      teardown: { teardown: true, dependsOn: ['s1', 's2', 'work'], run: ok },
    })
    expect(states).toEqual({ s1: 'success', s2: 'failed', work: 'skipped', teardown: 'success' })
    expect(runState).toBe('failed')
  })

  it('a mapped work task with one failed instance does not stop the teardown', async () => {
    const { states, runState } = await run('st_mapped_work', {
      setup: { setup: true, run: ok },
      work: { dependsOn: ['setup'], expand: [1, 2, 3], run: async (ctx: { mapValue?: unknown }) => { if (ctx.mapValue === 2) throw new Error('bad item') } },
      teardown: { teardown: true, dependsOn: ['setup', 'work'], run: ok },
    }, 14)
    expect(states.teardown).toBe('success')
    expect(runState).toBe('failed')
  })

  it('a teardown honours retries: fails once, succeeds on retry, run succeeds', async () => {
    // Task fns run in a forked child (serialised via toString), so closures are unavailable:
    // build the fn from source with the marker path baked in. First try creates the marker and fails.
    const marker = join(tmpdir(), `st_retry_${process.pid}_${Date.now()}`)
    const failOnce = new Function(`return async () => {
      const fs = await import('node:fs')
      if (!fs.existsSync(${JSON.stringify(marker)})) { fs.writeFileSync(${JSON.stringify(marker)}, '1'); throw new Error('first try') }
    }`)() as () => Promise<void>
    try {
      const { states, runState, tis } = await run('st_retry', {
        setup: { setup: true, run: ok },
        teardown: { teardown: true, dependsOn: ['setup'], retries: 1, run: failOnce },
      }, 14)
      expect(states.teardown).toBe('success')
      expect(tis.find(t => t.task_id === 'teardown')?.try_number).toBe(1)
      expect(runState).toBe('success')
    } finally {
      rmSync(marker, { force: true })
    }
  })

  it('cancelling a run cancels teardowns that have not started (documented limitation)', async () => {
    const dag: DagDefinition = {
      id: 'st_cancel', schedule: null,
      tasks: { setup: { setup: true, run: ok }, teardown: { teardown: true, dependsOn: ['setup'], run: ok } },
    }
    register(dag)
    const runId = await createRun(db, dag)
    expect(await cancelRun(db, runId)).toBe(true)
    const td = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: 'teardown' })
    expect(td?.state).toBe('cancelled')
  })

  it('two teardowns in a chain: the second waits for the first, both run after failure', async () => {
    const { states } = await run('st_chain', {
      setup: { setup: true, run: ok },
      work: { dependsOn: ['setup'], run: boom },
      td1: { teardown: true, dependsOn: ['setup', 'work'], run: ok },
      td2: { teardown: true, dependsOn: ['setup', 'td1'], run: ok },
    })
    expect(states).toMatchObject({ work: 'failed', td1: 'success', td2: 'success' })
  })
})

describe('warnings', () => {
  it('flags a task marked both setup and teardown', () => {
    const w = analyzeWarnings({ id: 'd', schedule: null, tasks: { t: { setup: true, teardown: true, run: ok } } })
    expect(w.map(x => x.warning_type)).toContain('setup_and_teardown')
  })
  it('does not flag a plain setup or teardown', () => {
    const w = analyzeWarnings({ id: 'd', schedule: null, tasks: { s: { setup: true, run: ok }, t: { teardown: true, dependsOn: ['s'], run: ok } } })
    expect(w.map(x => x.warning_type)).not.toContain('setup_and_teardown')
  })
})
