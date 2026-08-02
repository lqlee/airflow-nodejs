/**
 * Tests for deferrable tasks.
 *
 * A deferrable task calls ctx.defer(trigger) to:
 *  1. Free its worker/pool slot immediately
 *  2. Enter 'deferred' state (non-terminal — run doesn't complete yet)
 *  3. Have its trigger() polled on each scheduler tick
 *  4. Resume as 'success' when trigger() returns true
 *
 * Critical verify (from advisor): while deferred, the slot is FREE.
 * We assert a second task actually starts while the first is deferred
 * under a 1-slot pool — if the slot were held, the second would stay queued.
 *
 * What each test answers:
 *  - Does defer() mark task as 'deferred' (not 'success'/'failed')?
 *  - Does the run NOT complete while a task is deferred?
 *  - Does the task resume as 'success' when trigger returns true?
 *  - Does deadline exceeded → task failed, run terminates?
 *  - Does a throwing trigger → task failed, run terminates?
 *  - Is the slot freed while deferred (slot-free verify via pool)?
 *  - Does downstream task run after deferred task resumes?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { pollDeferredTasks } from '../executor.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_DB = 'airflow_test_deferrable'

let client: MongoClient
let db: Db

// ── helpers ───────────────────────────────────────────────────────────────────

async function startRun(dag: DagDefinition): Promise<string> {
  register(dag)
  const runId = await createRun(db, dag)
  await advanceRun(db, runId)  // one tick to start tasks
  return runId
}

/** Wait until a task reaches a specific state (up to timeoutMs) */
async function waitForState(runId: string, taskId: string, state: string, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const ti = await db.collection('task_instances').findOne({ dag_run_id: runId, task_id: taskId })
    if (ti?.state === state) return
    await new Promise(r => setTimeout(r, 100))
  }
  throw new Error(`Task ${taskId} did not reach state '${state}' within ${timeoutMs}ms`)
}

/** Wait until a deferred task's next_poke_at is in the past (ready to poll),
 *  or until the task leaves 'deferred' state (already processed / timed out). */
async function waitUntilPollable(runId: string, taskId: string, timeoutMs = 5000): Promise<void> {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    const ti = await db.collection('task_instances').findOne({
      dag_run_id: runId,
      task_id: taskId,
    })
    // Already processed (timed out, failed, resumed)
    if (ti?.state !== 'deferred') return
    // next_poke_at in the past — ready to poll
    if (ti.next_poke_at && new Date(ti.next_poke_at) <= new Date()) return
    await new Promise(r => setTimeout(r, 50))
  }
  throw new Error(`Task ${taskId} did not become pollable within ${timeoutMs}ms`)
}

async function taskState(runId: string, taskId: string) {
  return db.collection('task_instances').findOne(
    { dag_run_id: runId, task_id: taskId },
    { projection: { state: 1, error: 1, deferred_at: 1, deferred_trigger_fn: 1, _id: 0 } }
  )
}

async function runState(runId: string): Promise<string | undefined> {
  const { ObjectId } = await import('mongodb')
  const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
  return run?.state
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  process.env.LOG_BACKEND = 'mongodb' // tests read from task_logs directly
  process.env.DB_NAME = TEST_DB
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(TEST_DB)
  clearRegistry()
  // Ensure pools collection exists for slot tests
  await db.collection('pools').deleteMany({ name: 'test_defer_pool' })
  await db.collection('pools').insertOne({ name: 'test_defer_pool', slots: 1, description: 'test' })
})

afterAll(async () => {
  await new Promise(r => setTimeout(r, 300))
  delete process.env.DB_NAME
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('task_logs').deleteMany({})
  await db.collection('xcoms').deleteMany({})
  clearRegistry()
})

// ══════════════════════════════════════════════════════════════════════════════
// DEFERRABLE TASK TESTS
// ══════════════════════════════════════════════════════════════════════════════

describe('deferrable tasks', () => {

  it('task calling ctx.defer() transitions to deferred state', async () => {
    const dag: DagDefinition = {
      id: 'defer_basic',
      schedule: null,
      tasks: {
        wait: {
          run: async (ctx) => {
            await ctx.defer(async () => false, { interval: 100 })
          }
        }
      }
    }
    const runId = await startRun(dag)
    await waitForState(runId, 'wait', 'deferred')

    const ti = await taskState(runId, 'wait')
    expect(ti?.state).toBe('deferred')
    expect(ti?.deferred_at).not.toBeNull()
    expect(ti?.deferred_trigger_fn).not.toBeNull()
  })

  it('run does NOT complete while a task is deferred', async () => {
    const dag: DagDefinition = {
      id: 'defer_nonterm',
      schedule: null,
      tasks: {
        wait: {
          run: async (ctx) => {
            await ctx.defer(async () => false, { interval: 100 })
          }
        }
      }
    }
    const runId = await startRun(dag)
    await waitForState(runId, 'wait', 'deferred')
    for (let i = 0; i < 3; i++) await advanceRun(db, runId)

    const state = await runState(runId)
    expect(state).toBe('running')
  })

  it('task resumes as success when trigger returns true', async () => {
    // Trigger fn must be self-contained (no closures) — uses XCom as state store
    const dag: DagDefinition = {
      id: 'defer_resume',
      schedule: null,
      tasks: {
        wait: {
          run: async (ctx) => {
            // Push initial count so trigger can read/increment it via XCom
            await ctx.xcom.push('poll_count', 0)
            await ctx.defer(
              // Self-contained trigger: reads poll_count from XCom, succeeds on 2nd poll
              async (tctx: any) => {
                const count = (await tctx.xcom.pull('wait', 'poll_count') as number) ?? 0
                await tctx.xcom.push('poll_count', count + 1)
                return count >= 1  // succeed when we've polled at least twice (count was 1)
              },
              { interval: 100 }
            )
          }
        }
      }
    }
    const runId = await startRun(dag)
    await waitForState(runId, 'wait', 'deferred')
    await waitUntilPollable(runId, 'wait')

    await pollDeferredTasks(db)  // 1st poll — count=0 → false, increments to 1; next_poke_at advances

    await waitUntilPollable(runId, 'wait')
    await pollDeferredTasks(db)  // 2nd poll — count=1 → true (resume!)

    const ti = await taskState(runId, 'wait')
    expect(ti?.state).toBe('success')
  })

  it('downstream task runs after deferred task resumes', async () => {
    const dag: DagDefinition = {
      id: 'defer_downstream',
      schedule: null,
      tasks: {
        wait: {
          run: async (ctx) => {
            await ctx.xcom.push('ready', false)
            await ctx.defer(
              // Self-contained: reads 'ready' flag from XCom
              async (tctx: any) => {
                const ready = await tctx.xcom.pull('wait', 'ready')
                return ready === true
              },
              { interval: 100 }
            )
          }
        },
        after: {
          dependsOn: ['wait'],
          run: async () => 'downstream ran'
        }
      }
    }
    const runId = await startRun(dag)
    await waitForState(runId, 'wait', 'deferred')
    await waitUntilPollable(runId, 'wait')

    await pollDeferredTasks(db)  // Poll 1: ready=false → still deferred

    // Set ready=true via API/DB directly (simulates external event)
    await db.collection('xcoms').updateOne(
      { dag_run_id: runId, task_id: 'wait', key: 'ready' },
      { $set: { value: true } }
    )

    // Poll 2: ready=true → resume
    await waitUntilPollable(runId, 'wait')
    await pollDeferredTasks(db)

    // Advance so 'after' gets claimed and executed
    for (let i = 0; i < 10; i++) await advanceRun(db, runId)

    expect((await taskState(runId, 'wait'))?.state).toBe('success')
    expect((await taskState(runId, 'after'))?.state).toBe('success')
    expect(await runState(runId)).toBe('success')
  })

  it('deadline exceeded → task fails, run terminates', async () => {
    const dag: DagDefinition = {
      id: 'defer_timeout',
      schedule: null,
      tasks: {
        wait: {
          run: async (ctx) => {
            await ctx.defer(
              async () => false,  // never resolves
              { timeout: 100, interval: 50 }  // 100ms deadline, 50ms between checks
            )
          }
        }
      }
    }
    const runId = await startRun(dag)
    await waitForState(runId, 'wait', 'deferred')
    // The fork itself takes ~1s; by the time the first poll runs the 100ms deadline is past
    await waitUntilPollable(runId, 'wait')
    await pollDeferredTasks(db)

    const ti = await taskState(runId, 'wait')
    expect(ti?.state).toBe('failed')
    expect(ti?.error).toMatch(/timed out/)

    for (let i = 0; i < 5; i++) await advanceRun(db, runId)
    expect(await runState(runId)).toBe('failed')
  })

  it('throwing trigger → task fails, run terminates', async () => {
    const dag: DagDefinition = {
      id: 'defer_throw',
      schedule: null,
      tasks: {
        wait: {
          run: async (ctx) => {
            await ctx.defer(
              // Self-contained trigger that always throws
              async () => { throw new Error('trigger exploded') },
              { interval: 50 }
            )
          }
        }
      }
    }
    const runId = await startRun(dag)
    await waitForState(runId, 'wait', 'deferred')

    await waitForState(runId, 'wait', 'deferred')
    await waitUntilPollable(runId, 'wait')
    await pollDeferredTasks(db)

    const ti = await taskState(runId, 'wait')
    expect(ti?.state).toBe('failed')
    expect(ti?.error).toMatch(/trigger exploded/)

    for (let i = 0; i < 5; i++) await advanceRun(db, runId)
    expect(await runState(runId)).toBe('failed')
  })

  // ── THE CRITICAL TEST: slot is freed while deferred ─────────────────────────
  it('SLOT-FREE: second task starts while first is deferred (1-slot pool)', async () => {
    // If defer() held the slot, 'concurrent' would stay queued while 'deferred_task' waits.
    // This test FAILS if the slot is held.
    const dag: DagDefinition = {
      id: 'defer_slot_free',
      schedule: null,
      tasks: {
        deferred_task: {
          pool: 'test_defer_pool',  // uses the 1-slot pool
          run: async (ctx) => {
            await ctx.defer(async () => false, { interval: 60_000 })  // defers, never resumes
          }
        },
        concurrent: {
          pool: 'test_defer_pool',  // same 1-slot pool
          run: async () => 'I ran while other task was deferred!'
        }
      }
    }
    const runId = await startRun(dag)
    await waitForState(runId, 'deferred_task', 'deferred')

    // Multiple ticks to let concurrent claim the freed slot
    for (let i = 0; i < 8; i++) await advanceRun(db, runId)

    const deferredState = (await taskState(runId, 'deferred_task'))?.state
    const concurrentState = (await taskState(runId, 'concurrent'))?.state

    // deferred_task should be deferred (slot freed)
    expect(deferredState).toBe('deferred')
    // concurrent should have run (slot was available)
    expect(concurrentState).toBe('success')
  })
})
