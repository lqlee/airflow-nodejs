/**
 * Tests for timetable scheduling.
 *
 * What each test answers:
 *  - Does a timetable fire when nextFireAt <= now?
 *  - Does it skip when nextFireAt is in the future?
 *  - Does returning null stop all future runs?
 *  - Is runCount passed correctly so limited-run timetables work?
 *  - Does a throwing timetable not crash the scheduler tick?
 *  - Does a paused dag skip timetable firing?
 *  - Does it recover lastRunAt from DB after restart (no in-memory entry)?
 *  - Does it fire on the first call when lastRunAt is null?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { createRun } from '../runs.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'
import {
  tickTimetables,
  resetTimetable,
  activeTimetableCount,
  getTimetableEntry,
} from '../cron.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'

let client: MongoClient
let db: Db

// ── helpers ───────────────────────────────────────────────────────────────────

async function runCount(dagId: string): Promise<number> {
  return db.collection('dag_runs').countDocuments({ dag_id: dagId })
}

async function lastRunTriggerType(dagId: string): Promise<string | null> {
  const runs = await db.collection('dag_runs')
    .find({ dag_id: dagId }, { projection: { trigger_type: 1 } })
    .toArray()
  // Return the trigger_type from any run (all timetable runs in this test have same type)
  return runs[0]?.trigger_type ?? null
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_timetable')
  clearRegistry()
})

afterAll(async () => {
  // Brief settle to let any void advanceRun() background operations complete
  // before closing the client, preventing MongoClientClosedError unhandled rejection.
  await new Promise(r => setTimeout(r, 200))
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  clearRegistry()
  // Reset all timetable entries between tests
  ;['tt_fire_now', 'tt_future', 'tt_stop', 'tt_count', 'tt_throws', 'tt_paused', 'tt_restart']
    .forEach(resetTimetable)
})

// ══════════════════════════════════════════════════════════════════════════════
// CORE BEHAVIOUR
// ══════════════════════════════════════════════════════════════════════════════

describe('tickTimetables', () => {
  it('fires a run when nextFireAt is in the past', async () => {
    const past = new Date(Date.now() - 1000)  // 1s ago
    const dag: DagDefinition = {
      id: 'tt_fire_now',
      schedule: null,
      // Always return past time → fires immediately every tick
      timetable: () => past,
      tasks: { step: { run: async () => 'done' } },
    }
    register(dag)
    await tickTimetables(db, [dag])
    expect(await runCount('tt_fire_now')).toBe(1)
    expect(await lastRunTriggerType('tt_fire_now')).toBe('timetable')
  })

  it('does not fire when nextFireAt is in the future', async () => {
    const future = new Date(Date.now() + 60_000)  // 1min from now
    const dag: DagDefinition = {
      id: 'tt_future',
      schedule: null,
      timetable: () => future,
      tasks: { step: { run: async () => 'done' } },
    }
    register(dag)
    await tickTimetables(db, [dag])
    expect(await runCount('tt_future')).toBe(0)
  })

  it('stops firing after timetable returns null', async () => {
    let calls = 0
    const dag: DagDefinition = {
      id: 'tt_stop',
      schedule: null,
      timetable: () => {
        calls++
        // Fire once (past), then return null
        return calls === 1 ? new Date(Date.now() - 1000) : null
      },
      tasks: { step: { run: async () => 'done' } },
    }
    register(dag)

    // Tick 1: should fire (returns past)
    await tickTimetables(db, [dag])
    expect(await runCount('tt_stop')).toBe(1)

    // Tick 2: nextFireAt is null — no run
    await tickTimetables(db, [dag])
    expect(await runCount('tt_stop')).toBe(1)  // still 1

    // Confirm stopped flag set
    const entry = getTimetableEntry('tt_stop')
    expect(entry?.stopped).toBe(true)
  })

  it('passes runCount correctly to timetable fn', async () => {
    const receivedCounts: number[] = []
    const dag: DagDefinition = {
      id: 'tt_count',
      schedule: null,
      timetable: (_last, count) => {
        receivedCounts.push(count)
        // Fire 3 times total, then stop
        return count < 3 ? new Date(Date.now() - 1000) : null
      },
      tasks: { step: { run: async () => 'done' } },
    }
    register(dag)

    // Tick 4 times
    for (let i = 0; i < 4; i++) {
      await tickTimetables(db, [dag])
    }

    expect(await runCount('tt_count')).toBe(3)
    // runCount passed to fn should be 0, 1, 2, 3 (4th call returns null)
    expect(receivedCounts).toContain(0)
    expect(receivedCounts).toContain(3)
  })

  it('does not crash the tick when timetable fn throws', async () => {
    const dag: DagDefinition = {
      id: 'tt_throws',
      schedule: null,
      timetable: () => { throw new Error('timetable exploded') },
      tasks: { step: { run: async () => 'done' } },
    }
    register(dag)

    // Should not throw
    await expect(tickTimetables(db, [dag])).resolves.not.toThrow()
    // No run created
    expect(await runCount('tt_throws')).toBe(0)
  })

  it('skips paused dags', async () => {
    // Pause the dag
    await db.collection('dag_paused').updateOne(
      { dag_id: 'tt_paused' },
      { $set: { dag_id: 'tt_paused', paused: true } },
      { upsert: true }
    )

    const dag: DagDefinition = {
      id: 'tt_paused',
      schedule: null,
      timetable: () => new Date(Date.now() - 1000),
      tasks: { step: { run: async () => 'done' } },
    }
    register(dag)
    await tickTimetables(db, [dag])
    expect(await runCount('tt_paused')).toBe(0)

    // Cleanup
    await db.collection('dag_paused').deleteMany({ dag_id: 'tt_paused' })
  })

  it('recovers lastRunAt from DB after server restart (no in-memory entry)', async () => {
    const receivedLastRunAt: (Date | null)[] = []
    const seededAt = new Date(Date.now() - 5_000)

    // Seed an existing timetable run in DB (simulates prior run before restart)
    await db.collection('dag_runs').insertOne({
      dag_id: 'tt_restart',
      state: 'success',
      trigger_type: 'timetable',
      created_at: seededAt,
      conf: {},
      tags: [],
      note: null,
      logical_date: null,
      dag_version: 'test',
      backfill_id: null,
      ended_at: seededAt,
    })

    const dag: DagDefinition = {
      id: 'tt_restart',
      schedule: null,
      timetable: (last) => {
        receivedLastRunAt.push(last)
        // Fire 10s after last run
        return new Date((last ?? new Date(0)).getTime() + 10_000)
      },
      tasks: { step: { run: async () => 'done' } },
    }
    register(dag)

    // First tick — should reconstruct from DB, seededAt was 5s ago so
    // nextFireAt = seededAt + 10s = 5s in future → no fire yet
    await tickTimetables(db, [dag])

    expect(receivedLastRunAt[0]).not.toBeNull()
    expect(receivedLastRunAt[0]?.getTime()).toBeCloseTo(seededAt.getTime(), -2)
    // Should not have fired (5s in future)
    const existing = await db.collection('dag_runs').countDocuments({ dag_id: 'tt_restart' })
    expect(existing).toBe(1)  // only the seeded run
  })

  it('removes timetable entry when dag no longer has timetable', async () => {
    const dag: DagDefinition = {
      id: 'tt_fire_now',
      schedule: null,
      timetable: () => new Date(Date.now() - 1000),
      tasks: { step: { run: async () => 'done' } },
    }
    register(dag)
    await tickTimetables(db, [dag])
    expect(activeTimetableCount()).toBeGreaterThanOrEqual(1)

    // Now tick with empty list (dag removed)
    await tickTimetables(db, [])
    expect(getTimetableEntry('tt_fire_now')).toBeUndefined()
  })
})
