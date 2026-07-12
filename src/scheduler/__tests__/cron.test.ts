import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'
import * as cronModule from '../cron.js'

// We test syncCronJobs / scheduleDag / unscheduleDag behaviour without
// waiting for real wall-clock cron fires. node-cron is called but the
// scheduled callback never fires in tests (cron fires on minute boundary).

const { scheduleDag, unscheduleDag, syncCronJobs, stopAllCronJobs } = cronModule

// Minimal fake db — none of the cron registration paths hit the db directly
const fakeDb = {} as never

const makeDag = (id: string, schedule: string | null) => ({
  id,
  schedule,
  tasks: { step1: { run: async () => {} } },
})

beforeEach(() => stopAllCronJobs())
afterEach(() => stopAllCronJobs())

describe('scheduleDag', () => {
  it('does nothing for a dag with no schedule', () => {
    // Should not throw
    expect(() => scheduleDag(fakeDb, makeDag('no_schedule', null))).not.toThrow()
  })

  it('does nothing for an invalid cron expression', () => {
    const consoleSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})
    scheduleDag(fakeDb, makeDag('bad_cron', 'not-a-cron'))
    expect(consoleSpy).toHaveBeenCalledWith(expect.stringContaining('invalid cron expression'))
    consoleSpy.mockRestore()
  })

  it('accepts a valid cron expression without throwing', () => {
    expect(() => scheduleDag(fakeDb, makeDag('hourly', '0 * * * *'))).not.toThrow()
  })
})

describe('unscheduleDag', () => {
  it('does not throw when no job exists for dagId', () => {
    expect(() => unscheduleDag('nonexistent')).not.toThrow()
  })

  it('removes a previously scheduled dag', () => {
    scheduleDag(fakeDb, makeDag('my_dag', '0 * * * *'))
    expect(() => unscheduleDag('my_dag')).not.toThrow()
    // Scheduling again should work (confirms it was removed cleanly)
    expect(() => scheduleDag(fakeDb, makeDag('my_dag', '0 * * * *'))).not.toThrow()
  })
})

describe('syncCronJobs', () => {
  it('registers jobs for dags with a schedule', () => {
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    const dags = [makeDag('dag_a', '0 * * * *'), makeDag('dag_b', null)]
    syncCronJobs(fakeDb, dags)
    // Only dag_a (with schedule) should log a registration message
    const scheduled = consoleSpy.mock.calls.filter(c => String(c[0]).includes("scheduled dag 'dag_a'"))
    expect(scheduled.length).toBe(1)
    consoleSpy.mockRestore()
  })

  it('does not re-register an already scheduled dag', () => {
    scheduleDag(fakeDb, makeDag('stable_dag', '0 * * * *'))
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    // Second sync — dag is already registered, should not log another "scheduled" line
    syncCronJobs(fakeDb, [makeDag('stable_dag', '0 * * * *')])
    const newRegistrations = consoleSpy.mock.calls.filter(c => String(c[0]).includes("scheduled dag 'stable_dag'"))
    expect(newRegistrations.length).toBe(0)
    consoleSpy.mockRestore()
  })

  it('removes jobs for dags no longer in the list', () => {
    scheduleDag(fakeDb, makeDag('gone_dag', '0 * * * *'))
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    // Sync with empty list — gone_dag should be removed
    syncCronJobs(fakeDb, [])
    const removed = consoleSpy.mock.calls.filter(c => String(c[0]).includes("removed job for dag 'gone_dag'"))
    expect(removed.length).toBe(1)
    consoleSpy.mockRestore()
  })

  it('removes jobs for dags whose schedule was set to null', () => {
    scheduleDag(fakeDb, makeDag('now_manual', '0 * * * *'))
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})
    // Same dagId but schedule: null — treated as "no longer scheduled"
    syncCronJobs(fakeDb, [makeDag('now_manual', null)])
    const removed = consoleSpy.mock.calls.filter(c => String(c[0]).includes("removed job for dag 'now_manual'"))
    expect(removed.length).toBe(1)
    consoleSpy.mockRestore()
  })
})

describe('stopAllCronJobs', () => {
  it('stops all registered jobs without throwing', () => {
    scheduleDag(fakeDb, makeDag('dag_1', '0 * * * *'))
    scheduleDag(fakeDb, makeDag('dag_2', '0 12 * * *'))
    expect(() => stopAllCronJobs()).not.toThrow()
  })
})
