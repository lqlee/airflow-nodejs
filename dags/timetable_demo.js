import { dag } from 'airflow-nodejs/dag/types';

/**
 * Timetable Demo — custom scheduling logic beyond standard cron expressions.
 *
 * A `timetable` function replaces `schedule` with arbitrary JavaScript logic.
 * It receives (lastRunAt: Date | null, runCount: number) and returns:
 *   - A Date → when the next run should fire
 *   - null    → stop scheduling permanently (no more runs)
 *
 * Set `schedule: null` when using timetable.
 *
 * Trigger manually to see each example running:
 *   POST /dags/timetable_interval/trigger   body: {}
 *
 * Or just wait — timetable dags fire automatically without manual trigger.
 */

// ── Example 1: Fixed interval (every 30 seconds for demo) ────────────────────
// Real world: replace 30_000 with 30 * 60 * 1000 for 30-minute intervals.
export const intervalDag = dag({
  id: 'timetable_interval',
  schedule: null,
  timetable: (lastRunAt) => {
    // Fire 30s after last run (or immediately on first run)
    const base = lastRunAt ?? new Date(0)
    return new Date(base.getTime() + 30_000)
  },
  tasks: {
    report: {
      run: async (ctx) => {
        console.log(`[timetable_interval] running at ${new Date().toISOString()}`)
        return { ts: new Date().toISOString() }
      }
    }
  }
})

// ── Example 2: Weekdays only at 09:00 UTC ────────────────────────────────────
export const weekdayDag = dag({
  id: 'timetable_weekdays',
  schedule: null,
  timetable: (lastRunAt) => {
    const now = new Date()
    // Start from today at 09:00 UTC
    const candidate = new Date(now)
    candidate.setUTCHours(9, 0, 0, 0)

    // If we already passed 09:00 today, move to tomorrow
    if (candidate <= now) {
      candidate.setUTCDate(candidate.getUTCDate() + 1)
    }

    // Skip weekends (0 = Sunday, 6 = Saturday)
    while (candidate.getUTCDay() === 0 || candidate.getUTCDay() === 6) {
      candidate.setUTCDate(candidate.getUTCDate() + 1)
    }

    return candidate
  },
  tasks: {
    daily_job: {
      run: async (ctx) => ({
        day: new Date().toUTCString(),
        weekday: ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'][new Date().getUTCDay()],
      })
    }
  }
})

// ── Example 3: Business hours only (Mon-Fri, 09:00-17:00 UTC, every hour) ────
export const businessHoursDag = dag({
  id: 'timetable_business_hours',
  schedule: null,
  timetable: (lastRunAt) => {
    const now = lastRunAt ?? new Date()
    // Next candidate: current time + 1 hour, rounded to hour
    let next = new Date(now.getTime() + 60 * 60 * 1000)
    next.setUTCMinutes(0, 0, 0)

    // Advance until we hit a valid business hour slot
    for (let i = 0; i < 200; i++) {  // max 200 iterations (~8 days)
      const day = next.getUTCDay()   // 0=Sun, 6=Sat
      const hour = next.getUTCHours()

      const isWeekday = day >= 1 && day <= 5
      const isBusinessHour = hour >= 9 && hour < 17

      if (isWeekday && isBusinessHour) return next

      // Advance to next hour
      next = new Date(next.getTime() + 60 * 60 * 1000)
      next.setUTCMinutes(0, 0, 0)
    }

    return null  // safety: should never reach here
  },
  tasks: {
    hourly_check: {
      run: async (ctx) => ({
        ts: new Date().toISOString(),
        hour: new Date().getUTCHours(),
      })
    }
  }
})

// ── Example 4: Run exactly N times then stop ──────────────────────────────────
// Fires every 60 seconds, stops after 5 runs total.
export const limitedRunsDag = dag({
  id: 'timetable_limited',
  schedule: null,
  timetable: (lastRunAt, runCount) => {
    if (runCount >= 5) return null  // done — no more runs
    const base = lastRunAt ?? new Date(0)
    return new Date(base.getTime() + 60_000)  // every 60s
  },
  tasks: {
    step: {
      run: async (ctx) => ({
        runNumber: ctx.conf.runNumber,
        message: 'This dag will stop after 5 runs',
      })
    }
  }
})

// ── Example 5: Exponential backoff (retry-like scheduling) ───────────────────
// First run fires immediately, then 1min, 2min, 4min, 8min gaps.
export const backoffDag = dag({
  id: 'timetable_backoff',
  schedule: null,
  timetable: (lastRunAt, runCount) => {
    if (runCount >= 5) return null
    const delayMs = runCount === 0
      ? 0                                    // immediate first run
      : Math.pow(2, runCount - 1) * 60_000   // 1min, 2min, 4min, 8min
    const base = lastRunAt ?? new Date()
    return new Date(base.getTime() + delayMs)
  },
  tasks: {
    attempt: {
      run: async (ctx) => ({
        attempt: ctx.conf.attempt,
        ts: new Date().toISOString(),
      })
    }
  }
})

// Default export — the interval demo (fires every 30s for easy verification)
export default intervalDag
