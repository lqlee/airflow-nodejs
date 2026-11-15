/**
 * Logical date and data interval of scheduled runs — Airflow's cron timetables.
 *
 *   'trigger' (default, Airflow 3 CronTriggerTimetable): the run fires at each cron time T.
 *       logical_date = T, data_interval = [T - triggerInterval, T]  (triggerInterval default 0)
 *   'interval' (Airflow CronDataIntervalTimetable, Airflow 2's default): the run covers the period between
 *       two cron times and fires when it ends. Fired at T: data_interval = [prev(T), T], logical_date = prev(T).
 *
 * Manual, dataset and timetable-function runs have no logical date and no data interval (as in Airflow 3).
 */
import cronParser from 'cron-parser'
import type { DagDefinition } from '../dag/types.js'

export interface DataInterval { start: Date; end: Date }
export interface ScheduledRunInfo { logicalDate: Date; dataInterval: DataInterval }

/** Cron occurrence strictly before / after `at`. `tz` undefined = local time (node-cron's default). */
function neighbour(schedule: string, at: Date, dir: 'prev' | 'next', tz?: string): Date {
  const iter = cronParser.parseExpression(schedule, { currentDate: at, ...(tz ? { tz } : {}) })
  return (dir === 'prev' ? iter.prev() : iter.next()).toDate()
}

const isIntervalMode = (dag: DagDefinition) => dag.scheduleMode === 'interval'

/**
 * Run info for a cron firing at `fireTime` (already truncated to the schedule's resolution).
 * `tz` must match the clock the schedule fired on: 'UTC' for catch-up, local for node-cron.
 */
export function runInfoForFire(dag: DagDefinition, fireTime: Date, tz?: string): ScheduledRunInfo {
  if (dag.schedule && isIntervalMode(dag)) {
    const start = neighbour(dag.schedule, fireTime, 'prev', tz)
    return { logicalDate: start, dataInterval: { start, end: fireTime } }
  }
  const lookback = Math.max(0, dag.triggerInterval ?? 0)
  return { logicalDate: fireTime, dataInterval: { start: new Date(fireTime.getTime() - lookback), end: fireTime } }
}

/**
 * Run info for a run whose logical date is a cron occurrence `logicalDate` (backfill picks logical dates).
 * In interval mode the run covers [logicalDate, next(logicalDate)].
 */
export function runInfoForLogicalDate(dag: DagDefinition, logicalDate: Date, tz = 'UTC'): ScheduledRunInfo {
  if (dag.schedule && isIntervalMode(dag)) {
    return { logicalDate, dataInterval: { start: logicalDate, end: neighbour(dag.schedule, logicalDate, 'next', tz) } }
  }
  return runInfoForFire(dag, logicalDate, tz)
}
