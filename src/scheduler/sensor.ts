/**
 * Sensor outcome logic — pure, no DB access.
 * Extracted so it can be unit-tested with fake timestamps.
 */

export type SensorResult = 'success' | 'reschedule' | 'timeout'

/**
 * Determine what to do after a poke() call.
 *
 * @param pokeResult - true if the condition is met (sensor done)
 * @param firstPokedAt - when poke was first invoked (NOT started_at — claim overwrites it)
 * @param now - current time
 * @param sensorTimeoutMs - total deadline in ms (0 = no timeout)
 */
export function sensorOutcome(
  pokeResult: boolean,
  firstPokedAt: Date,
  now: Date,
  sensorTimeoutMs: number,
): SensorResult {
  if (pokeResult) return 'success'

  if (sensorTimeoutMs > 0) {
    const elapsed = now.getTime() - firstPokedAt.getTime()
    if (elapsed >= sensorTimeoutMs) return 'timeout'
  }

  return 'reschedule'
}
