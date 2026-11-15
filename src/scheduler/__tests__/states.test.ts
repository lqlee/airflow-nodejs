import { describe, it, expect } from 'vitest'
import { aggregateState, aggregateByTask, TI_STATES, TI_FINISHED, TI_UNFINISHED } from '../states.js'
import { isSatisfied, unsatisfiedState } from '../claim.js'

describe('aggregateState — what downstream tasks see of a (mapped) task', () => {
  it.each([
    [['success', 'success'], 'success'],
    [['skipped'], 'skipped'],
    [['success', 'failed', 'running'], 'failed'],             // one failed instance fails the task at once
    [['success', 'upstream_failed'], 'upstream_failed'],
    [['failed', 'upstream_failed'], 'failed'],                // a real failure wins
    [['success', 'cancelled'], 'skipped'],                    // finished, neither all-success nor failed
    [['success', 'up_for_retry'], 'pending'],                 // a retry is still coming
    [['success', 'up_for_reschedule'], 'pending'],
    [['success', 'removed'], 'success'],                      // removed instances are ignored
    [['removed', 'removed'], 'removed'],
    [[], 'pending'],
  ] as const)('%j → %s', (states, expected) => {
    expect(aggregateState([...states])).toBe(expected)
  })

  it('groups by task_id', () => {
    const agg = aggregateByTask([
      { task_id: 'a', state: 'success' }, { task_id: 'a', state: 'failed' }, { task_id: 'b', state: 'skipped' },
    ])
    expect(Object.fromEntries(agg)).toEqual({ a: 'failed', b: 'skipped' })
  })
})

describe('state sets', () => {
  it('every state is either finished or unfinished, never both', () => {
    for (const s of TI_STATES) {
      expect(TI_FINISHED.has(s) !== (TI_UNFINISHED as readonly string[]).includes(s), s).toBe(true)
    }
  })
})

describe('trigger rules and upstream_failed (Airflow TriggerRuleDep)', () => {
  it('one_failed fires on an upstream_failed upstream; one_done does not count it as done', () => {
    expect(isSatisfied('one_failed', ['upstream_failed'])).toBe(true)
    expect(isSatisfied('one_done', ['upstream_failed'])).toBe(false)
    expect(isSatisfied('one_done', ['failed'])).toBe(true)
  })

  it('upstream_failed counts as a failure for all_failed and none_failed', () => {
    expect(isSatisfied('all_failed', ['failed', 'upstream_failed'])).toBe(true)
    expect(isSatisfied('none_failed', ['success', 'upstream_failed'])).toBe(false)
    expect(isSatisfied('none_failed_min_one_success', ['success', 'upstream_failed'])).toBe(false)
  })

  it.each([
    // rule, upstream states, setup flags, state the task gets
    ['all_success', ['success', 'failed'], [], 'upstream_failed'],
    ['all_success', ['success', 'upstream_failed'], [], 'upstream_failed'],
    ['all_success', ['success', 'skipped'], [], 'skipped'],
    ['all_failed', ['failed', 'success'], [], 'skipped'],
    ['one_success', ['failed', 'skipped'], [], 'upstream_failed'],
    ['one_success', ['skipped', 'skipped'], [], 'skipped'],
    ['one_failed', ['success', 'skipped'], [], 'skipped'],
    ['one_done', ['skipped', 'upstream_failed'], [], 'skipped'],
    ['none_failed', ['success', 'failed'], [], 'upstream_failed'],
    ['none_failed_min_one_success', ['skipped', 'skipped'], [], 'skipped'],
    ['none_failed_min_one_success', ['skipped', 'failed'], [], 'upstream_failed'],
    ['none_skipped', ['success', 'skipped'], [], 'skipped'],
    ['all_skipped', ['skipped', 'failed'], [], 'skipped'],
    ['all_done_min_one_success', ['success', 'skipped'], [], 'skipped'],
    ['all_done_min_one_success', ['failed', 'failed'], [], 'upstream_failed'],
    ['all_done_setup_success', ['failed', 'success'], [true, false], 'upstream_failed'],
    ['all_done_setup_success', ['skipped', 'success'], [true, false], 'skipped'],
  ] as const)('%s with %j → %s', (rule, states, flags, expected) => {
    expect(unsatisfiedState(rule, [...states], [...flags])).toBe(expected)
  })

  it('is null while the rule can still be met, or is met', () => {
    expect(unsatisfiedState('all_success', ['success', 'pending'])).toBeNull()
    expect(unsatisfiedState('all_success', ['failed', 'pending'])).toBeNull()   // waits for every upstream
    expect(unsatisfiedState('all_done', ['failed', 'skipped'])).toBeNull()
    expect(unsatisfiedState('one_failed', ['upstream_failed'])).toBeNull()
  })
})
