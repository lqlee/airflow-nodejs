import { dag } from 'airflow-nodejs/dag/types';

/**
 * Trigger Rules Demo — shows all 6 trigger rules in action.
 *
 * Trigger rules control when a task runs based on upstream task states.
 * Tasks whose rule can never be satisfied are automatically marked 'skipped'.
 *
 *   all_success  — default; run when all upstreams succeeded
 *   all_failed   — run when all upstreams failed (useful for cleanup tasks)
 *   all_done     — run when all upstreams finished (any outcome)
 *   one_success  — run if at least one upstream succeeded
 *   one_failed   — run if at least one upstream failed (useful for alerts)
 *   none_failed  — run if no upstream failed (success or skipped only)
 *
 * Trigger:
 *   POST /dags/trigger_rules_demo/trigger   body: {}
 *
 * Expected outcome:
 *   - work_success: success
 *   - work_fail:    failed
 *   - on_all_done:  success  (all_done → runs regardless)
 *   - on_one_failed: success (one_failed → work_fail triggered it)
 *   - on_none_failed: skipped (none_failed → work_fail disqualifies it)
 *   - cleanup:       success  (all_failed on work_fail only → depends_on work_fail)
 *   - summary:       success  (all_done → always runs last)
 */
export default dag({
  id: 'trigger_rules_demo',
  schedule: null,

  tasks: {

    // ── Upstream tasks ──────────────────────────────────────────────────────
    work_success: {
      run: async () => {
        console.log('[trigger_rules_demo] work_success: ok')
        return { status: 'ok' }
      }
    },

    work_fail: {
      run: async () => {
        console.log('[trigger_rules_demo] work_fail: about to fail')
        throw new Error('Intentional failure for trigger rule demo')
      }
    },

    // ── Downstream tasks — each with a different trigger rule ───────────────

    // all_done: runs regardless of upstream outcomes
    on_all_done: {
      dependsOn: ['work_success', 'work_fail'],
      triggerRule: 'all_done',
      run: async (ctx) => {
        console.log('[trigger_rules_demo] on_all_done: running (all upstreams finished)')
        return { ran: true }
      }
    },

    // one_failed: runs because work_fail failed
    on_one_failed: {
      dependsOn: ['work_success', 'work_fail'],
      triggerRule: 'one_failed',
      run: async () => {
        console.log('[trigger_rules_demo] on_one_failed: alert! something failed')
        return { alert: 'sent' }
      }
    },

    // none_failed: skipped because work_fail failed
    on_none_failed: {
      dependsOn: ['work_success', 'work_fail'],
      triggerRule: 'none_failed',
      run: async () => {
        console.log('[trigger_rules_demo] on_none_failed: all good! (should be skipped)')
        return { status: 'all_good' }
      }
    },

    // all_failed: only depends on work_fail — runs because work_fail failed
    cleanup: {
      dependsOn: ['work_fail'],
      triggerRule: 'all_failed',
      run: async () => {
        console.log('[trigger_rules_demo] cleanup: running cleanup after failure')
        return { cleaned: true }
      }
    },

    // all_done on all branches: always runs last as a summary/notification
    summary: {
      dependsOn: ['on_all_done', 'on_one_failed', 'on_none_failed', 'cleanup'],
      triggerRule: 'all_done',
      run: async (ctx) => {
        console.log('[trigger_rules_demo] summary: pipeline complete')
        return { summary: 'done' }
      }
    },

  }
})
