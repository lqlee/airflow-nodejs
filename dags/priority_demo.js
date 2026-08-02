import { dag } from 'airflow-nodejs/dag/types';

/**
 * Priority Demo — tasks with higher priority run first when multiple are ready.
 *
 * Use cases:
 *  - Critical cleanup runs before optional reporting
 *  - SLA-sensitive tasks preempt background work
 *  - Multiple pipelines share a pool — high-value ones get slots first
 *
 * Trigger:
 *   POST /dags/priority_demo/trigger   body: {}
 *
 * Expected order (all share priority_pool with 1 slot, so sequential):
 *   critical (100) → important (50) → normal (0) → background (-10)
 */
export default dag({
  id: 'priority_demo',
  schedule: null,

  tasks: {
    // Higher priority → runs first when pool is constrained
    critical: {
      priority: 100,
      run: async () => {
        console.log('[priority_demo] critical task (priority: 100)')
        return { step: 'critical', ts: new Date().toISOString() }
      }
    },

    important: {
      priority: 50,
      run: async () => {
        console.log('[priority_demo] important task (priority: 50)')
        return { step: 'important', ts: new Date().toISOString() }
      }
    },

    normal: {
      // priority: 0  (default — omit is same as priority: 0)
      run: async () => {
        console.log('[priority_demo] normal task (default priority: 0)')
        return { step: 'normal', ts: new Date().toISOString() }
      }
    },

    background: {
      priority: -10,
      run: async () => {
        console.log('[priority_demo] background task (priority: -10) — runs last')
        return { step: 'background', ts: new Date().toISOString() }
      }
    },
  }
})
