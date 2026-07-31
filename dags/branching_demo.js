import { dag } from 'airflow-nodejs/dag/types';

/**
 * Branching Demo — equivalent to Airflow's @task.branch / BranchPythonOperator.
 *
 * A branch task returns task_id(s) to activate.
 * All other direct dependents are automatically skipped.
 * Downstream chains of skipped tasks also cascade to skipped.
 *
 * Key patterns:
 *   - Join tasks after a branch should use triggerRule: 'none_failed'
 *     so they run even when one branch was skipped.
 *   - The branch decision is persisted as XCom key '_branch_decision'.
 *
 * Trigger with conf to control which path runs:
 *   POST /dags/branching_demo/trigger
 *   body: { "conf": { "score": 0.95 } }   → fast_path (score >= 0.9)
 *   body: { "conf": { "score": 0.5 } }    → slow_path
 *   body: {}                               → slow_path (default)
 */
export default dag({
  id: 'branching_demo',
  schedule: null,

  tasks: {

    // ── Step 1: Compute a score ──────────────────────────────────────────────
    score: {
      run: async (ctx) => {
        const s = Number(ctx.conf.score ?? 0.5)
        console.log(`[branching_demo] score = ${s}`)
        await ctx.xcom.push('score', s)
        return s
      }
    },

    // ── Step 2: Branch based on score ────────────────────────────────────────
    route: {
      dependsOn: ['score'],
      branch: async (ctx) => {
        const score = /** @type {number} */ (await ctx.xcom.pull('score', 'score'))
        if (score >= 0.9) {
          console.log('[branching_demo] route → fast_path')
          return 'fast_path'
        } else {
          console.log('[branching_demo] route → slow_path')
          return 'slow_path'
        }
      }
    },

    // ── Step 3a: Fast path ───────────────────────────────────────────────────
    fast_path: {
      dependsOn: ['route'],
      run: async () => {
        console.log('[branching_demo] fast_path: running optimized pipeline')
        return { path: 'fast', duration_ms: 100 }
      }
    },

    // ── Step 3b: Slow path ───────────────────────────────────────────────────
    slow_path: {
      dependsOn: ['route'],
      run: async () => {
        console.log('[branching_demo] slow_path: running full pipeline')
        return { path: 'slow', duration_ms: 5000 }
      }
    },

    // ── Step 4: Join — runs after EITHER branch completes ────────────────────
    // triggerRule: 'none_failed' ensures this runs even when one branch is skipped.
    join: {
      dependsOn: ['fast_path', 'slow_path'],
      triggerRule: 'none_failed',
      run: async (ctx) => {
        console.log('[branching_demo] join: both branches resolved')
        return { joined: true }
      }
    },

    // ── Step 5: Notify — always runs at the end ──────────────────────────────
    notify: {
      dependsOn: ['join'],
      run: async () => {
        console.log('[branching_demo] notify: pipeline complete')
        return { notified: true }
      }
    },

  }
})
