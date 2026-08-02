import { dag } from 'airflow-nodejs/dag/types';

/**
 * Deferrable Tasks Demo — suspend a task and free its worker slot.
 *
 * A deferrable task calls ctx.defer(trigger, opts) from its run: function.
 * This immediately:
 *   1. Frees the worker/pool slot (other tasks can run)
 *   2. Marks the task as 'deferred' (non-terminal — run doesn't complete yet)
 *   3. Polls trigger() on each scheduler tick (~5–10s)
 *   4. When trigger() returns true → task resumes as 'success'
 *
 * This is equivalent to Airflow's deferrable operators / Triggerer pattern.
 *
 * CRITICAL: trigger() runs in the scheduler process (not a worker fork).
 * It must be SELF-CONTAINED — no closures over module-scope variables.
 * Use ctx.xcom, ctx.conf, or HTTP calls to pass state.
 *
 * Trigger:
 *   POST /dags/deferrable_demo/trigger
 *   body: { "conf": { "wait_seconds": 15 } }
 */
export default dag({
  id: 'deferrable_demo',
  schedule: null,

  tasks: {

    // ── Step 1: Start a "long-running job" and defer ─────────────────────────
    long_job: {
      run: async (ctx) => {
        const waitSeconds = Number(ctx.conf.wait_seconds ?? 10)
        const readyAt = Date.now() + waitSeconds * 1000

        console.log(`[deferrable_demo] starting job, will be ready in ${waitSeconds}s`)
        // Store "ready_at" timestamp in XCom so the trigger can check it
        await ctx.xcom.push('ready_at', readyAt)

        // Defer: free the worker slot, poll every 5s until ready
        await ctx.defer(
          // Trigger fn — runs in scheduler, must be self-contained
          async (tctx) => {
            const readyAt = await tctx.xcom.pull('long_job', 'ready_at')
            const now = Date.now()
            const ready = typeof readyAt === 'number' && now >= readyAt
            if (!ready) {
              const remaining = Math.ceil(((readyAt ?? now) - now) / 1000)
              console.log(`[deferrable_demo] job not ready yet, ${remaining}s remaining`)
            }
            return ready
          },
          {
            interval: 5_000,                    // poll every 5s
            timeout: (waitSeconds + 60) * 1000, // deadline = wait + 60s buffer
          }
        )
        // This line is NEVER reached — ctx.defer() throws DeferSignal
      }
    },

    // ── Step 2: Runs after long_job resumes ──────────────────────────────────
    process_result: {
      dependsOn: ['long_job'],
      run: async (ctx) => {
        console.log('[deferrable_demo] job completed, processing results')
        return { status: 'processed', dag: ctx.dagId, run: ctx.runId }
      }
    },

  }
})
