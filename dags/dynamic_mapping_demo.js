import { dag } from 'airflow-nodejs/dag/types';

/**
 * Dynamic Mapping Demo — fan out over a list produced at runtime.
 *
 * Two forms of dynamic mapping:
 *
 *   Literal (static):
 *     expand: ['a', 'b', 'c']          — array known at authoring time
 *
 *   XCom-driven (dynamic):
 *     expand: { from: 'task', key: 'k' }  — list produced at runtime
 *
 * The XCom source must push an array before this task runs:
 *   await ctx.xcom.push('items', [item1, item2, ...])
 *
 * Behaviors:
 *   - source pushes N items → N task instances, each gets mapIndex + mapValue
 *   - source pushes []      → mapped task automatically skipped
 *   - source fails          → mapped task skipped (cascade)
 *
 * Trigger:
 *   POST /dags/dynamic_mapping_demo/trigger
 *   body: { "conf": { "region_count": 3 } }   → processes 3 regions
 *   body: { "conf": { "region_count": 0 } }   → skips processing (empty list)
 *   body: {}                                   → default: 2 regions
 */
export default dag({
  id: 'dynamic_mapping_demo',
  schedule: null,

  tasks: {

    // ── Step 1: Discover items to process ────────────────────────────────────
    // In a real pipeline: query a DB, list S3 files, call an API, etc.
    discover: {
      run: async (ctx) => {
        const count = Number(ctx.conf.region_count ?? 2)
        const regions = ['us-east-1', 'us-west-2', 'eu-west-1', 'ap-southeast-1'].slice(0, count)
        console.log(`[discover] found ${regions.length} regions: ${regions.join(', ')}`)
        await ctx.xcom.push('regions', regions)
        return regions.length
      }
    },

    // ── Step 2: Process each item in parallel ─────────────────────────────────
    // expand: { from, key } → one instance per element of the pushed array
    process_region: {
      dependsOn: ['discover'],
      expand: { from: 'discover', key: 'regions' },
      run: async (ctx) => {
        console.log(`[process_region] [${ctx.mapIndex}] processing: ${ctx.mapValue}`)
        // Simulate work
        return { region: ctx.mapValue, status: 'deployed', index: ctx.mapIndex }
      }
    },

    // ── Step 3: Collect results after all instances complete ──────────────────
    summarize: {
      dependsOn: ['process_region'],
      run: async (ctx) => {
        // xcom.pull from a mapped task returns array of all instances' values
        const results = await ctx.xcom.pull('process_region', 'return_value')
        console.log(`[summarize] all done: ${JSON.stringify(results)}`)
        return { total: Array.isArray(results) ? results.length : 0, results }
      }
    },

  }
})
