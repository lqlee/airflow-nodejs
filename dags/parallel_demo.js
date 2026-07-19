import { dag } from '../src/dag/types.js';
/**
 * Demonstrates parallel task execution.
 * fetch_a and fetch_b both depend only on start — they run concurrently.
 * merge waits for both before running.
 *
 *   start
 *   ├── fetch_a ─┐
 *   └── fetch_b ─┤
 *               merge
 */
export default dag({
    id: 'parallel_demo',
    schedule: null,
    tasks: {
        start: {
            run: async (ctx) => {
                console.log(`[${ctx.taskId}] starting pipeline...`);
                await ctx.xcom.push('config', { batchSize: 100 });
            }
        },
        fetch_a: {
            dependsOn: ['start'],
            run: async (ctx) => {
                const config = await ctx.xcom.pull('start', 'config');
                console.log(`[${ctx.taskId}] fetching source A (batch=${config.batchSize})...`);
                await new Promise(r => setTimeout(r, 800));
                await ctx.xcom.push('rows', 420);
                console.log(`[${ctx.taskId}] done`);
            }
        },
        fetch_b: {
            dependsOn: ['start'],
            run: async (ctx) => {
                const config = await ctx.xcom.pull('start', 'config');
                console.log(`[${ctx.taskId}] fetching source B (batch=${config.batchSize})...`);
                await new Promise(r => setTimeout(r, 600));
                await ctx.xcom.push('rows', 380);
                console.log(`[${ctx.taskId}] done`);
            }
        },
        merge: {
            dependsOn: ['fetch_a', 'fetch_b'],
            run: async (ctx) => {
                const rowsA = await ctx.xcom.pull('fetch_a', 'rows');
                const rowsB = await ctx.xcom.pull('fetch_b', 'rows');
                console.log(`[${ctx.taskId}] merging ${rowsA} + ${rowsB} = ${rowsA + rowsB} rows`);
            }
        }
    }
});
