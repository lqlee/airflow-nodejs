import { dag } from '../src/dag/types.js'

export default dag({
  id: 'hello_world',
  schedule: '* * * * *',  // every minute
  tasks: {
    extract: {
      run: async (ctx) => {
        console.log(`[${ctx.taskId}] extracting data...`)
        await new Promise(r => setTimeout(r, 500))
        const result = { rows: 42, source: 'warehouse' }
        // Push result to XCom so downstream tasks can read it
        await ctx.xcom.push('result', result)
        console.log(`[${ctx.taskId}] pushed xcom:`, result)
      }
    },
    transform: {
      dependsOn: ['extract'],
      run: async (ctx) => {
        // Pull the result from extract
        const extracted = await ctx.xcom.pull('extract', 'result') as { rows: number; source: string }
        console.log(`[${ctx.taskId}] pulled from extract:`, extracted)
        const transformed = { rows: extracted.rows * 2, source: extracted.source, processed: true }
        await ctx.xcom.push('result', transformed)
        console.log(`[${ctx.taskId}] pushed xcom:`, transformed)
        await new Promise(r => setTimeout(r, 300))
      }
    },
    load: {
      dependsOn: ['transform'],
      run: async (ctx) => {
        const data = await ctx.xcom.pull('transform', 'result') as { rows: number }
        console.log(`[${ctx.taskId}] loading ${data.rows} rows...`)
        await new Promise(r => setTimeout(r, 200))
        console.log(`[${ctx.taskId}] done ✓`)
      }
    }
  }
})
