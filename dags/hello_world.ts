import { dag } from '../src/dag/types.js'

export default dag({
  id: 'hello_world',
  schedule: null,  // manual trigger only
  tasks: {
    extract: {
      run: async (ctx) => {
        console.log(`[${ctx.taskId}] extracting data...`)
        await new Promise(r => setTimeout(r, 500))
        return { rows: 42 }
      }
    },
    transform: {
      dependsOn: ['extract'],
      run: async (ctx) => {
        console.log(`[${ctx.taskId}] transforming data...`)
        await new Promise(r => setTimeout(r, 300))
      }
    },
    load: {
      dependsOn: ['transform'],
      run: async (ctx) => {
        console.log(`[${ctx.taskId}] loading data...`)
        await new Promise(r => setTimeout(r, 200))
        console.log(`[${ctx.taskId}] done ✓`)
      }
    }
  }
})
