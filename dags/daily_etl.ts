import { dag } from '../src/dag/types.js'

/**
 * Daily ETL pipeline — runs at midnight.
 * Demonstrates: sequential dependency chain + XCom data passing + retries.
 *
 *   extract_users → transform → load → notify
 */
export default dag({
  id: 'daily_etl',
  schedule: '0 0 * * *',  // midnight every day
  tasks: {
    extract_users: {
      retries: 3,
      retryDelay: 10_000,
      run: async (ctx) => {
        console.log('[extract_users] querying user database...')
        // Simulate DB query
        await new Promise(r => setTimeout(r, 300))
        const users = [
          { id: 1, name: 'Alice', active: true },
          { id: 2, name: 'Bob',   active: false },
          { id: 3, name: 'Carol', active: true },
        ]
        await ctx.xcom.push('users', users)
        await ctx.xcom.push('count', users.length)
        console.log(`[extract_users] extracted ${users.length} users`)
      },
    },

    transform: {
      dependsOn: ['extract_users'],
      run: async (ctx) => {
        const users = await ctx.xcom.pull('extract_users', 'users') as Array<{ id: number; name: string; active: boolean }>
        console.log('[transform] filtering active users...')
        const active = users.filter(u => u.active)
        const enriched = active.map(u => ({ ...u, processed_at: new Date().toISOString() }))
        await ctx.xcom.push('enriched', enriched)
        console.log(`[transform] ${enriched.length}/${users.length} users active`)
      },
    },

    load: {
      dependsOn: ['transform'],
      retries: 2,
      run: async (ctx) => {
        const enriched = await ctx.xcom.pull('transform', 'enriched') as unknown[]
        console.log(`[load] writing ${enriched.length} records to data warehouse...`)
        await new Promise(r => setTimeout(r, 200))
        await ctx.xcom.push('rows_written', enriched.length)
        console.log('[load] done ✓')
      },
    },

    notify: {
      dependsOn: ['load'],
      run: async (ctx) => {
        const total   = await ctx.xcom.pull('extract_users', 'count') as number
        const written = await ctx.xcom.pull('load', 'rows_written') as number
        console.log(`[notify] ETL complete — ${written}/${total} records loaded`)
        // In production: send Slack/email/PagerDuty alert here
      },
    },
  },
})
