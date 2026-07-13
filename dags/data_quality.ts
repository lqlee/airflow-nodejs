import { dag } from '../src/dag/types.js'

/**
 * Data quality checks — runs every 6 hours.
 * Demonstrates: fan-out parallel checks + fan-in aggregation.
 *
 *              ┌── check_nulls    ─┐
 *   start ────►│── check_dupes    ─├──► report
 *              └── check_freshness─┘
 */
export default dag({
  id: 'data_quality',
  schedule: '0 */6 * * *',  // every 6 hours
  tasks: {
    start: {
      run: async (ctx) => {
        console.log('[start] beginning data quality checks...')
        await ctx.xcom.push('table', 'orders')
        await ctx.xcom.push('started_at', new Date().toISOString())
      },
    },

    check_nulls: {
      dependsOn: ['start'],
      run: async (ctx) => {
        const table = await ctx.xcom.pull('start', 'table') as string
        console.log(`[check_nulls] scanning ${table} for null values...`)
        await new Promise(r => setTimeout(r, 400))
        const nullCount = Math.floor(Math.random() * 5)  // simulate
        await ctx.xcom.push('result', { check: 'nulls', passed: nullCount === 0, count: nullCount })
        console.log(`[check_nulls] found ${nullCount} nulls — ${nullCount === 0 ? 'PASS ✓' : 'FAIL ✗'}`)
      },
    },

    check_dupes: {
      dependsOn: ['start'],
      run: async (ctx) => {
        const table = await ctx.xcom.pull('start', 'table') as string
        console.log(`[check_dupes] scanning ${table} for duplicates...`)
        await new Promise(r => setTimeout(r, 500))
        const dupeCount = 0  // simulate clean
        await ctx.xcom.push('result', { check: 'duplicates', passed: true, count: dupeCount })
        console.log(`[check_dupes] found ${dupeCount} dupes — PASS ✓`)
      },
    },

    check_freshness: {
      dependsOn: ['start'],
      run: async (ctx) => {
        const table = await ctx.xcom.pull('start', 'table') as string
        console.log(`[check_freshness] checking ${table} row recency...`)
        await new Promise(r => setTimeout(r, 300))
        const ageHours = 1.5  // simulate recent data
        const passed = ageHours < 24
        await ctx.xcom.push('result', { check: 'freshness', passed, ageHours })
        console.log(`[check_freshness] newest row is ${ageHours}h old — ${passed ? 'PASS ✓' : 'FAIL ✗'}`)
      },
    },

    report: {
      dependsOn: ['check_nulls', 'check_dupes', 'check_freshness'],
      run: async (ctx) => {
        const table      = await ctx.xcom.pull('start', 'table') as string
        const startedAt  = await ctx.xcom.pull('start', 'started_at') as string
        const nullResult = await ctx.xcom.pull('check_nulls', 'result')    as { check: string; passed: boolean }
        const dupeResult = await ctx.xcom.pull('check_dupes', 'result')    as { check: string; passed: boolean }
        const freshResult= await ctx.xcom.pull('check_freshness', 'result')as { check: string; passed: boolean }

        const checks = [nullResult, dupeResult, freshResult]
        const allPassed = checks.every(c => c.passed)
        const failedChecks = checks.filter(c => !c.passed).map(c => c.check)

        console.log(`[report] ── Data Quality Report: ${table} ──`)
        console.log(`[report] started: ${startedAt}`)
        for (const c of checks) {
          console.log(`[report]   ${c.check.padEnd(12)} ${c.passed ? '✓ PASS' : '✗ FAIL'}`)
        }
        console.log(`[report] overall: ${allPassed ? '✓ ALL CHECKS PASSED' : `✗ FAILED (${failedChecks.join(', ')})`}`)
      },
    },
  },
})
