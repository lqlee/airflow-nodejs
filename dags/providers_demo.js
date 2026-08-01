/**
 * Providers Demo — uses operator factories from dags/providers/.
 *
 * Demonstrates the providers ecosystem:
 *   - Operators are factory functions → produce TaskDefinition objects
 *   - Reusable across DAGs (DRY)
 *   - Appear in GET /providers as 'local' providers
 *
 * Trigger:
 *   POST /dags/providers_demo/trigger   body: {}
 *
 * Verify providers loaded:
 *   GET /providers → check local_providers array
 */

import { getOperator } from 'airflow-nodejs/providers'

// Resolve operators from the loaded provider registry
const HttpGetOperator    = getOperator('http-provider',    'HttpGetOperator')
const LogNotifyOperator  = getOperator('notify-provider',  'LogNotifyOperator')
const SlackNotifyOperator = getOperator('notify-provider', 'SlackNotifyOperator')

// Safe fallback if provider not loaded (shouldn't happen — providers load before DAGs)
const noop = () => ({ shell: { interpreter: 'sh', command: 'echo "provider not loaded"' } })

export default {
  id: 'providers_demo',
  schedule: null,

  tasks: {
    // ── Use HttpGetOperator from http-provider ──────────────────────────────
    // Health-checks the airflow-nodejs server itself (always reachable)
    // Uses wget (pre-installed in Debian slim base) to make the HTTP request
    health_check: {
      run: async () => {
        // Use Node.js built-in http module — no curl/wget needed
        const http = await import('node:http')
        return new Promise((resolve, reject) => {
          const req = http.get('http://localhost:3000/health', (res) => {
            let body = ''
            res.on('data', c => body += c)
            res.on('end', () => {
              if (res.statusCode === 200) {
                console.log(`HTTP GET /health → 200 OK: ${body.trim()}`)
                resolve({ status: 200, body })
              } else {
                reject(new Error(`Expected 200, got ${res.statusCode}`))
              }
            })
          })
          req.on('error', reject)
          req.setTimeout(5000, () => { req.destroy(); reject(new Error('timeout')) })
        })
      }
    },

    // ── Use LogNotifyOperator from notify-provider ──────────────────────────
    log_start: (LogNotifyOperator ?? noop)({
      message: 'Pipeline started — providers_demo',
      level: 'info',
    }),

    // ── Summary after both complete ──────────────────────────────────────────
    summarize: {
      dependsOn: ['health_check', 'log_start'],
      run: async (ctx) => {
        return {
          dag: ctx.dagId,
          run: ctx.runId,
          status: 'Providers demo complete',
        }
      }
    },

    // ── Notify on completion ────────────────────────────────────────────────
    notify: {
      dependsOn: ['summarize'],
      ...(SlackNotifyOperator ?? noop)({
        message: 'providers_demo pipeline completed successfully',
        channel: '#airflow-alerts',
      }),
    },

  }
}
