/**
 * HTTP Provider — reusable operators for HTTP tasks.
 *
 * Operators ship as factory functions that return TaskDefinition objects.
 * They lower to shell/python/container tasks — fully serializable.
 *
 * Usage in a DAG file:
 *   import { getOperator } from 'airflow-nodejs/providers'
 *   const HttpGetOperator = getOperator('http-provider', 'HttpGetOperator')
 *
 *   tasks: {
 *     ping: HttpGetOperator({ url: 'https://httpbin.org/get', timeout: 5000 })
 *   }
 */

/** @param {{ name: string, version: string, description: string, operators: Record<string, Function>, connectionTypes?: string[] }} def */
function provider(def) { return def }

export default provider({
  name: 'http-provider',
  version: '1.0.0',
  description: 'HTTP request operators — GET, POST, health-check',
  connectionTypes: ['http', 'https'],

  operators: {
    /**
     * HttpGetOperator — make an HTTP GET request via curl.
     * @param {{ url: string, timeout?: number, expectedStatus?: number, headers?: Record<string,string> }} opts
     */
    HttpGetOperator: (opts = {}) => {
      const url = opts.url ?? 'http://localhost'
      const timeout = opts.timeout ?? 10000
      const expectedStatus = opts.expectedStatus ?? 200
      const headers = Object.entries(opts.headers ?? {})
        .map(([k, v]) => `-H "${k}: ${v}"`)
        .join(' ')

      return {
        shell: {
          interpreter: 'sh',
          timeout,
          command: [
            `STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time ${Math.floor(timeout / 1000)} ${headers} "${url}")`,
            `echo "HTTP GET ${url} → $STATUS"`,
            `[ "$STATUS" = "${expectedStatus}" ] && echo "✓ status $STATUS OK" || (echo "✗ expected ${expectedStatus}, got $STATUS" && exit 1)`,
          ].join('\n'),
        },
      }
    },

    /**
     * HealthCheckOperator — poll an endpoint until it returns 200.
     * @param {{ url: string, retries?: number, retryDelay?: number }} opts
     */
    HealthCheckOperator: (opts = {}) => {
      const url = opts.url ?? 'http://localhost/health'
      const retries = opts.retries ?? 3
      const retryDelay = opts.retryDelay ?? 2000

      return {
        retries,
        retryDelay,
        shell: {
          interpreter: 'sh',
          command: [
            `STATUS=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "${url}")`,
            `echo "Health check ${url} → $STATUS"`,
            `[ "$STATUS" = "200" ] || (echo "Health check failed: $STATUS" && exit 1)`,
          ].join('\n'),
        },
      }
    },

    /**
     * HttpPostOperator — POST a JSON body to a URL.
     * @param {{ url: string, body?: string, timeout?: number }} opts
     */
    HttpPostOperator: (opts = {}) => {
      const url = opts.url ?? 'http://localhost'
      const body = opts.body ?? '{}'
      const timeout = opts.timeout ?? 10000

      return {
        shell: {
          interpreter: 'sh',
          timeout,
          command: [
            `RESPONSE=$(curl -s -w "\\n%{http_code}" --max-time ${Math.floor(timeout / 1000)} \\`,
            `  -X POST -H "Content-Type: application/json" \\`,
            `  -d '${body}' "${url}")`,
            `HTTP_CODE=$(echo "$RESPONSE" | tail -1)`,
            `BODY=$(echo "$RESPONSE" | head -n -1)`,
            `echo "POST ${url} → $HTTP_CODE"`,
            `echo "Response: $BODY"`,
            `echo "$HTTP_CODE" | grep -q "^2" || (echo "HTTP error: $HTTP_CODE" && exit 1)`,
          ].join('\n'),
        },
      }
    },
  },
})
