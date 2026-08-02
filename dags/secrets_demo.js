import { dag } from 'airflow-nodejs/dag/types';

/**
 * Secrets Backend Demo — read connections and variables from a secrets backend.
 *
 * Configure the backend via environment variables:
 *
 *   # File backend:
 *   SECRETS_BACKEND=file
 *   SECRETS_FILE_PATH=/path/to/secrets.json
 *
 *   # Env backend:
 *   SECRETS_BACKEND=env
 *   AIRFLOW_VAR_API_KEY=my-secret-key
 *   AIRFLOW_CONN_MY_API={"conn_type":"http","host":"api.example.com","password":"token"}
 *
 * secrets.json format:
 * {
 *   "variables": { "api_key": "my-secret" },
 *   "connections": {
 *     "my_api": { "conn_type": "http", "host": "api.example.com", "password": "token" }
 *   }
 * }
 *
 * Trigger:
 *   POST /dags/secrets_demo/trigger   body: {}
 */
export default dag({
  id: 'secrets_demo',
  schedule: null,

  tasks: {
    read_secrets: {
      run: async (ctx) => {
        // Read variable — checks DB first, then secrets backend fallback
        const apiKey = await ctx.variables.get('api_key')
        console.log(`[secrets_demo] api_key: ${apiKey ? '***' + apiKey.slice(-4) : '(not found)'}`)

        // Read connection — checks DB first, then secrets backend fallback
        const conn = await ctx.connections.get('my_api')
        if (conn) {
          console.log(`[secrets_demo] my_api: conn_type=${conn.conn_type}, host=${conn.host}`)
        } else {
          console.log('[secrets_demo] my_api: (not found in DB or backend)')
        }

        return {
          api_key_found: apiKey !== null,
          connection_found: conn !== null,
        }
      }
    }
  }
})
