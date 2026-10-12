import { defineConfig, mergeConfig } from 'vitest/config'
import { fileURLToPath } from 'node:url'
import base from './vitest.config.js'

// Runs the whole suite against the Spanner emulator:
//   docker run -d -p 9010:9010 -p 9020:9020 <emulator image>
//   npm run test:spanner
export default mergeConfig(base, defineConfig({
  resolve: {
    alias: [{ find: /^mongodb$/, replacement: fileURLToPath(new URL('./src/db/spanner/test-support/mongodb-shim.ts', import.meta.url)) }],
  },
  test: {
    exclude: ['**/node_modules/**', 'src/db/spanner/**'],   // those compare against real MongoDB
    env: { DB_BACKEND: 'spanner', SPANNER_EMULATOR_HOST: process.env.SPANNER_EMULATOR_HOST ?? 'localhost:9010', SPANNER_AUTO_DDL: 'true' },
    // The emulator is single-process: too many parallel files cause client timeouts + retries
    maxWorkers: 4,
    minWorkers: 1,
    testTimeout: 60_000,
    hookTimeout: 120_000,
  },
}))
