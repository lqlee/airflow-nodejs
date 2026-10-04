import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    include: ['src/**/*.test.ts'],
    environment: 'node',
    // API tests call buildServer+app.ready() in beforeAll which loads all DAGs.
    // Under parallel execution this can take >10s (MongoDB + DAG import contention).
    // Raise hookTimeout to match reality; tests themselves remain fast.
    hookTimeout: 30_000,
    // Many scheduler tests fork a worker process per task; under a loaded full run a
    // multi-task test can pass 5s (default) while being fast in isolation (~1.7s).
    testTimeout: 20_000,
  },
})
