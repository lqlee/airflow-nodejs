import Fastify, { type FastifyInstance } from 'fastify'
import fastifyStatic from '@fastify/static'
import { fileURLToPath } from 'node:url'
import { resolve, dirname } from 'node:path'
import type { Db } from 'mongodb'
import { dagsRoutes } from './routes/dags.js'
import { dagRunsRoutes } from './routes/dag-runs.js'
import { slaRoutes } from './routes/sla.js'
import { apiKeysRoutes } from './routes/api-keys.js'
import { activeWorkers, queueDepth } from '../scheduler/pool.js'
import { authHook, AUTH_ENABLED, setDb } from '../auth/index.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PUBLIC_DIR = resolve(__dirname, '../../public')

declare module 'fastify' {
  interface FastifyInstance {
    mongo: Db
  }
}

export function buildServer(db: Db): FastifyInstance {
  const app = Fastify({ logger: false })

  app.decorate('mongo', db)

  // Wire DB into auth so DB-backed keys are validated
  setDb(db)

  // Auth hook — runs before every request
  app.addHook('preHandler', authHook)

  // Serve web UI from public/
  app.register(fastifyStatic, { root: PUBLIC_DIR, prefix: '/' })

  // Health check (public)
  app.get('/health', async () => ({
    status: 'ok',
    auth: AUTH_ENABLED,
    workers: { active: activeWorkers(), queued: queueDepth() },
  }))

  app.register(dagsRoutes)
  app.register(dagRunsRoutes)
  app.register(slaRoutes)
  app.register(apiKeysRoutes)

  return app
}
