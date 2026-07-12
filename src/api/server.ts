import Fastify, { type FastifyInstance } from 'fastify'
import fastifyStatic from '@fastify/static'
import { fileURLToPath } from 'node:url'
import { resolve, dirname } from 'node:path'
import type { Db } from 'mongodb'
import { dagsRoutes } from './routes/dags.js'
import { dagRunsRoutes } from './routes/dag-runs.js'
import { activeWorkers, queueDepth } from '../scheduler/pool.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PUBLIC_DIR = resolve(__dirname, '../../public')

// Augment Fastify to carry the db reference
declare module 'fastify' {
  interface FastifyInstance {
    mongo: Db
  }
}

export function buildServer(db: Db): FastifyInstance {
  const app = Fastify({ logger: false })

  // Attach db to app instance
  app.decorate('mongo', db)

  // Serve web UI from public/
  app.register(fastifyStatic, { root: PUBLIC_DIR, prefix: '/' })

  // Health check
  app.get('/health', async () => ({
    status: 'ok',
    workers: { active: activeWorkers(), queued: queueDepth() },
  }))

  // Routes
  app.register(dagsRoutes)
  app.register(dagRunsRoutes)

  return app
}
