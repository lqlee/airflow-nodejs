import Fastify, { type FastifyInstance } from 'fastify'
import type { Db } from 'mongodb'
import { dagsRoutes } from './routes/dags.js'
import { dagRunsRoutes } from './routes/dag-runs.js'

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

  // Health check
  app.get('/health', async () => ({ status: 'ok' }))

  // Routes
  app.register(dagsRoutes)
  app.register(dagRunsRoutes)

  return app
}
