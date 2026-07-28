import Fastify, { type FastifyInstance } from 'fastify'
import fastifyStatic from '@fastify/static'
import fastifyRateLimit from '@fastify/rate-limit'
import fastifyMultipart from '@fastify/multipart'
import { readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import { resolve, dirname } from 'node:path'
import type { Db } from 'mongodb'
import { dagsRoutes } from './routes/dags.js'
import { dagRunsRoutes } from './routes/dag-runs.js'
import { slaRoutes } from './routes/sla.js'
import { apiKeysRoutes } from './routes/api-keys.js'
import { datasetsRoutes } from './routes/datasets.js'
import { connectionsRoutes } from './routes/connections.js'
import { variablesRoutes } from './routes/variables.js'
import { importErrorsRoutes } from './routes/import-errors.js'
import { eventLogsRoutes } from './routes/event-logs.js'
import { backfillsRoutes } from './routes/backfills.js'
import { poolsRoutes } from './routes/pools.js'
import { hitlRoutes } from './routes/hitl.js'
import { providersRoutes } from './routes/providers.js'
import { pluginsRoutes } from './routes/plugins.js'
import { dagWarningsRoutes } from './routes/dag-warnings.js'
import { configRoutes } from './routes/config.js'
import { imagesRoutes } from './routes/images.js'
import { activeWorkers, queueDepth } from '../scheduler/pool.js'
import { authHook, AUTH_ENABLED, setDb } from '../auth/index.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const PUBLIC_DIR = resolve(__dirname, '../../public')

declare module 'fastify' {
  interface FastifyInstance {
    mongo: Db
  }
}

export interface ServerOptions {
  /** Global API rate limit — requests per minute per IP. Default: 120. */
  rateLimitMax?: number
  /** Stricter limit for unauthenticated endpoints (/health). Default: 10. */
  rateLimitAuthMax?: number
}

export function buildServer(db: Db, opts: ServerOptions = {}): FastifyInstance {
  const rateLimitMax = opts.rateLimitMax ?? parseInt(process.env.RATE_LIMIT_MAX ?? '120', 10)
  const rateLimitAuthMax =
    opts.rateLimitAuthMax ?? parseInt(process.env.RATE_LIMIT_AUTH_MAX ?? '10', 10)

  const app = Fastify({ logger: false })

  app.decorate('mongo', db)

  // Wire DB into auth so DB-backed keys are validated
  setDb(db)

  // Multipart support for file uploads (docker image .tar files)
  // Limit: 2 GB per file (docker images can be large)
  app.register(fastifyMultipart, {
    limits: { fileSize: 2 * 1024 * 1024 * 1024 },
  })

  // Global rate limit: 120 req/min per IP by default (env: RATE_LIMIT_MAX).
  // Set RATE_LIMIT_MAX=0 to disable entirely (recommended for local dev).
  // Routes must be registered AFTER this plugin initialises (inside after() cb).
  if (rateLimitMax > 0) {
    app.register(fastifyRateLimit, {
      global: true,
      max: rateLimitMax,
      timeWindow: '1 minute',
      keyGenerator: (req) => req.ip,
      errorResponseBuilder: (_req, context) => ({
        statusCode: 429,
        error: 'Too Many Requests',
        message: `Rate limit exceeded. Retry after ${context.after}.`,
        retryAfter: context.after,
      }),
    })
  }

  // Auth hook — runs before every API request
  app.addHook('preHandler', authHook)

  // Serve static assets; disable automatic Cache-Control so we control it via onSend.
  app.register(fastifyStatic, { root: PUBLIC_DIR, prefix: '/', cacheControl: false })

  // Set cache headers per resource type:
  // - index.html: no-store so browsers always fetch fresh HTML after image rebuild
  // - everything else: 1 hour (assets are content-addressed or versioned by the browser)
  app.addHook('onSend', async (req, reply, payload) => {
    if (req.url === '/' || req.url === '/index.html') {
      reply.header('Cache-Control', 'no-store, no-cache, must-revalidate')
    } else if (!req.url.startsWith('/api/') && !req.url.startsWith('/dags')) {
      reply.header('Cache-Control', 'public, max-age=3600')
    }
    return payload
  })

  // All routes are registered inside after() so the rate-limit plugin
  // decorators are present when per-route config is evaluated.
  app.after(() => {
    // Health check — stricter per-route limit (unauthenticated endpoint)
    app.get(
      '/health',
      {
        config: {
          rateLimit: { max: rateLimitAuthMax, timeWindow: '1 minute' },
        },
      },
      async () => ({
        status: 'ok',
        auth: AUTH_ENABLED,
        workers: { active: activeWorkers(), queued: queueDepth() },
      }),
    )

    app.register(dagsRoutes)
    app.register(dagRunsRoutes)
    app.register(slaRoutes)
    app.register(apiKeysRoutes)
    app.register(datasetsRoutes)
    app.register(connectionsRoutes)
    app.register(variablesRoutes)
    app.register(importErrorsRoutes)
    app.register(eventLogsRoutes)
    app.register(backfillsRoutes)
    app.register(poolsRoutes)
    app.register(hitlRoutes)
    app.register(providersRoutes)
    app.register(pluginsRoutes)
    app.register(dagWarningsRoutes)
    app.register(configRoutes)
    app.register(imagesRoutes)
  })

  return app
}
