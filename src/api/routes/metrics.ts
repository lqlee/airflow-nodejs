import type { FastifyInstance } from 'fastify'
import { renderMetrics } from '../../metrics/index.js'

export const METRICS_CONTENT_TYPE = 'text/plain; version=0.0.4; charset=utf-8'

export async function metricsRoutes(app: FastifyInstance): Promise<void> {
  // GET /metrics — Prometheus text exposition. Goes through normal auth (viewer role when
  // auth is enabled): scrape with `authorization: Bearer <api key>`.
  app.get('/metrics', async (_req, reply) => {
    const body = await renderMetrics(app.mongo)
    return reply.header('Content-Type', METRICS_CONTENT_TYPE).header('Cache-Control', 'no-store').send(body)
  })
}
