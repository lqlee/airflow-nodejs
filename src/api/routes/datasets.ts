import type { FastifyInstance } from 'fastify'
import { listDatasetEvents } from '../../datasets/index.js'

export async function datasetsRoutes(app: FastifyInstance): Promise<void> {
  // GET /datasets — list recent dataset events, optionally filtered by URI
  app.get<{ Querystring: { uri?: string; limit?: string } }>('/datasets', async (req, reply) => {
    const rawLimit = parseInt(req.query.limit ?? '50', 10)
    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 500) : 50

    const events = await listDatasetEvents(app.mongo, {
      uri: req.query.uri,
      limit,
    })

    return reply.send(
      events.map(e => ({
        uri: e.uri,
        produced_by: e.produced_by,
        run_id: e.run_id,
        emitted_at: e.emitted_at,
      })),
    )
  })
}
