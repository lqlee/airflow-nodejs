import type { FastifyInstance } from 'fastify'
import { listDatasetEvents, listDatasetAliases } from '../../datasets/index.js'

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
        task_id: e.task_id ?? null,
        source_alias: e.source_alias ?? null,
        extra: e.extra ?? null,
        emitted_at: e.emitted_at,
      })),
    )
  })

  // GET /dataset-aliases — datasets attached to each alias at runtime, optionally for one alias
  app.get<{ Querystring: { alias?: string } }>('/dataset-aliases', async (req, reply) => {
    return reply.send(await listDatasetAliases(app.mongo, req.query.alias))
  })
}
