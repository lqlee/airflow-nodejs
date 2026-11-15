import type { FastifyInstance } from 'fastify'
import { listDeadlines } from '../../deadlines/index.js'

interface DeadlineQuery { dag_id?: string; dag_run_id?: string; missed?: string; limit?: string }

export async function deadlinesRoutes(app: FastifyInstance): Promise<void> {
  // GET /deadlines — deadline alerts per run (pending and missed), latest deadline first
  app.get<{ Querystring: DeadlineQuery }>('/deadlines', async (req, reply) => {
    const { dag_id, dag_run_id, missed, limit } = req.query
    if (missed !== undefined && missed !== 'true' && missed !== 'false') {
      return reply.status(400).send({ error: "missed must be 'true' or 'false'" })
    }
    const n = limit === undefined ? undefined : Number(limit)
    if (n !== undefined && (!Number.isInteger(n) || n < 1)) return reply.status(400).send({ error: 'limit must be a positive integer' })

    const rows = await listDeadlines(app.mongo, {
      dagId: dag_id, runId: dag_run_id, missed: missed === undefined ? undefined : missed === 'true', limit: n,
    })
    return reply.send(rows.map(d => ({
      id: String(d._id),
      dag_id: d.dag_id,
      dag_run_id: d.dag_run_id,
      name: d.name,
      reference: d.reference,
      deadline_time: d.deadline_time,
      missed: d.missed,
      missed_at: d.missed_at,
    })))
  })
}
