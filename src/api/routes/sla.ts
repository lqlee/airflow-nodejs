import type { FastifyInstance } from 'fastify'
import { listAlerts, ackAlert } from '../../sla/index.js'

export async function slaRoutes(app: FastifyInstance): Promise<void> {
  // GET /sla-alerts — list all SLA alerts (unacked first, then acked)
  app.get('/sla-alerts', async (req, reply) => {
    const unacked = (req.query as any).unacked === 'true'
    const alerts = await listAlerts(app.mongo, { unackedOnly: unacked })
    return reply.send(
      alerts.map(a => ({
        id: (a as any)._id?.toString(),
        dag_id: a.dag_id,
        dag_run_id: a.dag_run_id,
        sla_ms: a.sla_ms,
        elapsed_ms: a.elapsed_ms,
        fired_at: a.fired_at,
        acked: a.acked,
        acked_at: a.acked_at,
      }))
    )
  })

  // POST /sla-alerts/:alertId/ack — acknowledge an alert
  app.post<{ Params: { alertId: string } }>('/sla-alerts/:alertId/ack', async (req, reply) => {
    const ok = await ackAlert(app.mongo, req.params.alertId)
    if (!ok) return reply.status(404).send({ error: 'Alert not found' })
    return reply.send({ id: req.params.alertId, acked: true })
  })
}
