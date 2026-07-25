/**
 * Dag Warnings API — soft validation issues on successfully-loaded dags.
 *
 * Unlike import errors (hard failures that prevent loading), warnings are
 * advisory: the dag loads and runs, but something may not behave as expected.
 * Warnings are in-memory, reset on each loadDags() call.
 */

import type { FastifyInstance } from 'fastify'
import { getDagWarnings } from '../../dag/import-errors.js'
import { getDag } from '../../dag/registry.js'

export async function dagWarningsRoutes(app: FastifyInstance): Promise<void> {
  // GET /dag-warnings — all warnings across all loaded dags
  // Optional ?warning_type= filter
  app.get<{ Querystring: { warning_type?: string } }>(
    '/dag-warnings',
    async (req, reply) => {
      let warnings = getDagWarnings()
      if (req.query.warning_type) {
        warnings = warnings.filter(w => w.warning_type === req.query.warning_type)
      }
      return reply.send({
        warnings: warnings.map(w => ({
          dag_id: w.dag_id,
          warning_type: w.warning_type,
          message: w.message,
          task_ids: w.task_ids,
          detected_at: w.detected_at,
        })),
        total_entries: warnings.length,
      })
    },
  )

  // GET /dag-warnings/:dagId — warnings for a specific dag
  app.get<{ Params: { dagId: string } }>(
    '/dag-warnings/:dagId',
    async (req, reply) => {
      const { dagId } = req.params
      if (!getDag(dagId)) {
        return reply.status(404).send({ error: `Dag '${dagId}' not found` })
      }
      const warnings = getDagWarnings(dagId)
      return reply.send({
        dag_id: dagId,
        warnings: warnings.map(w => ({
          warning_type: w.warning_type,
          message: w.message,
          task_ids: w.task_ids,
          detected_at: w.detected_at,
        })),
        total_entries: warnings.length,
      })
    },
  )
}
