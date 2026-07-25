/**
 * Human-in-the-Loop (HITL) routes.
 *
 * Tasks with requiresApproval:true park at 'queued' (claim.ts excludes them)
 * until a human approves or rejects via this API.
 *
 * Approve → sets hitl_state='approved'; claim.ts picks it up on next tick/advance.
 * Reject  → marks task 'failed' directly; run will reach 'failed'.
 *
 * POST re-advances the run synchronously so approved tasks run without waiting
 * for the next 5-second scheduler tick.
 */

import type { FastifyInstance } from 'fastify'
import { ObjectId } from 'mongodb'
import { advanceRun } from '../../scheduler/index.js'

export async function hitlRoutes(app: FastifyInstance): Promise<void> {
  // GET /hitl — list all pending HITL task instances (across all runs)
  // Optional query: ?dag_id=  ?dag_run_id=
  app.get<{ Querystring: { dag_id?: string; dag_run_id?: string } }>(
    '/hitl',
    async (req, reply) => {
      const db = app.mongo
      const { dag_id, dag_run_id } = req.query

      if (dag_run_id && !ObjectId.isValid(dag_run_id)) {
        return reply.status(400).send({ error: 'Invalid "dag_run_id"' })
      }

      const filter: Record<string, unknown> = { is_hitl: true, hitl_state: 'pending' }
      if (dag_id) filter['dag_id'] = dag_id
      if (dag_run_id) filter['dag_run_id'] = dag_run_id

      const tasks = await db
        .collection('task_instances')
        .find(filter)
        .sort({ created_at: 1 })
        .toArray()

      return reply.send(tasks.map(t => ({
        run_id: t.dag_run_id,
        dag_id: t.dag_id,
        task_id: t.task_id,
        map_index: t.map_index ?? null,
        hitl_state: t.hitl_state,
        hitl_prompt: t.hitl_prompt ?? null,
        created_at: t.created_at,
      })))
    },
  )

  // GET /hitl/:runId/:taskId — single task's HITL detail
  app.get<{ Params: { runId: string; taskId: string }; Querystring: { map_index?: string } }>(
    '/hitl/:runId/:taskId',
    async (req, reply) => {
      const { runId, taskId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

      const filter: Record<string, unknown> = {
        dag_run_id: runId, task_id: taskId, is_hitl: true,
      }
      if (req.query.map_index !== undefined) {
        const mi = parseInt(req.query.map_index, 10)
        if (!Number.isFinite(mi) || mi < 0) {
          return reply.status(400).send({ error: '"map_index" must be a non-negative integer' })
        }
        filter['map_index'] = mi
      } else {
        filter['map_index'] = null
      }

      const t = await app.mongo.collection('task_instances').findOne(filter)
      if (!t) return reply.status(404).send({ error: `HITL task '${taskId}' not found in run '${runId}'` })

      return reply.send({
        run_id: t.dag_run_id,
        dag_id: t.dag_id,
        task_id: t.task_id,
        map_index: t.map_index ?? null,
        state: t.state,
        hitl_state: t.hitl_state,
        hitl_prompt: t.hitl_prompt ?? null,
        hitl_note: t.hitl_note ?? null,
        hitl_responded_at: t.hitl_responded_at ?? null,
        created_at: t.created_at,
      })
    },
  )

  // POST /hitl/:runId/:taskId — approve or reject a pending HITL task
  // Body: { decision: 'approve'|'reject', note?: string }
  app.post<{
    Params: { runId: string; taskId: string }
    Querystring: { map_index?: string }
    Body: { decision?: string; note?: string }
  }>(
    '/hitl/:runId/:taskId',
    async (req, reply) => {
      const { runId, taskId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

      const { decision, note } = req.body ?? {}
      if (decision !== 'approve' && decision !== 'reject') {
        return reply.status(400).send({ error: '"decision" must be "approve" or "reject"' })
      }

      const db = app.mongo
      const filter: Record<string, unknown> = {
        dag_run_id: runId,
        task_id: taskId,
        is_hitl: true,
        hitl_state: 'pending',    // only pending tasks can be responded to
      }
      if (req.query.map_index !== undefined) {
        const mi = parseInt(req.query.map_index, 10)
        if (!Number.isFinite(mi) || mi < 0) {
          return reply.status(400).send({ error: '"map_index" must be a non-negative integer' })
        }
        filter['map_index'] = mi
      } else {
        filter['map_index'] = null
      }

      const respondedAt = new Date()

      if (decision === 'approve') {
        // Set hitl_state=approved; claim.ts will pick it up on next advance
        const result = await db.collection('task_instances').findOneAndUpdate(
          filter,
          {
            $set: {
              hitl_state: 'approved',
              hitl_note: note ?? null,
              hitl_responded_at: respondedAt,
            },
          },
          { returnDocument: 'after' },
        )
        if (!result) {
          return reply.status(404).send({
            error: `HITL task '${taskId}' not found, not a HITL task, or already responded`,
          })
        }

        // Re-advance immediately so the approved task runs without waiting for the next tick
        await advanceRun(db, runId)

        return reply.send({
          run_id: runId,
          task_id: taskId,
          decision: 'approve',
          hitl_state: 'approved',
          note: note ?? null,
        })
      } else {
        // Reject: mark task failed directly (no retries — human decision is terminal)
        const result = await db.collection('task_instances').findOneAndUpdate(
          filter,
          {
            $set: {
              hitl_state: 'rejected',
              hitl_note: note ?? null,
              hitl_responded_at: respondedAt,
              state: 'failed',
              ended_at: respondedAt,
              error: note ? `Rejected by human: ${note}` : 'Rejected by human',
            },
          },
          { returnDocument: 'after' },
        )
        if (!result) {
          return reply.status(404).send({
            error: `HITL task '${taskId}' not found, not a HITL task, or already responded`,
          })
        }

        // Re-advance to finalize the run (the rejected task is now 'failed',
        // advanceRun will see allDone and transition the run to 'failed')
        await advanceRun(db, runId)

        return reply.send({
          run_id: runId,
          task_id: taskId,
          decision: 'reject',
          hitl_state: 'rejected',
          note: note ?? null,
        })
      }
    },
  )
}
