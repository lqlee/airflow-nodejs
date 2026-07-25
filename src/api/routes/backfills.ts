import type { FastifyInstance } from 'fastify'
import { ObjectId } from 'mongodb'
import {
  pauseBackfill,
  resumeBackfill,
  cancelBackfill,
  formatBackfill,
  type BackfillDoc,
} from '../../scheduler/backfill.js'

export async function backfillsRoutes(app: FastifyInstance): Promise<void> {
  // GET /backfills — list backfills; optional ?dag_id=  ?state=  ?limit=  ?cursor=
  app.get<{
    Querystring: { dag_id?: string; state?: string; limit?: string; cursor?: string }
  }>('/backfills', async (req, reply) => {
    const db = app.mongo
    const { dag_id, state, cursor } = req.query
    const VALID_STATES = new Set(['active', 'paused', 'cancelled'])

    if (state && !VALID_STATES.has(state)) {
      return reply.status(400).send({ error: '"state" must be one of: active, paused, cancelled' })
    }

    const rawLimit = parseInt(req.query.limit ?? '20', 10)
    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 100) : 20

    const filter: Record<string, unknown> = {}
    if (dag_id) filter['dag_id'] = dag_id
    if (state) filter['state'] = state

    if (cursor) {
      if (!ObjectId.isValid(cursor)) return reply.status(400).send({ error: 'Invalid cursor' })
      const pivot = await db.collection('backfills').findOne({ _id: new ObjectId(cursor) })
      if (pivot) filter['created_at'] = { $lt: pivot.created_at }
    }

    const docs = await db.collection<BackfillDoc>('backfills')
      .find(filter)
      .sort({ created_at: -1 })
      .limit(limit)
      .toArray()

    const nextCursor = docs.length === limit
      ? (docs[docs.length - 1] as BackfillDoc & { _id: ObjectId })._id.toString()
      : null

    return reply.send({
      items: docs.map(d => formatBackfill(d as BackfillDoc & { _id: ObjectId })),
      next_cursor: nextCursor,
    })
  })

  // GET /backfills/:backfillId — single backfill with derived completed status
  app.get<{ Params: { backfillId: string } }>('/backfills/:backfillId', async (req, reply) => {
    const { backfillId } = req.params
    if (!ObjectId.isValid(backfillId)) return reply.status(400).send({ error: 'Invalid backfill id' })

    const db = app.mongo
    const doc = await db.collection<BackfillDoc>('backfills').findOne({ _id: new ObjectId(backfillId) })
    if (!doc) return reply.status(404).send({ error: `Backfill '${backfillId}' not found` })

    // Derive completed: all runs terminal
    const runIds = (doc as BackfillDoc & { run_ids: string[] }).run_ids ?? []
    let runStates: string[] = []
    if (runIds.length > 0) {
      const runs = await db.collection('dag_runs')
        .find({ _id: { $in: runIds.map(id => new ObjectId(id)) } })
        .project({ state: 1 })
        .toArray()
      runStates = runs.map(r => r.state as string)
    }

    return reply.send(formatBackfill(doc as BackfillDoc & { _id: ObjectId }, runStates))
  })

  // POST /backfills/:backfillId/pause — pause advancement of all runs in this backfill
  app.post<{ Params: { backfillId: string } }>('/backfills/:backfillId/pause', async (req, reply) => {
    const { backfillId } = req.params
    if (!ObjectId.isValid(backfillId)) return reply.status(400).send({ error: 'Invalid backfill id' })

    const ok = await pauseBackfill(app.mongo, backfillId)
    if (!ok) return reply.status(409).send({ error: 'Backfill not found or not currently active' })
    return reply.send({ backfill_id: backfillId, state: 'paused' })
  })

  // POST /backfills/:backfillId/resume — resume a paused backfill
  app.post<{ Params: { backfillId: string } }>('/backfills/:backfillId/resume', async (req, reply) => {
    const { backfillId } = req.params
    if (!ObjectId.isValid(backfillId)) return reply.status(400).send({ error: 'Invalid backfill id' })

    const ok = await resumeBackfill(app.mongo, backfillId)
    if (!ok) return reply.status(409).send({ error: 'Backfill not found or not currently paused' })
    return reply.send({ backfill_id: backfillId, state: 'active' })
  })

  // POST /backfills/:backfillId/cancel — cancel backfill and all non-terminal runs
  app.post<{ Params: { backfillId: string } }>('/backfills/:backfillId/cancel', async (req, reply) => {
    const { backfillId } = req.params
    if (!ObjectId.isValid(backfillId)) return reply.status(400).send({ error: 'Invalid backfill id' })

    const result = await cancelBackfill(app.mongo, backfillId)
    if (!result.cancelled) return reply.status(409).send({ error: 'Backfill not found or already cancelled' })
    return reply.send({ backfill_id: backfillId, state: 'cancelled', runs_cancelled: result.runsCancelled })
  })
}
