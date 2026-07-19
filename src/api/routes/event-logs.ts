import type { FastifyInstance } from 'fastify'
import { ObjectId } from 'mongodb'
import { buildEventFilter, isEventType, type EventType } from '../../events/index.js'

export async function eventLogsRoutes(app: FastifyInstance): Promise<void> {
  /**
   * GET /event-logs — paginated audit log
   *
   * Query params:
   *   dag_id       — filter by dag
   *   dag_run_id   — filter by run (must be valid ObjectId if given)
   *   task_id      — filter by task
   *   event_type   — one of the known EventType values
   *   limit        — page size (1–200, default 50)
   *   cursor       — ObjectId of last seen event (for next-page)
   */
  app.get<{
    Querystring: {
      dag_id?: string
      dag_run_id?: string
      task_id?: string
      event_type?: string
      limit?: string
      cursor?: string
    }
  }>('/event-logs', async (req, reply) => {
    const db = app.mongo
    const {
      dag_id, dag_run_id, task_id, event_type,
      limit: limitStr, cursor,
    } = req.query

    // Validate event_type if provided
    if (event_type && !isEventType(event_type)) {
      return reply.status(400).send({
        error: `"event_type" must be one of: run_triggered, run_success, run_failed, dag_paused, dag_resumed, run_cancelled, task_cleared`,
      })
    }

    // Validate dag_run_id format if provided
    if (dag_run_id && !ObjectId.isValid(dag_run_id)) {
      return reply.status(400).send({ error: 'Invalid "dag_run_id"' })
    }

    const rawLimit = parseInt(limitStr ?? '50', 10)
    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 200) : 50

    const filter = buildEventFilter({
      dag_id,
      dag_run_id,
      task_id,
      event_type: event_type as EventType | undefined,
    })

    // Cursor pagination: newest-first; cursor is _id of last seen event
    if (cursor) {
      if (!ObjectId.isValid(cursor)) {
        return reply.status(400).send({ error: 'Invalid cursor' })
      }
      const pivot = await db.collection('event_logs').findOne({ _id: new ObjectId(cursor) })
      if (pivot) {
        filter['created_at'] = { $lt: pivot.created_at }
      }
    }

    const events = await db
      .collection('event_logs')
      .find(filter)
      .sort({ created_at: -1 })
      .limit(limit)
      .toArray()

    const nextCursor = events.length === limit
      ? (events[events.length - 1] as { _id: ObjectId })._id.toString()
      : null

    return reply.send({
      items: events.map(e => ({
        id: (e as { _id: ObjectId })._id.toString(),
        event_type: e.event_type,
        dag_id: e.dag_id ?? null,
        dag_run_id: e.dag_run_id ?? null,
        task_id: e.task_id ?? null,
        map_index: e.map_index ?? null,
        metadata: e.metadata ?? {},
        created_at: e.created_at,
      })),
      next_cursor: nextCursor,
    })
  })
}
