import type { FastifyInstance } from 'fastify'
import { ObjectId } from 'mongodb'
import { getTaskLogs } from '../../logs/index.js'
import { cancelRun } from '../../scheduler/index.js'

export async function dagRunsRoutes(app: FastifyInstance): Promise<void> {
  // GET /dag-runs/:runId — get a single run + task summary
  app.get<{ Params: { runId: string } }>('/dag-runs/:runId', async (req, reply) => {
    const { runId } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

    const db = app.mongo
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    if (!run) return reply.status(404).send({ error: `Run '${runId}' not found` })

    const tasks = await db
      .collection('task_instances')
      .find({ dag_run_id: runId })
      .project({ task_id: 1, group_id: 1, map_index: 1, map_value: 1, state: 1,
                 started_at: 1, ended_at: 1, error: 1, depends_on: 1,
                 is_sensor: 1, poke_count: 1, next_poke_at: 1, first_poked_at: 1 })
      .toArray()

    return reply.send({
      run_id: run._id.toString(),
      dag_id: run.dag_id,
      dag_version: run.dag_version ?? null,
      logical_date: run.logical_date ?? null,
      conf: run.conf ?? {},
      tags: run.tags ?? [],
      note: run.note ?? null,
      state: run.state,
      created_at: run.created_at,
      ended_at: run.ended_at ?? null,
      tasks: tasks.map(t => ({
        task_id: t.task_id,
        group_id: t.group_id ?? null,
        map_index: t.map_index ?? null,
        map_value: t.map_value ?? null,
        state: t.state,
        depends_on: t.depends_on,
        started_at: t.started_at,
        ended_at: t.ended_at,
        error: t.error ?? null,
        is_sensor: t.is_sensor ?? false,
        poke_count: t.poke_count ?? 0,
        next_poke_at: t.next_poke_at ?? null,
        first_poked_at: t.first_poked_at ?? null,
      })),
    })
  })

  // POST /dag-runs/:runId/cancel — cancel a queued or running run
  app.post<{ Params: { runId: string } }>('/dag-runs/:runId/cancel', async (req, reply) => {
    const { runId } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

    const cancelled = await cancelRun(app.mongo, runId)
    if (!cancelled) {
      return reply.status(409).send({ error: 'Run is already in a terminal state (success, failed, or cancelled)' })
    }
    return reply.send({ run_id: runId, state: 'cancelled' })
  })

  // GET /dag-runs/:runId/xcoms — all XCom values pushed during this run
  app.get<{ Params: { runId: string } }>('/dag-runs/:runId/xcoms', async (req, reply) => {
    const { runId } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

    const xcoms = await app.mongo
      .collection('xcoms')
      .find({ dag_run_id: runId })
      .sort({ pushed_at: 1 })
      .toArray()

    return reply.send(xcoms.map(x => ({
      task_id: x.task_id,
      key: x.key,
      value: x.value,
      pushed_at: x.pushed_at,
    })))
  })

  // GET /dag-runs/:runId/tasks/:taskId/logs — task log lines
  app.get<{ Params: { runId: string; taskId: string } }>(
    '/dag-runs/:runId/tasks/:taskId/logs',
    async (req, reply) => {
      const { runId, taskId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })
      const logs = await getTaskLogs(app.mongo, runId, taskId)
      return reply.send(logs.map(l => ({
        ts: l.ts,
        stream: l.stream,
        line: l.line,
      })))
    }
  )

  // GET /dags/:dagId/runs — list recent runs for a dag
  // Query params: limit, cursor, tag (exact single-tag filter)
  app.get<{
    Params: { dagId: string }
    Querystring: { limit?: string; cursor?: string; tag?: string }
  }>('/dags/:dagId/runs', async (req, reply) => {
    const db = app.mongo
    const rawLimit = parseInt(req.query.limit ?? '20', 10)
    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 200) : 20
    const cursor = req.query.cursor
    const tag = req.query.tag

    const filter: Record<string, unknown> = { dag_id: req.params.dagId }

    // Tag filter: ANDs with the existing dag_id filter — does not overwrite
    if (tag) {
      filter['tags'] = tag  // MongoDB matches docs where tags array contains this value
    }

    if (cursor) {
      if (!ObjectId.isValid(cursor))
        return reply.status(400).send({ error: 'Invalid cursor' })
      const pivot = await db.collection('dag_runs').findOne({ _id: new ObjectId(cursor) })
      if (pivot) {
        filter['created_at'] = { $lt: pivot.created_at }
      }
    }

    const runs = await db
      .collection('dag_runs')
      .find(filter)
      .sort({ created_at: -1 })
      .limit(limit)
      .toArray()

    const nextCursor = runs.length === limit ? runs[runs.length - 1]._id.toString() : null

    return reply.send({
      items: runs.map(r => ({
        run_id: r._id.toString(),
        dag_id: r.dag_id,
        dag_version: r.dag_version ?? null,
        logical_date: r.logical_date ?? null,
        conf: r.conf ?? {},
        tags: r.tags ?? [],
        note: r.note ?? null,
        state: r.state,
        created_at: r.created_at,
      })),
      next_cursor: nextCursor,
    })
  })

  // POST /dag-runs/:runId/note — add or update a free-text note on a run
  // Allowed on any state (terminal runs can be annotated post-hoc).
  app.post<{ Params: { runId: string }; Body: { note: string } }>(
    '/dag-runs/:runId/note',
    async (req, reply) => {
      const { runId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

      const { note } = req.body ?? {}
      if (typeof note !== 'string') {
        return reply.status(400).send({ error: '"note" must be a string' })
      }

      const result = await app.mongo.collection('dag_runs').findOneAndUpdate(
        { _id: new ObjectId(runId) },
        { $set: { note } },
        { returnDocument: 'after' },
      )
      if (!result) return reply.status(404).send({ error: `Run '${runId}' not found` })

      return reply.send({ run_id: runId, note: result.note ?? null })
    },
  )
}
