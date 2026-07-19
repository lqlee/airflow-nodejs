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
      .project({ task_id: 1, state: 1, started_at: 1, ended_at: 1, error: 1, depends_on: 1 })
      .toArray()

    return reply.send({
      run_id: run._id.toString(),
      dag_id: run.dag_id,
      state: run.state,
      created_at: run.created_at,
      ended_at: run.ended_at ?? null,
      tasks: tasks.map(t => ({
        task_id: t.task_id,
        state: t.state,
        depends_on: t.depends_on,
        started_at: t.started_at,
        ended_at: t.ended_at,
        error: t.error ?? null,
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
  app.get<{
    Params: { dagId: string }
    Querystring: { limit?: string; cursor?: string }
  }>('/dags/:dagId/runs', async (req, reply) => {
    const db = app.mongo
    const rawLimit = parseInt(req.query.limit ?? '20', 10)
    const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 200) : 20
    const cursor = req.query.cursor // ObjectId hex string of the last seen run

    const filter: Record<string, unknown> = { dag_id: req.params.dagId }
    if (cursor) {
      if (!ObjectId.isValid(cursor))
        return reply.status(400).send({ error: 'Invalid cursor' })
      // Runs are sorted created_at desc; cursor points to the last returned run's _id.
      // We want runs whose created_at is strictly before that run's created_at.
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
        state: r.state,
        created_at: r.created_at,
      })),
      next_cursor: nextCursor,
    })
  })
}
