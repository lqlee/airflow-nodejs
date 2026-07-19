import type { FastifyInstance } from 'fastify'
import { ObjectId } from 'mongodb'
import { listDags, getDag } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { pauseDag, resumeDag, isDagPaused, getPausedDagIds } from '../../dag/pause.js'
import { getDagStats } from '../../stats/index.js'
import { backfill, BACKFILL_MAX_RUNS } from '../../scheduler/backfill.js'

export async function dagsRoutes(app: FastifyInstance): Promise<void> {
  // GET /dags — list all registered dags with pause state
  app.get('/dags', async (_req, reply) => {
    const db = app.mongo
    const paused = await getPausedDagIds(db)
    const dags = listDags().map(d => ({
      id: d.id,
      schedule: d.schedule,
      tasks: Object.keys(d.tasks),
      is_paused: paused.has(d.id),
    }))
    return reply.send(dags)
  })

  // GET /dags/:dagId — get a single dag
  app.get<{ Params: { dagId: string } }>('/dags/:dagId', async (req, reply) => {
    const dag = getDag(req.params.dagId)
    if (!dag) return reply.status(404).send({ error: `Dag '${req.params.dagId}' not found` })

    const isPaused = await isDagPaused(app.mongo, dag.id)
    return reply.send({
      id: dag.id,
      schedule: dag.schedule,
      is_paused: isPaused,
      tasks: Object.entries(dag.tasks).map(([taskId, t]) => ({
        task_id: taskId,
        depends_on: t.dependsOn ?? [],
      })),
    })
  })

  // POST /dags/:dagId/trigger — manually trigger a dag run (works even when paused)
  app.post<{ Params: { dagId: string } }>('/dags/:dagId/trigger', async (req, reply) => {
    const dag = getDag(req.params.dagId)
    if (!dag) return reply.status(404).send({ error: `Dag '${req.params.dagId}' not found` })

    const db = app.mongo
    const runId = await createRun(db, dag)

    // Execute immediately — don't wait for the next scheduler tick
    await advanceRun(db, runId)

    // Return actual state from DB (will be success/failed if tasks are fast)
    const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    return reply.status(201).send({
      run_id: runId,
      dag_id: dag.id,
      state: run?.state ?? 'queued',
    })
  })

  // POST /dags/:dagId/pause — pause scheduled execution of a dag
  app.post<{ Params: { dagId: string } }>('/dags/:dagId/pause', async (req, reply) => {
    const dag = getDag(req.params.dagId)
    if (!dag) return reply.status(404).send({ error: `Dag '${req.params.dagId}' not found` })

    await pauseDag(app.mongo, dag.id)
    return reply.send({ dag_id: dag.id, is_paused: true })
  })

  // POST /dags/:dagId/resume — resume scheduled execution of a dag
  app.post<{ Params: { dagId: string } }>('/dags/:dagId/resume', async (req, reply) => {
    const dag = getDag(req.params.dagId)
    if (!dag) return reply.status(404).send({ error: `Dag '${req.params.dagId}' not found` })

    await resumeDag(app.mongo, dag.id)
    return reply.send({ dag_id: dag.id, is_paused: false })
  })

  // GET /dags/:dagId/stats?limit=20 — run statistics for a dag
  app.get<{ Params: { dagId: string }; Querystring: { limit?: string } }>(
    '/dags/:dagId/stats',
    async (req, reply) => {
      const dag = getDag(req.params.dagId)
      if (!dag) return reply.status(404).send({ error: `Dag '${req.params.dagId}' not found` })

      const rawLimit = parseInt(req.query.limit ?? '20', 10)
      const limit = Number.isFinite(rawLimit) && rawLimit > 0 ? Math.min(rawLimit, 500) : 20

      const stats = await getDagStats(app.mongo, dag.id, limit)
      return reply.send(stats)
    },
  )

  // POST /dags/:dagId/backfill — create queued runs for every scheduled date in [start, end]
  // Body: { start: ISO string, end: ISO string }
  // Skips (dag_id, logical_date) pairs that already have a run (idempotent).
  // Returns: { created: string[], skipped: number, total_dates: number }
  app.post<{
    Params: { dagId: string }
    Body: { start: string; end: string }
  }>('/dags/:dagId/backfill', async (req, reply) => {
    const dag = getDag(req.params.dagId)
    if (!dag) return reply.status(404).send({ error: `Dag '${req.params.dagId}' not found` })

    if (!dag.schedule) {
      return reply.status(400).send({
        error: `Dag '${dag.id}' has no schedule — backfill requires a cron schedule`,
      })
    }

    const { start: startStr, end: endStr } = req.body ?? {}
    if (!startStr || !endStr) {
      return reply.status(400).send({ error: 'Body must include "start" and "end" ISO date strings' })
    }

    const start = new Date(startStr)
    const end = new Date(endStr)

    if (isNaN(start.getTime()) || isNaN(end.getTime())) {
      return reply.status(400).send({ error: '"start" and "end" must be valid ISO date strings' })
    }

    if (start > end) {
      return reply.status(400).send({ error: '"start" must be before or equal to "end"' })
    }

    try {
      const result = await backfill(app.mongo, dag, { start, end })
      return reply.status(201).send({
        dag_id: dag.id,
        created: result.created,
        created_count: result.created.length,
        skipped: result.skipped,
        total_dates: result.dates.length,
      })
    } catch (err) {
      if (err instanceof RangeError) {
        return reply.status(400).send({ error: err.message })
      }
      throw err
    }
  })
}
