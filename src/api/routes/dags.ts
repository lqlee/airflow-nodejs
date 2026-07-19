import type { FastifyInstance } from 'fastify'
import { ObjectId } from 'mongodb'
import { listDags, getDag } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import { pauseDag, resumeDag, isDagPaused, getPausedDagIds } from '../../dag/pause.js'

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
}
