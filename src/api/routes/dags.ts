import type { FastifyInstance } from 'fastify'
import { ObjectId } from 'mongodb'
import { listDags, getDag } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'

export async function dagsRoutes(app: FastifyInstance): Promise<void> {
  // GET /dags — list all registered dags
  app.get('/dags', async (_req, reply) => {
    const dags = listDags().map(d => ({
      id: d.id,
      schedule: d.schedule,
      tasks: Object.keys(d.tasks),
    }))
    return reply.send(dags)
  })

  // GET /dags/:dagId — get a single dag
  app.get<{ Params: { dagId: string } }>('/dags/:dagId', async (req, reply) => {
    const dag = getDag(req.params.dagId)
    if (!dag) return reply.status(404).send({ error: `Dag '${req.params.dagId}' not found` })

    return reply.send({
      id: dag.id,
      schedule: dag.schedule,
      tasks: Object.entries(dag.tasks).map(([taskId, t]) => ({
        task_id: taskId,
        depends_on: t.dependsOn ?? [],
      })),
    })
  })

  // POST /dags/:dagId/trigger — manually trigger a dag run
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
}
