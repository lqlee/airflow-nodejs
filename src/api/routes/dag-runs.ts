import type { FastifyInstance } from 'fastify'
import { ObjectId } from 'mongodb'
import { getTaskLogs } from '../../logs/index.js'
import { cancelRun, clearTaskInstance, advanceRun } from '../../scheduler/index.js'
import { xcomPush } from '../../xcom/index.js'

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
  // Optional query: ?task_id=  ?key=  (both independent filters)
  app.get<{
    Params: { runId: string }
    Querystring: { task_id?: string; key?: string }
  }>('/dag-runs/:runId/xcoms', async (req, reply) => {
    const { runId } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

    const filter: Record<string, unknown> = { dag_run_id: runId }
    if (req.query.task_id) filter['task_id'] = req.query.task_id
    if (req.query.key) filter['key'] = req.query.key

    const xcoms = await app.mongo
      .collection('xcoms')
      .find(filter)
      .sort({ task_id: 1, map_index: 1, key: 1, pushed_at: 1 })
      .toArray()

    return reply.send(xcoms.map(x => ({
      run_id: runId,
      dag_id: x.dag_id,
      task_id: x.task_id,
      map_index: x.map_index ?? null,
      key: x.key,
      value: x.value,
      pushed_at: x.pushed_at,
    })))
  })

  // GET /dag-runs/:runId/xcoms/:taskId/:key — single XCom entry
  // ?map_index=N to target a specific mapped instance; omit for non-mapped tasks.
  app.get<{
    Params: { runId: string; taskId: string; key: string }
    Querystring: { map_index?: string }
  }>('/dag-runs/:runId/xcoms/:taskId/:key', async (req, reply) => {
    const { runId, taskId, key } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

    const filter: Record<string, unknown> = { dag_run_id: runId, task_id: taskId, key }
    if (req.query.map_index !== undefined) {
      const mi = parseInt(req.query.map_index, 10)
      if (!Number.isFinite(mi) || mi < 0) {
        return reply.status(400).send({ error: '"map_index" must be a non-negative integer' })
      }
      filter['map_index'] = mi
    } else {
      filter['map_index'] = null  // non-mapped tasks store null
    }

    const xcom = await app.mongo.collection('xcoms').findOne(filter)
    if (!xcom) return reply.status(404).send({ error: `XCom '${key}' not found for task '${taskId}'` })

    return reply.send({
      run_id: runId,
      dag_id: xcom.dag_id,
      task_id: xcom.task_id,
      map_index: xcom.map_index ?? null,
      key: xcom.key,
      value: xcom.value,
      pushed_at: xcom.pushed_at,
    })
  })

  // POST /dag-runs/:runId/xcoms — push an XCom value via API (upserts by task_id+key+map_index)
  // Body: { task_id: string, key: string, value: unknown, map_index?: number }
  // Useful for injecting values in tests or human-in-the-loop workflows.
  app.post<{
    Params: { runId: string }
    Body: { task_id?: string; key?: string; value?: unknown; map_index?: number | null }
  }>('/dag-runs/:runId/xcoms', async (req, reply) => {
    const { runId } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

    const run = await app.mongo.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
    if (!run) return reply.status(404).send({ error: `Run '${runId}' not found` })

    const { task_id, key, value, map_index = null } = req.body ?? {}
    if (!task_id || typeof task_id !== 'string') {
      return reply.status(400).send({ error: '"task_id" is required (string)' })
    }
    if (!key || typeof key !== 'string') {
      return reply.status(400).send({ error: '"key" is required (string)' })
    }
    if (value === undefined) {
      return reply.status(400).send({ error: '"value" is required' })
    }
    if (map_index !== null && (typeof map_index !== 'number' || !Number.isFinite(map_index) || map_index < 0)) {
      return reply.status(400).send({ error: '"map_index" must be a non-negative integer or null' })
    }

    await xcomPush(app.mongo, runId, run.dag_id as string, task_id, map_index, key, value)

    return reply.status(201).send({
      run_id: runId,
      dag_id: run.dag_id,
      task_id,
      map_index: map_index ?? null,
      key,
      value,
    })
  })

  // DELETE /dag-runs/:runId/xcoms/:taskId/:key — delete a single XCom entry
  // ?map_index=N for mapped tasks; omit to delete the non-mapped entry.
  app.delete<{
    Params: { runId: string; taskId: string; key: string }
    Querystring: { map_index?: string }
  }>('/dag-runs/:runId/xcoms/:taskId/:key', async (req, reply) => {
    const { runId, taskId, key } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

    const filter: Record<string, unknown> = { dag_run_id: runId, task_id: taskId, key }
    if (req.query.map_index !== undefined) {
      const mi = parseInt(req.query.map_index, 10)
      if (!Number.isFinite(mi) || mi < 0) {
        return reply.status(400).send({ error: '"map_index" must be a non-negative integer' })
      }
      filter['map_index'] = mi
    } else {
      filter['map_index'] = null
    }

    const result = await app.mongo.collection('xcoms').deleteOne(filter)
    if (result.deletedCount === 0) {
      return reply.status(404).send({ error: `XCom '${key}' not found for task '${taskId}'` })
    }
    return reply.status(204).send()
  })

  // DELETE /dag-runs/:runId/xcoms — delete ALL XComs for a run
  // Useful after clearing tasks to start fresh. Returns count of deleted entries.
  app.delete<{ Params: { runId: string } }>('/dag-runs/:runId/xcoms', async (req, reply) => {
    const { runId } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

    const result = await app.mongo.collection('xcoms').deleteMany({ dag_run_id: runId })
    return reply.send({ run_id: runId, deleted_count: result.deletedCount })
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

  // GET /dag-runs/:runId/tasks/:taskId — single task instance (all map_index instances if mapped)
  app.get<{ Params: { runId: string; taskId: string }; Querystring: { map_index?: string } }>(
    '/dag-runs/:runId/tasks/:taskId',
    async (req, reply) => {
      const { runId, taskId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

      const filter: Record<string, unknown> = { dag_run_id: runId, task_id: taskId }
      const rawMapIndex = req.query.map_index
      if (rawMapIndex !== undefined) {
        const mi = parseInt(rawMapIndex, 10)
        if (!Number.isFinite(mi) || mi < 0) {
          return reply.status(400).send({ error: '"map_index" must be a non-negative integer' })
        }
        filter['map_index'] = mi
      }

      const instances = await app.mongo
        .collection('task_instances')
        .find(filter)
        .sort({ map_index: 1 })
        .toArray()

      if (instances.length === 0) {
        return reply.status(404).send({ error: `Task '${taskId}' not found in run '${runId}'` })
      }

      const format = (t: Record<string, unknown>) => ({
        run_id: runId,
        dag_id: t.dag_id,
        task_id: t.task_id,
        group_id: t.group_id ?? null,
        map_index: t.map_index ?? null,
        map_value: t.map_value ?? null,
        state: t.state,
        depends_on: t.depends_on,
        try_number: t.try_number ?? 0,
        max_retries: t.max_retries ?? 0,
        retry_delay_ms: t.retry_delay ?? 0,
        timeout_ms: t.timeout_ms ?? 0,
        started_at: t.started_at ?? null,
        ended_at: t.ended_at ?? null,
        error: t.error ?? null,
        is_sensor: t.is_sensor ?? false,
        poke_count: t.poke_count ?? 0,
        poke_interval_ms: t.poke_interval_ms ?? null,
        sensor_timeout_ms: t.sensor_timeout_ms ?? null,
        next_poke_at: t.next_poke_at ?? null,
        first_poked_at: t.first_poked_at ?? null,
        created_at: t.created_at,
      })

      // If map_index filter was given → return single object; otherwise array
      return reply.send(rawMapIndex !== undefined ? format(instances[0]) : instances.map(format))
    },
  )

  // GET /dag-runs/:runId/tasks — list all task instances for a run (with optional state filter)
  app.get<{ Params: { runId: string }; Querystring: { state?: string } }>(
    '/dag-runs/:runId/tasks',
    async (req, reply) => {
      const { runId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

      const VALID_STATES = new Set(['queued', 'running', 'success', 'failed', 'cancelled'])
      const filter: Record<string, unknown> = { dag_run_id: runId }
      if (req.query.state) {
        if (!VALID_STATES.has(req.query.state)) {
          return reply.status(400).send({ error: `"state" must be one of: ${[...VALID_STATES].join(', ')}` })
        }
        filter['state'] = req.query.state
      }

      const instances = await app.mongo
        .collection('task_instances')
        .find(filter)
        .sort({ task_id: 1, map_index: 1 })
        .toArray()

      return reply.send(instances.map(t => ({
        run_id: runId,
        dag_id: t.dag_id,
        task_id: t.task_id,
        group_id: t.group_id ?? null,
        map_index: t.map_index ?? null,
        state: t.state,
        try_number: t.try_number ?? 0,
        started_at: t.started_at ?? null,
        ended_at: t.ended_at ?? null,
        error: t.error ?? null,
        is_sensor: t.is_sensor ?? false,
      })))
    },
  )

  // POST /dag-runs/:runId/tasks/:taskId/clear — reset a terminal task instance back to queued
  // Query param: ?map_index=N to target a specific mapped instance; omit to clear all instances.
  // Only clears instances in {success, failed, cancelled} — returns 409 for running instances.
  // Also resets the parent run to 'queued' so the scheduler re-advances it.
  app.post<{
    Params: { runId: string; taskId: string }
    Querystring: { map_index?: string }
  }>(
    '/dag-runs/:runId/tasks/:taskId/clear',
    async (req, reply) => {
      const { runId, taskId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })

      let mapIndex: number | undefined
      if (req.query.map_index !== undefined) {
        const mi = parseInt(req.query.map_index, 10)
        if (!Number.isFinite(mi) || mi < 0) {
          return reply.status(400).send({ error: '"map_index" must be a non-negative integer' })
        }
        mapIndex = mi
      }

      const result = await clearTaskInstance(app.mongo, runId, taskId, mapIndex)

      if (!result.cleared) {
        if (result.reason === 'run_not_found') {
          return reply.status(404).send({ error: `Run '${runId}' not found` })
        }
        if (result.reason === 'task_not_found') {
          return reply.status(404).send({ error: `Task '${taskId}' not found in run '${runId}'` })
        }
        // task_not_terminal → task is still running or already queued
        return reply.status(409).send({
          error: `Task '${taskId}' is not in a terminal state — cannot clear a running or queued task`,
        })
      }

      // Re-advance immediately so the cleared task runs without waiting for the next tick
      await advanceRun(app.mongo, runId)

      return reply.send({
        run_id: runId,
        task_id: taskId,
        cleared_count: result.clearedCount,
        map_index: mapIndex ?? null,
      })
    },
  )

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
