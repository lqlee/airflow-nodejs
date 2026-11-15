/**
 * Task and run actions — clear, mark success / failed / skipped (scheduler/actions.ts).
 *
 *   POST  /dag-runs/:runId/tasks/:taskId/clear  clear one task (optionally downstream / upstream / failed only)
 *   POST  /dag-runs/:runId/clear                 clear a whole run (re-run), optionally failed only
 *   PATCH /dag-runs/:runId/tasks/:taskId         mark task instances success / failed / skipped
 *   PATCH /dag-runs/:runId                       mark a run success / failed
 *
 * Every route takes `dry_run: true` to return the instances that would change without changing them.
 */
import type { FastifyInstance, FastifyReply } from 'fastify'
import { ObjectId } from 'mongodb'
import { advanceRun } from '../../scheduler/index.js'
import { clearTaskInstances, setTaskInstanceState, setRunState, type ActionResult } from '../../scheduler/actions.js'
import { TI_SETTABLE, type TiSettableState } from '../../scheduler/states.js'

interface ActionBody {
  include_downstream?: boolean
  include_upstream?: boolean
  only_failed?: boolean
  dry_run?: boolean
  map_index?: number
  state?: string
}

type Parsed<T> = { ok: true; value: T } | { ok: false; error: string }

function parseBool(body: ActionBody, key: keyof ActionBody): Parsed<boolean> {
  const v = body[key]
  if (v === undefined) return { ok: true, value: false }
  if (typeof v !== 'boolean') return { ok: false, error: `"${key}" must be a boolean` }
  return { ok: true, value: v }
}

function parseMapIndex(raw: unknown): Parsed<number | undefined> {
  if (raw === undefined) return { ok: true, value: undefined }
  const mi = typeof raw === 'string' ? Number(raw) : raw
  if (typeof mi !== 'number' || !Number.isInteger(mi) || mi < 0) return { ok: false, error: '"map_index" must be a non-negative integer' }
  return { ok: true, value: mi }
}

/** Parse the shared flags; on error the reply is already sent and null returned. */
function parseFlags(body: ActionBody, keys: Array<keyof ActionBody>, reply: FastifyReply): Record<string, boolean> | null {
  const out: Record<string, boolean> = {}
  for (const k of keys) {
    const p = parseBool(body, k)
    if (!p.ok) { void reply.status(400).send({ error: p.error }); return null }
    out[k] = p.value
  }
  return out
}

function notFound(reply: FastifyReply, runId: string, res: Extract<ActionResult, { ok: false }>) {
  return reply.status(404).send({
    error: res.reason === 'run_not_found' ? `Run '${runId}' not found` : `Task '${res.taskId}' not found in run '${runId}'`,
  })
}

/** Let the scheduler pick up the change now instead of on the next tick (in the background: tasks may run long). */
function kick(app: FastifyInstance, runId: string): void {
  void advanceRun(app.mongo, runId).catch(err => app.log.error({ err, runId }, 'advanceRun after task action failed'))
}

export async function taskActionsRoutes(app: FastifyInstance): Promise<void> {
  // Clear one task. ?map_index=N targets one mapped instance. Only finished instances are cleared —
  // 409 when the task (and its selected relatives) has nothing finished to clear.
  app.post<{ Params: { runId: string; taskId: string }; Querystring: { map_index?: string }; Body: ActionBody }>(
    '/dag-runs/:runId/tasks/:taskId/clear',
    async (req, reply) => {
      const { runId, taskId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })
      const mi = parseMapIndex(req.query.map_index)
      if (!mi.ok) return reply.status(400).send({ error: mi.error })
      const flags = parseFlags(req.body ?? {}, ['include_downstream', 'include_upstream', 'only_failed', 'dry_run'], reply)
      if (!flags) return reply

      const res = await clearTaskInstances(app.mongo, runId, {
        taskIds: [taskId], mapIndex: mi.value,
        includeDownstream: flags.include_downstream, includeUpstream: flags.include_upstream,
        onlyFailed: flags.only_failed, dryRun: flags.dry_run,
      })
      if (!res.ok) return notFound(reply, runId, res)
      if (res.changed.length === 0 && !flags.dry_run) {
        return reply.status(409).send({
          error: `Task '${taskId}' is not in a finished state — cannot clear a running or queued task`,
        })
      }

      // Re-advance right away so the cleared task runs without waiting for the next tick
      if (!flags.dry_run) await advanceRun(app.mongo, runId)

      return reply.send({
        run_id: runId,
        task_id: taskId,
        map_index: mi.value ?? null,
        dry_run: flags.dry_run,
        cleared_count: res.changed.length,
        task_instances: res.changed,
      })
    },
  )

  // Clear a whole run (re-run it). only_failed: re-run just the failed / upstream_failed instances.
  app.post<{ Params: { runId: string }; Body: ActionBody }>('/dag-runs/:runId/clear', async (req, reply) => {
    const { runId } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })
    const flags = parseFlags(req.body ?? {}, ['only_failed', 'dry_run'], reply)
    if (!flags) return reply

    const res = await clearTaskInstances(app.mongo, runId, { onlyFailed: flags.only_failed, dryRun: flags.dry_run })
    if (!res.ok) return notFound(reply, runId, res)
    if (!flags.dry_run && res.changed.length > 0) kick(app, runId)
    return reply.send({ run_id: runId, dry_run: flags.dry_run, cleared_count: res.changed.length, task_instances: res.changed })
  })

  // Mark task instances. Body: { state: success|failed|skipped, map_index?, include_downstream?, include_upstream?, dry_run? }
  app.patch<{ Params: { runId: string; taskId: string }; Body: ActionBody }>(
    '/dag-runs/:runId/tasks/:taskId',
    async (req, reply) => {
      const { runId, taskId } = req.params
      if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })
      const body = req.body ?? {}
      if (!(TI_SETTABLE as readonly string[]).includes(body.state as string)) {
        return reply.status(400).send({ error: `"state" must be one of: ${TI_SETTABLE.join(', ')}` })
      }
      const mi = parseMapIndex(body.map_index)
      if (!mi.ok) return reply.status(400).send({ error: mi.error })
      const flags = parseFlags(body, ['include_downstream', 'include_upstream', 'dry_run'], reply)
      if (!flags) return reply

      const state = body.state as TiSettableState
      const res = await setTaskInstanceState(app.mongo, runId, state, {
        taskIds: [taskId], mapIndex: mi.value,
        includeDownstream: flags.include_downstream, includeUpstream: flags.include_upstream, dryRun: flags.dry_run,
      })
      if (!res.ok) return notFound(reply, runId, res)
      if (!flags.dry_run && res.changed.length > 0) kick(app, runId)
      return reply.send({
        run_id: runId, task_id: taskId, state, map_index: mi.value ?? null,
        dry_run: flags.dry_run, changed_count: res.changed.length, task_instances: res.changed,
      })
    },
  )

  // Mark a run. Body: { state: success|failed, dry_run? }
  app.patch<{ Params: { runId: string }; Body: ActionBody }>('/dag-runs/:runId', async (req, reply) => {
    const { runId } = req.params
    if (!ObjectId.isValid(runId)) return reply.status(400).send({ error: 'Invalid run id' })
    const body = req.body ?? {}
    if (body.state !== 'success' && body.state !== 'failed') {
      return reply.status(400).send({ error: '"state" must be one of: success, failed' })
    }
    const flags = parseFlags(body, ['dry_run'], reply)
    if (!flags) return reply

    const res = await setRunState(app.mongo, runId, body.state, { dryRun: flags.dry_run })
    if (!res.ok) return notFound(reply, runId, res)
    return reply.send({ run_id: runId, state: body.state, dry_run: flags.dry_run, changed_count: res.changed.length, task_instances: res.changed })
  })
}
