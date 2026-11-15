/**
 * Execution API over HTTP — for workers that run away from the control plane (Kubernetes pods).
 * Callers present a task token (execution/token.ts), not a user credential: every call is scoped to the
 * token's task instance and is refused once that try stops running.
 *
 *   GET  /execution/v1/context                 run conf/params/dates + mapValue
 *   POST /execution/v1/xcom/:key               { value }      push as this task instance
 *   GET  /execution/v1/xcom/:taskId/:key       → { value }    pull within this run
 *   GET  /execution/v1/connections/:connId     decrypted connection (404 when unknown)
 *   GET  /execution/v1/variables/:key          → { value, secret }
 *   POST /execution/v1/datasets                { events }
 *   POST /execution/v1/logs                    { lines: [{ stream?, line, level? }] }
 *   GET  /execution/v1/task                    → { file, version, run }  the task's Dag file and run source
 *   POST /execution/v1/token/refresh           → { token }    a fresh token for the same try
 *
 * Not rate-limited (workers call it in loops); authHook skips /execution/ — the token check is here.
 */
import type { FastifyInstance, FastifyReply, FastifyRequest } from 'fastify'
import { verifyTaskToken, signTaskToken, type TaskScope } from '../../execution/token.js'
import { assertTaskLive, handleExecutionOp, ExecutionApiError, type ExecutionOp } from '../../execution/handlers.js'

declare module 'fastify' {
  interface FastifyRequest { taskScope?: TaskScope }
}

export async function executionRoutes(app: FastifyInstance): Promise<void> {
  const db = app.mongo
  const config = { rateLimit: false as const }

  app.addHook('preHandler', async (req, reply) => {
    if (!req.url.startsWith('/execution/')) return
    const header = req.headers.authorization ?? ''
    const scope = header.startsWith('Bearer ') ? verifyTaskToken(header.slice(7).trim()) : null
    if (!scope) return reply.status(401).send({ error: 'Unauthorized — a valid task token is required' })
    try {
      await assertTaskLive(db, scope)
    } catch (err) {
      return reply.status((err as ExecutionApiError).status ?? 500).send({ error: (err as Error).message })
    }
    req.taskScope = scope
  })

  const run = async (req: FastifyRequest, reply: FastifyReply, op: ExecutionOp, args: unknown[] = []) => {
    try {
      return await handleExecutionOp(db, req.taskScope!, op, args)
    } catch (err) {
      const status = err instanceof ExecutionApiError ? err.status : 500
      if (status === 500) console.error(`[execution-api] ${op} failed:`, err)
      return reply.status(status).send({ error: (err as Error).message })
    }
  }

  type P<T> = { Params: T }
  app.get('/execution/v1/context', { config }, (req, reply) => run(req, reply, 'context'))
  app.post<P<{ key: string }>>('/execution/v1/xcom/:key', { config }, async (req, reply) => {
    const r = await run(req, reply, 'xcom.push', [req.params.key, (req.body as { value?: unknown } | null)?.value])
    return reply.sent ? r : { ok: true }
  })
  app.get<P<{ taskId: string; key: string }>>('/execution/v1/xcom/:taskId/:key', { config }, async (req, reply) => {
    const value = await run(req, reply, 'xcom.pull', [req.params.taskId, req.params.key])
    return reply.sent ? value : { value: value ?? null }
  })
  app.get<P<{ connId: string }>>('/execution/v1/connections/:connId', { config }, async (req, reply) => {
    const conn = await run(req, reply, 'connection.get', [req.params.connId])
    if (reply.sent) return conn
    return conn ?? reply.status(404).send({ error: `connection '${req.params.connId}' not found` })
  })
  app.get<P<{ key: string }>>('/execution/v1/variables/:key', { config }, (req, reply) => run(req, reply, 'variable.get', [req.params.key]))
  app.post('/execution/v1/datasets', { config }, async (req, reply) => {
    const r = await run(req, reply, 'datasets.record', [(req.body as { events?: unknown } | null)?.events])
    return reply.sent ? r : { ok: true }
  })
  app.post('/execution/v1/logs', { config }, async (req, reply) => {
    const r = await run(req, reply, 'logs.append', [(req.body as { lines?: unknown } | null)?.lines])
    return reply.sent ? r : { ok: true }
  })
  app.get('/execution/v1/task', { config }, (req, reply) => run(req, reply, 'task.source'))
  app.post('/execution/v1/token/refresh', { config }, async (req) => ({ token: signTaskToken(req.taskScope!) }))
}
