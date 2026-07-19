import type { FastifyInstance } from 'fastify'
import { listVariables, getVariable, setVariable, deleteVariable } from '../../variables/index.js'

export async function variablesRoutes(app: FastifyInstance): Promise<void> {
  // GET /variables — list all variables (secret values masked as null)
  app.get('/variables', async (_req, reply) => {
    const vars = await listVariables(app.mongo)
    return reply.send(vars)
  })

  // GET /variables/:key — single variable (value null if secret)
  app.get<{ Params: { key: string } }>('/variables/:key', async (req, reply) => {
    const v = await getVariable(app.mongo, req.params.key)
    if (!v) return reply.status(404).send({ error: `Variable '${req.params.key}' not found` })
    return reply.send(v)
  })

  // POST /variables — create or update a variable
  app.post<{
    Body: { key: string; value: string; is_secret?: boolean; description?: string }
  }>('/variables', async (req, reply) => {
    const { key, value, ...rest } = req.body ?? {}
    if (!key || value === undefined || value === null) {
      return reply.status(400).send({ error: 'key and value are required' })
    }
    try {
      const v = await setVariable(app.mongo, { key, value: String(value), ...rest })
      return reply.status(201).send(v)
    } catch (err) {
      if (err instanceof Error && err.message.includes('ENCRYPTION_KEY')) {
        return reply.status(400).send({ error: err.message })
      }
      throw err
    }
  })

  // DELETE /variables/:key — remove a variable
  app.delete<{ Params: { key: string } }>('/variables/:key', async (req, reply) => {
    const deleted = await deleteVariable(app.mongo, req.params.key)
    if (!deleted) return reply.status(404).send({ error: `Variable '${req.params.key}' not found` })
    return reply.status(204).send()
  })
}
