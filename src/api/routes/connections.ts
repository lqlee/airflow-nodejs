import type { FastifyInstance } from 'fastify'
import { listConnections, getConnection, upsertConnection, deleteConnection } from '../../connections/index.js'

export async function connectionsRoutes(app: FastifyInstance): Promise<void> {
  // GET /connections — list all connections (password/extra never returned)
  app.get('/connections', async (_req, reply) => {
    const conns = await listConnections(app.mongo)
    return reply.send(conns)
  })

  // GET /connections/:connId — single connection summary (no secret fields)
  app.get<{ Params: { connId: string } }>('/connections/:connId', async (req, reply) => {
    const conn = await getConnection(app.mongo, req.params.connId)
    if (!conn) return reply.status(404).send({ error: `Connection '${req.params.connId}' not found` })
    return reply.send(conn)
  })

  // POST /connections — create or update a connection
  app.post<{
    Body: {
      conn_id: string
      conn_type: string
      host?: string
      port?: number | null
      schema?: string
      login?: string
      password?: string
      extra?: string
      description?: string
    }
  }>('/connections', async (req, reply) => {
    const { conn_id, conn_type, ...rest } = req.body ?? {}
    if (!conn_id || !conn_type) {
      return reply.status(400).send({ error: 'conn_id and conn_type are required' })
    }
    try {
      const conn = await upsertConnection(app.mongo, { conn_id, conn_type, ...rest })
      return reply.status(201).send(conn)
    } catch (err) {
      if (err instanceof Error && err.message.includes('ENCRYPTION_KEY')) {
        return reply.status(400).send({ error: err.message })
      }
      throw err
    }
  })

  // DELETE /connections/:connId — remove a connection
  app.delete<{ Params: { connId: string } }>('/connections/:connId', async (req, reply) => {
    const deleted = await deleteConnection(app.mongo, req.params.connId)
    if (!deleted) return reply.status(404).send({ error: `Connection '${req.params.connId}' not found` })
    return reply.status(204).send()
  })
}
