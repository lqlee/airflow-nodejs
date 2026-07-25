import type { FastifyInstance } from 'fastify'
import { listPools, getPool, createPool, updatePool, deletePool } from '../../pools/index.js'

export async function poolsRoutes(app: FastifyInstance): Promise<void> {
  // GET /pools — list all pools with open/occupied slot counts
  app.get('/pools', async (_req, reply) => {
    return reply.send(await listPools(app.mongo))
  })

  // GET /pools/:name — single pool
  app.get<{ Params: { name: string } }>('/pools/:name', async (req, reply) => {
    const pool = await getPool(app.mongo, req.params.name)
    if (!pool) return reply.status(404).send({ error: `Pool '${req.params.name}' not found` })
    return reply.send(pool)
  })

  // POST /pools — create a pool
  // Body: { name: string, slots: number, description?: string }
  app.post<{ Body: { name?: string; slots?: number; description?: string } }>(
    '/pools',
    async (req, reply) => {
      const { name, slots, description } = req.body ?? {}
      if (!name || typeof name !== 'string' || !name.trim()) {
        return reply.status(400).send({ error: '"name" is required (non-empty string)' })
      }
      if (slots === undefined || typeof slots !== 'number' || !Number.isInteger(slots) || slots < 1) {
        return reply.status(400).send({ error: '"slots" must be an integer >= 1' })
      }

      // Check for duplicate before insert (works with or without unique index)
      const existing = await app.mongo.collection('pools').findOne({ name: name.trim() })
      if (existing) return reply.status(409).send({ error: `Pool '${name}' already exists` })

      const pool = await createPool(app.mongo, name.trim(), slots, description ?? '')
      return reply.status(201).send(pool)
    },
  )

  // PATCH /pools/:name — update slots and/or description
  app.patch<{
    Params: { name: string }
    Body: { slots?: number; description?: string }
  }>('/pools/:name', async (req, reply) => {
    const { slots, description } = req.body ?? {}

    if (slots !== undefined && (typeof slots !== 'number' || !Number.isInteger(slots) || slots < 1)) {
      return reply.status(400).send({ error: '"slots" must be an integer >= 1' })
    }

    const pool = await updatePool(app.mongo, req.params.name, { slots, description })
    if (!pool) return reply.status(404).send({ error: `Pool '${req.params.name}' not found` })
    return reply.send(pool)
  })

  // DELETE /pools/:name — remove a pool
  app.delete<{ Params: { name: string } }>('/pools/:name', async (req, reply) => {
    const ok = await deletePool(app.mongo, req.params.name)
    if (!ok) return reply.status(404).send({ error: `Pool '${req.params.name}' not found` })
    return reply.status(204).send()
  })
}
