import type { FastifyInstance } from 'fastify'
import { createApiKey, listApiKeys, revokeApiKey } from '../../auth/keys.js'

export async function apiKeysRoutes(app: FastifyInstance): Promise<void> {
  // GET /api-keys — list all keys (no hashes returned)
  app.get('/api-keys', async (_req, reply) => {
    const keys = await listApiKeys(app.mongo)
    return reply.send(keys)
  })

  // POST /api-keys — create a new key (stricter rate limit — key creation is rare)
  // Body: { name: string }
  app.post<{ Body: { name?: string } }>(
    '/api-keys',
    { config: { rateLimit: { max: 5, timeWindow: '1 minute' } } },
    async (req, reply) => {
    const name = req.body?.name?.trim()
    if (!name) return reply.status(400).send({ error: '"name" is required' })

    const { raw, id } = await createApiKey(app.mongo, name)
    return reply.status(201).send({
      id,
      name,
      key: raw,  // shown ONCE — store it now
      note: 'Save this key — it will not be shown again.',
    })
  },
  )

  // DELETE /api-keys/:keyId — revoke a key
  app.delete<{ Params: { keyId: string } }>('/api-keys/:keyId', async (req, reply) => {
    const ok = await revokeApiKey(app.mongo, req.params.keyId)
    if (!ok) return reply.status(404).send({ error: 'Key not found' })
    return reply.send({ id: req.params.keyId, revoked: true })
  })
}
