import type { FastifyInstance } from 'fastify'
import { createApiKey, listApiKeys, revokeApiKey, isRole } from '../../auth/keys.js'

export async function apiKeysRoutes(app: FastifyInstance): Promise<void> {
  // GET /api-keys — list all keys (no hashes returned); includes role
  app.get('/api-keys', async (_req, reply) => {
    const keys = await listApiKeys(app.mongo)
    return reply.send(keys)
  })

  // POST /api-keys — create a new key (rate limit 5/min, body { name: string, role?: Role })
  // Only admin callers can reach this route (enforced by authHook).
  app.post<{ Body: { name?: string; role?: string } }>(
    '/api-keys',
    { config: { rateLimit: { max: 5, timeWindow: '1 minute' } } },
    async (req, reply) => {
      const name = req.body?.name?.trim()
      if (!name) return reply.status(400).send({ error: '"name" is required' })

      const roleInput = req.body?.role ?? 'viewer'
      if (!isRole(roleInput)) {
        return reply.status(400).send({ error: '"role" must be one of: viewer, editor, admin' })
      }

      const { raw, id } = await createApiKey(app.mongo, name, roleInput)
      return reply.status(201).send({
        id,
        name,
        role: roleInput,
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
