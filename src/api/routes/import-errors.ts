import type { FastifyInstance } from 'fastify'
import { getImportErrors } from '../../dag/import-errors.js'

export async function importErrorsRoutes(app: FastifyInstance): Promise<void> {
  // GET /import-errors — list Dag files that failed to load in the most recent reload
  // Empty array when all files loaded successfully.
  app.get('/import-errors', async (_req, reply) => {
    const errors = getImportErrors()
    return reply.send(errors)
  })
}
