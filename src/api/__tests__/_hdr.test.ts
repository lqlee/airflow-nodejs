import { it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient } from 'mongodb'
import { buildServer } from '../server.js'
import { register, clearRegistry } from '../../dag/registry.js'
let client: any, db: any
beforeAll(async () => {
  client = new MongoClient('mongodb://localhost:27017'); await client.connect()
  db = client.db('debug_hdr'); clearRegistry()
  register({ id: 'x', schedule: null, tasks: { t: { run: async () => {} } } })
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
it('health headers', async () => {
  const app = buildServer(db, { rateLimitMax: 10, rateLimitAuthMax: 5 })
  await app.ready()
  const r = await app.inject({ method: 'GET', url: '/health' })
  console.log('health headers:', JSON.stringify(r.headers, null, 2))
  await app.close()
})
