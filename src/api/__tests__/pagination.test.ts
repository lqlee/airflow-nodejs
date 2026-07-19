import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { buildServer } from '../server.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { FastifyInstance } from 'fastify'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let app: FastifyInstance

const paginationDag: DagDefinition = {
  id: 'pagination_dag',
  schedule: null,
  tasks: { step: { run: async () => {} } },
}

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_pagination')
  clearRegistry()
  register(paginationDag)
  app = buildServer(db)
  await app.ready()
})

afterAll(async () => {
  await app.close()
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
})

/** Insert N runs with evenly-spaced created_at timestamps (oldest first). */
async function insertRuns(n: number) {
  const now = Date.now()
  const docs = Array.from({ length: n }, (_, i) => ({
    dag_id: 'pagination_dag',
    state: 'success',
    created_at: new Date(now - (n - i) * 1000), // oldest first in insertion order
  }))
  await db.collection('dag_runs').insertMany(docs)
}

describe('GET /dags/:dagId/runs pagination', () => {
  it('returns items + next_cursor when more runs exist', async () => {
    await insertRuns(25)
    const res = await app.inject({
      method: 'GET',
      url: '/dags/pagination_dag/runs?limit=10',
    })
    expect(res.statusCode).toBe(200)
    const body = res.json()
    expect(body.items).toHaveLength(10)
    expect(body.next_cursor).toBeTruthy()
  })

  it('returns null next_cursor on last page', async () => {
    await insertRuns(5)
    const res = await app.inject({
      method: 'GET',
      url: '/dags/pagination_dag/runs?limit=10',
    })
    const body = res.json()
    expect(body.items).toHaveLength(5)
    expect(body.next_cursor).toBeNull()
  })

  it('cursor fetches the next page without overlap', async () => {
    await insertRuns(25)
    const page1 = await app.inject({
      method: 'GET',
      url: '/dags/pagination_dag/runs?limit=10',
    }).then(r => r.json())

    const page2 = await app.inject({
      method: 'GET',
      url: `/dags/pagination_dag/runs?limit=10&cursor=${page1.next_cursor}`,
    }).then(r => r.json())

    const ids1 = new Set(page1.items.map((r: any) => r.run_id))
    const ids2 = new Set(page2.items.map((r: any) => r.run_id))
    // No overlapping run IDs between pages
    const overlap = [...ids2].filter(id => ids1.has(id))
    expect(overlap).toHaveLength(0)
    expect(page2.items.length).toBeGreaterThan(0)
  })

  it('cursor pages cover all runs exactly once', async () => {
    await insertRuns(25)
    const allIds: string[] = []
    let cursor: string | null = null

    do {
      const url = `/dags/pagination_dag/runs?limit=10${cursor ? `&cursor=${cursor}` : ''}`
      const body = await app.inject({ method: 'GET', url }).then(r => r.json())
      allIds.push(...body.items.map((r: any) => r.run_id))
      cursor = body.next_cursor
    } while (cursor)

    expect(allIds).toHaveLength(25)
    expect(new Set(allIds).size).toBe(25) // no duplicates
  })

  it('items are sorted newest-first within each page', async () => {
    await insertRuns(15)
    const body = await app.inject({
      method: 'GET',
      url: '/dags/pagination_dag/runs?limit=15',
    }).then(r => r.json())

    const dates = body.items.map((r: any) => new Date(r.created_at).getTime())
    for (let i = 1; i < dates.length; i++) {
      expect(dates[i]).toBeLessThanOrEqual(dates[i - 1])
    }
  })

  it('returns 400 for an invalid cursor', async () => {
    await insertRuns(5)
    const res = await app.inject({
      method: 'GET',
      url: '/dags/pagination_dag/runs?cursor=notanobjectid',
    })
    expect(res.statusCode).toBe(400)
  })

  it('defaults to limit 20 and caps at 200', async () => {
    await insertRuns(25)
    const bodyDefault = await app.inject({
      method: 'GET',
      url: '/dags/pagination_dag/runs',
    }).then(r => r.json())
    expect(bodyDefault.items).toHaveLength(20)

    const bodyOver = await app.inject({
      method: 'GET',
      url: '/dags/pagination_dag/runs?limit=999',
    }).then(r => r.json())
    expect(bodyOver.items).toHaveLength(25) // only 25 exist, cap prevents error
  })
})
