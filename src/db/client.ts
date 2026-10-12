import type { Db } from 'mongodb'
import { createDbClient, type DbClient } from './factory.js'

const DB_NAME = process.env.DB_NAME ?? 'airflow'

let client: DbClient | null = null
let db: Db | null = null

export async function connectDb(): Promise<Db> {
  if (db) return db
  client = createDbClient()
  await client.connect()
  db = client.db(DB_NAME)
  return db
}

export async function closeDb(): Promise<void> {
  if (client) {
    await client.close()
    client = null
    db = null
  }
}

export function getDb(): Db {
  if (!db) throw new Error('DB not connected. Call connectDb() first.')
  return db
}
