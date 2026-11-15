/** Re-encrypting stored connection / variable secrets onto the primary key. */
import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { rotateEncryptedValues } from '../rotate.js'
import { upsertConnection, getConnectionRuntime } from '../../connections/index.js'
import { setVariable, getVariableRuntime } from '../../variables/index.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const OLD = '1'.repeat(64)
const NEW = '2'.repeat(64)
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_rotate_keys')
})
afterAll(async () => { await db.dropDatabase(); await client.close() })
afterEach(async () => {
  delete process.env.ENCRYPTION_KEYS
  await db.collection('connections').deleteMany({})
  await db.collection('variables').deleteMany({})
})

async function seedWithOldKey(): Promise<void> {
  process.env.ENCRYPTION_KEYS = OLD
  await upsertConnection(db, { conn_id: 'pg', conn_type: 'postgres', password: 'pg-pass', extra: '{"token":"t-123456"}' })
  await upsertConnection(db, { conn_id: 'plain', conn_type: 'http', host: 'h' })   // nothing encrypted
  await setVariable(db, { key: 'api_key', value: 'var-secret', is_secret: true })
  await setVariable(db, { key: 'region', value: 'us-east1' })
}

describe('rotateEncryptedValues', () => {
  it('re-encrypts every secret so the old key can be retired', async () => {
    await seedWithOldKey()
    process.env.ENCRYPTION_KEYS = `${NEW},${OLD}`

    const res = await rotateEncryptedValues(db)
    expect(res).toEqual({
      rotated: { connections: 1, variables: 1 },
      current: { connections: 1, variables: 1 },
      failed: [],
    })

    process.env.ENCRYPTION_KEYS = NEW   // old key retired
    expect(await getConnectionRuntime(db, 'pg')).toMatchObject({ password: 'pg-pass', extra: { token: 't-123456' } })
    expect(await getVariableRuntime(db, 'api_key')).toBe('var-secret')
    expect(await rotateEncryptedValues(db)).toMatchObject({ rotated: { connections: 0, variables: 0 } })
  })

  it('dry run reports what would change and writes nothing', async () => {
    await seedWithOldKey()
    process.env.ENCRYPTION_KEYS = `${NEW},${OLD}`
    expect(await rotateEncryptedValues(db, { dryRun: true })).toMatchObject({ rotated: { connections: 1, variables: 1 } })

    process.env.ENCRYPTION_KEYS = NEW
    await expect(getVariableRuntime(db, 'api_key')).rejects.toThrow(/not in ENCRYPTION_KEYS/)
  })

  it('reports values no configured key can decrypt and leaves them untouched', async () => {
    await seedWithOldKey()
    process.env.ENCRYPTION_KEYS = NEW   // old key dropped before rotating
    const before = await db.collection('variables').findOne({ key: 'api_key' })

    const res = await rotateEncryptedValues(db)
    expect(res.failed.map(f => `${f.collection}:${f.id}`).sort()).toEqual(['connections:pg', 'variables:api_key'])
    expect(await db.collection('variables').findOne({ key: 'api_key' })).toEqual(before)
  })

  it('a value edited between rotation reading and writing it keeps the edit', async () => {
    await seedWithOldKey()
    process.env.ENCRYPTION_KEYS = `${NEW},${OLD}`
    // Rotation reads this snapshot; the edit then lands before its write
    const snapshot = {
      connections: await db.collection('connections').find().toArray(),
      variables: await db.collection('variables').find().toArray(),
    }
    await setVariable(db, { key: 'api_key', value: 'edited-meanwhile', is_secret: true })
    const staleReads = {
      collection: (name: 'connections' | 'variables') => ({
        find: () => ({ toArray: async () => snapshot[name] }),
        updateOne: (...args: Parameters<ReturnType<Db['collection']>['updateOne']>) => db.collection(name).updateOne(...args),
      }),
    } as unknown as Db

    const res = await rotateEncryptedValues(staleReads)
    expect(res.rotated).toEqual({ connections: 1, variables: 0 })   // the variable's CAS missed
    process.env.ENCRYPTION_KEYS = NEW
    expect(await getVariableRuntime(db, 'api_key')).toBe('edited-meanwhile')
  })
})
