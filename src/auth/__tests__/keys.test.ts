import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { createApiKey, validateApiKey, revokeApiKey, listApiKeys } from '../keys.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_keys')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('api_keys').deleteMany({})
})

describe('createApiKey', () => {
  it('returns a raw key with an_ prefix', async () => {
    const { raw } = await createApiKey(db, 'test-key')
    expect(raw).toMatch(/^an_/)
  })

  it('returns an id', async () => {
    const { id } = await createApiKey(db, 'test-key')
    expect(ObjectId.isValid(id)).toBe(true)
  })

  it('stores the key hashed (not the raw value)', async () => {
    const { raw } = await createApiKey(db, 'test-key')
    const doc = await db.collection('api_keys').findOne({ name: 'test-key' })
    expect(doc!.key_hash).not.toBe(raw)
    expect(doc!.key_hash).toMatch(/^[0-9a-f]+:[0-9a-f]+$/)  // salt:hash hex
  })

  it('stores the key as not revoked', async () => {
    await createApiKey(db, 'test-key')
    const doc = await db.collection('api_keys').findOne({ name: 'test-key' })
    expect(doc!.revoked).toBe(false)
    expect(doc!.last_used_at).toBeNull()
  })

  it('two keys for same name have different raw values', async () => {
    const { raw: raw1 } = await createApiKey(db, 'dupe')
    const { raw: raw2 } = await createApiKey(db, 'dupe')
    expect(raw1).not.toBe(raw2)
  })
})

describe('validateApiKey', () => {
  it('returns false for empty string', async () => {
    expect(await validateApiKey(db, '')).toBe(false)
  })

  it('returns false for random garbage', async () => {
    expect(await validateApiKey(db, 'not-a-real-key')).toBe(false)
  })

  it('returns true for a valid created key', async () => {
    const { raw } = await createApiKey(db, 'valid')
    expect(await validateApiKey(db, raw)).toBe(true)
  })

  it('returns false for a revoked key', async () => {
    const { raw, id } = await createApiKey(db, 'will-revoke')
    await revokeApiKey(db, id)
    expect(await validateApiKey(db, raw)).toBe(false)
  })

  it('updates last_used_at on successful validation', async () => {
    const { raw } = await createApiKey(db, 'track-usage')
    await validateApiKey(db, raw)
    // Small delay for the fire-and-forget update
    await new Promise(r => setTimeout(r, 50))
    const doc = await db.collection('api_keys').findOne({ name: 'track-usage' })
    expect(doc!.last_used_at).not.toBeNull()
  })
})

describe('revokeApiKey', () => {
  it('returns false for non-existent id', async () => {
    expect(await revokeApiKey(db, new ObjectId().toString())).toBe(false)
  })

  it('returns false for invalid id', async () => {
    expect(await revokeApiKey(db, 'bad-id')).toBe(false)
  })

  it('marks the key as revoked', async () => {
    const { id } = await createApiKey(db, 'to-revoke')
    const result = await revokeApiKey(db, id)
    expect(result).toBe(true)
    const doc = await db.collection('api_keys').findOne({ _id: new ObjectId(id) })
    expect(doc!.revoked).toBe(true)
  })
})

describe('listApiKeys', () => {
  it('returns empty array when no keys exist', async () => {
    const keys = await listApiKeys(db)
    expect(keys).toHaveLength(0)
  })

  it('returns keys without hash', async () => {
    await createApiKey(db, 'listed')
    const keys = await listApiKeys(db)
    expect(keys).toHaveLength(1)
    expect(keys[0].name).toBe('listed')
    expect((keys[0] as any).key_hash).toBeUndefined()
  })

  it('includes revoked keys in list', async () => {
    const { id } = await createApiKey(db, 'revoked-one')
    await revokeApiKey(db, id)
    const keys = await listApiKeys(db)
    expect(keys.find(k => k.name === 'revoked-one')!.revoked).toBe(true)
  })

  it('returns all keys with correct shape', async () => {
    await createApiKey(db, 'k1')
    await createApiKey(db, 'k2')
    const keys = await listApiKeys(db)
    expect(keys).toHaveLength(2)
    for (const k of keys) {
      expect(k.id).toBeDefined()
      expect(k.name).toBeDefined()
      expect(k.created_at).toBeDefined()
      expect('last_used_at' in k).toBe(true)
      expect('revoked' in k).toBe(true)
    }
  })
})
