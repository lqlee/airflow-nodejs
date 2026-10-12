/**
 * The JS evaluator is the reference for the Spanner backend, so it must agree with REAL MongoDB.
 * Every case here runs the same filter/update/sort against mongod and against eval.ts and compares.
 */
import { describe, it, expect, beforeAll, afterAll } from 'vitest'
import { MongoClient, ObjectId, type Collection, type Db } from 'mongodb'
import { matchesFilter, applyUpdate, sortDocs, projectDoc, equalityFields, compareValues, type Doc } from '../eval.js'
import { cloneDoc } from '../codec.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
let client: MongoClient
let db: Db
let col: Collection<Doc>

const D = (s: string) => new Date(s)
const id = (n: number) => new ObjectId(n.toString(16).padStart(24, '0'))

const docs: Doc[] = [
  { _id: id(1), name: 'a', n: 1, state: 'queued', created_at: D('2026-01-01T00:00:00Z'), tags: ['x', 'y'], nested: { k: 1, arr: [{ z: 1 }, { z: 2 }] }, nul: null, flag: true },
  { _id: id(2), name: 'b', n: 2, state: 'running', created_at: D('2026-01-02T00:00:00Z'), tags: ['y'], nested: { k: 2 }, flag: false },
  { _id: id(3), name: 'c', n: 3, state: 'queued', created_at: D('2026-01-03T00:00:00Z'), tags: [], nested: { k: 1 }, nul: 5 },
  { _id: id(4), name: 'd', n: 2.5, state: 'success', tags: ['z'], other: 'str' },
  { _id: id(5), name: 'e', n: '2', state: null, created_at: null, tags: 'x', nested: 7 },
  { _id: id(6), name: 'f', state: 'failed', created_at: D('2026-01-02T00:00:00Z'), arr: [1, 2, 3], ref: id(2) },
]

beforeAll(async () => {
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_eval_parity')
  col = db.collection<Doc>('c')
  await col.deleteMany({})
  await col.insertMany(docs.map(d => cloneDoc(d)) as never)
})
afterAll(async () => { await db.dropDatabase(); await client.close() })

const ids = (ds: Doc[]) => ds.map(d => (d._id as ObjectId).toHexString()).sort()

const filters: Array<[string, Record<string, unknown>]> = [
  ['plain equality', { state: 'queued' }],
  ['null matches null AND missing', { nul: null }],
  ['null on missing-only field', { other2: null }],
  ['equality on array element', { tags: 'y' }],
  ['equality on whole array', { tags: ['y'] }],
  ['equality on empty array', { tags: [] }],
  ['scalar field holding string vs array query', { tags: 'x' }],
  ['$ne on field (missing/null/array)', { state: { $ne: 'queued' } }],
  ['$ne on array field', { tags: { $ne: 'y' } }],
  ['$ne null', { state: { $ne: null } }],
  ['$in', { state: { $in: ['queued', 'success'] } }],
  ['$in with null', { state: { $in: [null, 'running'] } }],
  ['$in on array field', { tags: { $in: ['z', 'q'] } }],
  ['$nin', { state: { $nin: ['queued', 'running'] } }],
  ['$lt number (type bracket: string "2" excluded)', { n: { $lt: 3 } }],
  ['$gte number', { n: { $gte: 2 } }],
  ['$gt string', { name: { $gt: 'b' } }],
  ['$lt date', { created_at: { $lt: D('2026-01-03T00:00:00Z') } }],
  ['$gte date range', { created_at: { $gte: D('2026-01-02T00:00:00Z'), $lte: D('2026-01-02T00:00:00Z') } }],
  ['$lt with null operand', { n: { $lt: null } }],
  ['$lte null', { nul: { $lte: null } }],
  ['$exists true', { other: { $exists: true } }],
  ['$exists false', { nul: { $exists: false } }],
  ['$exists true on null value', { nul: { $exists: true } }],
  ['dotted path', { 'nested.k': 1 }],
  ['dotted path through array of objects', { 'nested.arr.z': 2 }],
  ['dotted path numeric index', { 'arr.1': 2 }],
  ['dotted on scalar parent', { 'nested.k': 7 }],
  ['$or', { $or: [{ state: 'running' }, { n: 3 }] }],
  ['$and', { $and: [{ state: 'queued' }, { n: { $gt: 1 } }] }],
  ['$nor', { $nor: [{ state: 'queued' }, { state: 'running' }] }],
  ['implicit AND of several fields', { state: 'queued', n: { $gte: 3 } }],
  ['$not', { n: { $not: { $gt: 2 } } }],
  ['$regex', { name: { $regex: '^[ab]$' } }],
  ['$regex with options', { other: { $regex: 'STR', $options: 'i' } }],
  ['$size', { tags: { $size: 1 } }],
  ['$all', { tags: { $all: ['x', 'y'] } }],
  ['ObjectId equality', { _id: id(3) }],
  ['ObjectId $lt (tie-break pagination)', { _id: { $lt: id(3) } }],
  ['ObjectId $in', { _id: { $in: [id(1), id(6)] } }],
  ['ObjectId in a field', { ref: id(2) }],
  ['boolean', { flag: true }],
  ['boolean $ne', { flag: { $ne: true } }],
  ['cursor-style tuple filter', { $or: [{ created_at: { $lt: D('2026-01-03T00:00:00Z') } }, { created_at: D('2026-01-02T00:00:00Z'), _id: { $lt: id(6) } }] }],
  ['empty filter', {}],
]

describe('matchesFilter ≡ MongoDB', () => {
  it.each(filters)('%s', async (_name, filter) => {
    const mongo = await col.find(filter).toArray()
    const mine = docs.filter(d => matchesFilter(cloneDoc(d), filter))
    expect(ids(mine)).toEqual(ids(mongo))
  })
})

describe('applyUpdate ≡ MongoDB', () => {
  const updates: Array<[string, Record<string, unknown>, Record<string, unknown>]> = [
    ['$set scalar', { _id: id(1) }, { $set: { state: 'running' } }],
    ['$set no-op (unchanged)', { _id: id(1) }, { $set: { state: 'queued' } }],
    ['$set dotted creates nesting', { _id: id(2) }, { $set: { 'a.b.c': 1 } }],
    ['$set dotted into existing', { _id: id(1) }, { $set: { 'nested.k': 9 } }],
    ['$set replaces scalar parent', { _id: id(5) }, { $set: { 'nested.k': 1 } }],
    ['$set undefined stores null', { _id: id(1) }, { $set: { x: undefined } }],
    ['$unset', { _id: id(1) }, { $unset: { state: '' } }],
    ['$unset missing field (no-op)', { _id: id(1) }, { $unset: { nope: '' } }],
    ['$inc existing', { _id: id(1) }, { $inc: { n: 5 } }],
    ['$inc missing creates', { _id: id(2) }, { $inc: { cnt: 1 } }],
    ['$inc null treated as 0? (Mongo: error)', { _id: id(3) }, { $inc: { nul: 1 } }],
    ['$min', { _id: id(1) }, { $min: { n: 0 } }],
    ['$max no change', { _id: id(1) }, { $max: { n: 0 } }],
    ['$push', { _id: id(1) }, { $push: { tags: 'new' } }],
    ['$push $each', { _id: id(1) }, { $push: { tags: { $each: ['a', 'b'] } } }],
    ['$addToSet duplicate (no-op)', { _id: id(1) }, { $addToSet: { tags: 'x' } }],
    ['$addToSet new', { _id: id(1) }, { $addToSet: { tags: 'w' } }],
    ['$pull', { _id: id(1) }, { $pull: { tags: 'x' } }],
    ['$setOnInsert ignored on update', { _id: id(1) }, { $setOnInsert: { q: 1 } }],
    ['multiple operators', { _id: id(1) }, { $set: { state: 'done' }, $inc: { n: 1 }, $unset: { flag: '' } }],
  ]
  it.each(updates)('%s', async (_name, filter, update) => {
    const scratch = client.db('airflow_test_eval_parity').collection<Doc>('u')
    await scratch.deleteMany({})
    await scratch.insertMany(docs.map(d => cloneDoc(d)) as never)

    let mongoErr: string | null = null
    let res: { modifiedCount: number } | null = null
    try { res = await scratch.updateOne(filter, update as never) } catch (e) { mongoErr = (e as Error).message }

    const before = cloneDoc(docs.find(d => matchesFilter(d, filter))!)
    const after = cloneDoc(before)
    let myErr: string | null = null
    try { applyUpdate(after, update) } catch (e) { myErr = (e as Error).message }

    expect(myErr !== null).toBe(mongoErr !== null)   // both reject, or both accept
    if (mongoErr === null) {
      const mongoAfter = (await scratch.findOne({ _id: before._id } as never))!
      expect(after).toEqual(mongoAfter)
      expect(JSON.stringify(before) !== JSON.stringify(after)).toBe(res!.modifiedCount === 1)
    }
  })

  it('upsert seeds the document from the filter equality fields + $setOnInsert (and ignores operator conditions)', async () => {
    const scratch = client.db('airflow_test_eval_parity').collection<Doc>('up')
    await scratch.deleteMany({})
    const filter = { a: 1, 'b.c': 2, n: { $gt: 5 }, $or: [{ z: 1 }] }
    const update = { $set: { s: 1 }, $setOnInsert: { si: 2 } }
    await scratch.updateOne(filter, update, { upsert: true })
    const mongoDoc = (await scratch.findOne({}))!
    const mine: Doc = equalityFields(filter)
    applyUpdate(mine, update, { isInsert: true })
    const { _id, ...mongoRest } = mongoDoc
    expect(_id).toBeInstanceOf(ObjectId)
    expect(mine).toEqual(mongoRest)
  })
})

describe('sortDocs ≡ MongoDB', () => {
  const sorts: Array<[string, Record<string, 1 | -1>]> = [
    ['numeric asc (mixed types, missing)', { n: 1 }],
    ['numeric desc', { n: -1 }],
    ['date desc then _id desc (null/missing dates)', { created_at: -1, _id: -1 }],
    ['string asc', { name: 1 }],
    ['state asc with null/missing', { state: 1, _id: 1 }],
    ['nested path', { 'nested.k': 1, _id: 1 }],
  ]
  it.each(sorts)('%s', async (_name, spec) => {
    const mongo = (await col.find({}).sort(spec).toArray()).map(d => (d._id as ObjectId).toHexString())
    const mine = sortDocs(docs.map(d => cloneDoc(d)), spec).map(d => (d._id as ObjectId).toHexString())
    expect(mine).toEqual(mongo)
  })
})

describe('projectDoc ≡ MongoDB', () => {
  it.each([
    ['inclusion keeps _id', { name: 1, n: 1 }],
    ['inclusion without _id', { name: 1, _id: 0 }],
    ['exclusion', { tags: 0, nested: 0 }],
    ['nested inclusion', { 'nested.k': 1 }],
  ])('%s', async (_name, projection) => {
    const [mongo] = await col.find({ _id: id(1) }, { projection }).toArray()
    expect(projectDoc(cloneDoc(docs[0]), projection as never)).toEqual(mongo)
  })
})

describe('compareValues basics', () => {
  it('orders BSON types null < number < string < object < array < ObjectId < boolean < Date', () => {
    const order = [null, 1, 'a', { a: 1 }, [1], id(1), false, D('2026-01-01T00:00:00Z')]
    for (let i = 0; i < order.length - 1; i++) expect(compareValues(order[i], order[i + 1])).toBeLessThan(0)
  })
})
