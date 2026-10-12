/**
 * Backend contract: the SAME behaviour must hold on MongoDB and on Spanner (emulator).
 * Run the Spanner half with:  SPANNER_EMULATOR_HOST=localhost:9010 npx vitest run src/db/spanner
 */
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest'
import { MongoClient, ObjectId, type Db } from 'mongodb'
import { SpannerClient } from '../connect.js'
import { ensureIndexes } from '../../indexes.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const HAVE_EMULATOR = Boolean(process.env.SPANNER_EMULATOR_HOST)

type Opened = { db: Db; close: () => Promise<void> }
const backends: Array<[string, (ns: string) => Promise<Opened>, boolean]> = [
  ['mongo', async (ns) => { const c = new MongoClient(MONGO_URL); await c.connect(); return { db: c.db(ns), close: () => c.close() } }, true],
  ['spanner', async (ns) => { const c = new SpannerClient(); await c.connect(); return { db: c.db(ns) as unknown as Db, close: () => c.close() } }, HAVE_EMULATOR],
]

describe.each(backends)('%s backend contract', (name, open, enabled) => {
  const d = enabled ? describe : describe.skip
  d('collection semantics', () => {
    let o: Opened
    let db: Db
    beforeAll(async () => { o = await open(`contract_${name}_${process.pid}`); db = o.db; await ensureIndexes(db) }, 120_000)
    afterAll(async () => { await db.dropDatabase(); await o.close() }, 60_000)
    // Clear per-collection instead of dropDatabase: dropping + recreating ~30 WiredTiger tables per test exhausts mongod's fd limit
    beforeEach(async () => { for (const c of ['task_instances', 'dag_runs', 'dataset_watermarks', 'dataset_events', 'variables']) await db.collection(c).deleteMany({}) }, 60_000)

    const ti = (over: Record<string, unknown> = {}) => ({
      dag_run_id: 'r1', dag_id: 'd', task_id: 't1', map_index: null, state: 'queued', priority: 0, depends_on: [], created_at: new Date('2026-01-01T00:00:00Z'), ...over,
    })

    it('insertOne assigns an ObjectId and findOne round-trips types (Date, null, nested, arrays)', async () => {
      const col = db.collection('task_instances')
      const res = await col.insertOne(ti({ nested: { a: [1, { b: 2 }] } }))
      expect(res.insertedId).toBeInstanceOf(ObjectId)
      const got = await col.findOne({ _id: res.insertedId })
      expect(got).toMatchObject({ dag_run_id: 'r1', map_index: null, nested: { a: [1, { b: 2 }] } })
      expect(got!.created_at).toBeInstanceOf(Date)
      expect((got!.created_at as Date).toISOString()).toBe('2026-01-01T00:00:00.000Z')
      expect(got!._id).toBeInstanceOf(ObjectId)
    })

    it('insertMany + find with sort/limit/skip/projection', async () => {
      const col = db.collection('task_instances')
      await col.insertMany([1, 2, 3, 4, 5].map(i => ti({ task_id: `t${i}`, priority: i, map_index: null })))
      const top = await col.find({ dag_run_id: 'r1' }).sort({ priority: -1 }).limit(2).toArray()
      expect(top.map(t => t.task_id)).toEqual(['t5', 't4'])
      const page = await col.find({}).sort({ priority: 1 }).skip(1).limit(2).project({ task_id: 1, _id: 0 }).toArray()
      expect(page).toEqual([{ task_id: 't2' }, { task_id: 't3' }])
    })

    it('equality / $in / range / $ne / null / array-contains filters', async () => {
      const col = db.collection('task_instances')
      await col.insertMany([
        ti({ task_id: 'a', state: 'queued', tags: ['x'] }), ti({ task_id: 'b', state: 'running', tags: ['y'] }),
        ti({ task_id: 'c', state: 'success', next_poke_at: new Date('2026-02-01T00:00:00Z') }),
      ])
      const ids = async (f: Record<string, unknown>) => (await col.find(f).sort({ task_id: 1 }).toArray()).map(t => t.task_id)
      expect(await ids({ state: 'queued' })).toEqual(['a'])
      expect(await ids({ state: { $in: ['queued', 'success'] } })).toEqual(['a', 'c'])
      expect(await ids({ state: { $ne: 'queued' } })).toEqual(['b', 'c'])
      expect(await ids({ next_poke_at: null })).toEqual(['a', 'b'])
      expect(await ids({ next_poke_at: { $lte: new Date('2026-02-01T00:00:00Z') } })).toEqual(['c'])
      expect(await ids({ tags: 'y' })).toEqual(['b'])
      expect(await ids({ $or: [{ state: 'running' }, { task_id: 'c' }] })).toEqual(['b', 'c'])
    })

    it('countDocuments', async () => {
      const col = db.collection('task_instances')
      await col.insertMany([ti({ task_id: 'a' }), ti({ task_id: 'b', state: 'running' }), ti({ task_id: 'c', tags: ['q'] })])
      expect(await col.countDocuments({})).toBe(3)
      expect(await col.countDocuments({ state: 'queued' })).toBe(2)
      expect(await col.countDocuments({ tags: 'q' })).toBe(1)
    })

    it('updateOne reports matched/modified; a no-op $set is not "modified"', async () => {
      const col = db.collection('task_instances')
      await col.insertOne(ti())
      const r1 = await col.updateOne({ task_id: 't1' }, { $set: { state: 'running' } })
      expect([r1.matchedCount, r1.modifiedCount]).toEqual([1, 1])
      const r2 = await col.updateOne({ task_id: 't1' }, { $set: { state: 'running' } })
      expect([r2.matchedCount, r2.modifiedCount]).toEqual([1, 0])
      const r3 = await col.updateOne({ task_id: 'nope' }, { $set: { state: 'x' } })
      expect([r3.matchedCount, r3.modifiedCount]).toEqual([0, 0])
    })

    it('$inc and $set on promoted columns keep SQL filters in sync with the document', async () => {
      const col = db.collection('task_instances')
      await col.insertOne(ti({ try_number: 0 }))
      await col.updateOne({ task_id: 't1' }, { $set: { state: 'success' }, $inc: { try_number: 1 } })
      expect(await col.countDocuments({ state: 'queued' })).toBe(0)
      expect(await col.countDocuments({ state: 'success' })).toBe(1)
      expect((await col.findOne({ task_id: 't1' }))!.try_number).toBe(1)
    })

    it('upsert inserts once, then updates; $setOnInsert only on insert', async () => {
      const col = db.collection('dataset_watermarks')
      const f = { consumer_dag_id: 'c', dataset_uri: 'u' }
      const a = await col.updateOne(f, { $set: { last: 1 }, $setOnInsert: { first: 1 } }, { upsert: true })
      expect(a.upsertedCount).toBe(1)
      const b = await col.updateOne(f, { $set: { last: 2 }, $setOnInsert: { first: 99 } }, { upsert: true })
      expect(b.upsertedCount).toBe(0)
      expect(await col.countDocuments({})).toBe(1)
      expect(await col.findOne(f)).toMatchObject({ consumer_dag_id: 'c', dataset_uri: 'u', last: 2, first: 1 })
    })

    it('updateMany modifies every match', async () => {
      const col = db.collection('task_instances')
      await col.insertMany([ti({ task_id: 'a' }), ti({ task_id: 'b' }), ti({ task_id: 'c', state: 'success' })])
      const r = await col.updateMany({ state: 'queued' }, { $set: { state: 'cancelled' } })
      expect([r.matchedCount, r.modifiedCount]).toEqual([2, 2])
      expect(await col.countDocuments({ state: 'cancelled' })).toBe(2)
    })

    it('findOneAndUpdate returns the document (after/before) or null, and is atomic under contention', async () => {
      const col = db.collection('task_instances')
      await col.insertOne(ti())
      const after = await col.findOneAndUpdate({ task_id: 't1', state: 'queued' }, { $set: { state: 'running' } }, { returnDocument: 'after' })
      expect(after).toMatchObject({ task_id: 't1', state: 'running' })
      expect(await col.findOneAndUpdate({ task_id: 't1', state: 'queued' }, { $set: { state: 'running' } }, { returnDocument: 'after' })).toBeNull()

      await col.insertOne(ti({ task_id: 'race' }))
      const claims = await Promise.all(Array.from({ length: 6 }, () =>
        col.findOneAndUpdate({ task_id: 'race', state: 'queued' }, { $set: { state: 'running' } }, { returnDocument: 'after' })))
      expect(claims.filter(Boolean)).toHaveLength(1)   // exactly one claimer wins
    })

    it('findOneAndUpdate honours sort', async () => {
      const col = db.collection('task_instances')
      await col.insertMany([ti({ task_id: 'lo', priority: 1 }), ti({ task_id: 'hi', priority: 9 })])
      const got = await col.findOneAndUpdate({ state: 'queued' }, { $set: { state: 'running' } }, { sort: { priority: -1 }, returnDocument: 'after' })
      expect(got!.task_id).toBe('hi')
    })

    it('unique index: duplicate insert fails with code 11000 (incl. NULL map_index)', async () => {
      const col = db.collection('task_instances')
      await col.insertOne(ti())
      await expect(col.insertOne(ti())).rejects.toMatchObject({ code: 11000 })
      await col.insertOne(ti({ map_index: 0 }))              // different map_index → fine
      await expect(col.insertOne(ti({ map_index: 0 }))).rejects.toMatchObject({ code: 11000 })
    })

    it('unique index: a racing upsert does not create two documents', async () => {
      const col = db.collection('dataset_watermarks')
      const f = { consumer_dag_id: 'c', dataset_uri: 'u' }
      const results = await Promise.allSettled(Array.from({ length: 5 }, () => col.updateOne(f, { $set: { n: 1 } }, { upsert: true })))
      expect(await col.countDocuments(f)).toBe(1)
      for (const r of results) if (r.status === 'rejected') expect((r.reason as { code?: number }).code).toBe(11000)
    })

    it('deleteOne / deleteMany report counts', async () => {
      const col = db.collection('task_instances')
      await col.insertMany([ti({ task_id: 'a' }), ti({ task_id: 'b' }), ti({ task_id: 'c', dag_run_id: 'r2' })])
      expect((await col.deleteOne({ task_id: 'a' })).deletedCount).toBe(1)
      expect((await col.deleteMany({ dag_run_id: 'r1' })).deletedCount).toBe(1)
      expect((await col.deleteMany({})).deletedCount).toBe(1)
      expect(await col.countDocuments({})).toBe(0)
    })

    it('string _id documents work (variables keyed by name)', async () => {
      const col = db.collection('variables')
      await col.insertOne({ _id: 'my_var' as never, key: 'my_var', value: 'v' })
      expect(await col.findOne({ _id: 'my_var' as never })).toMatchObject({ value: 'v' })
      await expect(col.insertOne({ _id: 'my_var' as never, key: 'other' })).rejects.toMatchObject({ code: 11000 })
    })

    it('_id range filters + (created_at, _id) tuple pagination match across backends', async () => {
      const col = db.collection('dag_runs')
      const t = new Date('2026-03-01T00:00:00Z')
      const ids = [1, 2, 3, 4].map(n => new ObjectId(n.toString(16).padStart(24, '0')))
      await col.insertMany(ids.map(_id => ({ _id, dag_id: 'd', state: 'success', created_at: t })) as never)
      const page1 = await col.find({ dag_id: 'd' }).sort({ created_at: -1, _id: -1 }).limit(2).toArray()
      expect(page1.map(r => r._id.toHexString().slice(-1))).toEqual(['4', '3'])
      const last = page1[1]
      const page2 = await col.find({ dag_id: 'd', $or: [{ created_at: { $lt: t } }, { created_at: t, _id: { $lt: last._id } }] })
        .sort({ created_at: -1, _id: -1 }).limit(2).toArray()
      expect(page2.map(r => r._id.toHexString().slice(-1))).toEqual(['2', '1'])
    })

    it('aggregate: $match/$sort/$group with $first (latest event per uri)', async () => {
      const col = db.collection('dataset_events')
      await col.insertMany([
        { uri: 'a', emitted_at: new Date('2026-01-01T00:00:00Z') }, { uri: 'a', emitted_at: new Date('2026-01-03T00:00:00Z') },
        { uri: 'b', emitted_at: new Date('2026-01-02T00:00:00Z') }, { uri: 'c', emitted_at: new Date('2026-01-09T00:00:00Z') },
      ])
      const rows = await col.aggregate([
        { $match: { uri: { $in: ['a', 'b'] } } }, { $sort: { emitted_at: -1 } }, { $group: { _id: '$uri', last_id: { $first: '$_id' } } },
      ]).toArray()
      expect(rows.map(r => r._id).sort()).toEqual(['a', 'b'])
      const a = await col.findOne({ uri: 'a', emitted_at: new Date('2026-01-03T00:00:00Z') })
      expect(rows.find(r => r._id === 'a')!.last_id.toString()).toBe(a!._id.toString())
    })

    it('aggregate: $group on a compound _id with $sum (per dag/state counts)', async () => {
      const col = db.collection('dag_runs')
      await col.insertMany([
        { dag_id: 'x', state: 'queued' }, { dag_id: 'x', state: 'queued' },
        { dag_id: 'x', state: 'failed' }, { dag_id: 'y', state: 'queued' },
      ])
      const rows = await col.aggregate([{ $group: { _id: { dag_id: '$dag_id', state: '$state' }, n: { $sum: 1 } } }]).toArray()
      const got = rows.map(r => `${r._id.dag_id}/${r._id.state}=${r.n}`).sort()
      expect(got).toEqual(['x/failed=1', 'x/queued=2', 'y/queued=1'])
    })

    it('dropDatabase only clears its own namespace', async () => {
      await db.collection('variables').insertOne({ key: 'k', value: 1 })
      const other = await open(`contract_${name}_${process.pid}_other`)
      try {
        await other.db.collection('variables').insertOne({ key: 'k', value: 2 })
        await db.dropDatabase()
        expect(await db.collection('variables').countDocuments({})).toBe(0)
        expect(await other.db.collection('variables').countDocuments({})).toBe(1)
      } finally { await other.db.dropDatabase(); await other.close() }
    })

    it('command ping and listCollections', async () => {
      await db.command({ ping: 1 })
      await db.collection('variables').insertOne({ key: 'x' })
      expect((await db.listCollections().toArray()).map(c => c.name)).toContain('variables')
    })

    it('cursor: next / forEach / async iteration', async () => {
      const col = db.collection('task_instances')
      await col.insertMany([ti({ task_id: 'a' }), ti({ task_id: 'b' })])
      const seen: string[] = []
      await col.find({}).sort({ task_id: 1 }).forEach(d => { seen.push(d.task_id) })
      expect(seen).toEqual(['a', 'b'])
      const c = col.find({}).sort({ task_id: 1 })
      expect((await c.next())!.task_id).toBe('a')
      expect((await c.next())!.task_id).toBe('b')
      expect(await c.next()).toBeNull()
    })
  })
})
