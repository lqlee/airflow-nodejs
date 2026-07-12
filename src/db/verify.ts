import { connectDb, closeDb } from './client.js'
import { ensureIndexes } from './indexes.js'

const db = await connectDb()
await db.command({ ping: 1 })
console.log('✅ MongoDB connected to:', db.databaseName)

await ensureIndexes(db)
console.log('✅ Indexes created')

const cols = await db.listCollections().toArray()
console.log('Collections:', cols.length ? cols.map(c => c.name) : '(none yet — expected)')

await closeDb()
console.log('✅ Phase 1 complete')
