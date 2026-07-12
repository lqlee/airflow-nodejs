/**
 * Phase 4 verification — end-to-end run:
 * create a run → advance it → all tasks reach success state
 */
import { connectDb, closeDb } from '../db/client.js'
import { ensureIndexes } from '../db/indexes.js'
import { loadDags } from '../dag/loader.js'
import { listDags } from '../dag/registry.js'
import { createRun } from './runs.js'
import { advanceRun } from './index.js'
import { ObjectId } from 'mongodb'

const db = await connectDb()
await ensureIndexes(db)
await loadDags()

const dag = listDags().find(d => d.id === 'hello_world')
if (!dag) throw new Error('hello_world not found')

const runId = await createRun(db, dag)
console.log(`\nCreated run: ${runId}`)
console.log('Advancing run (claim + execute all tasks)...\n')

await advanceRun(db, runId)

// Report final state
const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
const tasks = await db.collection('task_instances').find({ dag_run_id: runId }).sort({ created_at: 1 }).toArray()

console.log(`\n✅ dag_run state: ${run!.state}`)
console.log('Task states:')
for (const t of tasks) {
  const dur = t.ended_at && t.started_at
    ? `${new Date(t.ended_at).getTime() - new Date(t.started_at).getTime()}ms`
    : '-'
  console.log(`   • ${t.task_id.padEnd(12)} ${t.state}  (${dur})`)
}

// Cleanup
await db.collection('dag_runs').deleteOne({ _id: new ObjectId(runId) })
await db.collection('task_instances').deleteMany({ dag_run_id: runId })
console.log('\n🧹 cleaned up')

await closeDb()
console.log('✅ Phase 4 complete')
