import { connectDb, closeDb } from '../db/client.js'
import { loadDags } from '../dag/loader.js'
import { listDags } from '../dag/registry.js'
import { createRun } from './runs.js'
import { ObjectId } from 'mongodb'

const db = await connectDb()

// Load dags
await loadDags()
const dag = listDags().find(d => d.id === 'hello_world')
if (!dag) throw new Error('hello_world dag not found')

// Create a run manually
const runId = await createRun(db, dag)

// Verify dag_run in DB
const run = await db.collection('dag_runs').findOne({ _id: new ObjectId(runId) })
console.log('\n✅ dag_run in DB:')
console.log(`   id:     ${run!._id}`)
console.log(`   dag_id: ${run!.dag_id}`)
console.log(`   state:  ${run!.state}`)

// Verify task_instances in DB
const tasks = await db.collection('task_instances').find({ dag_run_id: runId }).toArray()
console.log(`\n✅ task_instances in DB (${tasks.length}):`)
for (const t of tasks) {
  const deps = t.depends_on.length ? ` (depends_on: ${t.depends_on.join(', ')})` : ''
  console.log(`   • ${t.task_id} — state: ${t.state}${deps}`)
}

// Cleanup
await db.collection('dag_runs').deleteOne({ _id: new ObjectId(runId) })
await db.collection('task_instances').deleteMany({ dag_run_id: runId })
console.log('\n🧹 cleaned up test data')

await closeDb()
console.log('✅ Phase 3 complete')
