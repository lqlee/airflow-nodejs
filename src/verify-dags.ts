import { loadDags } from './dag/loader.js'
import { connectDb, closeDb } from './db/client.js'
import { createRun } from './scheduler/runs.js'
import { advanceRun } from './scheduler/index.js'
import { listDags } from './dag/registry.js'
import { getTaskLogs } from './logs/index.js'
import { ObjectId } from 'mongodb'

const db = await connectDb()
await loadDags()

const dags = listDags()
console.log(`\nLoaded ${dags.length} dags:`, dags.map(d => `${d.id} (${d.schedule ?? 'manual'})`))

// Test daily_etl
const etl = dags.find(d => d.id === 'daily_etl')!
console.log('\n=== daily_etl ===')
const etlRunId = await createRun(db, etl)
await advanceRun(db, etlRunId)

const etlTasks = await db.collection('task_instances').find({ dag_run_id: etlRunId }).sort({ created_at: 1 }).toArray()
for (const t of etlTasks) console.log(`  ${t.task_id.padEnd(16)} ${t.state}`)
const notifyLogs = await getTaskLogs(db, etlRunId, 'notify')
notifyLogs.forEach(l => console.log(' ', l.line))

// Test data_quality
const dq = dags.find(d => d.id === 'data_quality')!
console.log('\n=== data_quality ===')
const dqRunId = await createRun(db, dq)
await advanceRun(db, dqRunId)

const dqTasks = await db.collection('task_instances').find({ dag_run_id: dqRunId }).sort({ created_at: 1 }).toArray()
for (const t of dqTasks) console.log(`  ${t.task_id.padEnd(18)} ${t.state}`)
const reportLogs = await getTaskLogs(db, dqRunId, 'report')
reportLogs.forEach(l => console.log(' ', l.line))

// Cleanup
await db.collection('dag_runs').deleteMany({ _id: { $in: [new ObjectId(etlRunId), new ObjectId(dqRunId)] } })
await db.collection('task_instances').deleteMany({ dag_run_id: { $in: [etlRunId, dqRunId] } })
await db.collection('task_logs').deleteMany({ dag_run_id: { $in: [etlRunId, dqRunId] } })
await db.collection('xcoms').deleteMany({ dag_run_id: { $in: [etlRunId, dqRunId] } })

await closeDb()
console.log('\n✅ all done')
