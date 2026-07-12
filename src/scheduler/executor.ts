import { fork } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { resolve as pathResolve, dirname } from 'node:path'
import type { Db } from 'mongodb'
import type { TaskInstance } from './runs.js'
import { getDag } from '../dag/registry.js'

const __dirname = dirname(fileURLToPath(import.meta.url))
const WORKER_SCRIPT = pathResolve(__dirname, 'worker.ts')
const TSX_BIN = pathResolve(__dirname, '../../node_modules/.bin/tsx')

type WorkerMsg = { type: 'done'; success: boolean; error?: string }

/**
 * Execute a claimed task in a child process.
 * Worker handles XCom directly via its own MongoDB connection — no IPC for data.
 */
export async function executeTask(db: Db, ti: TaskInstance): Promise<void> {
  const dag = getDag(ti.dag_id)
  if (!dag) {
    await markFailed(db, ti, `Dag '${ti.dag_id}' not found in registry`)
    return
  }

  const taskDef = dag.tasks[ti.task_id]
  if (!taskDef) {
    await markFailed(db, ti, `Task '${ti.task_id}' not found in dag '${ti.dag_id}'`)
    return
  }

  console.log(`[executor] running ${ti.dag_id}.${ti.task_id} (run: ${ti.dag_run_id})`)

  return new Promise((done) => {
    const child = fork(WORKER_SCRIPT, [], {
      execPath: TSX_BIN,
      env: { ...process.env },
    })

    child.send({
      type: 'run',
      fn: taskDef.run.toString(),
      ctx: { dagId: ti.dag_id, runId: ti.dag_run_id, taskId: ti.task_id },
    })

    child.on('message', async (msg: WorkerMsg) => {
      if (msg.type !== 'done') return
      if (msg.success) {
        await markSuccess(db, ti)
        console.log(`[executor] ✓ ${ti.dag_id}.${ti.task_id}`)
      } else {
        await markFailed(db, ti, msg.error ?? 'unknown error')
        console.error(`[executor] ✗ ${ti.dag_id}.${ti.task_id}: ${msg.error}`)
      }
      done()
    })

    child.on('error', async (err) => {
      await markFailed(db, ti, err.message)
      done()
    })

    child.on('exit', (code) => {
      if (code !== 0 && code !== null) {
        console.error(`[executor] worker exited with code ${code} for ${ti.task_id}`)
      }
    })
  })
}

async function markSuccess(db: Db, ti: TaskInstance): Promise<void> {
  await db.collection('task_instances').updateOne(
    { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
    { $set: { state: 'success', ended_at: new Date() } }
  )
}

async function markFailed(db: Db, ti: TaskInstance, error: string): Promise<void> {
  await db.collection('task_instances').updateOne(
    { dag_run_id: ti.dag_run_id, task_id: ti.task_id },
    { $set: { state: 'failed', ended_at: new Date(), error } }
  )
}
