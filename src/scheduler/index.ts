import { ObjectId, type Db } from 'mongodb'
import { loadDags } from '../dag/loader.js'
import { listDags } from '../dag/registry.js'
import { createRun } from './runs.js'
import { claimNextTask } from './claim.js'
import { executeTask } from './executor.js'

const POLL_INTERVAL_MS = 5_000

let timer: ReturnType<typeof setInterval> | null = null

export function startScheduler(db: Db): void {
  console.log(`[scheduler] starting — polling every ${POLL_INTERVAL_MS / 1000}s`)

  void tick(db)
  timer = setInterval(() => void tick(db), POLL_INTERVAL_MS)
}

export function stopScheduler(): void {
  if (timer) {
    clearInterval(timer)
    timer = null
    console.log('[scheduler] stopped')
  }
}

async function tick(db: Db): Promise<void> {
  try {
    await loadDags()
    const dags = listDags()

    for (const dag of dags) {
      if (!dag.schedule) continue
      await maybeCreateRun(db, dag.id)
    }

    // Advance all running dag_runs — claim + execute ready tasks
    const activeRuns = await db
      .collection('dag_runs')
      .find({ state: { $in: ['queued', 'running'] } })
      .toArray()

    for (const run of activeRuns) {
      await advanceRun(db, run._id.toString())
    }
  } catch (err) {
    console.error('[scheduler] tick error:', err)
  }
}

/**
 * Drive a single dag_run forward:
 * claim any ready tasks, execute them, then update run state when all done.
 */
export async function advanceRun(db: Db, dagRunId: string): Promise<void> {
  // Mark run as running if still queued
  await db.collection('dag_runs').updateOne(
    { _id: new ObjectId(dagRunId), state: 'queued' },
    { $set: { state: 'running' } }
  )

  // Keep claiming + executing until no more ready tasks
  let claimed = await claimNextTask(db, dagRunId)
  while (claimed) {
    await executeTask(db, claimed)
    claimed = await claimNextTask(db, dagRunId)
  }

  // Check overall run completion
  const tasks = await db.collection('task_instances').find({ dag_run_id: dagRunId }).toArray()
  const allDone = tasks.every(t => t.state === 'success' || t.state === 'failed')
  const anyFailed = tasks.some(t => t.state === 'failed')

  if (allDone) {
    const finalState = anyFailed ? 'failed' : 'success'
    await db.collection('dag_runs').updateOne(
      { _id: new ObjectId(dagRunId) },
      { $set: { state: finalState, ended_at: new Date() } }
    )
    console.log(`[scheduler] run ${dagRunId} → ${finalState}`)
  }
}

async function maybeCreateRun(db: Db, dagId: string): Promise<void> {
  const active = await db.collection('dag_runs').findOne({
    dag_id: dagId,
    state: { $in: ['queued', 'running'] },
  })
  if (active) return

  const dag = listDags().find(d => d.id === dagId)
  if (!dag) return
  await createRun(db, dag)
}
