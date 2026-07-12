import type { Db } from 'mongodb'
import { loadDags } from '../dag/loader.js'
import { listDags } from '../dag/registry.js'
import { createRun } from './runs.js'

const POLL_INTERVAL_MS = 5_000

let timer: ReturnType<typeof setInterval> | null = null

export function startScheduler(db: Db): void {
  console.log(`[scheduler] starting — polling every ${POLL_INTERVAL_MS / 1000}s`)

  // Run immediately on start, then on interval
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
    // Reload Dags on every tick (picks up file changes)
    await loadDags()
    const dags = listDags()

    for (const dag of dags) {
      // Skip manually-triggered Dags (no schedule)
      if (!dag.schedule) continue
      await maybeCreateRun(db, dag.id)
    }
  } catch (err) {
    console.error('[scheduler] tick error:', err)
  }
}

/**
 * Check if a Dag is due for a new run.
 * For MVP: only create a run if there is no active (queued/running) run.
 */
async function maybeCreateRun(db: Db, dagId: string): Promise<void> {
  const active = await db.collection('dag_runs').findOne({
    dag_id: dagId,
    state: { $in: ['queued', 'running'] },
  })

  if (active) {
    console.log(`[scheduler] dag '${dagId}' already has an active run — skipping`)
    return
  }

  const dags = listDags()
  const dag = dags.find(d => d.id === dagId)
  if (!dag) return

  await createRun(db, dag)
}
