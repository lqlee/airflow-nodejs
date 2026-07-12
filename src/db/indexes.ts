import type { Db } from 'mongodb'

export async function ensureIndexes(db: Db): Promise<void> {
  // dag_runs: find runs by dag_id + state
  await db.collection('dag_runs').createIndexes([
    { key: { dag_id: 1, state: 1 } },
    { key: { run_after: 1 } },
  ])

  // task_instances: claim query (state + dag_run_id) + dependency checks
  await db.collection('task_instances').createIndexes([
    { key: { state: 1, dag_run_id: 1 } },
    { key: { dag_run_id: 1, task_id: 1 }, unique: true },
    { key: { dag_id: 1, state: 1 } },
  ])
}
