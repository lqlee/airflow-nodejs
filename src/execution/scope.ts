import type { TaskInstance } from '../scheduler/runs.js'
import type { TaskScope } from './token.js'

/** The execution-API scope of a task instance's current try. */
export function taskScope(ti: Pick<TaskInstance, 'dag_id' | 'dag_run_id' | 'task_id' | 'map_index' | 'try_number'>): TaskScope {
  return { dagId: ti.dag_id, runId: ti.dag_run_id, taskId: ti.task_id, mapIndex: ti.map_index ?? null, tryNumber: ti.try_number }
}
