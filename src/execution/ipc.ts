/**
 * Execution API over IPC — for workers forked by a process that holds the database (the local
 * executor, a BullMQ consumer). The parent answers the child's requests within the child's task scope;
 * the child itself gets no database credentials (sandbox/env.ts).
 *
 *   child  → parent  { type: 'exec', id, op, args }
 *   parent → child   { type: 'exec-result', id, ok, value?, error?, status? }
 */
import type { ChildProcess } from 'node:child_process'
import type { Db } from 'mongodb'
import { handleExecutionOp, ExecutionApiError, EXECUTION_OPS, type ExecutionOp } from './handlers.js'
import type { TaskScope } from './token.js'

export interface ExecRequest { type: 'exec'; id: number; op: ExecutionOp; args: unknown[] }
export interface ExecResult { type: 'exec-result'; id: number; ok: boolean; value?: unknown; error?: string; status?: number }

export function serveExecutionApi(child: ChildProcess, db: Db, scope: TaskScope): void {
  child.on('message', (msg: ExecRequest) => {
    if (msg?.type !== 'exec') return
    const reply = (r: Omit<ExecResult, 'type' | 'id'>) => {
      if (child.connected) child.send({ type: 'exec-result', id: msg.id, ...r } satisfies ExecResult)
    }
    if (!EXECUTION_OPS.includes(msg.op)) return reply({ ok: false, status: 404, error: `unknown operation '${String(msg.op)}'` })
    handleExecutionOp(db, scope, msg.op, Array.isArray(msg.args) ? msg.args : []).then(
      value => reply({ ok: true, value }),
      err => reply({ ok: false, status: err instanceof ExecutionApiError ? err.status : 500, error: (err as Error).message }),
    )
  })
}
