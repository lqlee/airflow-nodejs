/**
 * Dag parse child — forked by the Dag processor (dag/processor.ts). The only place the scheduler side
 * imports Dag files: user code runs here, in a short-lived process without database credentials, and
 * only serialized results go back.
 *
 * IPC:
 *   parent → child   { type: 'parse', dagsDir, files }
 *   child  → parent  { type: 'providers', records }
 *                    { type: 'start', file }                         before each file (per-file timeout)
 *                    { type: 'file', file, ok: true, dag } | { type: 'file', file, ok: false, error }
 *                    { type: 'done' }
 */
import { resolve } from 'node:path'
import { parseDagFile } from './loader.js'
import { serializeDag } from './serialize.js'
import { loadProviders } from '../providers/loader.js'
import { listProviders, type ProviderRecord } from '../providers/registry.js'
import type { DagWarning } from './warnings.js'

export interface ParsedDagMsg {
  dag_id: string
  version: string
  data: string
  source: string
  task_ids: string[]
  required_images: string[]
  warnings: DagWarning[]
}

export type ParseChildMsg =
  | { type: 'providers'; records: ProviderRecord[] }
  | { type: 'start'; file: string }
  | { type: 'file'; file: string; ok: true; dag: ParsedDagMsg }
  | { type: 'file'; file: string; ok: false; error: string }
  | { type: 'done' }

export interface ParseRequest { type: 'parse'; dagsDir: string; files: string[] }

function send(msg: ParseChildMsg): Promise<void> {
  return new Promise((done, fail) => process.send!(msg, (err: Error | null) => err ? fail(err) : done()))
}

async function parseAll(req: ParseRequest): Promise<void> {
  await loadProviders(req.dagsDir)
  await send({ type: 'providers', records: listProviders() })
  for (const file of req.files) {
    await send({ type: 'start', file })
    try {
      const { dag, source, version, warnings } = await parseDagFile(resolve(req.dagsDir, file))
      const data = serializeDag(dag)
      await send({
        type: 'file', file, ok: true,
        dag: {
          dag_id: dag.id, version, data, source, task_ids: Object.keys(dag.tasks),
          required_images: dag.requiredImages ?? [], warnings,
        },
      })
    } catch (err) {
      await send({ type: 'file', file, ok: false, error: err instanceof Error ? err.message : String(err) })
    }
  }
  await send({ type: 'done' })
}

process.on('message', (msg: ParseRequest) => {
  if (msg?.type !== 'parse') return
  parseAll(msg).then(
    () => process.exit(0),
    err => { console.error('[dag-parser] fatal:', err); process.exit(1) },
  )
})
