/**
 * Keep this process's Dag registry in line with the serialized Dags the Dag processor wrote
 * (dag/processor.ts). Used by the scheduler on every tick and by the API — neither imports Dag files.
 * Only Dags whose serialized form changed are re-hydrated; import errors, warnings and provider
 * records come from the processor's state row.
 */
import type { Db } from 'mongodb'
import { register, unregister } from './registry.js'
import { hydrateDag } from './serialize.js'
import { setImportErrors, setDagWarnings } from './import-errors.js'
import { setSyncedProviders } from '../providers/registry.js'
import { PROCESSOR_STATE, SERIALIZED_DAGS, type ProcessorStateDoc, type SerializedDagDoc } from './processor.js'

/** dag_id → data_hash of what is registered. */
const synced = new Map<string, string>()

export interface SyncResult { updated: string[]; removed: string[] }

export async function syncDagsFromDb(db: Db): Promise<SyncResult> {
  const coll = db.collection<SerializedDagDoc>(SERIALIZED_DAGS)
  const heads = await coll.find({}, { projection: { dag_id: 1, data_hash: 1 } }).toArray()
  const stale = heads.filter(h => synced.get(h.dag_id) !== h.data_hash).map(h => h.dag_id)

  const updated: string[] = []
  if (stale.length) {
    for (const doc of await coll.find({ dag_id: { $in: stale } }).toArray()) {
      try {
        register(hydrateDag(doc.data, doc.fileloc))
        synced.set(doc.dag_id, doc.data_hash)
        updated.push(doc.dag_id)
      } catch (err) {
        console.error(`[dag-sync] cannot load serialized Dag '${doc.dag_id}': ${(err as Error).message}`)
      }
    }
  }

  const present = new Set(heads.map(h => h.dag_id))
  const removed = [...synced.keys()].filter(id => !present.has(id))
  for (const id of removed) { unregister(id); synced.delete(id) }

  const state = await db.collection<ProcessorStateDoc>(PROCESSOR_STATE).findOne({ _id: 'state' })
  setImportErrors(state?.import_errors ?? [])
  setDagWarnings(state?.warnings ?? [])
  setSyncedProviders(state?.providers ?? [])
  return { updated, removed }
}

/** Forget what was synced, so the next sync re-hydrates everything (tests). */
export function resetDagSync(): void {
  synced.clear()
}
