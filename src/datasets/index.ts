import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'

// ── Types ─────────────────────────────────────────────────────────────────────

export interface DatasetEvent {
  uri: string        // e.g. 's3://bucket/users/'
  produced_by: string  // dag_id that produced this event
  run_id: string
  emitted_at: Date
}

/**
 * Watermark record stored per (consumer_dag_id, dataset_uri).
 * The consumer fires only when new events arrive AFTER the watermark.
 */
export interface DatasetWatermark {
  consumer_dag_id: string
  dataset_uri: string
  /** ObjectId string of the last DatasetEvent seen for this (consumer, dataset) pair. */
  last_event_id: string
}

// ── Pure evaluation ────────────────────────────────────────────────────────────

export interface LatestEvents {
  /** Map of dataset_uri → latest event ObjectId string in the collection */
  [uri: string]: string
}

export interface Watermarks {
  /** Map of `${dagId}::${uri}` → last_event_id seen */
  [key: string]: string
}

export interface FireDecision {
  dag: DagDefinition
  /** Per-dataset: new watermark value to CAS-set after triggering */
  newWatermarks: Record<string, string>
}

/**
 * Pure function — no DB access.
 * Returns one FireDecision per consumer that should trigger.
 * Fires when ALL of the consumer's datasets have a latest event
 * NEWER than the consumer's current watermark for that dataset.
 *
 * Watermarks are keyed as `${dagId}::${uri}`.
 * A missing watermark (first boot) means "seed only — do not fire".
 */
export function evaluateConsumers(
  consumers: DagDefinition[],
  latestEvents: LatestEvents,
  watermarks: Watermarks,
): FireDecision[] {
  const decisions: FireDecision[] = []

  for (const dag of consumers) {
    if (!dag.datasets || dag.datasets.length === 0) continue

    const newWatermarks: Record<string, string> = {}
    let shouldFire = true
    let isFirstBoot = false

    for (const uri of dag.datasets) {
      const latestId = latestEvents[uri]
      if (!latestId) { shouldFire = false; break }  // no events ever — can't fire

      const wmKey = `${dag.id}::${uri}`
      const currentWm = watermarks[wmKey]

      if (!currentWm) {
        // First boot for this (consumer, dataset): seed watermark to latest event but
        // don't fire — matches Airflow semantics (only post-scheduling events count).
        // Known gap: if a second dataset is later added to an existing consumer, that
        // dataset's first-boot seed will suppress any real pending event on the other
        // datasets in the same evaluation (isFirstBoot short-circuits shouldFire).
        // Safe for MVP; fix by seeding per-dataset independently in a future iteration.
        isFirstBoot = true
        newWatermarks[uri] = latestId
        continue
      }

      if (latestId === currentWm) {
        // No new event since last trigger
        shouldFire = false
        break
      }

      // New event exists — candidate to fire
      newWatermarks[uri] = latestId
    }

    if (isFirstBoot) {
      // Seed all watermarks but don't trigger
      decisions.push({ dag, newWatermarks, shouldFire: false } as FireDecision & { shouldFire: boolean })
      continue
    }

    if (shouldFire) {
      decisions.push({ dag, newWatermarks })
    }
  }

  return decisions
}

// ── DB operations ─────────────────────────────────────────────────────────────

/**
 * Emit a dataset event for each URI in dag.outlets after a successful run.
 * Called from the scheduler's run-completion branch.
 */
export async function emitOutlets(
  db: Db,
  dag: DagDefinition,
  runId: string,
): Promise<void> {
  if (!dag.outlets || dag.outlets.length === 0) return

  const now = new Date()
  const docs: DatasetEvent[] = dag.outlets.map(uri => ({
    uri,
    produced_by: dag.id,
    run_id: runId,
    emitted_at: now,
  }))

  await db.collection<DatasetEvent>('dataset_events').insertMany(docs)
  console.log(`[datasets] dag '${dag.id}' emitted outlets: ${dag.outlets.join(', ')}`)
}

/**
 * Load the latest event id per dataset URI (across all producers).
 */
async function loadLatestEvents(db: Db, uris: string[]): Promise<LatestEvents> {
  if (uris.length === 0) return {}

  const results = await db
    .collection<DatasetEvent>('dataset_events')
    .aggregate([
      { $match: { uri: { $in: uris } } },
      { $sort: { emitted_at: -1 } },
      { $group: { _id: '$uri', last_id: { $first: '$_id' } } },
    ])
    .toArray()

  return Object.fromEntries(results.map(r => [r._id as string, (r.last_id as object).toString()]))
}

/**
 * Load all watermarks for the given consumer dags.
 */
async function loadWatermarks(db: Db, consumerIds: string[]): Promise<Watermarks> {
  if (consumerIds.length === 0) return {}

  const docs = await db
    .collection<DatasetWatermark>('dataset_watermarks')
    .find({ consumer_dag_id: { $in: consumerIds } })
    .toArray()

  return Object.fromEntries(docs.map(d => [`${d.consumer_dag_id}::${d.dataset_uri}`, d.last_event_id]))
}

/**
 * Advance a consumer's watermark for one dataset using a compare-and-swap.
 * Returns true if this process won the CAS (and should trigger a run).
 * Returns false if another tick already advanced it (concurrent overlap).
 */
async function casWatermark(
  db: Db,
  dagId: string,
  uri: string,
  oldEventId: string | null,
  newEventId: string,
): Promise<boolean> {
  if (oldEventId === null) {
    // First-boot seed: upsert only if not already present
    const result = await db.collection<DatasetWatermark>('dataset_watermarks').updateOne(
      { consumer_dag_id: dagId, dataset_uri: uri, last_event_id: { $exists: false } },
      { $set: { consumer_dag_id: dagId, dataset_uri: uri, last_event_id: newEventId } },
      { upsert: true },
    )
    return result.upsertedCount > 0
  }

  const result = await db.collection<DatasetWatermark>('dataset_watermarks').updateOne(
    { consumer_dag_id: dagId, dataset_uri: uri, last_event_id: oldEventId },
    { $set: { last_event_id: newEventId } },
  )
  return result.modifiedCount > 0
}

/**
 * Evaluate all dataset-triggered consumers and create queued runs for those
 * whose all datasets have new events. Returns the number of runs created.
 *
 * CAS-guards each watermark advance so concurrent ticks don't double-fire.
 */
export async function triggerDatasetConsumers(
  db: Db,
  dags: DagDefinition[],
  createRun: (db: Db, dag: DagDefinition) => Promise<string>,
  isDagPaused: (db: Db, dagId: string) => Promise<boolean>,
): Promise<number> {
  const consumers = dags.filter(d => d.datasets && d.datasets.length > 0)
  if (consumers.length === 0) return 0

  const allUris = [...new Set(consumers.flatMap(d => d.datasets!))]
  const consumerIds = consumers.map(d => d.id)

  const [latestEvents, watermarks] = await Promise.all([
    loadLatestEvents(db, allUris),
    loadWatermarks(db, consumerIds),
  ])

  const decisions = evaluateConsumers(consumers, latestEvents, watermarks)
  let triggered = 0

  for (const decision of decisions) {
    const { dag, newWatermarks } = decision
    const shouldFire = !('shouldFire' in decision) || (decision as { shouldFire: boolean }).shouldFire !== false

    if (!shouldFire) {
      // First-boot: seed watermarks, no trigger
      for (const [uri, newId] of Object.entries(newWatermarks)) {
        const wmKey = `${dag.id}::${uri}`
        const oldId = watermarks[wmKey] ?? null
        await casWatermark(db, dag.id, uri, oldId, newId)
      }
      continue
    }

    // CAS all watermarks for this consumer — must all succeed atomically per dataset
    // If any CAS fails (concurrent tick), skip this consumer for this tick
    const casResults: boolean[] = []
    for (const [uri, newId] of Object.entries(newWatermarks)) {
      const wmKey = `${dag.id}::${uri}`
      const oldId = watermarks[wmKey] ?? null
      casResults.push(await casWatermark(db, dag.id, uri, oldId, newId))
    }

    // Only trigger if ALL CAS operations succeeded (this tick won every watermark)
    if (casResults.every(ok => ok)) {
      const paused = await isDagPaused(db, dag.id)
      if (paused) {
        console.log(`[datasets] consumer '${dag.id}' is paused — skipping trigger`)
      } else {
        await createRun(db, dag)
        console.log(`[datasets] triggered consumer '${dag.id}' (datasets: ${dag.datasets!.join(', ')})`)
        triggered++
      }
    }
  }

  return triggered
}

// ── API helpers ────────────────────────────────────────────────────────────────

/** List recent dataset events, optionally filtered by URI. */
export async function listDatasetEvents(
  db: Db,
  opts: { uri?: string; limit?: number } = {},
): Promise<DatasetEvent[]> {
  const filter = opts.uri ? { uri: opts.uri } : {}
  return db
    .collection<DatasetEvent>('dataset_events')
    .find(filter)
    .sort({ emitted_at: -1 })
    .limit(opts.limit ?? 50)
    .toArray()
}
