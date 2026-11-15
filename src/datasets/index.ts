import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
import { normalizeCondition, conditionUris, conditionAliases, evaluateCondition, describeCondition } from './conditions.js'

// ── Types ─────────────────────────────────────────────────────────────────────

export interface DatasetEvent {
  uri: string        // e.g. 's3://bucket/users/'
  produced_by: string  // dag_id that produced this event
  run_id: string
  emitted_at: Date
  /** Task that emitted it at runtime (ctx.emitDataset); absent for Dag outlets */
  task_id?: string
  /** Alias it was emitted through */
  source_alias?: string
  extra?: Record<string, unknown>
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

/** What aliases resolve to, and when consumers started watching them. */
export interface AliasState {
  /** alias name → datasets attached to it at runtime (ctx.emitDataset(uri, { alias })) */
  links: Record<string, Array<{ uri: string; linkedAt: Date }>>
  /** dataset uri → time of its latest event */
  latestAt: Record<string, Date>
  /** `${dagId}::alias::${name}` → when the consumer started watching the alias */
  since: Record<string, Date>
}

const EMPTY_ALIASES: AliasState = { links: {}, latestAt: {}, since: {} }
export const aliasKey = (dagId: string, alias: string) => `${dagId}::alias::${alias}`

export interface FireDecision {
  dag: DagDefinition
  /** Per-dataset watermark to CAS-set: consumed events (when firing) and first-sight seeds */
  newWatermarks: Record<string, string>
  /** false = only seed watermarks; absent/true = trigger a run */
  shouldFire?: boolean
  /** Datasets whose events this run consumes — every one of their CASes must win to trigger */
  consumed?: string[]
  /** Aliases this consumer sees for the first time: events from now on count */
  aliasSeeds?: string[]
}

/**
 * Pure function — no DB access. One decision per consumer that should trigger or seed.
 *
 * A dataset is *ready* when its latest event is newer than the consumer's watermark. The first time a
 * consumer sees a dataset it only seeds the watermark (events from before it was scheduled do not
 * count, as in Airflow). A dataset reached through an alias counts once an event arrives after the
 * consumer started watching the alias. When the condition (all / any / alias) holds, the run consumes
 * every ready dataset — many queued events still make one run.
 */
export function evaluateConsumers(
  consumers: DagDefinition[],
  latestEvents: LatestEvents,
  watermarks: Watermarks,
  aliases: AliasState = EMPTY_ALIASES,
): FireDecision[] {
  const decisions: FireDecision[] = []

  for (const dag of consumers) {
    const cond = normalizeCondition(dag.datasets)
    if (!cond) continue

    const seeds: Record<string, string> = {}
    const ready = new Set<string>()
    const direct = conditionUris(cond)
    for (const uri of direct) {
      const latest = latestEvents[uri]
      if (!latest) continue
      const wm = watermarks[`${dag.id}::${uri}`]
      if (!wm) seeds[uri] = latest
      else if (wm !== latest) ready.add(uri)
    }

    const aliasSeeds: string[] = []
    const aliasUris: Record<string, string[]> = {}
    for (const name of conditionAliases(cond)) {
      const links = aliases.links[name] ?? []
      aliasUris[name] = links.map(l => l.uri)
      const since = aliases.since[aliasKey(dag.id, name)]
      if (!since) { aliasSeeds.push(name); continue }
      for (const { uri } of links) {
        if (direct.includes(uri) || ready.has(uri) || seeds[uri]) continue
        const latest = latestEvents[uri]
        if (!latest) continue
        const wm = watermarks[`${dag.id}::${uri}`]
        if (wm) { if (wm !== latest) ready.add(uri) }
        else if ((aliases.latestAt[uri]?.getTime() ?? 0) >= since.getTime()) ready.add(uri)
        else seeds[uri] = latest
      }
    }

    const fire = evaluateCondition(cond, u => ready.has(u), n => aliasUris[n] ?? [])
    const consumed = fire ? [...ready] : []
    const newWatermarks: Record<string, string> = { ...seeds }
    for (const uri of consumed) newWatermarks[uri] = latestEvents[uri]

    if (fire) decisions.push({ dag, newWatermarks, shouldFire: true, consumed, aliasSeeds })
    else if (Object.keys(seeds).length > 0 || aliasSeeds.length > 0) decisions.push({ dag, newWatermarks, shouldFire: false, aliasSeeds })
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

/** A dataset event a task emitted at runtime (ctx.emitDataset). */
export interface EmittedDataset { uri: string; alias?: string; extra?: Record<string, unknown> }

/**
 * Record events a task emitted while it ran — called once the task function succeeded, so a failed try
 * emits nothing. An event emitted through an alias also attaches the dataset to the alias.
 */
export async function recordTaskDatasetEvents(
  db: Db,
  source: { dagId: string; runId: string; taskId: string },
  events: EmittedDataset[],
): Promise<void> {
  if (events.length === 0) return
  const now = new Date()
  for (const e of events) {
    if (typeof e.uri !== 'string' || !e.uri) throw new Error('emitDataset: uri must be a non-empty string')
  }
  await db.collection<DatasetEvent>('dataset_events').insertMany(events.map(e => ({
    uri: e.uri,
    produced_by: source.dagId,
    run_id: source.runId,
    task_id: source.taskId,
    emitted_at: now,
    ...(e.alias ? { source_alias: e.alias } : {}),
    ...(e.extra ? { extra: e.extra } : {}),
  })))
  for (const e of events.filter(x => x.alias)) {
    await db.collection('dataset_alias_links').updateOne(
      { alias: e.alias, uri: e.uri },
      { $setOnInsert: { alias: e.alias, uri: e.uri, linked_at: now } },
      { upsert: true },
    ).catch((err: { code?: number }) => { if (err.code !== 11000) throw err })   // raced another task
  }
}

/** Latest event (id + time) per dataset URI, across all producers. */
async function loadLatestEvents(db: Db, uris: string[]): Promise<{ ids: LatestEvents; at: Record<string, Date> }> {
  if (uris.length === 0) return { ids: {}, at: {} }

  const results = await db
    .collection<DatasetEvent>('dataset_events')
    .aggregate([
      { $match: { uri: { $in: uris } } },
      { $sort: { emitted_at: -1 } },
      { $group: { _id: '$uri', last_id: { $first: '$_id' }, last_at: { $first: '$emitted_at' } } },
    ])
    .toArray()

  return {
    ids: Object.fromEntries(results.map(r => [r._id as string, (r.last_id as object).toString()])),
    at: Object.fromEntries(results.map(r => [r._id as string, new Date(r.last_at as Date)])),
  }
}

/** Watermarks (and alias watch-start times) of the given consumer Dags. */
async function loadWatermarks(db: Db, consumerIds: string[]): Promise<{ watermarks: Watermarks; since: Record<string, Date> }> {
  if (consumerIds.length === 0) return { watermarks: {}, since: {} }

  const docs = await db
    .collection<DatasetWatermark & { since?: Date }>('dataset_watermarks')
    .find({ consumer_dag_id: { $in: consumerIds } })
    .toArray()

  const watermarks: Watermarks = {}
  const since: Record<string, Date> = {}
  for (const d of docs) {
    const key = `${d.consumer_dag_id}::${d.dataset_uri}`
    if (d.dataset_uri.startsWith('alias::')) { if (d.since) since[key] = new Date(d.since) }
    else if (d.last_event_id) watermarks[key] = d.last_event_id
  }
  return { watermarks, since }
}

async function loadAliasLinks(db: Db, names: string[]): Promise<AliasState['links']> {
  if (names.length === 0) return {}
  const docs = await db.collection('dataset_alias_links').find({ alias: { $in: names } }).toArray()
  const links: AliasState['links'] = {}
  for (const d of docs) (links[d.alias as string] ??= []).push({ uri: d.uri as string, linkedAt: new Date(d.linked_at as Date) })
  return links
}

/** First sight of an alias by a consumer: events attached from now on count. Insert-if-absent. */
async function seedAlias(db: Db, dagId: string, alias: string, now: Date): Promise<void> {
  await db.collection('dataset_watermarks').updateOne(
    { consumer_dag_id: dagId, dataset_uri: `alias::${alias}` },
    { $setOnInsert: { consumer_dag_id: dagId, dataset_uri: `alias::${alias}`, since: now } },
    { upsert: true },
  ).catch((err: { code?: number }) => { if (err.code !== 11000) throw err })
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
    // First watermark for this pair: insert only if absent
    const result = await db.collection<DatasetWatermark>('dataset_watermarks').updateOne(
      { consumer_dag_id: dagId, dataset_uri: uri, last_event_id: { $exists: false } },
      { $set: { consumer_dag_id: dagId, dataset_uri: uri, last_event_id: newEventId } },
      { upsert: true },
    ).catch((err: { code?: number }) => {
      if (err.code === 11000) return { upsertedCount: 0 }   // another tick inserted it first
      throw err
    })
    return result.upsertedCount > 0
  }

  const result = await db.collection<DatasetWatermark>('dataset_watermarks').updateOne(
    { consumer_dag_id: dagId, dataset_uri: uri, last_event_id: oldEventId },
    { $set: { last_event_id: newEventId } },
  )
  return result.modifiedCount > 0
}

/**
 * Evaluate all dataset-scheduled consumers and create queued runs for those whose condition holds.
 * Returns the number of runs created. CAS-guards each watermark advance so concurrent ticks don't
 * double-fire. A Dag with both a cron `schedule` and `datasets` runs on either (Airflow AssetOrTimeSchedule).
 */
export async function triggerDatasetConsumers(
  db: Db,
  dags: DagDefinition[],
  createRun: (db: Db, dag: DagDefinition, opts?: Record<string, unknown>) => Promise<string>,
  isDagPaused: (db: Db, dagId: string) => Promise<boolean>,
  now: Date = new Date(),
): Promise<number> {
  const consumers = dags.filter(d => normalizeCondition(d.datasets) !== null)
  if (consumers.length === 0) return 0

  const aliasNames = [...new Set(consumers.flatMap(d => conditionAliases(normalizeCondition(d.datasets))))]
  const links = await loadAliasLinks(db, aliasNames)
  const allUris = [...new Set([
    ...consumers.flatMap(d => conditionUris(normalizeCondition(d.datasets))),
    ...Object.values(links).flat().map(l => l.uri),
  ])]

  const [latest, wm] = await Promise.all([
    loadLatestEvents(db, allUris),
    loadWatermarks(db, consumers.map(d => d.id)),
  ])

  const decisions = evaluateConsumers(consumers, latest.ids, wm.watermarks, { links, latestAt: latest.at, since: wm.since })
  let triggered = 0

  for (const decision of decisions) {
    const { dag, newWatermarks } = decision
    for (const alias of decision.aliasSeeds ?? []) await seedAlias(db, dag.id, alias, now)

    const consumed = new Set(decision.consumed ?? [])
    let wonAll = true
    for (const [uri, newId] of Object.entries(newWatermarks)) {
      const won = await casWatermark(db, dag.id, uri, wm.watermarks[`${dag.id}::${uri}`] ?? null, newId)
      if (consumed.has(uri) && !won) wonAll = false
    }
    // Trigger only if this tick consumed every ready dataset (another tick may have won some)
    if (decision.shouldFire === false || !wonAll) continue

    if (await isDagPaused(db, dag.id)) {
      console.log(`[datasets] consumer '${dag.id}' is paused — skipping trigger`)
      continue
    }
    await createRun(db, dag, { triggerType: 'dataset' })
    console.log(`[datasets] triggered consumer '${dag.id}' (${describeCondition(normalizeCondition(dag.datasets))}; new: ${[...consumed].join(', ')})`)
    triggered++
  }

  return triggered
}

// ── API helpers ────────────────────────────────────────────────────────────────

/** Alias → attached datasets (first attached first). */
export async function listDatasetAliases(db: Db, alias?: string): Promise<Array<{ alias: string; datasets: Array<{ uri: string; linked_at: Date }> }>> {
  const docs = await db.collection('dataset_alias_links').find(alias ? { alias } : {}).sort({ alias: 1, linked_at: 1 }).limit(1000).toArray()
  const byAlias = new Map<string, Array<{ uri: string; linked_at: Date }>>()
  for (const d of docs) {
    const list = byAlias.get(d.alias as string) ?? []
    list.push({ uri: d.uri as string, linked_at: d.linked_at as Date })
    byAlias.set(d.alias as string, list)
  }
  return [...byAlias].map(([name, datasets]) => ({ alias: name, datasets }))
}

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
