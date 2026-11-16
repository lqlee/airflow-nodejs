/**
 * Dataset lineage: which Dags produce which datasets and which Dags they trigger.
 *
 * Nodes are Dags, datasets and aliases; edges come from the Dag definitions (outlets, the dataset
 * condition) and from what actually happened (dataset events emitted at runtime, aliases resolved to
 * URIs). Each node gets a layer — its longest distance from a source — so a UI can lay the graph out
 * left to right without its own graph code.
 */
import type { Db } from 'mongodb'
import type { DagDefinition } from '../dag/types.js'
import { normalizeCondition, conditionUris, conditionAliases } from './conditions.js'

export type LineageNodeKind = 'dag' | 'dataset' | 'alias'

export interface LineageNode {
  id: string            // `dag:<id>` | `dataset:<uri>` | `alias:<name>`
  kind: LineageNodeKind
  name: string          // Dag id, URI or alias name
  layer: number
  /** Datasets only: the newest event, or null when it was never produced. */
  last_event?: { produced_by: string; run_id: string; emitted_at: Date } | null
}

export interface LineageEdge {
  from: string
  to: string
  /** produces: Dag → dataset/alias; consumes: dataset/alias → Dag; resolves: alias → dataset. */
  kind: 'produces' | 'consumes' | 'resolves'
  /** Seen in dataset events / alias links only — not declared in the Dag definition. */
  runtime: boolean
}

export interface Lineage { nodes: LineageNode[]; edges: LineageEdge[] }

/** A distinct (producer, uri, alias) seen in dataset events. */
export interface ObservedProduction { produced_by: string; uri: string; source_alias?: string | null }

export const dagNode = (id: string) => `dag:${id}`
export const datasetNode = (uri: string) => `dataset:${uri}`
export const aliasNode = (name: string) => `alias:${name}`

/**
 * Build the lineage graph. Pure: callers pass the Dags plus what the database observed.
 * With `focusDagId`, only the Dags and datasets upstream or downstream of that Dag are kept.
 */
export function buildLineage(
  dags: DagDefinition[],
  observed: ObservedProduction[] = [],
  aliasLinks: Array<{ alias: string; uri: string }> = [],
  focusDagId?: string,
): Lineage {
  const nodes = new Map<string, { kind: LineageNodeKind; name: string }>()
  const edges = new Map<string, LineageEdge>()
  const node = (kind: LineageNodeKind, name: string) => {
    const id = kind === 'dag' ? dagNode(name) : kind === 'dataset' ? datasetNode(name) : aliasNode(name)
    if (!nodes.has(id)) nodes.set(id, { kind, name })
    return id
  }
  const edge = (from: string, to: string, kind: LineageEdge['kind'], runtime: boolean) => {
    const key = `${from}→${to}`
    const existing = edges.get(key)
    if (!existing) edges.set(key, { from, to, kind, runtime })
    else if (!runtime) existing.runtime = false   // declared wins over observed
  }

  const known = new Set(dags.map(d => d.id))
  for (const d of dags) {
    const consumer = node('dag', d.id)
    for (const uri of d.outlets ?? []) edge(consumer, node('dataset', uri), 'produces', false)
    const cond = normalizeCondition(d.datasets)
    for (const uri of conditionUris(cond)) edge(node('dataset', uri), consumer, 'consumes', false)
    for (const name of conditionAliases(cond)) edge(node('alias', name), consumer, 'consumes', false)
  }
  for (const o of observed) {
    if (!known.has(o.produced_by)) continue   // the producing Dag was removed: nothing to draw from
    const producer = node('dag', o.produced_by)
    const dataset = node('dataset', o.uri)
    if (o.source_alias) {
      const alias = node('alias', o.source_alias)
      edge(producer, alias, 'produces', true)
      edge(alias, dataset, 'resolves', true)
    } else {
      edge(producer, dataset, 'produces', true)
    }
  }
  for (const l of aliasLinks) edge(node('alias', l.alias), node('dataset', l.uri), 'resolves', true)

  let keep = new Set(nodes.keys())
  if (focusDagId !== undefined) keep = connectedTo(dagNode(focusDagId), [...edges.values()], nodes.has(dagNode(focusDagId)))
  const keptEdges = [...edges.values()].filter(e => keep.has(e.from) && keep.has(e.to))
  const layers = computeLayers([...keep], keptEdges)

  const out = [...keep].map(id => ({ id, ...nodes.get(id)!, layer: layers.get(id)! }))
  out.sort((a, b) => a.layer - b.layer || a.name.localeCompare(b.name))
  return { nodes: out, edges: keptEdges }
}

/** The node plus everything upstream and everything downstream of it (not siblings of those). */
function connectedTo(start: string, edges: LineageEdge[], exists: boolean): Set<string> {
  if (!exists) return new Set()
  const walk = (next: (e: LineageEdge) => [string, string]) => {
    const seen = new Set([start])
    const queue = [start]
    while (queue.length) {
      const cur = queue.shift()!
      for (const e of edges) {
        const [from, to] = next(e)
        if (from === cur && !seen.has(to)) { seen.add(to); queue.push(to) }
      }
    }
    return seen
  }
  return new Set([...walk(e => [e.from, e.to]), ...walk(e => [e.to, e.from])])
}

/** Longest path from a source; nodes on a cycle stay at the layer they reached when the cycle closed. */
function computeLayers(ids: string[], edges: LineageEdge[]): Map<string, number> {
  const inDeg = new Map(ids.map(id => [id, 0]))
  const out = new Map<string, string[]>(ids.map(id => [id, []]))
  for (const e of edges) { inDeg.set(e.to, inDeg.get(e.to)! + 1); out.get(e.from)!.push(e.to) }
  const layer = new Map(ids.map(id => [id, 0]))
  const queue = ids.filter(id => inDeg.get(id) === 0)
  const done = new Set<string>()
  while (queue.length) {
    const cur = queue.shift()!
    done.add(cur)
    for (const to of out.get(cur)!) {
      layer.set(to, Math.max(layer.get(to)!, layer.get(cur)! + 1))
      inDeg.set(to, inDeg.get(to)! - 1)
      if (inDeg.get(to) === 0) queue.push(to)
    }
  }
  // A cycle (a Dag consuming what it produces) never reaches in-degree 0: place it after its known inputs
  for (const id of ids.filter(i => !done.has(i))) {
    const inputs = edges.filter(e => e.to === id && done.has(e.from)).map(e => layer.get(e.from)! + 1)
    layer.set(id, Math.max(0, ...inputs))
  }
  return layer
}

/**
 * Lineage from the database: runtime productions seen in the last `sinceDays` days, all alias links,
 * and the newest event per dataset.
 */
export async function getLineage(db: Db, dags: DagDefinition[], opts: { sinceDays?: number; focusDagId?: string } = {}): Promise<Lineage> {
  const since = new Date(Date.now() - (opts.sinceDays ?? 30) * 86_400_000)
  const [observed, links] = await Promise.all([
    db.collection('dataset_events').aggregate([
      { $match: { emitted_at: { $gte: since } } },
      { $group: { _id: { produced_by: '$produced_by', uri: '$uri', source_alias: '$source_alias' } } },
    ]).toArray(),
    db.collection('dataset_alias_links').find({}).limit(5000).toArray(),
  ])
  const lineage = buildLineage(
    dags,
    observed.map(r => r._id as ObservedProduction),
    links.map(l => ({ alias: l.alias as string, uri: l.uri as string })),
    opts.focusDagId,
  )
  // Newest event per dataset: one indexed (uri, emitted_at) lookup each
  const datasets = lineage.nodes.filter(n => n.kind === 'dataset')
  const latest = await Promise.all(datasets.map(n => db.collection('dataset_events').findOne(
    { uri: n.name }, { sort: { emitted_at: -1 }, projection: { produced_by: 1, run_id: 1, emitted_at: 1 } },
  )))
  datasets.forEach((n, i) => {
    const e = latest[i]
    n.last_event = e ? { produced_by: e.produced_by as string, run_id: e.run_id as string, emitted_at: e.emitted_at as Date } : null
  })
  return lineage
}
