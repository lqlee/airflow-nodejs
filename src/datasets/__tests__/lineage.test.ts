/**
 * buildLineage: the graph the lineage view draws — pure, no database.
 */
import { describe, it, expect } from 'vitest'
import { buildLineage, type Lineage } from '../lineage.js'
import type { DagDefinition } from '../../dag/types.js'

const dag = (id: string, over: Partial<DagDefinition> = {}): DagDefinition =>
  ({ id, schedule: null, tasks: { t: { run: async () => {} } }, ...over })

const edgeList = (l: Lineage) => l.edges.map(e => `${e.from} -${e.kind}${e.runtime ? '*' : ''}-> ${e.to}`).sort()
const layersOf = (l: Lineage) => Object.fromEntries(l.nodes.map(n => [n.id, n.layer]))

describe('buildLineage', () => {
  it('links producers to consumers through datasets and lays them out left to right', () => {
    const l = buildLineage([
      dag('ingest', { outlets: ['s3://raw'] }),
      dag('clean', { datasets: ['s3://raw'], outlets: ['s3://clean'] }),
      dag('report', { datasets: { all: ['s3://clean', 's3://ref'] } }),
      dag('lonely'),
    ])
    expect(edgeList(l)).toEqual([
      'dag:clean -produces-> dataset:s3://clean',
      'dag:ingest -produces-> dataset:s3://raw',
      'dataset:s3://clean -consumes-> dag:report',
      'dataset:s3://raw -consumes-> dag:clean',
      'dataset:s3://ref -consumes-> dag:report',
    ])
    expect(layersOf(l)).toMatchObject({
      'dag:ingest': 0, 'dataset:s3://raw': 1, 'dag:clean': 2, 'dataset:s3://clean': 3, 'dag:report': 4,
      'dataset:s3://ref': 0,   // nobody produces it: a source
      'dag:lonely': 0,
    })
  })

  it('adds runtime productions and alias resolution, marking them as runtime unless also declared', () => {
    const l = buildLineage(
      [
        dag('emitter', { outlets: ['s3://declared'] }),
        dag('watcher', { datasets: { alias: 'daily' } }),
      ],
      [
        { produced_by: 'emitter', uri: 's3://declared' },                         // also declared
        { produced_by: 'emitter', uri: 's3://2026-03-01', source_alias: 'daily' },
        { produced_by: 'deleted_dag', uri: 's3://orphan' },                       // producer no longer exists
      ],
      [{ alias: 'daily', uri: 's3://2026-03-02' }],
    )
    expect(edgeList(l)).toEqual([
      'alias:daily -consumes-> dag:watcher',
      'alias:daily -resolves*-> dataset:s3://2026-03-01',
      'alias:daily -resolves*-> dataset:s3://2026-03-02',
      'dag:emitter -produces*-> alias:daily',
      'dag:emitter -produces-> dataset:s3://declared',
    ])
    expect(l.nodes.map(n => n.id)).not.toContain('dataset:s3://orphan')
  })

  it('focus keeps what is upstream and downstream of a Dag, not unrelated branches', () => {
    const dags = [
      dag('a', { outlets: ['x'] }),
      dag('b', { datasets: ['x'], outlets: ['y'] }),
      dag('c', { datasets: ['y'] }),
      dag('side', { datasets: ['x'] }),   // shares b's input, but is neither up- nor downstream of b
      dag('other', { outlets: ['z'] }),
    ]
    expect(buildLineage(dags, [], [], 'b').nodes.map(n => n.id).sort())
      .toEqual(['dag:a', 'dag:b', 'dag:c', 'dataset:x', 'dataset:y'])
    expect(buildLineage(dags, [], [], 'unknown')).toEqual({ nodes: [], edges: [] })
  })

  it('places a Dag that consumes its own output without looping forever', () => {
    const l = buildLineage([dag('src', { outlets: ['in'] }), dag('loop', { datasets: ['in', 'state'], outlets: ['state'] })])
    expect(layersOf(l)).toMatchObject({ 'dag:src': 0, 'dataset:in': 1 })
    expect(layersOf(l)['dag:loop']).toBeGreaterThan(1)
  })
})
