/**
 * Unit tests for public/ui-utils.js — pure UI helper functions.
 * No React, no DOM, no network, no MongoDB required.
 *
 * Tests verify real behavior: correct output for given input.
 * Each test answers: "what breaks if this function is wrong?"
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { duration, expandForGraph, topoLayers, computeGanttLayout, buildCalendarData, calendarCellCategory } from '../../../public/ui-utils.js'

// ── duration() ────────────────────────────────────────────────────────────────

describe('duration()', () => {
  it('returns "-" when start is null', () => {
    expect(duration(null)).toBe('-')
  })

  it('returns "-" when start is undefined', () => {
    expect(duration(undefined)).toBe('-')
  })

  it('formats sub-second durations as ms', () => {
    const start = new Date('2024-01-01T00:00:00.000Z')
    const end   = new Date('2024-01-01T00:00:00.340Z')
    expect(duration(start, end)).toBe('340ms')
  })

  it('formats 0ms correctly', () => {
    const t = new Date('2024-01-01T00:00:00.000Z')
    expect(duration(t, t)).toBe('0ms')
  })

  it('formats seconds with one decimal place', () => {
    const start = new Date('2024-01-01T00:00:00.000Z')
    const end   = new Date('2024-01-01T00:00:01.500Z')
    expect(duration(start, end)).toBe('1.5s')
  })

  it('formats exactly 59.9s correctly', () => {
    const start = new Date('2024-01-01T00:00:00.000Z')
    const end   = new Date('2024-01-01T00:00:59.900Z')
    expect(duration(start, end)).toBe('59.9s')
  })

  it('formats minutes + seconds for durations >= 60s', () => {
    const start = new Date('2024-01-01T00:00:00.000Z')
    const end   = new Date('2024-01-01T00:03:14.000Z')
    expect(duration(start, end)).toBe('3m 14s')
  })

  it('formats exactly 1 minute', () => {
    const start = new Date('2024-01-01T00:00:00.000Z')
    const end   = new Date('2024-01-01T00:01:00.000Z')
    expect(duration(start, end)).toBe('1m 0s')
  })

  it('accepts ISO string timestamps', () => {
    expect(duration('2024-01-01T00:00:00.000Z', '2024-01-01T00:00:00.500Z')).toBe('500ms')
  })

  it('uses Date.now() as end when end is omitted', () => {
    // Pin Date.now to a fixed offset from start
    const start = new Date(Date.now() - 2500)
    const result = duration(start)
    // Should be around 2.5s — be lenient for test timing jitter
    expect(result).toMatch(/^[23]\.\d s$|^2500ms$|^2\.\d+s$/)
  })
})

// ── expandForGraph() ──────────────────────────────────────────────────────────

describe('expandForGraph()', () => {
  it('returns one node per non-mapped task with node_id = task_id', () => {
    const tasks = [
      { task_id: 'extract', map_index: null, depends_on: [] },
      { task_id: 'load',    map_index: null, depends_on: ['extract'] },
    ]
    const nodes = expandForGraph(tasks)
    expect(nodes[0].node_id).toBe('extract')
    expect(nodes[1].node_id).toBe('load')
  })

  it('generates indexed node_ids for mapped tasks', () => {
    const tasks = [
      { task_id: 'process', map_index: 0, depends_on: [] },
      { task_id: 'process', map_index: 1, depends_on: [] },
      { task_id: 'process', map_index: 2, depends_on: [] },
    ]
    const nodes = expandForGraph(tasks)
    expect(nodes.map(n => n.node_id)).toEqual(['process[0]', 'process[1]', 'process[2]'])
  })

  it('display_label for mapped task uses [index] suffix', () => {
    const tasks = [{ task_id: 'step', map_index: 0, depends_on: [] }]
    const [node] = expandForGraph(tasks)
    expect(node.display_label).toBe('step[0]')
  })

  it('truncates long mapped task_ids in display_label with ellipsis', () => {
    // task_id longer than 10 chars → truncated to 9 chars + '…' + '[index]'
    const tasks = [{ task_id: 'very_long_task_name', map_index: 0, depends_on: [] }]
    const [node] = expandForGraph(tasks)
    expect(node.display_label).toBe('very_long…[0]')
  })

  it('display_label for non-mapped task is just task_id', () => {
    const tasks = [{ task_id: 'my_task', map_index: null, depends_on: [] }]
    const [node] = expandForGraph(tasks)
    expect(node.display_label).toBe('my_task')
  })

  it('fans out edges from a non-mapped task to all instances of a mapped upstream', () => {
    // extract has 2 mapped instances; transform depends_on extract → should get 2 edges
    const tasks = [
      { task_id: 'extract',   map_index: 0,    depends_on: [] },
      { task_id: 'extract',   map_index: 1,    depends_on: [] },
      { task_id: 'transform', map_index: null, depends_on: ['extract'] },
    ]
    const nodes = expandForGraph(tasks)
    const transform = nodes.find(n => n.task_id === 'transform')!
    expect(transform.resolved_deps).toEqual(['extract[0]', 'extract[1]'])
  })

  it('preserves non-mapped dependency edges unchanged', () => {
    const tasks = [
      { task_id: 'a', map_index: null, depends_on: [] },
      { task_id: 'b', map_index: null, depends_on: ['a'] },
    ]
    const nodes = expandForGraph(tasks)
    expect(nodes[1].resolved_deps).toEqual(['a'])
  })

  it('handles empty task list', () => {
    expect(expandForGraph([])).toEqual([])
  })

  it('handles tasks with no depends_on field', () => {
    const tasks = [{ task_id: 'solo', map_index: null }]
    const [node] = expandForGraph(tasks)
    expect(node.resolved_deps).toEqual([])
  })
})

// ── topoLayers() ──────────────────────────────────────────────────────────────

describe('topoLayers()', () => {
  /** Helper: build expanded nodes from a simple dep map */
  function nodes(deps: Record<string, string[]>) {
    return Object.entries(deps).map(([id, d]) => ({
      node_id: id,
      resolved_deps: d,
    }))
  }

  it('puts a single independent node in layer 0', () => {
    const layers = topoLayers(nodes({ a: [] }))
    expect(layers).toEqual([['a']])
  })

  it('assigns a linear chain to sequential layers', () => {
    const layers = topoLayers(nodes({ a: [], b: ['a'], c: ['b'] }))
    expect(layers[0]).toContain('a')
    expect(layers[1]).toContain('b')
    expect(layers[2]).toContain('c')
  })

  it('puts independent nodes in the same layer', () => {
    const layers = topoLayers(nodes({ a: [], b: [], c: ['a', 'b'] }))
    expect(layers[0]).toEqual(expect.arrayContaining(['a', 'b']))
    expect(layers[0]).toHaveLength(2)
    expect(layers[1]).toContain('c')
  })

  it('diamond dependency: both branches in layer 1, merge in layer 2', () => {
    // a → b, a → c, b+c → d
    const layers = topoLayers(nodes({ a: [], b: ['a'], c: ['a'], d: ['b', 'c'] }))
    expect(layers[0]).toEqual(['a'])
    expect(layers[1]).toEqual(expect.arrayContaining(['b', 'c']))
    expect(layers[2]).toEqual(['d'])
  })

  it('returns all nodes even when some are in a cycle (cycle nodes in their own layer)', () => {
    // a → b → a  (cycle); c is independent
    const layers = topoLayers(nodes({ a: ['b'], b: ['a'], c: [] }))
    const allNodes = layers.flat()
    expect(allNodes).toEqual(expect.arrayContaining(['a', 'b', 'c']))
    expect(allNodes).toHaveLength(3)
  })

  it('returns empty array for empty input', () => {
    expect(topoLayers([])).toEqual([])
  })

  it('total nodes across all layers equals input length', () => {
    const input = nodes({ a: [], b: ['a'], c: ['a'], d: ['b', 'c'], e: ['d'] })
    const layers = topoLayers(input)
    expect(layers.flat()).toHaveLength(5)
  })

  it('each node appears in exactly one layer', () => {
    const input = nodes({ a: [], b: ['a'], c: ['b'], d: ['a'], e: ['c', 'd'] })
    const layers = topoLayers(input)
    const flat = layers.flat()
    const unique = new Set(flat)
    expect(flat.length).toBe(unique.size)
  })
})

// ── computeGanttLayout() ──────────────────────────────────────────────────────

describe('computeGanttLayout()', () => {
  const t0 = '2024-01-01T00:00:00.000Z'
  const t1 = '2024-01-01T00:00:10.000Z'  // +10s
  const t2 = '2024-01-01T00:00:20.000Z'  // +20s

  it('returns empty array for empty input', () => {
    expect(computeGanttLayout([])).toEqual([])
  })

  it('all tasks without started_at → xStart=xEnd=0', () => {
    const tasks = [{ task_id: 'a', state: 'queued', started_at: null, ended_at: null }]
    const result = computeGanttLayout(tasks)
    expect(result[0].xStart).toBe(0)
    expect(result[0].xEnd).toBe(0)
    expect(result[0].durationMs).toBe(0)
  })

  it('single started+ended task fills full width (xStart=0, xEnd=1)', () => {
    const tasks = [{ task_id: 'a', state: 'success', started_at: t0, ended_at: t1 }]
    const result = computeGanttLayout(tasks)
    expect(result[0].xStart).toBe(0)
    expect(result[0].xEnd).toBe(1)
    expect(result[0].durationMs).toBe(10000)
  })

  it('two sequential tasks: first from 0-0.5, second from 0.5-1', () => {
    const tasks = [
      { task_id: 'a', state: 'success', started_at: t0, ended_at: t1 },
      { task_id: 'b', state: 'success', started_at: t1, ended_at: t2 },
    ]
    const result = computeGanttLayout(tasks)
    expect(result[0].xStart).toBe(0)
    expect(result[0].xEnd).toBeCloseTo(0.5)
    expect(result[1].xStart).toBeCloseTo(0.5)
    expect(result[1].xEnd).toBe(1)
  })

  it('task_id and state are preserved', () => {
    const tasks = [{ task_id: 'my_task', state: 'failed', started_at: t0, ended_at: t1 }]
    const result = computeGanttLayout(tasks)
    expect(result[0].task_id).toBe('my_task')
    expect(result[0].state).toBe('failed')
  })

  it('running task (no ended_at) uses nowMs for xEnd', () => {
    const startMs = new Date(t0).getTime()
    const nowMs = startMs + 5000
    const tasks = [{ task_id: 'r', state: 'running', started_at: t0, ended_at: null }]
    const result = computeGanttLayout(tasks, nowMs)
    expect(result[0].xEnd).toBe(1)
    expect(result[0].durationMs).toBe(5000)
  })
})

// ── buildCalendarData() / calendarCellCategory() ─────────────────────────────

describe('buildCalendarData()', () => {
  it('groups runs by YYYY-MM-DD', () => {
    const runs = [
      { created_at: '2024-03-15T10:00:00Z', state: 'success' },
      { created_at: '2024-03-15T12:00:00Z', state: 'success' },
      { created_at: '2024-03-16T08:00:00Z', state: 'failed' },
    ]
    const data = buildCalendarData(runs)
    expect(data['2024-03-15'].total).toBe(2)
    expect(data['2024-03-15'].success).toBe(2)
    expect(data['2024-03-15'].failed).toBe(0)
    expect(data['2024-03-16'].total).toBe(1)
    expect(data['2024-03-16'].failed).toBe(1)
  })

  it('returns empty object for empty runs', () => {
    expect(buildCalendarData([])).toEqual({})
  })

  it('ignores runs with no created_at', () => {
    const data = buildCalendarData([{ created_at: '', state: 'success' }])
    expect(Object.keys(data).length).toBe(0)
  })
})

describe('calendarCellCategory()', () => {
  it('no data → none', () => {
    expect(calendarCellCategory(undefined)).toBe('none')
    expect(calendarCellCategory({ total: 0, success: 0, failed: 0 })).toBe('none')
  })

  it('100% success → success', () => {
    expect(calendarCellCategory({ total: 3, success: 3, failed: 0 })).toBe('success')
  })

  it('>=70% success → mostly-success', () => {
    expect(calendarCellCategory({ total: 4, success: 3, failed: 1 })).toBe('mostly-success')
  })

  it('>=40% and <70% → mixed', () => {
    expect(calendarCellCategory({ total: 5, success: 2, failed: 3 })).toBe('mixed')
  })

  it('<40% success → failure', () => {
    expect(calendarCellCategory({ total: 5, success: 1, failed: 4 })).toBe('failure')
  })
})
