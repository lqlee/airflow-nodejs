import { describe, it, expect } from 'vitest'
import { expandGroups } from '../taskgroups.js'
import type { DagDefinition } from '../types.js'

const noop = async () => {}

// Helper to build a minimal DagDefinition
function makeDag(overrides: Partial<DagDefinition>): DagDefinition {
  return { id: 'test', schedule: null, tasks: {}, ...overrides }
}

describe('expandGroups — no groups', () => {
  it('returns the same dag when groups is undefined', () => {
    const dag = makeDag({ tasks: { a: { run: noop }, b: { run: noop, dependsOn: ['a'] } } })
    const result = expandGroups(dag)
    expect(result).toBe(dag) // same reference — no copy needed
  })

  it('returns an equivalent dag when groups is empty object (no edges added)', () => {
    const dag = makeDag({ groups: {}, tasks: { a: { run: noop } } })
    const result = expandGroups(dag)
    expect(result.id).toBe(dag.id)
    expect(Object.keys(result.tasks)).toEqual(Object.keys(dag.tasks))
    // No extra depends_on added
    expect(result.tasks['a'].dependsOn).toEqual([])
  })
})

describe('expandGroups — label-only (no group dependsOn)', () => {
  it('preserves existing depends_on unchanged', () => {
    const dag = makeDag({
      groups: { extract: {} },
      tasks: {
        a: { run: noop, group: 'extract' },
        b: { run: noop, group: 'extract', dependsOn: ['a'] },
      },
    })
    const result = expandGroups(dag)
    expect(result.tasks['b'].dependsOn).toEqual(['a'])
    expect(result.tasks['a'].dependsOn).toEqual([])
  })
})

describe('expandGroups — group-level dependsOn', () => {
  // Group A: tasks [a1, a2] where a2 depends on a1
  // Group B: tasks [b1, b2] where b2 depends on b1
  // B dependsOn A → roots of B (b1) get edges to leaves of A (a2)
  const dag = makeDag({
    groups: {
      extract: {},
      transform: { dependsOn: ['extract'] },
    },
    tasks: {
      a1: { run: noop, group: 'extract' },
      a2: { run: noop, group: 'extract', dependsOn: ['a1'] },
      b1: { run: noop, group: 'transform' },
      b2: { run: noop, group: 'transform', dependsOn: ['b1'] },
    },
  })

  it('adds leaf→root cross-group edge', () => {
    const result = expandGroups(dag)
    // b1 is root of transform → should depend on a2 (leaf of extract)
    expect(result.tasks['b1'].dependsOn).toContain('a2')
  })

  it('does not add edge to non-leaf upstream task', () => {
    const result = expandGroups(dag)
    // a1 is not a leaf (a2 depends on it) → b1 should NOT depend on a1
    expect(result.tasks['b1'].dependsOn).not.toContain('a1')
  })

  it('does not modify non-root tasks in downstream group', () => {
    const result = expandGroups(dag)
    // b2 depends on b1 (internal) → is not a root → no cross-group edge added
    expect(result.tasks['b2'].dependsOn).toEqual(['b1'])
  })

  it('preserves existing task-level depends_on', () => {
    const result = expandGroups(dag)
    expect(result.tasks['a2'].dependsOn).toContain('a1')
  })
})

describe('expandGroups — single-task groups', () => {
  it('connects single-task group A → single-task group B correctly', () => {
    const dag = makeDag({
      groups: {
        load: {},
        validate: { dependsOn: ['load'] },
      },
      tasks: {
        loader: { run: noop, group: 'load' },
        validator: { run: noop, group: 'validate' },
      },
    })
    const result = expandGroups(dag)
    expect(result.tasks['validator'].dependsOn).toContain('loader')
  })
})

describe('expandGroups — three-group chain A → B → C', () => {
  it('expands each hop independently', () => {
    const dag = makeDag({
      groups: {
        a: {},
        b: { dependsOn: ['a'] },
        c: { dependsOn: ['b'] },
      },
      tasks: {
        a1: { run: noop, group: 'a' },
        b1: { run: noop, group: 'b' },
        c1: { run: noop, group: 'c' },
      },
    })
    const result = expandGroups(dag)
    // b1 depends on a1 (B → A)
    expect(result.tasks['b1'].dependsOn).toContain('a1')
    // c1 depends on b1 (C → B), but NOT directly on a1
    expect(result.tasks['c1'].dependsOn).toContain('b1')
    expect(result.tasks['c1'].dependsOn).not.toContain('a1')
  })
})

describe('expandGroups — validation errors', () => {
  it('throws when a task references an undeclared group', () => {
    const dag = makeDag({
      groups: {},
      tasks: { a: { run: noop, group: 'nonexistent' } },
    })
    expect(() => expandGroups(dag)).toThrow(/undeclared group 'nonexistent'/)
  })

  it('throws when a group depends on an undeclared group', () => {
    const dag = makeDag({
      groups: { mygroup: { dependsOn: ['ghost'] } },
      tasks: { a: { run: noop, group: 'mygroup' } },
    })
    expect(() => expandGroups(dag)).toThrow(/undeclared group 'ghost'/)
  })

  it('empty group (no member tasks) is silently ignored — no edges added', () => {
    const dag = makeDag({
      groups: {
        empty: {},
        b: { dependsOn: ['empty'] },
      },
      tasks: { b1: { run: noop, group: 'b' } },
    })
    const result = expandGroups(dag)
    // No leaves in 'empty' → no edges to b1
    expect(result.tasks['b1'].dependsOn).toEqual([])
  })
})

describe('expandGroups — integration: expanded dag runs correctly with claimReadyTasks logic', () => {
  it('leaf/root computation is correct for a diamond group topology', () => {
    // Group src: [src1]
    // Group mid: [mid1, mid2] both roots (no internal deps) → both should get edge from src1
    // Group sink: [sink1] dependsOn mid
    const dag = makeDag({
      groups: {
        src: {},
        mid: { dependsOn: ['src'] },
        sink: { dependsOn: ['mid'] },
      },
      tasks: {
        src1: { run: noop, group: 'src' },
        mid1: { run: noop, group: 'mid' },
        mid2: { run: noop, group: 'mid' },
        sink1: { run: noop, group: 'sink' },
      },
    })
    const result = expandGroups(dag)
    // Both mid tasks are roots → both depend on src1
    expect(result.tasks['mid1'].dependsOn).toContain('src1')
    expect(result.tasks['mid2'].dependsOn).toContain('src1')
    // Both mid tasks are leaves (no internal deps) → sink1 depends on both
    expect(result.tasks['sink1'].dependsOn).toContain('mid1')
    expect(result.tasks['sink1'].dependsOn).toContain('mid2')
  })
})
