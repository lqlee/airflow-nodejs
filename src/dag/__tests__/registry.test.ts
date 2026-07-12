import { describe, it, expect, beforeEach } from 'vitest'
import { register, getDag, listDags, clearRegistry } from '../registry.js'
import type { DagDefinition } from '../types.js'

const makeDag = (id: string): DagDefinition => ({
  id,
  schedule: null,
  tasks: {
    task_a: { run: async () => {} },
    task_b: { dependsOn: ['task_a'], run: async () => {} },
  },
})

beforeEach(() => clearRegistry())

describe('register + getDag', () => {
  it('returns a registered dag by id', () => {
    register(makeDag('my_dag'))
    const dag = getDag('my_dag')
    expect(dag?.id).toBe('my_dag')
  })

  it('returns undefined for unknown dag', () => {
    expect(getDag('nonexistent')).toBeUndefined()
  })

  it('overwrites an existing dag with same id', () => {
    register(makeDag('dup'))
    const updated = { ...makeDag('dup'), schedule: '0 * * * *' }
    register(updated)
    expect(getDag('dup')?.schedule).toBe('0 * * * *')
  })
})

describe('listDags', () => {
  it('returns empty array when registry is empty', () => {
    expect(listDags()).toEqual([])
  })

  it('returns all registered dags', () => {
    register(makeDag('dag_a'))
    register(makeDag('dag_b'))
    const ids = listDags().map(d => d.id)
    expect(ids).toContain('dag_a')
    expect(ids).toContain('dag_b')
    expect(ids).toHaveLength(2)
  })
})

describe('clearRegistry', () => {
  it('removes all registered dags', () => {
    register(makeDag('dag_a'))
    clearRegistry()
    expect(listDags()).toHaveLength(0)
  })
})
