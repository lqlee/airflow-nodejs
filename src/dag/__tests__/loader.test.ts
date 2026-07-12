import { describe, it, expect, beforeEach, vi } from 'vitest'
import { loadDags } from '../loader.js'
import { listDags, clearRegistry } from '../registry.js'

beforeEach(() => clearRegistry())

describe('loadDags', () => {
  it('loads the hello_world dag from dags/ directory', async () => {
    await loadDags()
    const dags = listDags()
    expect(dags.length).toBeGreaterThan(0)
    const dag = dags.find(d => d.id === 'hello_world')
    expect(dag).toBeDefined()
  })

  it('hello_world has extract, transform, load tasks', async () => {
    await loadDags()
    const dag = listDags().find(d => d.id === 'hello_world')
    expect(Object.keys(dag!.tasks)).toEqual(['extract', 'transform', 'load'])
  })

  it('transform depends on extract', async () => {
    await loadDags()
    const dag = listDags().find(d => d.id === 'hello_world')
    expect(dag!.tasks.transform.dependsOn).toContain('extract')
  })

  it('load depends on transform', async () => {
    await loadDags()
    const dag = listDags().find(d => d.id === 'hello_world')
    expect(dag!.tasks.load.dependsOn).toContain('transform')
  })

  it('does not throw when dags/ is missing (just warns)', async () => {
    // Point loader at a non-existent dir by overriding cwd
    const original = process.cwd
    process.cwd = () => '/tmp/nonexistent_airflow_test_dir'
    await expect(loadDags()).resolves.not.toThrow()
    process.cwd = original
  })
})
