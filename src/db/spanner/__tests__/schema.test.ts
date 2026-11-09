import { describe, it, expect } from 'vitest'
import { buildDdl, SCHEMA_BY_COLLECTION } from '../schema.js'

const ddl = buildDdl()

describe('Spanner DDL', () => {
  it('declares the dag_runs (dag_id, logical_date) constraint as UNIQUE NULL_FILTERED', () => {
    expect(ddl).toContain('CREATE UNIQUE NULL_FILTERED INDEX dag_runs_dag_id_logical_date_uq ON dag_runs(ns, dag_id, logical_date)')
  })

  it('keeps NULLs in the other unique indexes (NULL map_index must still be unique per task)', () => {
    expect(ddl).toContain('CREATE UNIQUE INDEX task_instances_dag_run_id_task_id_map_index_uq ON task_instances(ns, dag_run_id, task_id, map_index)')
  })

  it('has tables for the liveness registry and the leader lease — a table with no promoted columns is still valid', () => {
    expect(SCHEMA_BY_COLLECTION.has('scheduler_instances')).toBe(true)
    const lease = ddl.find(s => s.startsWith('CREATE TABLE scheduler_leases '))!
    expect(lease).toBe('CREATE TABLE scheduler_leases (\n  ns STRING(128) NOT NULL,\n  id STRING(64) NOT NULL,\n  doc STRING(MAX) NOT NULL\n) PRIMARY KEY (ns, id)')
  })

  it('every index name is unique and every index targets a declared table', () => {
    const tables = new Set(ddl.map(s => /^CREATE TABLE (\w+)/.exec(s)?.[1]).filter(Boolean))
    const indexes = ddl.map(s => /INDEX (\w+) ON (\w+)\(/.exec(s)).filter(Boolean) as RegExpExecArray[]
    expect(new Set(indexes.map(m => m[1])).size).toBe(indexes.length)
    for (const m of indexes) expect(tables.has(m[2]), m[0]).toBe(true)
  })
})
