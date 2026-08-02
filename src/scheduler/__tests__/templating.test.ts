/**
 * Tests for template rendering.
 *
 * Two levels:
 *  1. Pure unit tests on renderTemplate/renderArgs/renderEnv
 *  2. Integration test: shell task with {{ conf.env }} / {{ ds }} / {{ dag_id }}
 *     verifies substitution actually reaches the task log (catches the
 *     "executor doesn't load conf" class of bugs).
 *
 * What each test answers:
 *  - Are {{ dag_id }}, {{ run_id }}, {{ task_id }} substituted?
 *  - Are {{ ds }}, {{ ts }}, {{ ts_nodash }} derived from created_at?
 *  - Is {{ conf.key }} and {{ conf.nested.key }} resolved?
 *  - Does an undefined path render as '' (not 'undefined' or crash)?
 *  - Does renderEnv/renderArgs apply to all values?
 *  - Does a live shell task with templates show substituted values in the log?
 */

import { describe, it, expect, beforeAll, afterAll, afterEach } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { renderTemplate, renderArgs, renderEnv, type TemplateContext } from '../template.js'
import { createRun } from '../runs.js'
import { advanceRun } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'
const TEST_DB = 'airflow_test_templating'

let client: MongoClient
let db: Db

// ── helpers ───────────────────────────────────────────────────────────────────

function makeCtx(overrides: Partial<TemplateContext> = {}): TemplateContext {
  return {
    dag_id:       'my_dag',
    run_id:       'abc123',
    task_id:      'my_task',
    logical_date: null,
    created_at:   new Date('2024-03-15T09:30:00.000Z'),
    conf:         { env: 'prod', region: 'us-east-1', nested: { key: 'val' } },
    ...overrides,
  }
}

async function runDag(dag: DagDefinition): Promise<string> {
  register(dag)
  const runId = await createRun(db, dag, { conf: { env: 'staging', date_label: '2024-01-01' } })
  for (let i = 0; i < 15; i++) await advanceRun(db, runId)
  return runId
}

async function taskLogs(runId: string, taskId: string): Promise<string[]> {
  const docs = await db.collection('task_logs')
    .find({ dag_run_id: runId, task_id: taskId })
    .sort({ ts: 1 })
    .toArray()
  return docs.map(d => d.line as string)
}

// ── setup ─────────────────────────────────────────────────────────────────────

beforeAll(async () => {
  process.env.LOG_BACKEND = 'mongodb'  // tests read from task_logs collection directly
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db(TEST_DB)
  clearRegistry()
})

afterAll(async () => {
  await new Promise(r => setTimeout(r, 200))
  await db.dropDatabase()
  await client.close()
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('task_logs').deleteMany({})
  clearRegistry()
})

// ══════════════════════════════════════════════════════════════════════════════
// renderTemplate — pure unit tests
// ══════════════════════════════════════════════════════════════════════════════

describe('renderTemplate', () => {
  const ctx = makeCtx()

  it('renders {{ dag_id }}', () => {
    expect(renderTemplate('dag={{ dag_id }}', ctx)).toBe('dag=my_dag')
  })

  it('renders {{ run_id }}', () => {
    expect(renderTemplate('run={{ run_id }}', ctx)).toBe('run=abc123')
  })

  it('renders {{ task_id }}', () => {
    expect(renderTemplate('task={{ task_id }}', ctx)).toBe('task=my_task')
  })

  it('renders {{ ds }} as YYYY-MM-DD from created_at', () => {
    expect(renderTemplate('{{ ds }}', ctx)).toBe('2024-03-15')
  })

  it('renders {{ ts }} as full ISO timestamp', () => {
    expect(renderTemplate('{{ ts }}', ctx)).toBe('2024-03-15T09:30:00.000Z')
  })

  it('renders {{ ts_nodash }} without dashes/colons', () => {
    const result = renderTemplate('{{ ts_nodash }}', ctx)
    expect(result).not.toContain('-')
    expect(result).toMatch(/^\d{8}T\d{6}/)
  })

  it('renders {{ logical_date }} as ISO when set', () => {
    const ctxWithDate = makeCtx({ logical_date: new Date('2024-01-01T00:00:00.000Z') })
    expect(renderTemplate('{{ logical_date }}', ctxWithDate)).toBe('2024-01-01T00:00:00.000Z')
  })

  it('renders {{ logical_date }} as empty string when null (manual run)', () => {
    expect(renderTemplate('{{ logical_date }}', ctx)).toBe('')
  })

  it('uses logical_date for {{ ds }} when set', () => {
    const ctxWithDate = makeCtx({ logical_date: new Date('2024-01-15T00:00:00.000Z') })
    expect(renderTemplate('{{ ds }}', ctxWithDate)).toBe('2024-01-15')
  })

  it('renders {{ conf.key }}', () => {
    expect(renderTemplate('env={{ conf.env }}', ctx)).toBe('env=prod')
  })

  it('renders {{ conf.nested.key }}', () => {
    expect(renderTemplate('val={{ conf.nested.key }}', ctx)).toBe('val=val')
  })

  it('renders unknown path as empty string', () => {
    expect(renderTemplate('x={{ unknown_var }}', ctx)).toBe('x=')
  })

  it('renders unknown conf path as empty string', () => {
    expect(renderTemplate('x={{ conf.missing.deep }}', ctx)).toBe('x=')
  })

  it('renders multiple variables in one string', () => {
    const result = renderTemplate('{{ dag_id }}/{{ ds }}/{{ conf.env }}', ctx)
    expect(result).toBe('my_dag/2024-03-15/prod')
  })

  it('leaves non-template text unchanged', () => {
    expect(renderTemplate('hello world', ctx)).toBe('hello world')
  })

  it('handles whitespace inside braces: {{ dag_id  }} and {{dag_id}}', () => {
    expect(renderTemplate('{{ dag_id  }}', ctx)).toBe('my_dag')
    expect(renderTemplate('{{dag_id}}', ctx)).toBe('my_dag')
  })
})

describe('renderArgs', () => {
  it('renders all items in an array', () => {
    const ctx = makeCtx()
    const result = renderArgs(['--date', '{{ ds }}', '--env', '{{ conf.env }}'], ctx)
    expect(result).toEqual(['--date', '2024-03-15', '--env', 'prod'])
  })

  it('returns empty array unchanged', () => {
    expect(renderArgs([], makeCtx())).toEqual([])
  })
})

describe('renderEnv', () => {
  it('renders all values in env object', () => {
    const ctx = makeCtx()
    const result = renderEnv({ ENV: '{{ conf.env }}', DATE: '{{ ds }}', STATIC: 'unchanged' }, ctx)
    expect(result).toEqual({ ENV: 'prod', DATE: '2024-03-15', STATIC: 'unchanged' })
  })
})

// ══════════════════════════════════════════════════════════════════════════════
// Integration: shell task — verifies substitution reaches the log
// ══════════════════════════════════════════════════════════════════════════════

describe('shell task templating — integration', () => {
  it('{{ dag_id }}, {{ conf.env }}, {{ ds }} are substituted in shell command and visible in task log', async () => {
    const dag: DagDefinition = {
      id: 'tmpl_shell',
      schedule: null,
      tasks: {
        check: {
          shell: {
            interpreter: 'sh',
            command: 'echo "dag={{ dag_id }} env={{ conf.env }} date={{ ds }}"',
          }
        }
      }
    }
    const runId = await runDag(dag)

    const logs = await taskLogs(runId, 'check')
    const output = logs.join(' ')

    // These must appear substituted — not as literal {{ }} placeholders
    expect(output).toContain('dag=tmpl_shell')
    expect(output).toContain('env=staging')
    expect(output).toMatch(/date=\d{4}-\d{2}-\d{2}/)  // YYYY-MM-DD
    expect(output).not.toContain('{{ dag_id }}')
    expect(output).not.toContain('{{ conf.env }}')
    expect(output).not.toContain('{{ ds }}')
  })

  it('shell env vars are also templated', async () => {
    const dag: DagDefinition = {
      id: 'tmpl_shell_env',
      schedule: null,
      tasks: {
        check: {
          shell: {
            interpreter: 'sh',
            command: 'echo "MY_ENV=$MY_ENV"',
            env: { MY_ENV: '{{ conf.env }}-{{ dag_id }}' },
          }
        }
      }
    }
    const runId = await runDag(dag)
    const logs = await taskLogs(runId, 'check')
    expect(logs.join(' ')).toContain('MY_ENV=staging-tmpl_shell_env')
  })

  it('undefined template variable renders as empty string (not literal)', async () => {
    const dag: DagDefinition = {
      id: 'tmpl_undef',
      schedule: null,
      tasks: {
        check: {
          shell: {
            interpreter: 'sh',
            command: 'echo "UNDEF={{ conf.does_not_exist }}"',
          }
        }
      }
    }
    const runId = await runDag(dag)
    const logs = await taskLogs(runId, 'check')
    const output = logs.join(' ')
    expect(output).toContain('UNDEF=')
    expect(output).not.toContain('{{ conf.does_not_exist }}')
  })
})
