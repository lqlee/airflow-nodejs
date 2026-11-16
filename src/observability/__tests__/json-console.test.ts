/**
 * LOG_FORMAT=json: console output of the server processes as one JSON object per line.
 */
import { describe, it, expect, afterEach, vi } from 'vitest'
import { formatLogRecord, installJsonConsole, uninstallJsonConsole, echoTaskLine } from '../json-console.js'
import { registerSecret, clearSecrets } from '../../secrets/masker.js'

afterEach(() => {
  uninstallJsonConsole()
  clearSecrets()
  vi.unstubAllEnvs()
  vi.restoreAllMocks()
})

const ti = { dag_id: 'etl', dag_run_id: 'r1', task_id: 'load', map_index: 2 }

describe('formatLogRecord', () => {
  it('turns the [component] prefix into the logger and formats the arguments like console does', () => {
    const now = new Date('2026-03-02T06:00:00.000Z')
    expect(formatLogRecord('info', ['\n[scheduler] run %s → %s', 'r1', 'success'], now)).toEqual({
      ts: '2026-03-02T06:00:00.000Z', level: 'info', logger: 'scheduler', msg: 'run r1 → success',
      service: 'airflow-nodejs', pid: process.pid,
    })
    expect(formatLogRecord('warn', ['no prefix', { n: 1 }], now)).toMatchObject({ msg: 'no prefix { n: 1 }' })
    expect(formatLogRecord('warn', ['x'], now).logger).toBeUndefined()
  })

  it('puts an Error argument in err, keeping the message readable', () => {
    const err = new TypeError('boom')
    const rec = formatLogRecord('error', ['[main] fatal error:', err])
    expect(rec).toMatchObject({ level: 'error', logger: 'main', msg: 'fatal error: boom', err: { name: 'TypeError', message: 'boom' } })
    expect(rec.err!.stack).toContain('TypeError: boom')
  })

  it('masks known secret values in the message and the error', () => {
    registerSecret('hunter2-token')
    const rec = formatLogRecord('error', ['[conn] login with hunter2-token failed', new Error('bad hunter2-token')])
    expect(JSON.stringify(rec)).not.toContain('hunter2-token')
  })

  it('takes the service name from OTEL_SERVICE_NAME', () => {
    vi.stubEnv('OTEL_SERVICE_NAME', 'airflow-prod')
    expect(formatLogRecord('info', ['x']).service).toBe('airflow-prod')
  })
})

describe('installJsonConsole', () => {
  it('is off unless LOG_FORMAT=json', () => {
    vi.stubEnv('LOG_FORMAT', '')
    expect(installJsonConsole({ write: () => {} })).toBe(false)
    vi.stubEnv('LOG_FORMAT', 'JSON')
    expect(installJsonConsole({ write: () => {} })).toBe(true)
  })

  it('writes every console method as one parseable line at its level, and uninstall restores console', () => {
    const out: string[] = []
    const original = console.warn
    installJsonConsole({ force: true, write: l => out.push(l) })
    console.log('[api] listening')
    console.warn('[pool] full')
    console.error('[x] multi\nline')
    expect(out.every(l => l.endsWith('\n') && l.indexOf('\n') === l.length - 1)).toBe(true)
    expect(out.map(l => JSON.parse(l)).map(r => [r.level, r.logger, r.msg])).toEqual([
      ['info', 'api', 'listening'], ['warn', 'pool', 'full'], ['error', 'x', 'multi\nline'],
    ])
    uninstallJsonConsole()
    expect(console.warn).toBe(original)
  })
})

describe('echoTaskLine', () => {
  it('writes the plain text when JSON logging is off', () => {
    const write = vi.spyOn(process.stderr, 'write').mockImplementation(() => true)
    echoTaskLine(ti, 'stderr', 'oops', '[load] oops')
    expect(write).toHaveBeenCalledWith('[load] oops\n')
  })

  it('writes a record tagged with the task instance, at the level the line declares', () => {
    const out: string[] = []
    installJsonConsole({ force: true, write: l => out.push(l) })
    echoTaskLine(ti, 'stdout', '[WARN] disk almost full')
    echoTaskLine(ti, 'stderr', 'Traceback…')
    expect(out.map(l => JSON.parse(l))).toEqual([
      expect.objectContaining({ level: 'warn', logger: 'task', msg: '[WARN] disk almost full', dag_id: 'etl', run_id: 'r1', task_id: 'load', map_index: 2 }),
      expect.objectContaining({ level: 'error', msg: 'Traceback…' }),
    ])
  })
})
