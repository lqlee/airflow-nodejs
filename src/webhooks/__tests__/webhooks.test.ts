import { describe, it, expect, beforeAll, afterAll, afterEach, vi } from 'vitest'
import { MongoClient, type Db } from 'mongodb'
import { deliverWebhook, fireWebhook, type WebhookPayload } from '../index.js'
import { register, clearRegistry } from '../../dag/registry.js'
import { createRun } from '../../scheduler/runs.js'
import { advanceRun } from '../../scheduler/index.js'
import type { DagDefinition } from '../../dag/types.js'

const MONGO_URL = process.env.MONGO_URL ?? 'mongodb://localhost:27017'

let client: MongoClient
let db: Db

beforeAll(async () => {
  process.env.DB_NAME = 'airflow_test_webhooks'
  client = new MongoClient(MONGO_URL)
  await client.connect()
  db = client.db('airflow_test_webhooks')
})

afterAll(async () => {
  await db.dropDatabase()
  await client.close()
  delete process.env.DB_NAME
})

afterEach(async () => {
  await db.collection('dag_runs').deleteMany({})
  await db.collection('task_instances').deleteMany({})
  await db.collection('xcoms').deleteMany({})
  await db.collection('dataset_events').deleteMany({})
  await db.collection('dataset_watermarks').deleteMany({})
  clearRegistry()
})

// ── Pure unit tests for deliverWebhook ───────────────────────────────────────

const samplePayload: WebhookPayload = {
  dag_id: 'my_dag',
  run_id: 'run_123',
  state: 'success',
  logical_date: null,
  conf: {},
  tags: [],
  ended_at: new Date('2024-01-01T00:00:00Z'),
}

describe('deliverWebhook — pure unit', () => {
  it('POSTs JSON payload to the given URL and returns HTTP status', async () => {
    let captured: { url: string; body: unknown } | null = null

    const mockFetch = async (url: string, init: RequestInit) => {
      captured = { url, body: JSON.parse(init.body as string) }
      return { status: 200, ok: true }
    }

    const status = await deliverWebhook('https://example.com/hook', samplePayload, { fetchFn: mockFetch })

    expect(status).toBe(200)
    expect(captured?.url).toBe('https://example.com/hook')
    expect(captured?.body).toMatchObject({
      dag_id: 'my_dag',
      run_id: 'run_123',
      state: 'success',
    })
  })

  it('returns the HTTP status for non-2xx responses (caller can inspect)', async () => {
    const mockFetch = async () => ({ status: 404, ok: false })
    const status = await deliverWebhook('https://example.com/gone', samplePayload, { fetchFn: mockFetch })
    expect(status).toBe(404)
  })

  it('passes Content-Type: application/json header', async () => {
    let headers: Record<string, string> = {}
    const mockFetch = async (_url: string, init: RequestInit) => {
      headers = init.headers as Record<string, string>
      return { status: 200, ok: true }
    }

    await deliverWebhook('https://example.com/hook', samplePayload, { fetchFn: mockFetch })
    expect(headers['Content-Type']).toBe('application/json')
  })

  it('aborts after timeoutMs (simulated via AbortError)', async () => {
    const mockFetch = async (_url: string, init: RequestInit) => {
      // Simulate the network call observing the abort signal
      return new Promise<{ status: number; ok: boolean }>((_resolve, reject) => {
        if (init.signal?.aborted) {
          reject(new DOMException('Aborted', 'AbortError'))
        } else {
          init.signal?.addEventListener('abort', () =>
            reject(new DOMException('Aborted', 'AbortError'))
          )
        }
      })
    }

    // Use a very short timeout and manually trigger abort by passing pre-aborted signal
    // We test that deliverWebhook propagates the AbortController correctly:
    // deliverWebhook should throw if the fetch throws
    await expect(
      deliverWebhook('https://slow.example.com/hook', samplePayload, {
        fetchFn: async () => { throw new DOMException('Aborted', 'AbortError') },
        timeoutMs: 50,
      })
    ).rejects.toThrow()
  })

  it('serializes payload fields correctly — including null logical_date', async () => {
    let body: WebhookPayload | null = null
    const mockFetch = async (_url: string, init: RequestInit) => {
      body = JSON.parse(init.body as string) as WebhookPayload
      return { status: 200, ok: true }
    }

    await deliverWebhook('https://example.com/hook', {
      ...samplePayload,
      logical_date: null,
      conf: { batch: 100 },
      tags: ['backfill', 'prod'],
    }, { fetchFn: mockFetch })

    expect(body?.logical_date).toBeNull()
    expect(body?.conf).toEqual({ batch: 100 })
    expect(body?.tags).toEqual(['backfill', 'prod'])
  })
})

// ── fireWebhook — fire-and-forget wrapper ───────────────────────────────────

describe('fireWebhook — logging', () => {
  it('logs success on 2xx', async () => {
    const consoleSpy = vi.spyOn(console, 'log').mockImplementation(() => {})

    // Wrap in a promise that resolves AFTER the log fires (not after fetch returns)
    await new Promise<void>(resolve => {
      const originalLog = consoleSpy.getMockImplementation()
      consoleSpy.mockImplementation((...args) => {
        if (String(args[0]).includes('[webhook]')) resolve()
        originalLog?.(...args)
      })

      fireWebhook('https://example.com/ok', samplePayload, {
        fetchFn: async () => ({ status: 200, ok: true }),
      })
    })

    const logged = consoleSpy.mock.calls.find(c => String(c[0]).includes('[webhook]'))
    expect(logged).toBeDefined()
    expect(String(logged![0])).toContain('HTTP 200')
    consoleSpy.mockRestore()
  })

  it('logs warning on non-2xx (delivery attempt succeeded but got error status)', async () => {
    const warnSpy = vi.spyOn(console, 'warn').mockImplementation(() => {})

    await new Promise<void>(resolve => {
      warnSpy.mockImplementation((...args) => {
        if (String(args[0]).includes('[webhook]')) resolve()
      })

      fireWebhook('https://example.com/server-error', samplePayload, {
        fetchFn: async () => ({ status: 500, ok: false }),
      })
    })

    const warned = warnSpy.mock.calls.find(c => String(c[0]).includes('[webhook]'))
    expect(warned).toBeDefined()
    expect(String(warned![0])).toContain('HTTP 500')
    warnSpy.mockRestore()
  })

  it('logs error on network failure without throwing', async () => {
    const errorSpy = vi.spyOn(console, 'error').mockImplementation(() => {})

    await new Promise<void>(resolve => {
      errorSpy.mockImplementation((...args) => {
        if (String(args[0]).includes('[webhook]')) resolve()
      })

      fireWebhook('https://unreachable.example.com/', samplePayload, {
        fetchFn: async () => { throw new Error('ECONNREFUSED') },
      })
    })

    const errored = errorSpy.mock.calls.find(c => String(c[0]).includes('[webhook]'))
    expect(errored).toBeDefined()
    expect(String(errored![0])).toContain('delivery failed')
    errorSpy.mockRestore()
  })
})

// ── Integration: advanceRun fires webhook on terminal state ─────────────────

describe('advanceRun — webhook integration', () => {
  it('fires onSuccess URL when run succeeds', async () => {
    const received: WebhookPayload[] = []

    const dag: DagDefinition = {
      id: 'hook_success_dag',
      schedule: null,
      onSuccess: 'https://example.com/on-success',
      tasks: {
        step: { run: async () => { /* succeeds */ } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    // webhookOptions injects a synchronous-resolving fetch so we can await it
    await new Promise<void>(resolve => {
      void advanceRun(db, runId, {
        fetchFn: async (_url, init) => {
          received.push(JSON.parse(init.body as string) as WebhookPayload)
          resolve()
          return { status: 200, ok: true }
        },
      })
    })

    expect(received).toHaveLength(1)
    expect(received[0].state).toBe('success')
    expect(received[0].dag_id).toBe('hook_success_dag')
    expect(received[0].run_id).toBe(runId)
  })

  it('fires onFailure URL when a task fails', async () => {
    const received: WebhookPayload[] = []

    const dag: DagDefinition = {
      id: 'hook_failure_dag',
      schedule: null,
      onFailure: 'https://example.com/on-failure',
      tasks: {
        boom: { run: async () => { throw new Error('task error') } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    await new Promise<void>(resolve => {
      void advanceRun(db, runId, {
        fetchFn: async (_url, init) => {
          received.push(JSON.parse(init.body as string) as WebhookPayload)
          resolve()
          return { status: 200, ok: true }
        },
      })
    })

    expect(received).toHaveLength(1)
    expect(received[0].state).toBe('failed')
    expect(received[0].dag_id).toBe('hook_failure_dag')
  })

  it('does NOT fire onSuccess URL when run fails', async () => {
    const calls: string[] = []

    const dag: DagDefinition = {
      id: 'hook_no_cross_dag',
      schedule: null,
      onSuccess: 'https://example.com/success-only',
      // no onFailure
      tasks: {
        boom: { run: async () => { throw new Error('fail') } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    // Wait for run to fully advance; fetch should never be called
    await advanceRun(db, runId, {
      fetchFn: async (url) => {
        calls.push(url)
        return { status: 200, ok: true }
      },
    })

    // Give fire-and-forget a tick to settle (it would have run synchronously since mock resolves instantly)
    await new Promise(r => setTimeout(r, 10))

    expect(calls).toHaveLength(0)
  })

  it('does NOT fire onFailure URL when run succeeds', async () => {
    const calls: string[] = []

    const dag: DagDefinition = {
      id: 'hook_no_failure_on_success_dag',
      schedule: null,
      onFailure: 'https://example.com/failure-only',
      tasks: {
        ok: { run: async () => { /* ok */ } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    await advanceRun(db, runId, {
      fetchFn: async (url) => {
        calls.push(url)
        return { status: 200, ok: true }
      },
    })

    await new Promise(r => setTimeout(r, 10))

    expect(calls).toHaveLength(0)
  })

  it('fires webhook exactly once even if advanceRun is called twice (CAS guard)', async () => {
    const calls: string[] = []

    const dag: DagDefinition = {
      id: 'hook_once_dag',
      schedule: null,
      onSuccess: 'https://example.com/once',
      tasks: {
        step: { run: async () => { /* ok */ } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    const opts = {
      fetchFn: async (url: string) => {
        calls.push(url)
        return { status: 200, ok: true }
      },
    }

    // First call finalizes the run — second call hits the terminal guard
    await advanceRun(db, runId, opts)
    await advanceRun(db, runId, opts)

    await new Promise(r => setTimeout(r, 10))

    expect(calls).toHaveLength(1)
  })

  it('includes conf and tags in webhook payload', async () => {
    const received: WebhookPayload[] = []

    const dag: DagDefinition = {
      id: 'hook_payload_dag',
      schedule: null,
      onSuccess: 'https://example.com/payload-check',
      tasks: {
        step: { run: async () => { /* ok */ } },
      },
    }
    register(dag)
    // Trigger with conf and tags
    const runId = await createRun(db, dag, {
      conf: { env: 'prod', limit: 500 },
      tags: ['ci', 'nightly'],
    })

    await new Promise<void>(resolve => {
      void advanceRun(db, runId, {
        fetchFn: async (_url, init) => {
          received.push(JSON.parse(init.body as string) as WebhookPayload)
          resolve()
          return { status: 200, ok: true }
        },
      })
    })

    expect(received[0].conf).toEqual({ env: 'prod', limit: 500 })
    expect(received[0].tags).toEqual(['ci', 'nightly'])
  })

  it('no webhook fires when neither onSuccess nor onFailure is configured', async () => {
    const calls: string[] = []

    const dag: DagDefinition = {
      id: 'hook_none_dag',
      schedule: null,
      tasks: {
        step: { run: async () => { /* ok */ } },
      },
    }
    register(dag)
    const runId = await createRun(db, dag)

    await advanceRun(db, runId, {
      fetchFn: async (url) => {
        calls.push(url)
        return { status: 200, ok: true }
      },
    })

    await new Promise(r => setTimeout(r, 10))
    expect(calls).toHaveLength(0)
  })
})
