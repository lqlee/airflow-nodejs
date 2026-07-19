import { describe, it, expect, beforeEach, afterEach, vi } from 'vitest'

// We need to re-import the module after patching env vars.
// Use vi.resetModules() + dynamic import to get a fresh module each time.

describe('auth — disabled (no API_KEYS)', () => {
  let mod: typeof import('../index.js')

  beforeEach(async () => {
    vi.resetModules()
    delete process.env.API_KEYS
    mod = await import('../index.js')
  })

  it('AUTH_ENABLED is false when API_KEYS is not set', () => {
    expect(mod.AUTH_ENABLED).toBe(false)
  })

  it('isValidKey returns true for any key when auth is disabled', () => {
    expect(mod.isValidKey('anything')).toBe(true)
    expect(mod.isValidKey('')).toBe(true)
    expect(mod.isValidKey('garbage')).toBe(true)
  })

  it('authHook returns without replying when auth is disabled', async () => {
    const req = { url: '/dags', headers: {} } as any
    const reply = { status: vi.fn().mockReturnThis(), send: vi.fn() } as any
    await mod.authHook(req, reply)
    expect(reply.status).not.toHaveBeenCalled()
    expect(reply.send).not.toHaveBeenCalled()
  })
})

describe('auth — enabled (API_KEYS set)', () => {
  let mod: typeof import('../index.js')

  beforeEach(async () => {
    vi.resetModules()
    process.env.API_KEYS = 'key-alpha, key-beta , key-gamma'
    mod = await import('../index.js')
  })

  afterEach(() => {
    delete process.env.API_KEYS
  })

  it('AUTH_ENABLED is true when API_KEYS is set', () => {
    expect(mod.AUTH_ENABLED).toBe(true)
  })

  it('isValidKey accepts configured keys (trims whitespace)', () => {
    expect(mod.isValidKey('key-alpha')).toBe(true)
    expect(mod.isValidKey('key-beta')).toBe(true)
    expect(mod.isValidKey('key-gamma')).toBe(true)
  })

  it('isValidKey rejects unknown keys', () => {
    expect(mod.isValidKey('key-delta')).toBe(false)
    expect(mod.isValidKey('')).toBe(false)
    expect(mod.isValidKey('Bearer key-alpha')).toBe(false)
  })

  it('authHook passes through on public paths', async () => {
    const reply = { status: vi.fn().mockReturnThis(), send: vi.fn() } as any
    for (const url of ['/', '/health', '/index.js', '/app.css']) {
      const req = { url, headers: {} } as any
      await mod.authHook(req, reply)
    }
    expect(reply.status).not.toHaveBeenCalled()
  })

  it('authHook returns 401 when no Authorization header', async () => {
    const req = { url: '/dags', headers: {} } as any
    const reply = { status: vi.fn().mockReturnThis(), send: vi.fn() } as any
    await mod.authHook(req, reply)
    expect(reply.status).toHaveBeenCalledWith(401)
    expect(reply.send).toHaveBeenCalledWith(expect.objectContaining({ error: expect.stringContaining('Unauthorized') }))
  })

  it('authHook returns 401 for a valid-format but wrong key', async () => {
    const req = { url: '/dags', headers: { authorization: 'Bearer wrong-key' } } as any
    const reply = { status: vi.fn().mockReturnThis(), send: vi.fn() } as any
    await mod.authHook(req, reply)
    expect(reply.status).toHaveBeenCalledWith(401)
  })

  it('authHook passes through for a valid Bearer token', async () => {
    const req = { url: '/dags', headers: { authorization: 'Bearer key-alpha' } } as any
    const reply = { status: vi.fn().mockReturnThis(), send: vi.fn() } as any
    await mod.authHook(req, reply)
    expect(reply.status).not.toHaveBeenCalled()
  })

  it('authHook returns 401 for malformed header (no Bearer prefix)', async () => {
    const req = { url: '/dags', headers: { authorization: 'key-alpha' } } as any
    const reply = { status: vi.fn().mockReturnThis(), send: vi.fn() } as any
    await mod.authHook(req, reply)
    expect(reply.status).toHaveBeenCalledWith(401)
  })
})
