/**
 * Unit tests for dag/images.ts — ensureImages() behaviour.
 *
 * Tests verify real behaviour without needing a live Docker daemon:
 * - Empty list → no-op
 * - Plain image names → no-op (docker load not called)
 * - Missing .tar file → logged error, no crash
 * - Docker socket absent → skipped with warning
 * - Real .tar load (integration, skipped if docker unavailable)
 */

import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest'
import { execSync } from 'node:child_process'
import { writeFile, mkdir, rm } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { ensureImages } from '../images.js'

let logLines: string[] = []
let warnLines: string[] = []
let errorLines: string[] = []

beforeEach(() => {
  logLines = []; warnLines = []; errorLines = []
  vi.spyOn(console, 'log').mockImplementation((...a) => { logLines.push(a.join(' ')) })
  vi.spyOn(console, 'warn').mockImplementation((...a) => { warnLines.push(a.join(' ')) })
  vi.spyOn(console, 'error').mockImplementation((...a) => { errorLines.push(a.join(' ')) })
})

afterEach(() => {
  vi.restoreAllMocks()
})

function allOutput() {
  return [...logLines, ...warnLines, ...errorLines].join('\n')
}

function dockerAvailable(): boolean {
  try {
    execSync('docker info', { stdio: 'ignore', timeout: 5000 })
    return true
  } catch { return false }
}

// ══════════════════════════════════════════════════════════════════════════════

describe('ensureImages()', () => {
  it('does nothing when images array is empty', async () => {
    await ensureImages('my_dag', [])
    expect(logLines).toHaveLength(0)
    expect(warnLines).toHaveLength(0)
    expect(errorLines).toHaveLength(0)
  })

  it('does nothing for plain image names (no .tar extension)', async () => {
    await ensureImages('my_dag', ['python:3.13-slim', 'alpine:3.20', 'redis:7-alpine'])
    // Plain image names require no docker load — nothing should be logged
    expect(logLines).toHaveLength(0)
    expect(warnLines).toHaveLength(0)
    expect(errorLines).toHaveLength(0)
  })

  it('logs error for non-existent .tar file without crashing', async () => {
    await ensureImages('my_dag', ['/absolutely/does/not/exist/image.tar'])
    expect(allOutput()).toMatch(/not found/i)
    // No exception thrown — dag still loads
  })

  it('skips .tar loading when Docker socket is absent', async () => {
    // Override ALL socket discovery paths so none can be found
    const origSocket = process.env.DOCKER_SOCKET
    const origHost   = process.env.DOCKER_HOST
    const origHome   = process.env.HOME
    process.env.DOCKER_SOCKET = '/tmp/no-such-socket-xyz-abc'
    process.env.DOCKER_HOST   = 'unix:///tmp/no-such-socket-xyz-abc'
    process.env.HOME           = '/tmp/no-home-xyz'
    try {
      const tmpTar = join(tmpdir(), 'dummy-test-img.tar')
      await writeFile(tmpTar, 'not-a-real-tar')
      try {
        await ensureImages('my_dag', [tmpTar])
      } finally {
        await rm(tmpTar, { force: true })
      }
      expect(allOutput()).toMatch(/socket not found|Docker socket/i)
    } finally {
      if (origSocket === undefined) delete process.env.DOCKER_SOCKET; else process.env.DOCKER_SOCKET = origSocket
      if (origHost   === undefined) delete process.env.DOCKER_HOST;   else process.env.DOCKER_HOST   = origHost
      if (origHome   === undefined) delete process.env.HOME;           else process.env.HOME           = origHome
    }
  })

  it('handles mix of .tar and plain image names — only tars trigger load attempt', async () => {
    await ensureImages('my_dag', [
      'python:3.13-slim',          // plain — silent
      '/nonexistent/path/img.tar', // tar — should log error
      'ruby:3.3-slim',             // plain — silent
    ])
    // Plain image names are completely silent
    expect(allOutput()).not.toMatch(/python:3\.13-slim/)
    expect(allOutput()).not.toMatch(/ruby:3\.3-slim/)
    // The tar triggers an error (file not found)
    expect(errorLines.join(' ')).toMatch(/not found/i)
  })

  it('(integration) exports and reloads a real image via .tar', async () => {
    if (!dockerAvailable()) return

    // Confirm alpine is locally available (it's our 13 MB test image)
    try {
      execSync('docker image inspect alpine:latest', { stdio: 'ignore', timeout: 5000 })
    } catch { return } // not local — skip

    const tmpDir = join(tmpdir(), `airflow-img-test-${Date.now()}`)
    await mkdir(tmpDir, { recursive: true })
    const tarPath = join(tmpDir, 'alpine-test.tar')

    try {
      execSync(`docker save alpine:latest -o ${tarPath}`, { timeout: 30000, stdio: 'ignore' })

      // Let images.ts auto-discover the socket (DOCKER_HOST set by docker context)
      const origSocket = process.env.DOCKER_SOCKET
      delete process.env.DOCKER_SOCKET

      try {
        await ensureImages('test_dag', [tarPath])
      } finally {
        if (origSocket !== undefined) process.env.DOCKER_SOCKET = origSocket
        else delete process.env.DOCKER_SOCKET
      }

      // docker load on an already-present image outputs "Loaded image: alpine:latest"
      expect(allOutput()).toMatch(/✓|[Ll]oaded image/i)
    } finally {
      await rm(tmpDir, { recursive: true, force: true })
    }
  })
})
