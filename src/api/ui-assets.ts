/**
 * UI assets served without a CDN, and the browser security headers.
 *
 *   /vendor/react.js, /vendor/react-dom.js   React 18 UMD builds from node_modules (a pinned dependency)
 *   /app.js                                  the UI: dist/public/app.js when running the compiled server,
 *                                            compiled from src/ui/app.jsx on request in dev (tsx)
 *
 * Every response carries a Content-Security-Policy that only allows this origin's scripts, styles and
 * API calls — an injected <script> or a hijacked CDN cannot run, and the page cannot be framed.
 */
import { readFile, stat } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { dirname, join } from 'node:path'
import type { FastifyInstance } from 'fastify'
import { UI_OUTPUT, UI_SOURCE, compileUi } from '../ui/build.js'

const IS_COMPILED = import.meta.url.endsWith('.js')
const require = createRequire(import.meta.url)

/** [package, file inside it] — the UMD builds are not in the packages' `exports`, so resolve the package root. */
const VENDOR: Record<string, [string, string]> = {
  'react.js': ['react', 'umd/react.production.min.js'],
  'react-dom.js': ['react-dom', 'umd/react-dom.production.min.js'],
}

export const CONTENT_SECURITY_POLICY = [
  "default-src 'self'",
  "script-src 'self'",
  "style-src 'self'",
  "img-src 'self' data:",
  "connect-src 'self'",
  "object-src 'none'",
  "base-uri 'none'",
  "frame-ancestors 'none'",
  "form-action 'self'",
].join('; ')

export const SECURITY_HEADERS: Record<string, string> = {
  'content-security-policy': CONTENT_SECURITY_POLICY,
  'x-content-type-options': 'nosniff',
  'x-frame-options': 'DENY',
  'referrer-policy': 'no-referrer',
  'cross-origin-opener-policy': 'same-origin',
}

let devCache: { mtimeMs: number; code: string } | null = null

/** The UI script: the build output, or (dev) a fresh compile whenever app.jsx changes. */
async function uiScript(): Promise<{ code: string } | { error: string }> {
  if (IS_COMPILED) {
    try { return { code: await readFile(UI_OUTPUT, 'utf8') } } catch {
      return { error: `UI bundle missing at ${UI_OUTPUT} — run npm run build` }
    }
  }
  const { mtimeMs } = await stat(UI_SOURCE)
  if (devCache?.mtimeMs !== mtimeMs) {
    const { code, errors } = await compileUi(await readFile(UI_SOURCE, 'utf8'))
    if (errors.length) return { error: `src/ui/app.jsx does not compile:\n${errors.join('\n')}` }
    devCache = { mtimeMs, code }
  }
  return { code: devCache.code }
}

export function registerUiAssets(app: FastifyInstance): void {
  app.addHook('onSend', async (_req, reply, payload) => {
    for (const [k, v] of Object.entries(SECURITY_HEADERS)) if (!reply.hasHeader(k)) reply.header(k, v)
    return payload
  })

  app.get('/vendor/:file', async (req, reply) => {
    const target = VENDOR[(req.params as { file: string }).file]
    if (!target) return reply.status(404).send({ error: 'Not found' })
    const [pkg, file] = target
    const code = await readFile(join(dirname(require.resolve(`${pkg}/package.json`)), file), 'utf8')
    return reply.type('application/javascript; charset=utf-8').header('cache-control', 'public, max-age=86400').send(code)
  })

  app.get('/app.js', async (_req, reply) => {
    const result = await uiScript()
    if ('error' in result) {
      console.error(`[ui] ${result.error}`)
      return reply.status(500).type('text/plain; charset=utf-8').send(result.error)
    }
    // Revalidate on every load: a redeploy must not leave browsers on an old UI
    return reply.type('application/javascript; charset=utf-8').header('cache-control', 'no-cache').send(result.code)
  })
}
