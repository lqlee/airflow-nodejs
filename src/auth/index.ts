import type { FastifyRequest, FastifyReply } from 'fastify'

// Comma-separated API keys from env: API_KEYS=key1,key2,key3
const rawKeys = process.env.API_KEYS ?? ''
const VALID_KEYS = new Set(
  rawKeys.split(',').map(k => k.trim()).filter(Boolean)
)

// Auth is disabled when no keys are configured
export const AUTH_ENABLED = VALID_KEYS.size > 0

if (AUTH_ENABLED) {
  console.log(`[auth] API key auth enabled (${VALID_KEYS.size} key(s) configured)`)
} else {
  console.log('[auth] no API_KEYS configured — auth disabled (open access)')
}

// Routes that skip auth
const PUBLIC_PATHS = new Set(['/', '/health'])
const isPublicPath = (path: string) =>
  PUBLIC_PATHS.has(path) || path.startsWith('/assets') || path.endsWith('.js') || path.endsWith('.css')

/**
 * Fastify preHandler hook — validates Bearer token on protected routes.
 */
export async function authHook(req: FastifyRequest, reply: FastifyReply): Promise<void> {
  if (!AUTH_ENABLED) return
  if (isPublicPath(req.url)) return

  const header = req.headers.authorization ?? ''
  const token = header.startsWith('Bearer ') ? header.slice(7).trim() : ''

  if (!token || !VALID_KEYS.has(token)) {
    return reply.status(401).send({ error: 'Unauthorized — provide a valid API key in Authorization: Bearer <key>' })
  }
}

/**
 * Validate a single key (for scripts/tests).
 */
export function isValidKey(key: string): boolean {
  return !AUTH_ENABLED || VALID_KEYS.has(key)
}
