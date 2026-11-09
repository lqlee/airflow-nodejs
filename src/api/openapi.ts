/**
 * OpenAPI 3.1 document generated from the registered Fastify routes.
 *
 * Covers paths, methods, path parameters, tags (first path segment), auth requirements, the minimum
 * role per operation, and — from openapi-schemas.ts — query parameters and request bodies.
 * Response bodies are not described.
 */
import type { FastifyInstance } from 'fastify'
import { AUTH_ENABLED, isPublicPath, requiredRoleForRoute, type Role } from '../auth/index.js'
import { OPERATION_DOCS, type OperationDoc } from './openapi-schemas.js'

export interface RouteInfo {
  method: string
  url: string
  requiredRole?: Role
}

export const BODY_METHODS = new Set(['post', 'put', 'patch'])
const ERROR_SCHEMA = { type: 'object', properties: { error: { type: 'string' } }, required: ['error'] }

/** Convert Fastify `:param` segments to OpenAPI `{param}`; returns the param names too. */
export function toOpenApiPath(url: string): { path: string; params: string[] } {
  const params: string[] = []
  const path = url.replace(/:([A-Za-z0-9_]+)\??/g, (_m, name: string) => { params.push(name); return `{${name}}` })
  return { path, params }
}

export function buildOpenApiDocument(
  routes: RouteInfo[],
  opts: { title: string; version: string; authEnabled: boolean; docs?: Record<string, OperationDoc> },
) {
  const docs = opts.docs ?? OPERATION_DOCS
  const paths: Record<string, Record<string, unknown>> = {}
  for (const r of routes) {
    const method = r.method.toLowerCase()
    if (method === 'head' || method === 'options' || r.url.includes('*')) continue
    const { path, params } = toOpenApiPath(r.url)
    const role = requiredRoleForRoute(method, path, r.requiredRole)
    const isPublic = isPublicPath(path)
    const segment = path.split('/').filter(Boolean)[0] ?? 'root'
    const doc = docs[`${method.toUpperCase()} ${r.url}`]

    const operation: Record<string, unknown> = {
      operationId: `${method}_${path.replace(/[^A-Za-z0-9]+/g, '_').replace(/^_|_$/g, '') || 'root'}`,
      ...(doc?.summary ? { summary: doc.summary } : {}),
      tags: [segment],
      parameters: [
        ...params.map(name => ({ name, in: 'path', required: true, schema: { type: 'string' } })),
        ...Object.entries(doc?.query ?? {}).map(([name, { description, ...schema }]) =>
          ({ name, in: 'query', required: false, ...(description ? { description } : {}), schema })),
      ],
      responses: {
        '200': { description: 'Success' },
        ...(doc?.body || doc?.query ? { '400': { description: 'Invalid input', content: { 'application/json': { schema: ERROR_SCHEMA } } } } : {}),
        ...(opts.authEnabled && !isPublic ? { '401': { description: 'Missing or invalid credentials' }, '403': { description: `Requires the '${role}' role` } } : {}),
      },
    }
    if (doc?.body) operation.requestBody = { required: true, content: { 'application/json': { schema: doc.body } } }
    else if (doc?.content) operation.requestBody = { required: true, content: doc.content }
    else if (BODY_METHODS.has(method) && !doc?.noBody) operation.requestBody = { required: false, content: { 'application/json': { schema: { type: 'object' } } } }
    if (opts.authEnabled && !isPublic) {
      operation.security = [{ bearerAuth: [] }]
      operation['x-required-role'] = role
    } else {
      operation.security = []
    }
    ;(paths[path] ??= {})[method] = operation
  }

  return {
    openapi: '3.1.0',
    info: { title: opts.title, version: opts.version },
    paths: Object.fromEntries(Object.entries(paths).sort(([a], [b]) => a.localeCompare(b))),
    components: {
      securitySchemes: {
        bearerAuth: { type: 'http', scheme: 'bearer', description: 'API key or OIDC JWT in Authorization: Bearer <token>' },
      },
    },
  }
}

/** Must be called BEFORE any route is registered so the onRoute hook sees them all. */
export function setupOpenApi(app: FastifyInstance, version: string): void {
  const routes: RouteInfo[] = []
  app.addHook('onRoute', (route) => {
    const cfg = route.config as { requiredRole?: Role } | undefined
    for (const method of [route.method].flat()) routes.push({ method, url: route.url, requiredRole: cfg?.requiredRole })
  })
  app.after(() => {
    app.get('/openapi.json', async (_req, reply) => {
      reply.header('Cache-Control', 'no-store')
      return buildOpenApiDocument(routes, { title: 'airflow-nodejs API', version, authEnabled: AUTH_ENABLED })
    })
  })
}
