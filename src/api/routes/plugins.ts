/**
 * Plugins API — exposes the registered route modules in this scheduler.
 *
 * In Apache Airflow, "plugins" are Python packages that extend the DAG authoring
 * surface (operators, hooks, macros, UI blueprints). The Node.js equivalent is
 * the set of Fastify route modules registered in server.ts.
 *
 * This is a static registry — plugins are registered at startup and don't change
 * at runtime. The list is defined here rather than auto-discovered, keeping the
 * API honest about what's actually wired.
 */

import type { FastifyInstance } from 'fastify'

export interface PluginInfo {
  name: string
  description: string
  /** Route prefix(es) this plugin registers */
  routes: string[]
  /** Feature category */
  category: 'core' | 'scheduling' | 'auth' | 'observability' | 'lifecycle' | 'discovery'
}

// Static registry of all registered Fastify route modules.
// Update when new route files are added to server.ts.
export const PLUGIN_REGISTRY: PluginInfo[] = [
  {
    name: 'dags',
    description: 'Dag listing, triggering, pause/resume, stats, backfill, versions, source',
    routes: ['/dags', '/dags/:dagId', '/dags/:dagId/trigger', '/dags/:dagId/pause',
      '/dags/:dagId/resume', '/dags/:dagId/stats', '/dags/:dagId/backfill',
      '/dags/:dagId/tasks', '/dags/:dagId/versions', '/dags/:dagId/source'],
    category: 'core',
  },
  {
    name: 'dag-runs',
    description: 'Dag run management, task instances, logs, XCom, cancellation',
    routes: ['/dag-runs/:runId', '/dag-runs/:runId/cancel', '/dag-runs/:runId/tasks',
      '/dag-runs/:runId/xcoms', '/dag-runs/:runId/note'],
    category: 'core',
  },
  {
    name: 'task-instances',
    description: 'Individual task instance query and clear-to-retry',
    routes: ['/dag-runs/:runId/tasks/:taskId', '/dag-runs/:runId/tasks/:taskId/clear',
      '/dag-runs/:runId/tasks/:taskId/logs'],
    category: 'core',
  },
  {
    name: 'connections',
    description: 'Connection store — encrypted credentials for external systems',
    routes: ['/connections', '/connections/:connId'],
    category: 'core',
  },
  {
    name: 'variables',
    description: 'Variable store — key/value config with optional encryption',
    routes: ['/variables', '/variables/:key'],
    category: 'core',
  },
  {
    name: 'xcom',
    description: 'XCom full CRUD — inter-task data exchange',
    routes: ['/dag-runs/:runId/xcoms', '/dag-runs/:runId/xcoms/:taskId/:key'],
    category: 'core',
  },
  {
    name: 'backfills',
    description: 'Backfill lifecycle — list, pause, resume, cancel',
    routes: ['/backfills', '/backfills/:backfillId', '/backfills/:backfillId/pause',
      '/backfills/:backfillId/resume', '/backfills/:backfillId/cancel'],
    category: 'scheduling',
  },
  {
    name: 'pools',
    description: 'Resource pool CRUD and slot tracking',
    routes: ['/pools', '/pools/:name'],
    category: 'scheduling',
  },
  {
    name: 'datasets',
    description: 'Dataset events and data-aware scheduling',
    routes: ['/datasets', '/datasets/:uri/events'],
    category: 'scheduling',
  },
  {
    name: 'sla',
    description: 'SLA breach alerts and acknowledgement',
    routes: ['/sla-alerts', '/sla-alerts/:alertId/ack'],
    category: 'observability',
  },
  {
    name: 'event-logs',
    description: 'Audit trail — paginated event log for all scheduler actions',
    routes: ['/event-logs'],
    category: 'observability',
  },
  {
    name: 'import-errors',
    description: 'Dag import errors from the most recent loader run',
    routes: ['/import-errors'],
    category: 'observability',
  },
  {
    name: 'api-keys',
    description: 'API key management (admin only)',
    routes: ['/api-keys', '/api-keys/:keyId'],
    category: 'auth',
  },
  {
    name: 'hitl',
    description: 'Human-in-the-Loop approval workflow',
    routes: ['/hitl', '/hitl/:runId/:taskId'],
    category: 'lifecycle',
  },
  {
    name: 'providers',
    description: 'Runtime npm package discovery',
    routes: ['/providers', '/providers/:packageName'],
    category: 'discovery',
  },
  {
    name: 'plugins',
    description: 'Registered plugin/route module discovery (this endpoint)',
    routes: ['/plugins', '/plugins/:name'],
    category: 'discovery',
  },
]

export async function pluginsRoutes(app: FastifyInstance): Promise<void> {
  // GET /plugins — list all registered route modules
  app.get('/plugins', async (_req, reply) => {
    return reply.send({
      plugins: PLUGIN_REGISTRY,
      total_entries: PLUGIN_REGISTRY.length,
    })
  })

  // GET /plugins/:name — single plugin details
  app.get<{ Params: { name: string } }>('/plugins/:name', async (req, reply) => {
    const plugin = PLUGIN_REGISTRY.find(p => p.name === req.params.name)
    if (!plugin) {
      return reply.status(404).send({ error: `Plugin '${req.params.name}' not found` })
    }
    return reply.send(plugin)
  })

  // GET /plugins/categories — unique category list
  app.get('/plugins/categories', async (_req, reply) => {
    const categories = [...new Set(PLUGIN_REGISTRY.map(p => p.category))].sort()
    return reply.send({ categories })
  })
}
