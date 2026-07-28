/**
 * Pure utility functions shared between the UI (index.html) and unit tests.
 * No React, no DOM, no network — fully testable with vitest/node.
 *
 * Dual-mode: loaded as a plain <script> in the browser (globals), and as an
 * ES module in tests (named exports). The `export` keyword is stripped when
 * loaded as a classic script — browsers ignore it inside non-module scripts,
 * but the functions still define as globals via `function` / `const` hoisting.
 *
 * Test import:  import { duration, expandForGraph, topoLayers } from '../public/ui-utils.js'
 * Browser load: <script src="/ui-utils.js"></script>  → globals available to Babel script
 */

/**
 * Format a duration between two timestamps.
 * @param {string|Date|null} start
 * @param {string|Date|null} [end]  defaults to now
 * @returns {string}  e.g. '-', '340ms', '1.2s', '3m 14s'
 */
export function duration(start, end) {
  if (!start) return '-';
  const ms = new Date(end ?? Date.now()) - new Date(start);
  if (ms < 1000) return `${ms}ms`;
  if (ms < 60_000) return `${(ms / 1000).toFixed(1)}s`;
  return `${Math.floor(ms / 60_000)}m ${Math.round((ms % 60_000) / 1000)}s`;
}

/**
 * Expand mapped tasks into uniquely-keyed graph nodes.
 * A task with map_index gets node_id = 'task[0]', 'task[1]', etc.
 * Downstream tasks that depend_on a mapped task get edges to ALL instances.
 *
 * @param {Array<{task_id:string, map_index:number|null, depends_on?:string[]}>} tasks
 * @returns {Array} enriched with node_id, display_label, resolved_deps
 */
export function expandForGraph(tasks) {
  const mappedIds = new Set(tasks.filter(t => t.map_index !== null).map(t => t.task_id));
  const nodesByTaskId = {};
  tasks.forEach(t => {
    const nodeId = t.map_index !== null ? `${t.task_id}[${t.map_index}]` : t.task_id;
    (nodesByTaskId[t.task_id] = nodesByTaskId[t.task_id] || []).push(nodeId);
  });

  return tasks.map(t => {
    const nodeId = t.map_index !== null ? `${t.task_id}[${t.map_index}]` : t.task_id;
    const label = t.map_index !== null
      ? `${t.task_id.length > 10 ? t.task_id.slice(0, 9) + '…' : t.task_id}[${t.map_index}]`
      : t.task_id;
    const resolvedDeps = (t.depends_on ?? []).flatMap(dep =>
      mappedIds.has(dep) ? (nodesByTaskId[dep] || [dep]) : [dep]
    );
    return { ...t, node_id: nodeId, display_label: label, resolved_deps: resolvedDeps };
  });
}

/**
 * Topological layer assignment (Kahn's algorithm).
 * Returns array of layers, each layer is an array of node_ids at that depth.
 * Nodes in layer 0 have no dependencies; layer N depends only on layers < N.
 * Cycles / unresolvable nodes land in their own layer at the end.
 *
 * @param {Array<{node_id:string, resolved_deps?:string[]}>} tasks  output of expandForGraph
 * @returns {string[][]}
 */
export function topoLayers(tasks) {
  const layers = [];
  const assigned = {};
  const inDeg = {};

  tasks.forEach(t => { inDeg[t.node_id] = 0; });
  tasks.forEach(t => t.resolved_deps?.forEach(() => {
    inDeg[t.node_id] = (inDeg[t.node_id] || 0) + 1;
  }));

  let queue = tasks.filter(t => (inDeg[t.node_id] || 0) === 0).map(t => t.node_id);
  while (queue.length) {
    layers.push([...queue]);
    queue.forEach(id => { assigned[id] = layers.length - 1; });
    const next = [];
    queue.forEach(id => {
      tasks.forEach(t => {
        if (t.resolved_deps?.includes(id)) {
          inDeg[t.node_id]--;
          if (inDeg[t.node_id] === 0) next.push(t.node_id);
        }
      });
    });
    queue = next;
  }
  tasks.forEach(t => {
    if (assigned[t.node_id] === undefined) layers.push([t.node_id]);
  });

  return layers;
}
