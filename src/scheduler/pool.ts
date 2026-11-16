/**
 * Simple semaphore — limits concurrent worker processes globally.
 * Default: 8 concurrent tasks. Override with MAX_WORKERS env var.
 */
const MAX_WORKERS = Number(process.env.MAX_WORKERS ?? 8)

let active = 0
const queue: Array<() => void> = []

/** Acquire a slot. Waits if at capacity. */
export function acquire(): Promise<void> {
  if (active < MAX_WORKERS) {
    active++
    return Promise.resolve()
  }
  return new Promise((resolve) => queue.push(resolve))
}

/** Release a slot, unblocking next waiter if any. */
export function release(): void {
  const next = queue.shift()
  if (next) {
    next()  // hand slot directly to next waiter (active stays same)
  } else {
    active--
  }
}

/**
 * Work started by task execution that must finish before the database closes: a forked task until its
 * outcome is written, and fire-and-forget writes such as task log lines. drainPool() waits for it.
 */
let pending = 0

export function trackWork<T>(work: Promise<T>): Promise<T> {
  pending++
  return work.finally(() => { pending-- })
}

export function pendingWork(): number {
  return pending
}

/** Current active worker count (for monitoring). */
export function activeWorkers(): number {
  return active
}

/** Slots free right now (claims are capped by this so tasks are not claimed only to wait here). */
export function freeWorkerSlots(): number {
  return Math.max(0, MAX_WORKERS - active - queue.length)
}

/** Current queue depth (for monitoring). */
export function queueDepth(): number {
  return queue.length
}

// ── Pool reset (for tests) ────────────────────────────────────────────────

/** Reset pool counters to zero — only for use in tests. */
export function resetPool(): void {
  active = 0
  queue.length = 0
}

// ── Drain ─────────────────────────────────────────────────────────────────

export interface DrainResult {
  /** True if active, queued and pending work all reached 0 before timeout. */
  drained: boolean
  /** Remaining active + queued workers + pending work when the function returned. */
  remaining: number
}

export interface DrainOptions {
  /** Total ms to wait before giving up. Default: 20 000. */
  timeoutMs?: number
  /** Poll interval in ms. Default: 200. */
  pollMs?: number
  /** Injected active counter for testing. Default: activeWorkers. */
  getActive?: () => number
  /** Injected queue depth counter for testing. Default: queueDepth. */
  getQueued?: () => number
  /** Injected pending-work counter for testing. Default: pendingWork. */
  getPending?: () => number
}

/**
 * Wait until active workers, the waiting queue and pending work (outcome and task-log writes, see
 * trackWork) reach zero, or until timeoutMs elapses.
 *
 * This MUST be called before closeDb() on graceful shutdown:
 * - active > 0 → a child process is still running and writing to the DB
 * - queued > 0 → a waiter will acquire() → fork() → write to DB once a slot
 *   frees, even if we stop forking new tasks.
 * - pending > 0 → a finished task's outcome or log lines are still being written.
 *
 * Only relevant in local-fork mode (USE_BULLMQ=false). In BullMQ mode the
 * pool is unused; stopWorker() (Worker.close()) handles its own drain.
 */
export async function drainPool(opts: DrainOptions = {}): Promise<DrainResult> {
  const {
    timeoutMs = 20_000,
    pollMs = 200,
    getActive = activeWorkers,
    getQueued = queueDepth,
    getPending = pendingWork,
  } = opts

  const deadline = Date.now() + timeoutMs

  while (Date.now() < deadline) {
    if (getActive() === 0 && getQueued() === 0 && getPending() === 0) return { drained: true, remaining: 0 }
    await new Promise<void>(resolve => setTimeout(resolve, pollMs))
  }

  const remaining = getActive() + getQueued() + getPending()
  return { drained: false, remaining }
}
