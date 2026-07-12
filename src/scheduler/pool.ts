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

/** Current active worker count (for monitoring). */
export function activeWorkers(): number {
  return active
}

/** Current queue depth (for monitoring). */
export function queueDepth(): number {
  return queue.length
}
