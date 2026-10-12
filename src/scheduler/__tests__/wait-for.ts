/**
 * Test helper: poll until `read()` returns a value that satisfies `ready`.
 *
 * The executor writes task logs and try records fire-and-forget (`void recordTry(...)`),
 * so a test that reads them right after advanceRun() resolves can race the DB write.
 * Returns the last value read, so the caller's own assertion reports a useful diff on timeout.
 */
export async function waitFor<T>(
  read: () => Promise<T>,
  ready: (value: T) => boolean,
  timeoutMs = 5000,
  intervalMs = 25,
): Promise<T> {
  const deadline = Date.now() + timeoutMs
  let value = await read()
  while (!ready(value) && Date.now() < deadline) {
    await new Promise(r => setTimeout(r, intervalMs))
    value = await read()
  }
  return value
}

/**
 * Scales a short "let the event loop settle" sleep for slower database backends: a Spanner round trip
 * is ~20x a local MongoDB one, so tests asserting "X has NOT happened yet" after a fixed delay need more time.
 */
export const scaled = (ms: number): number => ms * (process.env.DB_BACKEND === 'spanner' ? 20 : 1)
