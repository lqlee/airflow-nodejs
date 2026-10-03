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
