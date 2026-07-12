/**
 * Worker process — runs a single task function in isolation.
 * Receives the task fn as a string via IPC, eval()s it, runs it,
 * then reports success/failure back to the parent.
 */
process.on('message', async (msg: { fn: string; ctx: { dagId: string; runId: string; taskId: string } }) => {
  try {
    // Reconstruct the async function from its string representation
    // eslint-disable-next-line no-new-func
    const fn = new Function(`return (${msg.fn})`)() as (ctx: typeof msg.ctx) => Promise<unknown>
    await fn(msg.ctx)
    process.send!({ success: true })
  } catch (err: unknown) {
    const error = err instanceof Error ? err.message : String(err)
    process.send!({ success: false, error })
  } finally {
    process.exit(0)
  }
})
