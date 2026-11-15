/**
 * Fork a helper script that sits under src/ (dev) or dist/ (compiled) — `<name>.js` run by node when
 * compiled, `<name>.ts` with the tsx loader in development.
 *
 * In development tsx is loaded with `--import` into a single node process rather than used as the
 * execPath: the tsx CLI wraps a second process and relays IPC as JSON, which would turn Dates into
 * strings and leave the grandchild running when the child is killed.
 * IPC uses structured-clone serialization, so Dates (and Maps, typed arrays, …) cross intact.
 */
import { fork, type ChildProcess } from 'node:child_process'
import { createRequire } from 'node:module'
import { dirname, resolve } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import { userCodeEnv } from './env.js'

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..')   // src/ or dist/
const IS_COMPILED = import.meta.url.endsWith('.js')

/**
 * `relPath` is relative to src/ (or dist/) without extension, e.g. 'dag/parse-worker'.
 * `pipeOutput`: stdout/stderr are pipes the caller reads (task logs); otherwise inherited.
 */
export function forkUserCodeProcess(relPath: string, opts: { pipeOutput?: boolean } = {}): ChildProcess {
  const execArgv = IS_COMPILED ? [] : ['--import', pathToFileURL(createRequire(import.meta.url).resolve('tsx')).href]
  const out = opts.pipeOutput ? 'pipe' : 'inherit'
  return fork(resolve(ROOT, `${relPath}.${IS_COMPILED ? 'js' : 'ts'}`), [], {
    execArgv,
    env: userCodeEnv(),
    serialization: 'advanced',
    stdio: ['ignore', out, out, 'ipc'],
  })
}

/** A task worker (scheduler/worker.ts) whose output the caller captures as the task log. */
export function forkTaskWorker(): ChildProcess {
  return forkUserCodeProcess('scheduler/worker', { pipeOutput: true })
}
