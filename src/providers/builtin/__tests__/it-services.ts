/**
 * Service detection for the provider integration tests: each block runs when the services it needs
 * (docker-compose.providers-test.yml) are reachable, and is skipped — with a log line saying why and
 * how to start them — when they are not.
 *
 *   PROVIDERS_IT unset  auto: run what is reachable, skip the rest
 *   PROVIDERS_IT=1      required: run everything; an unreachable service fails its tests (CI)
 *   PROVIDERS_IT=0      off: skip everything
 */
import { connect } from 'node:net'

export const SERVICES = {
  postgres: 55432,
  mysql: 53306,
  kafka: 59092,
  mailpit: 51025,
  gcs: 54443,
  bigquery: 59050,
} as const

export type Service = keyof typeof SERVICES

const COMPOSE = 'docker-compose -f docker-compose.providers-test.yml'
const TEST_FILE = 'src/providers/builtin/__tests__/integration.test.ts'

function reachable(port: number, timeoutMs = 750): Promise<boolean> {
  return new Promise(resolve => {
    const socket = connect({ host: '127.0.0.1', port })
    const done = (ok: boolean) => { socket.destroy(); resolve(ok) }
    socket.setTimeout(timeoutMs, () => done(false))
    socket.once('connect', () => done(true))
    socket.once('error', () => done(false))
  })
}

export interface ServiceCheck {
  /** True when every service in `needed` may be used (reachable, or PROVIDERS_IT=1). */
  has: (...needed: Service[]) => boolean
}

/** Probe every service once and log what will run and what will be skipped (and why). */
export async function checkServices(): Promise<ServiceCheck> {
  const mode = process.env.PROVIDERS_IT === '1' ? 'required' : process.env.PROVIDERS_IT === '0' ? 'off' : 'auto'
  const names = Object.keys(SERVICES) as Service[]
  const up = new Set<Service>()
  if (mode !== 'off') {
    const results = await Promise.all(names.map(n => reachable(SERVICES[n])))
    names.forEach((n, i) => { if (results[i]) up.add(n) })
  }
  const down = names.filter(n => !up.has(n))
  const list = (ns: Service[]) => ns.map(n => `${n} (localhost:${SERVICES[n]})`).join(', ')

  if (mode === 'off') {
    console.info(`[providers-it] skipped: PROVIDERS_IT=0. To run them: ${COMPOSE} up -d && npx vitest run ${TEST_FILE}`)
  } else if (down.length === 0) {
    console.info(`[providers-it] running against ${list(names)}. Stop the services afterwards with: ${COMPOSE} down`)
  } else if (mode === 'required') {
    console.warn(`[providers-it] PROVIDERS_IT=1 but not reachable: ${list(down)} — their tests will fail. Start them with: ${COMPOSE} up -d`)
  } else {
    console.info(
      `[providers-it] skipping tests that need ${list(down)}: not reachable.` +
      (up.size ? ` Running those that need only ${list([...up])}.` : '') +
      ` To run them: ${COMPOSE} up -d && npx vitest run ${TEST_FILE}` +
      ` (stop with: ${COMPOSE} down; PROVIDERS_IT=1 makes an unreachable service a failure, PROVIDERS_IT=0 skips all)`)
  }
  return { has: (...needed) => mode === 'required' || (mode === 'auto' && needed.every(n => up.has(n))) }
}
