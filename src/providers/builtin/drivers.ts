/**
 * Lazy driver loading for the built-in providers. Drivers are optional peer dependencies: a deployment
 * installs only those its Dags use, and a task needing a missing one fails with an install hint.
 */
type Drivers = {
  pg: typeof import('pg')
  'mysql2/promise': typeof import('mysql2/promise')
  kafkajs: typeof import('kafkajs')
  nodemailer: typeof import('nodemailer')
}

const PACKAGE: Record<keyof Drivers, string> = { pg: 'pg', 'mysql2/promise': 'mysql2', kafkajs: 'kafkajs', nodemailer: 'nodemailer' }

export async function loadDriver<K extends keyof Drivers>(name: K): Promise<Drivers[K]> {
  let mod: { default?: unknown }
  try {
    mod = await import(name) as { default?: unknown }
  } catch (err) {
    const code = (err as { code?: string }).code
    if (code === 'ERR_MODULE_NOT_FOUND' || code === 'MODULE_NOT_FOUND') {
      throw new Error(`this operator needs the '${PACKAGE[name]}' package — add it to the deployment: npm install ${PACKAGE[name]}`)
    }
    throw err
  }
  // CommonJS drivers: the API is on the default export
  return (mod.default ?? mod) as Drivers[K]
}
