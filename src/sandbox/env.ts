/**
 * Environment for processes that run user code but must not hold control-plane credentials:
 * the Dag processor's parse child and the callback sandbox.
 *
 * Dropped: metadata-database connection settings, encryption keys, API/admin keys, the OIDC client
 * secret and secrets-backend credentials. Everything else (PATH, HOME, proxies, the Dag author's own
 * variables) is kept, since Dag files and callbacks may legitimately read it.
 */
const CONTROL_PLANE = [
  /^MONGO_URL$/, /^DB_BACKEND$/, /^DB_NAME$/, /^SPANNER_/, /^GOOGLE_APPLICATION_CREDENTIALS$/,
  /^REDIS_URL$/,
  /^ENCRYPTION_KEYS?$/,
  /^ADMIN_KEY$/, /^API_KEYS$/, /^EXECUTION_API_SECRET$/,
  /^OIDC_CLIENT_SECRET$/,
  /^VAULT_/, /^AKEYLESS_/,
]

export function isControlPlaneEnv(name: string): boolean {
  return CONTROL_PLANE.some(re => re.test(name))
}

/** A copy of `env` without control-plane credentials. */
export function userCodeEnv(env: NodeJS.ProcessEnv = process.env): NodeJS.ProcessEnv {
  const out: NodeJS.ProcessEnv = {}
  for (const [k, v] of Object.entries(env)) if (!isControlPlaneEnv(k)) out[k] = v
  return out
}
