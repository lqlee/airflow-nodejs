/**
 * Secret masker (Airflow's secrets_masker): values a task reads as secrets are replaced with `***` in
 * everything the task prints, so a task that logs a password does not leak it to the task log.
 *
 * What gets registered (in the process that reads it — the task worker):
 *   - a connection's password, and extra fields with a sensitive name (password, token, private_key, …)
 *   - a variable marked `is_secret`, or whose key has a sensitive name (e.g. `api_key`)
 * Masked forms: the value itself, its JSON-escaped and URL-encoded forms, and each line of a multi-line
 * value (private keys). Values shorter than MASK_MIN_LENGTH (default 5) are not masked — masking "abc"
 * would garble unrelated output.
 *
 * Where it applies: the worker's stdout/stderr (installStreamMasking), its failure message, and every
 * line the executor appends to a task log (appendLog).
 *
 * Known limit, same as Airflow: a secret split across two separate writes is not caught.
 */

/** Substrings that make a field or variable name sensitive (case-insensitive), as in Airflow. */
const DEFAULT_SENSITIVE_NAMES = [
  'access_token', 'api_key', 'apikey', 'authorization', 'passphrase', 'passwd', 'password',
  'private_key', 'secret', 'token', 'keyfile_dict', 'service_account', 'credentials',
]

const MASK = '***'

const secrets = new Set<string>()
let pattern: RegExp | null = null

function minLength(): number {
  const n = Number(process.env.MASK_MIN_LENGTH ?? 5)
  return Number.isInteger(n) && n > 0 ? n : 5
}

function sensitiveNames(): string[] {
  const extra = (process.env.SENSITIVE_FIELD_NAMES ?? '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean)
  return [...DEFAULT_SENSITIVE_NAMES, ...extra]
}

/** True when a field / variable name looks like it holds a secret. */
export function isSensitiveName(name: string): boolean {
  const lower = name.toLowerCase()
  return sensitiveNames().some(s => lower.includes(s))
}

function add(value: string): void {
  if (value.length < minLength() || secrets.has(value)) return
  secrets.add(value)
  pattern = null
}

/** Mask this value (and its escaped forms) from now on. Non-strings are stringified; null is ignored. */
export function registerSecret(value: unknown): void {
  if (value === null || value === undefined) return
  const s = typeof value === 'string' ? value : JSON.stringify(value)
  if (!s) return
  add(s)
  add(JSON.stringify(s).slice(1, -1))   // as it appears inside JSON output
  add(encodeURIComponent(s))            // as it appears in a URL
  if (s.includes('\n')) for (const line of s.split(/\r?\n/)) add(line.trim())
}

/**
 * Register the values under sensitive keys of an object (connection extra), at any depth.
 * Everything under a sensitive key is masked; other keys are searched for nested sensitive keys.
 */
export function registerSensitiveFields(value: unknown): void {
  if (Array.isArray(value)) { value.forEach(registerSensitiveFields); return }
  if (typeof value !== 'object' || value === null) return
  for (const [k, v] of Object.entries(value)) {
    if (isSensitiveName(k)) registerAll(v)
    else registerSensitiveFields(v)
  }
}

function registerAll(value: unknown): void {
  if (Array.isArray(value)) value.forEach(registerAll)
  else if (typeof value === 'object' && value !== null) Object.values(value).forEach(registerAll)
  else registerSecret(value)
}

const escapeRegExp = (s: string) => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')

/** Replace every registered secret in the text with `***`. */
export function redact(text: string): string {
  if (secrets.size === 0 || !text) return text
  if (!pattern) {
    // Longest first, so a secret that contains another is masked whole
    const alternatives = [...secrets].sort((a, b) => b.length - a.length).map(escapeRegExp)
    pattern = new RegExp(alternatives.join('|'), 'g')
  }
  return text.replace(pattern, MASK)
}

/** Number of registered masked values (for tests and diagnostics). */
export function maskedCount(): number {
  return secrets.size
}

/** Forget every registered secret (tests). */
export function clearSecrets(): void {
  secrets.clear()
  pattern = null
}

let installed = false

/**
 * Redact everything this process writes to stdout/stderr (console.*, ctx.log, process.stdout.write).
 * Call once at worker start-up, before running user code.
 */
export function installStreamMasking(): void {
  if (installed) return
  installed = true
  for (const stream of [process.stdout, process.stderr]) {
    const original = stream.write.bind(stream) as (...args: unknown[]) => boolean
    stream.write = ((chunk: unknown, ...rest: unknown[]) => {
      if (secrets.size > 0) {
        if (typeof chunk === 'string') chunk = redact(chunk)
        else if (chunk instanceof Uint8Array) chunk = redact(Buffer.from(chunk).toString('utf8'))
      }
      return original(chunk, ...rest)
    }) as typeof stream.write
  }
}
