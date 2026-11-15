/**
 * google provider: Cloud Storage and BigQuery over their REST APIs (google-auth-library for tokens).
 *
 *   connection (optional, type google_cloud_platform) — extra: project, keyfile_dict (JSON of a service
 *   account key), key_path (file on the worker), scope; the Airflow 2 `extra__google_cloud_platform__*`
 *   names work too. Without a connection, Application Default Credentials (workload identity on WCNP).
 *
 *   STORAGE_EMULATOR_HOST / BIGQUERY_EMULATOR_HOST (e.g. http://localhost:4443) point at emulators
 *   (no credentials sent).
 */
import { createReadStream, createWriteStream } from 'node:fs'
import { stat } from 'node:fs/promises'
import { Readable } from 'node:stream'
import { pipeline } from 'node:stream/promises'
import { setTimeout as sleep } from 'node:timers/promises'
import { randomBytes } from 'node:crypto'
import { GoogleAuth } from 'google-auth-library'
import type { ConnectionRuntime } from '../../connections/index.js'
import type { OperatorImpl, OperatorRuntime } from './runtime.js'
import { num, optStr, str, strList } from './args.js'
import { isTruthy } from './sql.js'

const GCP = ['google_cloud_platform', 'gcp', 'google_cloud']
const DEFAULT_SCOPE = 'https://www.googleapis.com/auth/cloud-platform'

interface GoogleClient {
  project: () => Promise<string>
  request: (url: string, init?: RequestInit & { duplex?: 'half' }) => Promise<Response>
}

function extraValue(extra: Record<string, unknown>, key: string): unknown {
  return extra[key] ?? extra[`extra__google_cloud_platform__${key}`]
}

async function googleClient(rt: OperatorRuntime, args: Record<string, unknown>, emulatorEnv: string): Promise<GoogleClient> {
  const conn: ConnectionRuntime | null = await rt.optionalConnection(optStr(args.connId, 'connId'), GCP)
  const extra = conn?.extra ?? {}
  const emulator = process.env[emulatorEnv]
  const keyfile = extraValue(extra, 'keyfile_dict')
  const auth = emulator ? null : new GoogleAuth({
    credentials: keyfile ? (typeof keyfile === 'string' ? JSON.parse(keyfile) : keyfile) as Record<string, string> : undefined,
    keyFile: (extraValue(extra, 'key_path') as string | undefined) || undefined,
    scopes: String(extraValue(extra, 'scope') ?? DEFAULT_SCOPE).split(',').map(s => s.trim()),
  })
  const project = async () => {
    const p = optStr(args.projectId, 'projectId') ?? (extraValue(extra, 'project') as string | undefined)
      ?? process.env.GOOGLE_CLOUD_PROJECT ?? (auth ? await auth.getProjectId().catch(() => undefined) : undefined)
    if (!p) throw new Error('no Google Cloud project — set projectId, the connection extra "project" or GOOGLE_CLOUD_PROJECT')
    return p
  }
  return {
    project,
    request: async (url, init = {}) => {
      const headers = new Headers(init.headers)
      if (auth) headers.set('authorization', `Bearer ${await auth.getAccessToken()}`)
      return fetch(url, { ...init, headers })
    },
  }
}

async function failure(res: Response, what: string): Promise<Error> {
  const body = await res.text().catch(() => '')
  let msg = body
  try { msg = (JSON.parse(body) as { error?: { message?: string } }).error?.message ?? body } catch { /* not JSON */ }
  return new Error(`${what}: HTTP ${res.status}${msg ? ` — ${msg.slice(0, 500)}` : ''}`)
}

// ── Cloud Storage ───────────────────────────────────────────────────────────

const gcsBase = () => (process.env.STORAGE_EMULATOR_HOST?.replace(/\/+$/, '') ?? 'https://storage.googleapis.com')
const objectUrl = (bucket: string, name: string) => `${gcsBase()}/storage/v1/b/${encodeURIComponent(bucket)}/o/${encodeURIComponent(name)}`

async function listObjects(gcs: GoogleClient, bucket: string, prefix?: string, matchGlob?: string): Promise<string[]> {
  const names: string[] = []
  let pageToken: string | undefined
  do {
    const url = new URL(`${gcsBase()}/storage/v1/b/${encodeURIComponent(bucket)}/o`)
    if (prefix) url.searchParams.set('prefix', prefix)
    if (matchGlob) url.searchParams.set('matchGlob', matchGlob)
    if (pageToken) url.searchParams.set('pageToken', pageToken)
    url.searchParams.set('fields', 'items(name),nextPageToken')
    const res = await gcs.request(url.href)
    if (!res.ok) throw await failure(res, `list gs://${bucket}/${prefix ?? ''}`)
    const page = await res.json() as { items?: { name: string }[]; nextPageToken?: string }
    names.push(...(page.items ?? []).map(i => i.name))
    pageToken = page.nextPageToken
  } while (pageToken)
  return names
}

// ── BigQuery ────────────────────────────────────────────────────────────────

const bqBase = () => `${process.env.BIGQUERY_EMULATOR_HOST?.replace(/\/+$/, '') ?? 'https://bigquery.googleapis.com'}/bigquery/v2`

interface BqField { name: string; type: string; mode?: string; fields?: BqField[] }
type BqCell = { v: unknown }

/** BigQuery's { f: [{ v }] } rows → plain objects typed by the schema. */
export function bqRowsToObjects(rows: { f: BqCell[] }[] | undefined, fields: BqField[]): Record<string, unknown>[] {
  const scalar = (type: string, v: unknown): unknown => {
    if (v === null || v === undefined) return null
    switch (type) {
      case 'INTEGER': case 'INT64': { const n = Number(v); return Number.isSafeInteger(n) ? n : v }
      case 'FLOAT': case 'FLOAT64': return Number(v)
      case 'BOOLEAN': case 'BOOL': return v === true || v === 'true'
      case 'TIMESTAMP': return new Date(Number(v) * 1000).toISOString()
      default: return v
    }
  }
  const value = (field: BqField, v: unknown): unknown => {
    if (field.mode === 'REPEATED') return ((v as BqCell[] | null) ?? []).map(c => value({ ...field, mode: 'NULLABLE' }, c.v))
    if (field.type === 'RECORD' || field.type === 'STRUCT') return v === null ? null : record(field.fields ?? [], v as { f: BqCell[] })
    return scalar(field.type, v)
  }
  const record = (fs: BqField[], row: { f: BqCell[] }) => Object.fromEntries(fs.map((f, i) => [f.name, value(f, row.f[i]?.v)]))
  return (rows ?? []).map(r => record(fields, r))
}

async function runBigQueryJob(args: Record<string, unknown>, rt: OperatorRuntime): Promise<{ rows: Record<string, unknown>[] | null; jobId: string }> {
  const bq = await googleClient(rt, args, 'BIGQUERY_EMULATOR_HOST')
  const project = await bq.project()
  const location = optStr(args.location, 'location')
  const sql = optStr(args.sql, 'sql')
  if (sql && args.configuration !== undefined) throw new TypeError('pass sql or configuration, not both')
  const configuration = sql ? { query: { query: sql, useLegacySql: false } } : args.configuration
  if (!configuration || typeof configuration !== 'object') throw new TypeError('BigQuery job needs sql or configuration')
  const jobId = `airflow_nodejs_${Date.now()}_${randomBytes(4).toString('hex')}`

  const insert = await bq.request(`${bqBase()}/projects/${encodeURIComponent(project)}/jobs`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ configuration, jobReference: { projectId: project, jobId, ...(location ? { location } : {}) } }),
  })
  if (!insert.ok) throw await failure(insert, 'BigQuery jobs.insert')
  rt.log(`[bigquery] job ${jobId} submitted`)

  const loc = location ? `?location=${encodeURIComponent(location)}` : ''
  for (let delay = 500; ; delay = Math.min(delay * 2, 5_000)) {
    const res = await bq.request(`${bqBase()}/projects/${encodeURIComponent(project)}/jobs/${jobId}${loc}`)
    if (!res.ok) throw await failure(res, `BigQuery jobs.get ${jobId}`)
    const job = await res.json() as { status?: { state?: string; errorResult?: { message?: string } } }
    if (job.status?.errorResult) throw new Error(`BigQuery job ${jobId} failed: ${job.status.errorResult.message ?? 'unknown error'}`)
    if (job.status?.state === 'DONE') break
    await sleep(delay)
  }
  if (!(configuration as { query?: unknown }).query) return { rows: null, jobId }

  const maxResults = num(args.maxResults, 'maxResults') ?? 1000
  const url = new URL(`${bqBase()}/projects/${encodeURIComponent(project)}/queries/${jobId}`)
  url.searchParams.set('maxResults', String(maxResults))
  if (location) url.searchParams.set('location', location)
  const res = await bq.request(url.href)
  if (!res.ok) throw await failure(res, `BigQuery getQueryResults ${jobId}`)
  const page = await res.json() as { schema?: { fields: BqField[] }; rows?: { f: BqCell[] }[]; totalRows?: string }
  const rows = bqRowsToObjects(page.rows, page.schema?.fields ?? [])
  rt.log(`[bigquery] job ${jobId} done — ${page.totalRows ?? rows.length} row(s)${Number(page.totalRows) > rows.length ? `, returning ${rows.length} (maxResults)` : ''}`)
  return { rows, jobId }
}

export const IMPLS: Record<string, OperatorImpl> = {
  'gcs.upload': async (args, rt) => {
    const bucket = str(args.bucket, 'bucket'); const dst = str(args.dst, 'dst')
    const src = optStr(args.src, 'src')
    if ((src === undefined) === (args.data === undefined)) throw new TypeError('pass exactly one of src (a local file) or data (a string)')
    const gcs = await googleClient(rt, args, 'STORAGE_EMULATOR_HOST')
    const url = new URL(`${gcsBase()}/upload/storage/v1/b/${encodeURIComponent(bucket)}/o`)
    url.searchParams.set('uploadType', 'media'); url.searchParams.set('name', dst)
    const headers: Record<string, string> = { 'content-type': optStr(args.mimeType, 'mimeType') ?? (src ? 'application/octet-stream' : 'text/plain; charset=utf-8') }
    let body: BodyInit
    if (src) {
      headers['content-length'] = String((await stat(src)).size)
      body = Readable.toWeb(createReadStream(src)) as ReadableStream
    } else {
      body = typeof args.data === 'string' ? args.data : JSON.stringify(args.data)
    }
    const res = await gcs.request(url.href, { method: 'POST', headers, body, duplex: 'half' })
    if (!res.ok) throw await failure(res, `upload gs://${bucket}/${dst}`)
    rt.log(`[gcs] uploaded ${src ?? 'data'} → gs://${bucket}/${dst}`)
    return `gs://${bucket}/${dst}`
  },
  'gcs.download': async (args, rt) => {
    const bucket = str(args.bucket, 'bucket'); const name = str(args.objectName, 'objectName')
    const filename = optStr(args.filename, 'filename')
    const gcs = await googleClient(rt, args, 'STORAGE_EMULATOR_HOST')
    const res = await gcs.request(`${objectUrl(bucket, name)}?alt=media`)
    if (!res.ok || !res.body) throw await failure(res, `download gs://${bucket}/${name}`)
    if (filename) {
      await pipeline(Readable.fromWeb(res.body as import('node:stream/web').ReadableStream), createWriteStream(filename))
      rt.log(`[gcs] downloaded gs://${bucket}/${name} → ${filename}`)
      return filename
    }
    const maxBytes = num(args.maxBytes, 'maxBytes') ?? 1_048_576
    const size = Number(res.headers.get('content-length') ?? 0)
    if (size > maxBytes) { await res.body.cancel(); throw new Error(`gs://${bucket}/${name} is ${size} bytes — over maxBytes ${maxBytes}; pass filename to download it`) }
    const text = await res.text()
    if (Buffer.byteLength(text) > maxBytes) throw new Error(`gs://${bucket}/${name} is over maxBytes ${maxBytes}; pass filename to download it`)
    return text
  },
  'gcs.list': async (args, rt) => {
    const gcs = await googleClient(rt, args, 'STORAGE_EMULATOR_HOST')
    const names = await listObjects(gcs, str(args.bucket, 'bucket'), optStr(args.prefix, 'prefix'), optStr(args.matchGlob, 'matchGlob'))
    rt.log(`[gcs] ${names.length} object(s)`)
    return names
  },
  'gcs.delete': async (args, rt) => {
    const bucket = str(args.bucket, 'bucket')
    const prefix = optStr(args.prefix, 'prefix')
    if ((args.objects === undefined) === (prefix === undefined)) throw new TypeError('pass exactly one of objects or a non-empty prefix')
    const gcs = await googleClient(rt, args, 'STORAGE_EMULATOR_HOST')
    const names = prefix ? await listObjects(gcs, bucket, prefix) : strList(args.objects, 'objects')
    for (const name of names) {
      const res = await gcs.request(objectUrl(bucket, name), { method: 'DELETE' })
      if (!res.ok && res.status !== 404) throw await failure(res, `delete gs://${bucket}/${name}`)
    }
    rt.log(`[gcs] deleted ${names.length} object(s)`)
    return names
  },
  'gcs.exists': async (args, rt) => {
    const bucket = str(args.bucket, 'bucket'); const name = str(args.object, 'object')
    const gcs = await googleClient(rt, args, 'STORAGE_EMULATOR_HOST')
    const res = await gcs.request(`${objectUrl(bucket, name)}?fields=name`)
    if (res.status === 404) return false
    if (!res.ok) throw await failure(res, `stat gs://${bucket}/${name}`)
    return true
  },
  'bigquery.job': async (args, rt) => {
    const { rows, jobId } = await runBigQueryJob(args, rt)
    return rows ?? { jobId }
  },
  'bigquery.check': async (args, rt) => {
    const { rows } = await runBigQueryJob({ ...args, maxResults: 1 }, rt)
    const row = rows?.[0]
    if (!row) throw new Error('BigQuery check failed: the query returned no rows')
    const falsy = Object.entries(row).filter(([, v]) => !isTruthy(v)).map(([k]) => k)
    if (falsy.length) throw new Error(`BigQuery check failed: falsy value(s) in ${falsy.join(', ')} — ${JSON.stringify(row)}`)
    return null
  },
}

