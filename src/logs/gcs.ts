/**
 * Cloud Storage for task logs (LOG_BACKEND=gcs): read and write whole objects over the JSON API.
 *
 *   LOG_GCS_BUCKET   bucket (required)
 *   LOG_GCS_PREFIX   object prefix, default `task-logs`
 *
 * Credentials: Application Default Credentials (workload identity on WCNP, or
 * GOOGLE_APPLICATION_CREDENTIALS). STORAGE_EMULATOR_HOST points at an emulator (no credentials sent).
 */
import { GoogleAuth } from 'google-auth-library'

let auth: GoogleAuth | null = null

const base = () => process.env.STORAGE_EMULATOR_HOST?.replace(/\/+$/, '') ?? 'https://storage.googleapis.com'

export function gcsLogLocation(): { bucket: string; prefix: string } {
  const bucket = process.env.LOG_GCS_BUCKET?.trim()
  if (!bucket) throw new Error('LOG_BACKEND=gcs needs LOG_GCS_BUCKET')
  return { bucket, prefix: (process.env.LOG_GCS_PREFIX ?? 'task-logs').replace(/^\/+|\/+$/g, '') }
}

async function request(url: string, init: RequestInit = {}): Promise<Response> {
  const headers = new Headers(init.headers)
  if (!process.env.STORAGE_EMULATOR_HOST) {
    auth ??= new GoogleAuth({ scopes: ['https://www.googleapis.com/auth/devstorage.read_write'] })
    headers.set('authorization', `Bearer ${await auth.getAccessToken()}`)
  }
  return fetch(url, { ...init, headers, signal: init.signal ?? AbortSignal.timeout(30_000) })
}

async function failure(res: Response, what: string): Promise<Error> {
  const body = (await res.text().catch(() => '')).slice(0, 300)
  return new Error(`${what}: HTTP ${res.status}${body ? ` — ${body}` : ''}`)
}

/** Object contents, or null when it does not exist. */
export async function downloadObject(bucket: string, name: string): Promise<string | null> {
  const res = await request(`${base()}/storage/v1/b/${encodeURIComponent(bucket)}/o/${encodeURIComponent(name)}?alt=media`)
  if (res.status === 404) return null
  if (!res.ok) throw await failure(res, `read gs://${bucket}/${name}`)
  return res.text()
}

export async function uploadObject(bucket: string, name: string, body: string): Promise<void> {
  const url = new URL(`${base()}/upload/storage/v1/b/${encodeURIComponent(bucket)}/o`)
  url.searchParams.set('uploadType', 'media')
  url.searchParams.set('name', name)
  const res = await request(url.href, { method: 'POST', headers: { 'content-type': 'application/x-ndjson' }, body })
  if (!res.ok) throw await failure(res, `write gs://${bucket}/${name}`)
}
