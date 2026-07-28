/**
 * Docker image loader for dag requiredImages.
 *
 * Handles two entry types:
 *   - *.tar paths → docker load -i <path>   (user-supplied exported image)
 *   - image names → no-op (must already be local)
 *
 * All operations are fire-and-forget with logged outcomes.
 * A missing Docker socket is a warning, not an error — the dag still loads.
 */

import { spawn } from 'node:child_process'
import { access } from 'node:fs/promises'
import { resolve, isAbsolute } from 'node:path'

const DAGS_DIR = resolve(process.cwd(), 'dags')

/**
 * Ensure all requiredImages for a dag are available in the local Docker daemon.
 * .tar entries are loaded via `docker load`; plain image names are left to Docker
 * to resolve at `docker run` time (they must already be local).
 *
 * @param dagId      Used for log prefixes only
 * @param images     The requiredImages array from the dag definition
 */
export async function ensureImages(dagId: string, images: string[]): Promise<void> {
  if (!images.length) return

  const tarEntries = images.filter(e => e.endsWith('.tar'))
  if (!tarEntries.length) return  // only plain image names — nothing to load

  // Quick check: is the docker socket accessible?
  // Check DOCKER_SOCKET env, then DOCKER_HOST, then common default locations.
  const socketCandidates = [
    process.env.DOCKER_SOCKET,
    process.env.DOCKER_HOST?.replace('unix://', ''),
    '/var/run/docker.sock',
    `${process.env.HOME}/.colima/default/docker.sock`,
    `${process.env.HOME}/.docker/run/docker.sock`,
    '/run/docker.sock',
  ].filter(Boolean) as string[]

  let socketPath: string | null = null
  for (const candidate of socketCandidates) {
    try { await access(candidate); socketPath = candidate; break } catch { /* try next */ }
  }

  if (!socketPath) {
    console.warn(`[images] dag '${dagId}' declares requiredImages with .tar files but Docker socket not found — skipping docker load`)
    return
  }

  for (const entry of tarEntries) {
    // Resolve path: relative paths are relative to dags/
    const tarPath = isAbsolute(entry) ? entry : resolve(DAGS_DIR, entry)

    // Verify the tar file exists
    try {
      await access(tarPath)
    } catch {
      console.error(`[images] dag '${dagId}': image tar not found: ${tarPath}`)
      continue
    }

    await loadImageTar(dagId, tarPath)
  }
}

function loadImageTar(dagId: string, tarPath: string): Promise<void> {
  return new Promise((resolve) => {
    console.log(`[images] dag '${dagId}': loading image from ${tarPath}...`)

    const child = spawn('docker', ['load', '-i', tarPath], { stdio: 'pipe' })
    const lines: string[] = []

    child.stdout.on('data', (d: Buffer) => lines.push(d.toString().trim()))
    child.stderr.on('data', (d: Buffer) => lines.push(d.toString().trim()))

    child.on('error', (err: NodeJS.ErrnoException) => {
      if (err.code === 'ENOENT') {
        console.warn(`[images] dag '${dagId}': docker binary not found — cannot load ${tarPath}`)
      } else {
        console.error(`[images] dag '${dagId}': docker load error: ${err.message}`)
      }
      resolve()
    })

    child.on('close', (code) => {
      const output = lines.filter(Boolean).join(' ')
      if (code === 0) {
        console.log(`[images] dag '${dagId}': ✓ ${output || tarPath}`)
      } else {
        console.error(`[images] dag '${dagId}': docker load failed (exit ${code}): ${output}`)
      }
      resolve()
    })
  })
}
