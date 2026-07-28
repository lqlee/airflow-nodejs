/**
 * Docker image management routes.
 *
 * POST /images/upload   — upload a .tar file, save to dags/images/, docker load it
 * GET  /images          — list .tar files in dags/images/ with their loaded status
 * DELETE /images/:name  — remove a .tar file from dags/images/
 *
 * Requires Docker socket mounted in the server container.
 * Requires role: admin.
 */

import type { FastifyInstance } from 'fastify'
import { createWriteStream } from 'node:fs'
import { readdir, stat, unlink, mkdir } from 'node:fs/promises'
import { resolve, join } from 'node:path'
import { pipeline } from 'node:stream/promises'
import { ensureImages } from '../../dag/images.js'

// Resolved lazily so TEST_IMAGES_DIR set in tests takes effect after module import
function getImagesDir(): string {
  if (process.env.TEST_IMAGES_DIR) return process.env.TEST_IMAGES_DIR
  const dagsDir = process.env.DAGS_DIR ?? resolve(process.cwd(), 'dags')
  return join(dagsDir, 'images')
}

export async function imagesRoutes(app: FastifyInstance): Promise<void> {

  // ── GET /images — list all .tar files in dags/images/ ──────────────────────
  app.get('/images', {
    config: { requiredRole: 'admin' },
  }, async (_req, reply) => {
    try {
      await mkdir(getImagesDir(), { recursive: true })
      const entries = await readdir(getImagesDir())
      const tars = entries.filter(f => f.endsWith('.tar'))

      const files = await Promise.all(tars.map(async (name) => {
        const filePath = join(getImagesDir(), name)
        const info = await stat(filePath).catch(() => null)
        return {
          name,
          path: `./images/${name}`,  // relative to dags/ — use in requiredImages
          size_bytes: info?.size ?? 0,
          uploaded_at: info?.mtime ?? null,
        }
      }))

      return reply.send(files)
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      return reply.status(500).send({ error: 'Failed to list images', message: msg })
    }
  })

  // ── POST /images/upload — upload a .tar and docker load it ──────────────────
  app.post('/images/upload', {
    config: { requiredRole: 'admin' },
  }, async (req, reply) => {
    let data: ReturnType<typeof req.file> extends Promise<infer T> ? T : never

    try {
      data = await (req as any).file()
    } catch {
      return reply.status(400).send({ error: 'No file uploaded — send multipart/form-data with a .tar file' })
    }

    if (!data) {
      return reply.status(400).send({ error: 'No file part in request' })
    }

    const filename = (data as any).filename as string
    if (!filename.endsWith('.tar')) {
      await (data as any).file.resume()  // drain the stream
      return reply.status(400).send({ error: `Only .tar files are accepted (got: ${filename})` })
    }

    // Sanitize filename — strip path components
    const safeName = filename.replace(/[^a-zA-Z0-9._-]/g, '_')
    const destPath = join(getImagesDir(), safeName)

    try {
      await mkdir(getImagesDir(), { recursive: true })
      await pipeline((data as any).file, createWriteStream(destPath))
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err)
      return reply.status(500).send({ error: 'Failed to save file', message: msg })
    }

    // Fire-and-forget docker load (may take time; client gets immediate response)
    const dagId = `upload:${safeName}`
    void ensureImages(dagId, [destPath])

    const info = await stat(destPath).catch(() => null)
    return reply.status(201).send({
      name: safeName,
      path: `./images/${safeName}`,
      size_bytes: info?.size ?? 0,
      message: `Uploaded successfully. Docker is loading the image in the background.`,
    })
  })

  // ── DELETE /images/:name — remove a .tar file ───────────────────────────────
  app.delete('/images/:name', {
    config: { requiredRole: 'admin' },
  }, async (req, reply) => {
    const { name } = req.params as { name: string }

    // Safety: only allow .tar files and no path traversal
    if (!name.endsWith('.tar') || name.includes('/') || name.includes('..')) {
      return reply.status(400).send({ error: 'Invalid filename' })
    }

    const filePath = join(getImagesDir(), name)
    try {
      await unlink(filePath)
      return reply.send({ deleted: name, message: 'File removed. The Docker image remains loaded in the daemon.' })
    } catch (err: any) {
      if (err.code === 'ENOENT') return reply.status(404).send({ error: `Not found: ${name}` })
      return reply.status(500).send({ error: err.message })
    }
  })
}
