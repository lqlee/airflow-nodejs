/**
 * AES-256-GCM symmetric encryption for Connections and Variables.
 *
 * Key source: ENCRYPTION_KEY env var (64 hex chars = 32 bytes).
 * Generate with: node -e "console.log(require('crypto').randomBytes(32).toString('hex'))"
 *
 * IMPORTANT: rotating ENCRYPTION_KEY invalidates ALL stored encrypted values.
 * There is no automatic re-encryption on key rotation — decrypt will throw on auth
 * tag mismatch, surfacing the corruption rather than silently returning garbage.
 *
 * Task stdout is NOT masked — a task that prints a secret leaks it to logs.
 */
import { createCipheriv, createDecipheriv, randomBytes } from 'node:crypto'

export const ALGORITHM = 'aes-256-gcm'
const IV_BYTES = 12   // 96-bit IV — GCM recommended
const TAG_BYTES = 16  // 128-bit auth tag

export interface EncryptedValue {
  iv: string        // hex
  authTag: string   // hex
  ciphertext: string // hex
}

/**
 * Returns the raw 32-byte key buffer from ENCRYPTION_KEY env var.
 * Throws if the env var is missing or malformed.
 */
export function getEncryptionKey(): Buffer {
  const hex = process.env.ENCRYPTION_KEY
  if (!hex) {
    throw new Error(
      'ENCRYPTION_KEY environment variable is required to store encrypted Connections/Variables. ' +
      'Generate one with: node -e "console.log(require(\'crypto\').randomBytes(32).toString(\'hex\'))"',
    )
  }
  if (!/^[0-9a-fA-F]{64}$/.test(hex)) {
    throw new Error('ENCRYPTION_KEY must be exactly 64 hex characters (32 bytes)')
  }
  return Buffer.from(hex, 'hex')
}

/**
 * Encrypt a plaintext string. Throws if ENCRYPTION_KEY is not set.
 */
export function encrypt(plaintext: string): EncryptedValue {
  const key = getEncryptionKey()
  const iv = randomBytes(IV_BYTES)
  const cipher = createCipheriv(ALGORITHM, key, iv)
  const ciphertext = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()])
  const authTag = cipher.getAuthTag()
  return {
    iv: iv.toString('hex'),
    authTag: authTag.toString('hex'),
    ciphertext: ciphertext.toString('hex'),
  }
}

/**
 * Decrypt an EncryptedValue. Throws if:
 * - ENCRYPTION_KEY is not set
 * - Key is wrong (GCM auth tag mismatch → distinguishes real encryption from base64)
 * - Data is corrupted
 */
export function decrypt(enc: EncryptedValue): string {
  const key = getEncryptionKey()
  const decipher = createDecipheriv(ALGORITHM, key, Buffer.from(enc.iv, 'hex'))
  decipher.setAuthTag(Buffer.from(enc.authTag, 'hex'))
  const plaintext = Buffer.concat([
    decipher.update(Buffer.from(enc.ciphertext, 'hex')),
    decipher.final(),
  ])
  return plaintext.toString('utf8')
}

/**
 * Returns true if ENCRYPTION_KEY is set and valid.
 * Used to gate encrypt/decrypt calls without throwing.
 */
export function isEncryptionConfigured(): boolean {
  const hex = process.env.ENCRYPTION_KEY
  return typeof hex === 'string' && /^[0-9a-fA-F]{64}$/.test(hex)
}
