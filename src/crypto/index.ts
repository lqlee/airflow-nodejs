/**
 * AES-256-GCM symmetric encryption for Connections and Variables, with key rotation.
 *
 * Keys: ENCRYPTION_KEYS — a comma-separated list of 64-hex-char (32-byte) keys. The FIRST key encrypts;
 * every key is tried to decrypt. ENCRYPTION_KEY (one key) is still accepted when ENCRYPTION_KEYS is unset.
 * Generate a key with: openssl rand -hex 32
 *
 * Rotation (like `airflow rotate-fernet-key`):
 *   1. ENCRYPTION_KEYS=<new>,<old>   — restart: new writes use <new>, old values still decrypt
 *   2. npm run rotate-keys           — re-encrypts every stored value with <new>
 *   3. ENCRYPTION_KEYS=<new>         — restart: <old> is retired
 *
 * Each value records the id of the key that encrypted it (`kid`, a short hash of the key — never the key),
 * so decryption picks the right key directly and rotation can tell what still needs re-encrypting.
 * Values written before key ids existed carry no `kid`; every key is tried for them.
 * A wrong key fails the GCM auth-tag check and throws — never returns garbage.
 */
import { createCipheriv, createDecipheriv, createHash, randomBytes } from 'node:crypto'

export const ALGORITHM = 'aes-256-gcm'
const IV_BYTES = 12   // 96-bit IV — GCM recommended
const KEY_PATTERN = /^[0-9a-fA-F]{64}$/

export interface EncryptedValue {
  iv: string        // hex
  authTag: string   // hex
  ciphertext: string // hex
  /** Id of the key that encrypted this value (absent on values written before rotation support). */
  kid?: string
}

const KEY_HELP = 'Generate one with: openssl rand -hex 32'

function rawKeyList(): string[] {
  const list = process.env.ENCRYPTION_KEYS ?? process.env.ENCRYPTION_KEY ?? ''
  return list.split(/[\s,]+/).filter(Boolean)
}

/** Short, non-reversible id of a key: the first 8 hex chars of its SHA-256. */
export function keyId(key: Buffer): string {
  return createHash('sha256').update(key).digest('hex').slice(0, 8)
}

/**
 * All configured keys, primary (encrypting) key first.
 * Throws if none is set or one is malformed.
 */
export function getEncryptionKeys(): Buffer[] {
  const raw = rawKeyList()
  if (raw.length === 0) {
    throw new Error(`ENCRYPTION_KEYS (or ENCRYPTION_KEY) environment variable is required to store encrypted Connections/Variables. ${KEY_HELP}`)
  }
  return raw.map((hex, i) => {
    if (!KEY_PATTERN.test(hex)) {
      throw new Error(`ENCRYPTION_KEY${raw.length > 1 ? `S entry ${i + 1}` : ''} must be exactly 64 hex characters (32 bytes)`)
    }
    return Buffer.from(hex, 'hex')
  })
}

/** The primary key — the one new values are encrypted with. */
export function getEncryptionKey(): Buffer {
  return getEncryptionKeys()[0]
}

/** Id of the primary key (shown by the config API and the rotation CLI). */
export function primaryKeyId(): string {
  return keyId(getEncryptionKey())
}

/** Encrypt a plaintext string with the primary key. Throws if no key is configured. */
export function encrypt(plaintext: string): EncryptedValue {
  const key = getEncryptionKey()
  const iv = randomBytes(IV_BYTES)
  const cipher = createCipheriv(ALGORITHM, key, iv)
  const ciphertext = Buffer.concat([cipher.update(plaintext, 'utf8'), cipher.final()])
  return {
    iv: iv.toString('hex'),
    authTag: cipher.getAuthTag().toString('hex'),
    ciphertext: ciphertext.toString('hex'),
    kid: keyId(key),
  }
}

function decryptWith(key: Buffer, enc: EncryptedValue): string {
  const decipher = createDecipheriv(ALGORITHM, key, Buffer.from(enc.iv, 'hex'))
  decipher.setAuthTag(Buffer.from(enc.authTag, 'hex'))
  return Buffer.concat([decipher.update(Buffer.from(enc.ciphertext, 'hex')), decipher.final()]).toString('utf8')
}

/**
 * Decrypt an EncryptedValue with whichever configured key encrypted it. Throws if:
 * - no key is configured
 * - the value's key is not among ENCRYPTION_KEYS (retired too early)
 * - no key passes the GCM auth-tag check (wrong keys or corrupted data)
 */
export function decrypt(enc: EncryptedValue): string {
  const keys = getEncryptionKeys()
  if (enc.kid) {
    const key = keys.find(k => keyId(k) === enc.kid)
    if (!key) throw new Error(`value was encrypted with key ${enc.kid}, which is not in ENCRYPTION_KEYS`)
    return decryptWith(key, enc)
  }
  let lastErr: unknown
  for (const key of keys) {
    try { return decryptWith(key, enc) } catch (err) { lastErr = err }
  }
  throw lastErr instanceof Error ? lastErr : new Error('decryption failed')
}

/** True when the value is not encrypted with the primary key (rotation should re-encrypt it). */
export function needsReencrypt(enc: EncryptedValue): boolean {
  return enc.kid !== primaryKeyId()
}

/** Returns true if at least one key is set and every key is valid. Never throws. */
export function isEncryptionConfigured(): boolean {
  const raw = rawKeyList()
  return raw.length > 0 && raw.every(k => KEY_PATTERN.test(k))
}
