import { describe, it, expect, beforeEach, afterEach } from 'vitest'
import { encrypt, decrypt, isEncryptionConfigured, getEncryptionKey, getEncryptionKeys, keyId, needsReencrypt } from '../index.js'

const TEST_KEY = 'a'.repeat(64) // valid 64-char hex key for tests

describe('encrypt / decrypt', () => {
  beforeEach(() => {
    process.env.ENCRYPTION_KEY = TEST_KEY
  })
  afterEach(() => {
    delete process.env.ENCRYPTION_KEY
  })

  it('round-trips: decrypt(encrypt(x)) === x', () => {
    const plaintext = 'super-secret-password'
    const enc = encrypt(plaintext)
    expect(decrypt(enc)).toBe(plaintext)
  })

  it('ciphertext !== plaintext', () => {
    const plaintext = 'my-password'
    const enc = encrypt(plaintext)
    expect(enc.ciphertext).not.toBe(plaintext)
    expect(enc.ciphertext).not.toContain(plaintext)
  })

  it('produces different ciphertext each call (random IV)', () => {
    const plaintext = 'same-value'
    const enc1 = encrypt(plaintext)
    const enc2 = encrypt(plaintext)
    expect(enc1.ciphertext).not.toBe(enc2.ciphertext)
    expect(enc1.iv).not.toBe(enc2.iv)
  })

  it('wrong key throws on auth tag mismatch — not just returns garbage', () => {
    const enc = encrypt('secret')
    process.env.ENCRYPTION_KEY = 'b'.repeat(64) // different key
    expect(() => decrypt(enc)).toThrow()
  })

  it('handles unicode strings correctly', () => {
    const plaintext = '日本語パスワード 🔐'
    expect(decrypt(encrypt(plaintext))).toBe(plaintext)
  })

  it('handles empty string', () => {
    expect(decrypt(encrypt(''))).toBe('')
  })

  it('handles JSON strings (connection extra)', () => {
    const extra = JSON.stringify({ sslmode: 'require', connect_timeout: 10 })
    expect(decrypt(encrypt(extra))).toBe(extra)
  })
})

describe('getEncryptionKey', () => {
  afterEach(() => {
    delete process.env.ENCRYPTION_KEY
  })

  it('throws when ENCRYPTION_KEY not set', () => {
    delete process.env.ENCRYPTION_KEY
    expect(() => getEncryptionKey()).toThrow(/ENCRYPTION_KEY/)
  })

  it('throws when key is wrong length', () => {
    process.env.ENCRYPTION_KEY = 'too-short'
    expect(() => getEncryptionKey()).toThrow(/64 hex/)
  })

  it('throws when key has non-hex chars', () => {
    process.env.ENCRYPTION_KEY = 'z'.repeat(64)
    expect(() => getEncryptionKey()).toThrow()
  })
})

describe('isEncryptionConfigured', () => {
  afterEach(() => {
    delete process.env.ENCRYPTION_KEY
  })

  it('returns true when key is valid', () => {
    process.env.ENCRYPTION_KEY = TEST_KEY
    expect(isEncryptionConfigured()).toBe(true)
  })

  it('returns false when key is not set', () => {
    delete process.env.ENCRYPTION_KEY
    expect(isEncryptionConfigured()).toBe(false)
  })
})

describe('key rotation — ENCRYPTION_KEYS', () => {
  const OLD = 'a'.repeat(64)
  const NEW = 'b'.repeat(64)
  afterEach(() => { delete process.env.ENCRYPTION_KEYS; delete process.env.ENCRYPTION_KEY })

  it('encrypts with the first key and still decrypts values written with an older one', () => {
    process.env.ENCRYPTION_KEYS = OLD
    const old = encrypt('rotate-me')

    process.env.ENCRYPTION_KEYS = `${NEW}, ${OLD}`
    const fresh = encrypt('new-value')
    expect(fresh.kid).toBe(keyId(Buffer.from(NEW, 'hex')))
    expect(fresh.kid).not.toBe(old.kid)
    expect(decrypt(old)).toBe('rotate-me')
    expect(needsReencrypt(old)).toBe(true)
    expect(needsReencrypt(fresh)).toBe(false)
  })

  it('a value whose key was retired fails with a message naming the missing key id', () => {
    process.env.ENCRYPTION_KEYS = OLD
    const old = encrypt('x')
    process.env.ENCRYPTION_KEYS = NEW
    expect(() => decrypt(old)).toThrow(new RegExp(`${old.kid}.*not in ENCRYPTION_KEYS`))
  })

  it('values written before key ids existed are decrypted by trying every key', () => {
    process.env.ENCRYPTION_KEYS = OLD
    const { kid: _kid, ...legacy } = encrypt('legacy')
    process.env.ENCRYPTION_KEYS = `${NEW},${OLD}`
    expect(decrypt(legacy)).toBe('legacy')
    expect(needsReencrypt(legacy)).toBe(true)
  })

  it('ENCRYPTION_KEYS wins over ENCRYPTION_KEY; a malformed entry is named', () => {
    process.env.ENCRYPTION_KEY = OLD
    process.env.ENCRYPTION_KEYS = NEW
    expect(getEncryptionKey().toString('hex')).toBe(NEW)
    process.env.ENCRYPTION_KEYS = `${NEW},nope`
    expect(() => getEncryptionKeys()).toThrow(/entry 2/)
    expect(isEncryptionConfigured()).toBe(false)
  })
})
