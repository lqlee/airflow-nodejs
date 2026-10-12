/**
 * Document ↔ text codec for the Spanner backend.
 *
 * Documents are stored as JSON text with type tags so Mongo-typed values survive a round trip:
 *   Date → {"$date": ISO}   ObjectId → {"$oid": hex}   Buffer → {"$bin": base64}   NaN/±Infinity → {"$num": "NaN"}
 * `undefined` object members are stored as null (what the MongoDB driver does by default).
 * Limitation: user data containing an object whose ONLY key is one of those tags is revived as that type.
 */
import { ObjectId } from 'mongodb'

export const isObjectId = (v: unknown): v is ObjectId =>
  v instanceof ObjectId || (typeof v === 'object' && v !== null && (v as { _bsontype?: string })._bsontype === 'ObjectId')

export function isPlainObject(v: unknown): v is Record<string, unknown> {
  if (typeof v !== 'object' || v === null) return false
  const proto = Object.getPrototypeOf(v)
  return proto === Object.prototype || proto === null
}

export function encodeDoc(doc: Record<string, unknown>): string {
  return JSON.stringify(doc, function (this: Record<string, unknown>, key: string, value: unknown) {
    const raw = this[key]   // toJSON has already turned Dates into strings in `value`
    if (raw instanceof Date) {
      if (Number.isNaN(raw.getTime())) throw new TypeError(`Invalid Date at '${key}'`)
      return { $date: raw.toISOString() }
    }
    if (isObjectId(raw)) return { $oid: raw.toHexString() }
    if (Buffer.isBuffer(raw) || raw instanceof Uint8Array) return { $bin: Buffer.from(raw).toString('base64') }
    if (typeof raw === 'number' && !Number.isFinite(raw)) return { $num: String(raw) }
    if (typeof raw === 'bigint') throw new TypeError(`BigInt is not supported (field '${key}')`)
    if (raw === undefined && key !== '') return null
    return value
  })
}

export function decodeDoc<T = Record<string, unknown>>(text: string): T {
  return JSON.parse(text, (_key, value: unknown) => {
    if (isPlainObject(value)) {
      const keys = Object.keys(value)
      if (keys.length === 1) {
        const [k] = keys
        if (k === '$date' && typeof value.$date === 'string') return new Date(value.$date)
        if (k === '$oid' && typeof value.$oid === 'string') return new ObjectId(value.$oid)
        if (k === '$bin' && typeof value.$bin === 'string') return Buffer.from(value.$bin, 'base64')
        if (k === '$num' && typeof value.$num === 'string') return Number(value.$num)
      }
    }
    return value
  }) as T
}

/** Deep clone through the codec — what a read returns is never aliased to stored state. */
export const cloneDoc = <T extends Record<string, unknown>>(doc: T): T => decodeDoc<T>(encodeDoc(doc))
