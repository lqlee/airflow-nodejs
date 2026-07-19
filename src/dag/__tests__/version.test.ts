import { describe, it, expect } from 'vitest'
import { hashDagSource } from '../version.js'

describe('hashDagSource', () => {
  it('returns a 12-char hex string', () => {
    const h = hashDagSource('export default { id: "test", tasks: {} }')
    expect(h).toMatch(/^[0-9a-f]{12}$/)
  })

  it('is deterministic — same input always produces same hash', () => {
    const src = 'const x = 1; export default { id: "dag", tasks: {} }'
    expect(hashDagSource(src)).toBe(hashDagSource(src))
  })

  it('different source produces different hash', () => {
    const h1 = hashDagSource('version A')
    const h2 = hashDagSource('version B')
    expect(h1).not.toBe(h2)
  })

  it('a single character change changes the hash', () => {
    const base = 'export default { id: "mydag", schedule: null, tasks: { a: {} } }'
    const changed = base.replace('mydag', 'mydag2')
    expect(hashDagSource(base)).not.toBe(hashDagSource(changed))
  })

  it('empty string produces a valid 12-char hash', () => {
    const h = hashDagSource('')
    expect(h).toMatch(/^[0-9a-f]{12}$/)
    // sha256('') well-known prefix
    expect(h).toBe('e3b0c44298fc')
  })

  it('handles unicode source correctly', () => {
    const h = hashDagSource('// 日本語コメント\nexport default {}')
    expect(h).toMatch(/^[0-9a-f]{12}$/)
  })
})
