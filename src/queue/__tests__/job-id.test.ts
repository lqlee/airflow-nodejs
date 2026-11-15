import { describe, it, expect } from 'vitest'
import { jobIdFor } from '../producer.js'

describe('jobIdFor — BullMQ de-duplication key', () => {
  const base = { dag_run_id: 'r1', task_id: 'fan', map_index: null, try_number: 0 }

  it('keeps the instances of a mapped task apart (same try, different map_index)', () => {
    const ids = [0, 1, 2].map(map_index => jobIdFor({ ...base, map_index }))
    expect(new Set(ids).size).toBe(3)
    expect(jobIdFor(base)).not.toBe(ids[0])   // the unmapped instance is distinct from map_index 0
  })

  it('a retry is a new job, a re-enqueue of the same try is not', () => {
    expect(jobIdFor({ ...base, try_number: 1 })).not.toBe(jobIdFor(base))
    expect(jobIdFor({ ...base })).toBe(jobIdFor(base))
  })
})
