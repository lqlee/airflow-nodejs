import { Redis } from 'ioredis'

const REDIS_URL = process.env.REDIS_URL ?? 'redis://localhost:6379'

// BullMQ requires a dedicated ioredis connection (not shared)
export function createRedisConnection(): Redis {
  return new Redis(REDIS_URL, {
    maxRetriesPerRequest: null,  // required by BullMQ
  })
}

export const QUEUE_NAME = 'airflow-tasks'
