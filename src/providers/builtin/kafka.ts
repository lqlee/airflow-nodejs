/**
 * apache-kafka provider (kafkajs): ProduceToTopicOperator, ConsumeFromTopicOperator, AwaitMessageSensor.
 *
 *   connection (type kafka): host = bootstrap servers ("b1:9092,b2:9092"; port applies to a host without
 *   one), login/password = SASL credentials; extra (Airflow's librdkafka-style keys): bootstrap.servers,
 *   client.id, group.id, security.protocol (PLAINTEXT | SSL | SASL_PLAINTEXT | SASL_SSL),
 *   sasl.mechanism (PLAIN | SCRAM-SHA-256 | SCRAM-SHA-512), sasl.username, sasl.password
 *
 * Consumers commit only the offsets of messages they returned (or, for the sensor, consumed), so a later
 * run continues where this one stopped.
 */
import type { Consumer, EachMessagePayload, Kafka, SASLOptions } from 'kafkajs'
import type { ConnectionRuntime } from '../../connections/index.js'
import { loadDriver } from './drivers.js'
import type { OperatorImpl, OperatorRuntime } from './runtime.js'
import { bool, num, optStr, str, strList } from './args.js'

const KAFKA = ['kafka', 'apache_kafka']

export function kafkaConfig(conn: ConnectionRuntime): { clientId: string; brokers: string[]; ssl: boolean; sasl?: SASLOptions; groupId?: string } {
  const extra = conn.extra ?? {}
  const get = (k: string) => (typeof extra[k] === 'string' && extra[k] !== '' ? extra[k] as string : undefined)
  const servers = get('bootstrap.servers') ?? conn.host
  if (!servers) throw new Error(`connection '${conn.conn_id}' has no bootstrap servers (host or extra "bootstrap.servers")`)
  const brokers = servers.split(',').map(s => s.trim()).filter(Boolean)
    .map(b => (/:\d+$/.test(b) || !conn.port ? b : `${b}:${conn.port}`))
  const protocol = (get('security.protocol') ?? 'PLAINTEXT').toUpperCase()
  const username = get('sasl.username') ?? conn.login ?? undefined
  const password = get('sasl.password') ?? conn.password ?? undefined
  const mechanism = (get('sasl.mechanism') ?? 'PLAIN').toLowerCase()
  const sasl = username && password
    ? ({ mechanism, username, password } as SASLOptions)
    : undefined
  return {
    clientId: get('client.id') ?? 'airflow-nodejs',
    brokers,
    ssl: protocol === 'SSL' || protocol === 'SASL_SSL' || extra.ssl === true,
    sasl,
    groupId: get('group.id'),
  }
}

async function client(args: Record<string, unknown>, rt: OperatorRuntime): Promise<{ kafka: Kafka; groupId?: string }> {
  const conn = await rt.connection(str(args.connId, 'connId'), KAFKA)
  const { Kafka, logLevel } = await loadDriver('kafkajs')
  const { groupId, ...cfg } = kafkaConfig(conn)
  return { kafka: new Kafka({ ...cfg, logLevel: logLevel.ERROR, retry: { retries: 3 } }), groupId }
}

type OutMessage = { key?: string | null; value: string | null; headers?: Record<string, string>; partition?: number }

function toMessages(v: unknown): OutMessage[] {
  const list = Array.isArray(v) ? v : [v]
  if (!list.length) throw new TypeError('messages must not be empty')
  const asText = (x: unknown) => (x === null || x === undefined ? null : typeof x === 'string' ? x : JSON.stringify(x))
  return list.map(m => {
    if (m && typeof m === 'object' && !Array.isArray(m) && 'value' in m) {
      const o = m as Record<string, unknown>
      return {
        key: asText(o.key), value: asText(o.value),
        ...(o.headers ? { headers: Object.fromEntries(Object.entries(o.headers as object).map(([k, h]) => [k, String(h)])) } : {}),
        ...(o.partition !== undefined ? { partition: Number(o.partition) } : {}),
      }
    }
    return { value: asText(m) }
  })
}

interface Consumed { topic: string; partition: number; offset: string; key: string | null; value: string | null; timestamp: string; headers: Record<string, string> }

/**
 * Consume until `stop(message)` says so or `maxWaitMs` passes after the group join; commits the offsets
 * of the messages kept. Returns the kept messages.
 */
async function consume(
  args: Record<string, unknown>, rt: OperatorRuntime,
  keep: (m: Consumed, kept: Consumed[]) => 'keep' | 'keep-and-stop' | 'skip',
  defaults: { maxWaitMs: number },
): Promise<Consumed[]> {
  const topics = strList(args.topics, 'topics')
  const { kafka, groupId: connGroup } = await client(args, rt)
  const groupId = optStr(args.groupId, 'groupId') ?? connGroup ?? 'airflow-nodejs'
  const maxWaitMs = num(args.maxWaitMs, 'maxWaitMs') ?? defaults.maxWaitMs
  const consumer: Consumer = kafka.consumer({ groupId, allowAutoTopicCreation: false })
  const kept: Consumed[] = []
  const commit = new Map<string, { topic: string; partition: number; offset: string }>()
  let done = false
  await consumer.connect()
  try {
    await consumer.subscribe({ topics, fromBeginning: bool(args.fromBeginning, 'fromBeginning') ?? false })
    await new Promise<void>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined
      const finish = () => { if (!done) { done = true; clearTimeout(timer); resolve() } }
      consumer.on(consumer.events.GROUP_JOIN, () => { timer ??= setTimeout(finish, maxWaitMs) })
      consumer.on(consumer.events.CRASH, e => { if (!done) { done = true; clearTimeout(timer); reject(e.payload.error) } })
      consumer.run({
        autoCommit: false,
        eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
          if (done) return
          const m: Consumed = {
            topic, partition, offset: message.offset,
            key: message.key?.toString('utf8') ?? null, value: message.value?.toString('utf8') ?? null,
            timestamp: new Date(Number(message.timestamp)).toISOString(),
            headers: Object.fromEntries(Object.entries(message.headers ?? {}).map(([k, h]) => [k, String(h)])),
          }
          commit.set(`${topic}:${partition}`, { topic, partition, offset: String(BigInt(message.offset) + 1n) })
          const verdict = keep(m, kept)
          if (verdict !== 'skip') kept.push(m)
          if (verdict === 'keep-and-stop') finish()
        },
      }).catch(reject)
    })
    if (commit.size) await consumer.commitOffsets([...commit.values()])
  } finally {
    await consumer.disconnect().catch(() => undefined)
  }
  return kept
}

export const IMPLS: Record<string, OperatorImpl> = {
  'kafka.produce': async (args, rt) => {
    const topic = str(args.topic, 'topic')
    const messages = toMessages(args.messages)
    const { kafka } = await client(args, rt)
    const producer = kafka.producer({ allowAutoTopicCreation: false })
    await producer.connect()
    try {
      const meta = await producer.send({ topic, messages })
      rt.log(`[kafka] produced ${messages.length} message(s) to ${topic}`)
      return { topic, count: messages.length, partitions: meta.map(m => ({ partition: m.partition, baseOffset: m.baseOffset ?? null })) }
    } finally {
      await producer.disconnect().catch(() => undefined)
    }
  },
  'kafka.consume': async (args, rt) => {
    const max = num(args.maxMessages, 'maxMessages') ?? 100
    const msgs = await consume(args, rt, (_m, kept) => (kept.length + 1 >= max ? 'keep-and-stop' : 'keep'), { maxWaitMs: 10_000 })
    rt.log(`[kafka] consumed ${msgs.length} message(s)`)
    return msgs
  },
  'kafka.await': async (args, rt) => {
    const match = optStr(args.match, 'match')
    const found = await consume(args, rt, m => (!match || (m.value ?? '').includes(match) ? 'keep-and-stop' : 'skip'), { maxWaitMs: 5_000 })
    if (found.length) rt.log(`[kafka] matched ${found[0].topic}[${found[0].partition}]@${found[0].offset}`)
    return found.length > 0
  },
}
