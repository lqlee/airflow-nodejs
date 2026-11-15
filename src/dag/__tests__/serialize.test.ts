import { describe, it, expect, afterEach } from 'vitest'
import {
  serializeDag, hydrateDag, isSerializedDag, normalizeFunctionSource, setUserCodeInvoker, userCodeRef, resolvePath,
  type UserCodeRef,
} from '../serialize.js'
import type { DagDefinition } from '../types.js'

const evalFn = (src: string) => new Function(`return (${src})`)() as (...a: unknown[]) => unknown

describe('normalizeFunctionSource', () => {
  it('keeps arrow and function expressions as-is', () => {
    const arrow = async (x: number) => x + 1
    expect(normalizeFunctionSource(arrow)).toBe(arrow.toString())
    expect(evalFn(normalizeFunctionSource(function named(a: number) { return a * 2 }))(4)).toBe(8)
  })

  it('turns method shorthand into an expression that evaluates to the method', async () => {
    const obj = { notify(info: { n: number }) { return info.n * 10 }, async run() { return 'ran' } }
    expect(evalFn(normalizeFunctionSource(obj.notify))({ n: 3 })).toBe(30)
    await expect(evalFn(normalizeFunctionSource(obj.run))()).resolves.toBe('ran')
  })

  it('rejects native and bound functions', () => {
    expect(() => normalizeFunctionSource(Math.max)).toThrow(/native or bound/)
    expect(() => normalizeFunctionSource((() => 1).bind(null))).toThrow(/native or bound/)
  })
})

describe('serializeDag / hydrateDag', () => {
  const ran: Array<{ ref: UserCodeRef; args: unknown[] }> = []
  afterEach(() => { ran.length = 0 })

  const dag: DagDefinition = {
    id: 'ser_dag',
    schedule: null,
    startDate: new Date('2026-01-02T03:04:05.000Z'),
    tags: ['a'],
    timetable: (last) => last,
    onSuccess: { notify(info: unknown) { return info } },
    tasks: {
      extract: { run: async () => 42, retries: 2 },
      wait: { poke: async () => true, dependsOn: ['extract'], onFailure: ['https://hooks.example/x', () => 'cb'] },
    },
  } as unknown as DagDefinition

  it('round-trips data, dates and function sources', () => {
    const back = hydrateDag(serializeDag(dag), '/dags/ser.js')
    expect(isSerializedDag(back)).toBe(true)
    expect(isSerializedDag(dag)).toBe(false)
    expect(back.id).toBe('ser_dag')
    expect((back as unknown as { startDate: Date }).startDate).toEqual(new Date('2026-01-02T03:04:05.000Z'))
    expect(back.tasks.extract.retries).toBe(2)
    expect(back.tasks.wait.dependsOn).toEqual(['extract'])
    expect(typeof back.tasks.extract.run).toBe('function')
    expect(typeof back.tasks.wait.poke).toBe('function')
    // toString is the (normalized) source — what workers evaluate
    expect(evalFn(String(back.tasks.extract.run))).toBeTypeOf('function')
    expect((back.tasks.wait.onFailure as unknown[])[0]).toBe('https://hooks.example/x')
  })

  it('stubs never run in-process: calling one goes to the installed invoker with its path', async () => {
    setUserCodeInvoker(async (ref, args) => { ran.push({ ref, args }); return 'from-sandbox' })
    const back = hydrateDag(serializeDag(dag), '/dags/ser.js')
    const notifier = back.onSuccess as { notify: (i: unknown) => Promise<unknown> }
    await expect(notifier.notify({ x: 1 })).resolves.toBe('from-sandbox')
    await expect((back.tasks.wait.onFailure as Array<(i: unknown) => unknown>)[1]('info')).resolves.toBe('from-sandbox')
    expect(ran.map(r => r.ref.path)).toEqual([['onSuccess', 'notify'], ['tasks', 'wait', 'onFailure', 1]])
    expect(ran[0].ref).toMatchObject({ dagId: 'ser_dag', fileloc: '/dags/ser.js' })
    expect(ran[0].args).toEqual([{ x: 1 }])
    expect(userCodeRef(back.timetable)?.path).toEqual(['timetable'])
    expect(userCodeRef(() => 1)).toBeNull()
  })

  it('a ref path resolves back to the original function', () => {
    const back = hydrateDag(serializeDag(dag))
    const ref = userCodeRef((back.tasks.wait.onFailure as unknown[])[1])!
    const { value } = resolvePath(dag, ref.path)
    expect((value as () => string)()).toBe('cb')
  })

  it.each([
    ['a Map', { m: new Map() }, /Map values cannot be serialized/],
    ['a class instance', { c: new (class Conn {})() }, /Conn values cannot be serialized/],
    ['a BigInt', { b: 1n }, /BigInt/],
    ['a bound function', { f: (() => 1).bind(null) }, /native or bound/],
  ])('rejects %s with the property path', (_label, extra, msg) => {
    const bad = { ...dag, params: extra } as unknown as DagDefinition
    expect(() => serializeDag(bad)).toThrow(msg)
    expect(() => serializeDag(bad)).toThrow(/ser_dag\.params\./)
  })

  it('rejects circular references', () => {
    const conf: Record<string, unknown> = {}
    conf.self = conf
    expect(() => serializeDag({ ...dag, params: conf } as unknown as DagDefinition)).toThrow(/circular reference/)
  })
})
