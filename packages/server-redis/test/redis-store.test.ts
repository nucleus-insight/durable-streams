/**
 * Implementation-specific tests for the Redis-backed store.
 * Protocol correctness is covered by the conformance suite; these tests
 * exercise what only a shared, durable backend can provide.
 */

import { afterEach, beforeEach, describe, expect, test } from "vitest"
import { DurableStreamRedisServer } from "../src/server"
import { RedisStreamStore } from "../src/redis-store"
import {
  TEST_REDIS_URL,
  decode,
  encode,
  uniquePrefix,
} from "./support/test-helpers"

const JSON_HEADERS = { "content-type": `application/json` }

let prefix: string
let servers: Array<DurableStreamRedisServer>

function startServer(
  options: ConstructorParameters<typeof DurableStreamRedisServer>[0] = {}
): Promise<DurableStreamRedisServer> {
  const server = new DurableStreamRedisServer({
    port: 0,
    longPollTimeout: 2000,
    redis: { url: TEST_REDIS_URL, keyPrefix: prefix },
    ...options,
  })
  servers.push(server)
  return server.start().then(() => server)
}

beforeEach(() => {
  prefix = uniquePrefix(`store`)
  servers = []
})

afterEach(async () => {
  const cleanup = new RedisStreamStore({
    url: TEST_REDIS_URL,
    keyPrefix: prefix,
  })
  await cleanup.connect()
  await cleanup.clear()
  await cleanup.close()
  for (const server of servers) {
    try {
      await server.stop()
    } catch {
      // Already stopped by the test
    }
  }
})

describe(`Multiple server instances sharing one Redis`, () => {
  test(`a stream created on one instance is readable from another`, async () => {
    const a = await startServer()
    const b = await startServer()
    const path = `/v1/stream/shared`

    const created = await fetch(`${a.url}${path}`, {
      method: `PUT`,
      headers: JSON_HEADERS,
      body: JSON.stringify([{ n: 1 }]),
    })
    expect(created.status).toBe(201)

    await fetch(`${a.url}${path}`, {
      method: `POST`,
      headers: JSON_HEADERS,
      body: JSON.stringify({ n: 2 }),
    })

    const head = await fetch(`${b.url}${path}`, { method: `HEAD` })
    expect(head.status).toBe(200)

    const read = await fetch(`${b.url}${path}?offset=-1`)
    expect(read.status).toBe(200)
    expect(await read.json()).toEqual([{ n: 1 }, { n: 2 }])
    expect(read.headers.get(`stream-next-offset`)).toBe(
      head.headers.get(`stream-next-offset`)
    )
  })

  test(`a long-poll on one instance is woken by an append on another`, async () => {
    const a = await startServer()
    const b = await startServer()
    const path = `/v1/stream/wake`

    await fetch(`${a.url}${path}`, { method: `PUT`, headers: JSON_HEADERS })
    const head = await fetch(`${a.url}${path}`, { method: `HEAD` })
    const offset = head.headers.get(`stream-next-offset`)!

    const started = Date.now()
    const pending = fetch(`${b.url}${path}?offset=${offset}&live=long-poll`)

    await new Promise((resolve) => setTimeout(resolve, 100))
    await fetch(`${a.url}${path}`, {
      method: `POST`,
      headers: JSON_HEADERS,
      body: JSON.stringify({ hello: `world` }),
    })

    const response = await pending
    expect(response.status).toBe(200)
    expect(await response.json()).toEqual([{ hello: `world` }])
    expect(Date.now() - started).toBeLessThan(1500)
  })

  test(`an SSE reader on one instance sees a close from another`, async () => {
    const a = await startServer()
    const b = await startServer()
    const path = `/v1/stream/sse-close`

    await fetch(`${a.url}${path}`, {
      method: `PUT`,
      headers: { "content-type": `text/plain` },
    })

    const controller = new AbortController()
    const timer = setTimeout(() => controller.abort(), 5000)
    const ssePromise = fetch(`${b.url}${path}?offset=-1&live=sse`, {
      signal: controller.signal,
    })

    await new Promise((resolve) => setTimeout(resolve, 100))
    await fetch(`${a.url}${path}`, {
      method: `POST`,
      headers: { "content-type": `text/plain`, "stream-closed": `true` },
      body: `final`,
    })

    const response = await ssePromise
    const text = await response.text()
    clearTimeout(timer)

    expect(text).toContain(`data:final`)
    expect(text).toContain(`"streamClosed":true`)
  })

  test(`producer sequencing is enforced across instances`, async () => {
    const a = await startServer()
    const b = await startServer()
    const path = `/v1/stream/producer`

    await fetch(`${a.url}${path}`, { method: `PUT`, headers: JSON_HEADERS })

    const producerHeaders = (seq: number) => ({
      ...JSON_HEADERS,
      "producer-id": `p1`,
      "producer-epoch": `0`,
      "producer-seq": String(seq),
    })

    const first = await fetch(`${a.url}${path}`, {
      method: `POST`,
      headers: producerHeaders(0),
      body: JSON.stringify({ seq: 0 }),
    })
    expect(first.status).toBe(200)

    const duplicate = await fetch(`${b.url}${path}`, {
      method: `POST`,
      headers: producerHeaders(0),
      body: JSON.stringify({ seq: 0 }),
    })
    expect(duplicate.status).toBe(204)

    const gap = await fetch(`${b.url}${path}`, {
      method: `POST`,
      headers: producerHeaders(5),
      body: JSON.stringify({ seq: 5 }),
    })
    expect(gap.status).toBe(409)
    expect(gap.headers.get(`producer-expected-seq`)).toBe(`1`)

    const read = await fetch(`${a.url}${path}?offset=-1`)
    expect(await read.json()).toEqual([{ seq: 0 }])
  })
})

describe(`Durability`, () => {
  test(`streams survive a server restart`, async () => {
    const first = await startServer()
    const path = `/v1/stream/durable`

    await fetch(`${first.url}${path}`, {
      method: `PUT`,
      headers: { "content-type": `text/plain` },
      body: `one`,
    })
    await fetch(`${first.url}${path}`, {
      method: `POST`,
      headers: { "content-type": `text/plain` },
      body: `two`,
    })
    const before = await fetch(`${first.url}${path}`, { method: `HEAD` })
    await first.stop()

    const second = await startServer()
    const after = await fetch(`${second.url}${path}`, { method: `HEAD` })
    expect(after.status).toBe(200)
    expect(after.headers.get(`stream-next-offset`)).toBe(
      before.headers.get(`stream-next-offset`)
    )

    const read = await fetch(`${second.url}${path}?offset=-1`)
    expect(await read.text()).toBe(`onetwo`)

    const appended = await fetch(`${second.url}${path}`, {
      method: `POST`,
      headers: { "content-type": `text/plain` },
      body: `three`,
    })
    expect(appended.status).toBe(204)
    const full = await fetch(`${second.url}${path}?offset=-1`)
    expect(await full.text()).toBe(`onetwothree`)
  })

  test(`offsets encode the byte position after each message`, async () => {
    const server = await startServer()
    const path = `/v1/stream/offsets`

    await fetch(`${server.url}${path}`, {
      method: `PUT`,
      headers: { "content-type": `text/plain` },
    })
    const appended = await fetch(`${server.url}${path}`, {
      method: `POST`,
      headers: { "content-type": `text/plain` },
      body: `abc`,
    })
    // 4-byte length prefix + 3 bytes + 1-byte newline
    expect(appended.headers.get(`stream-next-offset`)).toBe(
      `0000000000000000_0000000000000008`
    )

    const second = await fetch(`${server.url}${path}`, {
      method: `POST`,
      headers: { "content-type": `text/plain` },
      body: `de`,
    })
    expect(second.headers.get(`stream-next-offset`)).toBe(
      `0000000000000000_0000000000000015`
    )

    const tail = await fetch(
      `${server.url}${path}?offset=0000000000000000_0000000000000008`
    )
    expect(await tail.text()).toBe(`de`)
  })

  test(`binary payloads round-trip unchanged`, async () => {
    const server = await startServer()
    const path = `/v1/stream/binary`
    const bytes = new Uint8Array([0, 255, 10, 13, 0, 128, 7])

    await fetch(`${server.url}${path}`, {
      method: `PUT`,
      headers: { "content-type": `application/octet-stream` },
    })
    await fetch(`${server.url}${path}`, {
      method: `POST`,
      headers: { "content-type": `application/octet-stream` },
      body: bytes,
    })

    const read = await fetch(`${server.url}${path}?offset=-1`)
    expect(new Uint8Array(await read.arrayBuffer())).toEqual(bytes)
  })
})

describe(`Key management`, () => {
  test(`stores with different prefixes do not see each other's streams`, async () => {
    const a = await startServer()
    const b = await startServer({
      redis: { url: TEST_REDIS_URL, keyPrefix: `${prefix}other:` },
    })
    const path = `/v1/stream/isolated`

    await fetch(`${a.url}${path}`, { method: `PUT`, headers: JSON_HEADERS })
    expect((await fetch(`${a.url}${path}`, { method: `HEAD` })).status).toBe(
      200
    )
    expect((await fetch(`${b.url}${path}`, { method: `HEAD` })).status).toBe(
      404
    )
  })

  test(`TTL streams expire from Redis without being read`, async () => {
    const server = await startServer()
    const path = `/v1/stream/ttl`

    await fetch(`${server.url}${path}`, {
      method: `PUT`,
      headers: { "content-type": `text/plain`, "stream-ttl": `1` },
      body: `x`,
    })

    const store = new RedisStreamStore({
      url: TEST_REDIS_URL,
      keyPrefix: prefix,
    })
    await store.connect()
    try {
      expect(await store.list()).toEqual([path])
      await new Promise((resolve) => setTimeout(resolve, 1300))
      expect(await store.list()).toEqual([])
    } finally {
      await store.close()
    }

    expect(
      (await fetch(`${server.url}${path}`, { method: `HEAD` })).status
    ).toBe(404)
  })

  test(`a source with forks is kept until the last fork is deleted`, async () => {
    const server = await startServer()
    const source = `/v1/stream/fork-source`
    const fork = `/v1/stream/fork-child`

    await fetch(`${server.url}${source}`, {
      method: `PUT`,
      headers: { "content-type": `text/plain` },
      body: `base`,
    })
    const forked = await fetch(`${server.url}${fork}`, {
      method: `PUT`,
      headers: { "stream-forked-from": source },
    })
    expect(forked.status).toBe(201)

    expect(
      (await fetch(`${server.url}${source}`, { method: `DELETE` })).status
    ).toBe(204)
    expect(
      (await fetch(`${server.url}${source}`, { method: `HEAD` })).status
    ).toBe(410)

    const viaFork = await fetch(`${server.url}${fork}?offset=-1`)
    expect(await viaFork.text()).toBe(`base`)

    const store = new RedisStreamStore({
      url: TEST_REDIS_URL,
      keyPrefix: prefix,
    })
    await store.connect()
    try {
      expect((await store.list()).sort()).toEqual([fork, source].sort())
      expect(
        (await fetch(`${server.url}${fork}`, { method: `DELETE` })).status
      ).toBe(204)
      expect(await store.list()).toEqual([])
    } finally {
      await store.close()
    }
  })
})

describe(`Time offset scheme`, () => {
  test(`offsets are millisecond-sequence ids and stay monotonic across a fork`, async () => {
    const store = new RedisStreamStore({
      url: TEST_REDIS_URL,
      keyPrefix: prefix,
      offsetScheme: `time`,
    })
    await store.connect()
    try {
      const source = `/v1/stream/time-source`
      const fork = `/v1/stream/time-fork`
      await store.create(source, { contentType: `text/plain` })

      // Several appends inside one millisecond force sequence numbers > 0
      const offsets: Array<string> = []
      for (const word of [`a`, `b`, `c`, `d`, `e`, `f`]) {
        const result = await store.append(source, encode(word))
        offsets.push(result && `offset` in result ? result.offset : ``)
      }
      expect(offsets.every((o) => /^\d{16}_\d{16}$/.test(o))).toBe(true)
      expect([...offsets].sort()).toEqual(offsets)

      const forkOffset = offsets[offsets.length - 1]!
      const forked = await store.create(fork, {
        forkedFrom: source,
        forkOffset,
      })
      expect(forked.currentOffset).toBe(forkOffset)

      const appended = await store.append(fork, encode(`g`))
      const forkMessageOffset =
        appended && `offset` in appended ? appended.offset : ``
      expect(forkMessageOffset > forkOffset).toBe(true)

      const all = await store.read(fork, `-1`)
      expect(all.messages.map((m) => decode(m.data))).toEqual([
        `a`,
        `b`,
        `c`,
        `d`,
        `e`,
        `f`,
        `g`,
      ])
      const tail = await store.read(fork, forkOffset)
      expect(tail.messages.map((m) => decode(m.data))).toEqual([`g`])
    } finally {
      await store.clear()
      await store.close()
    }
  })
})

describe(`Store API`, () => {
  test(`append, read, and wait work without the HTTP layer`, async () => {
    const store = new RedisStreamStore({
      url: TEST_REDIS_URL,
      keyPrefix: prefix,
    })
    await store.connect()
    try {
      const path = `/v1/stream/direct`
      const stream = await store.create(path, { contentType: `text/plain` })
      expect(stream.currentOffset).toBe(`0000000000000000_0000000000000000`)

      const waiting = store.waitForMessages(path, stream.currentOffset, 2000)
      await new Promise((resolve) => setTimeout(resolve, 50))
      const appended = await store.append(path, encode(`hello`))
      expect(appended && `offset` in appended ? appended.offset : null).toBe(
        `0000000000000000_0000000000000010`
      )

      const woken = await waiting
      expect(woken.timedOut).toBe(false)
      expect(woken.messages.map((m) => decode(m.data))).toEqual([`hello`])

      const closed = await store.closeStream(path)
      expect(closed).toEqual({
        finalOffset: `0000000000000000_0000000000000010`,
        alreadyClosed: false,
      })
      const atTail = await store.waitForMessages(
        path,
        `0000000000000000_0000000000000010`,
        500
      )
      expect(atTail).toEqual({
        messages: [],
        timedOut: false,
        streamClosed: true,
      })
    } finally {
      await store.clear()
      await store.close()
    }
  })

  test(`reads page through large streams with readLimit`, async () => {
    const store = new RedisStreamStore({
      url: TEST_REDIS_URL,
      keyPrefix: prefix,
      readLimit: 2,
    })
    await store.connect()
    try {
      const path = `/v1/stream/paged`
      await store.create(path, { contentType: `text/plain` })
      for (const word of [`a`, `b`, `c`, `d`, `e`]) {
        await store.append(path, encode(word))
      }

      const collected: Array<string> = []
      let offset: string | undefined
      let upToDate = false
      let rounds = 0
      while (!upToDate) {
        const page = await store.read(path, offset)
        collected.push(...page.messages.map((m) => decode(m.data)))
        upToDate = page.upToDate
        offset = page.messages[page.messages.length - 1]?.offset ?? offset
        rounds++
      }
      expect(collected).toEqual([`a`, `b`, `c`, `d`, `e`])
      expect(rounds).toBe(3)
    } finally {
      await store.clear()
      await store.close()
    }
  })
})

describe(`Resilience`, () => {
  test(`waiters woken by one append share the notification and all receive it`, async () => {
    const store = new RedisStreamStore({
      url: TEST_REDIS_URL,
      keyPrefix: prefix,
    })
    await store.connect()
    try {
      const path = `/v1/stream/shared-wake`
      const created = await store.create(path, { contentType: `text/plain` })
      const tail = created.currentOffset

      const atTail = Array.from({ length: 25 }, () =>
        store.waitForMessages(path, tail, 5000)
      )
      const fromStart = store.waitForMessages(path, `-1`, 5000)
      await new Promise((resolve) => setTimeout(resolve, 50))

      await store.append(path, encode(`one`))

      const results = await Promise.all([...atTail, fromStart])
      for (const result of results) {
        expect(result.timedOut).toBe(false)
        expect(result.messages.map((m) => decode(m.data))).toEqual([`one`])
      }
    } finally {
      await store.clear()
      await store.close()
    }
  })

  test(`a waiter survives the notification connection being killed`, async () => {
    const clientName = `ds-test-${Math.random().toString(36).slice(2, 8)}`
    const store = new RedisStreamStore({
      url: TEST_REDIS_URL,
      keyPrefix: prefix,
      clientName,
    })
    await store.connect()
    const admin = new RedisStreamStore({
      url: TEST_REDIS_URL,
      keyPrefix: prefix,
    })
    await admin.connect()
    try {
      const path = `/v1/stream/killed-subscriber`
      const created = await store.create(path, { contentType: `text/plain` })

      const waiting = store.waitForMessages(path, created.currentOffset, 8000)
      await new Promise((resolve) => setTimeout(resolve, 50))

      const { createClient } = await import(`redis`)
      const killer = createClient({ url: TEST_REDIS_URL })
      await killer.connect()
      const list = await killer.sendCommand([`CLIENT`, `LIST`])
      const line = list
        .split(`\n`)
        .find((entry) => entry.includes(`name=${clientName}-notify`))
      expect(line).toBeDefined()
      const id = /\bid=(\d+)/.exec(line)![1]!
      await killer.sendCommand([`CLIENT`, `KILL`, `ID`, id])
      await killer.close()

      // Published while the subscriber is down: the notification is lost,
      // the reconnect wake-up must deliver it instead of the timeout.
      await admin.append(path, encode(`after-kill`))

      const started = Date.now()
      const result = await waiting
      expect(result.timedOut).toBe(false)
      expect(result.messages.map((m) => decode(m.data))).toEqual([`after-kill`])
      expect(Date.now() - started).toBeLessThan(5000)
    } finally {
      await admin.clear()
      await admin.close()
      await store.close()
    }
  })

  test(`commands fail within the command timeout instead of queueing forever`, async () => {
    const store = new RedisStreamStore({
      url: `redis://127.0.0.1:1`,
      keyPrefix: prefix,
      commandTimeoutMs: 300,
      connectTimeoutMs: 300,
    })
    const started = Date.now()
    await expect(store.get(`/v1/stream/unreachable`)).rejects.toThrow()
    expect(Date.now() - started).toBeLessThan(2000)
    await store.close()
  })
})
