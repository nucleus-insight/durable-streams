/**
 * Run the protocol conformance suite against the Redis-backed server, once
 * per offset scheme. Requires a reachable Redis (REDIS_URL, default
 * redis://127.0.0.1:6379).
 */

import { afterAll, beforeAll, describe } from "vitest"
import { runConformanceTests } from "@durable-streams/server-conformance-tests"
import { DurableStreamRedisServer } from "../src/server"
import { TEST_REDIS_URL, uniquePrefix } from "./support/test-helpers"
import type { OffsetScheme } from "../src/redis-store"

const SCHEMES: Array<OffsetScheme> = [`bytes`, `time`]

describe.each(SCHEMES)(`Redis Server Implementation (%s offsets)`, (scheme) => {
  let server: DurableStreamRedisServer

  // Use object with mutable property so conformance tests can access it
  const config = { baseUrl: ``, subscriptions: true }

  beforeAll(async () => {
    server = new DurableStreamRedisServer({
      port: 0,
      longPollTimeout: 500,
      webhooks: true,
      redis: {
        url: TEST_REDIS_URL,
        keyPrefix: uniquePrefix(`conformance-${scheme}`),
        offsetScheme: scheme,
      },
    })
    await server.start()
    config.baseUrl = server.url
  })

  afterAll(async () => {
    await server.clear()
    await server.stop()
  })

  runConformanceTests(config)
})
