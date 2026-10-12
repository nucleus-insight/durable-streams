/**
 * Drop-in stand-in for `@durable-streams/server` used to run the monorepo's
 * client-side test suites against the Redis-backed server. Aliased in
 * `vitest.server-redis-clients.config.ts`.
 */

import { DurableStreamRedisServer } from "../../src/server"
import { RedisStreamStore } from "../../src/redis-store"
import type { IncomingMessage, ServerResponse } from "node:http"
import type { RedisServerOptions } from "../../src/types"
import type { OffsetScheme } from "../../src/redis-store"

const REDIS_URL = process.env.REDIS_URL ?? `redis://127.0.0.1:6379`
const OFFSET_SCHEME = (process.env.DS_OFFSET_SCHEME ?? `bytes`) as OffsetScheme

function uniquePrefix(): string {
  return `dsclients:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}:`
}

export type StreamStore = RedisStreamStore

type RequestHandler = (
  req: IncomingMessage,
  res: ServerResponse
) => Promise<void>

export class DurableStreamTestServer extends DurableStreamRedisServer {
  /**
   * The reference store is synchronous, so its fixtures call `create` and
   * `append` without awaiting. Track those calls and let every request wait
   * for them so the Redis store observes the same ordering.
   */
  private readonly pendingWrites = new Set<Promise<unknown>>()

  constructor(options: RedisServerOptions & { dataDir?: string } = {}) {
    const { dataDir: _dataDir, ...rest } = options
    super({
      ...rest,
      longPollTimeout:
        rest.longPollTimeout ??
        (process.env.DS_LONG_POLL_TIMEOUT
          ? Number(process.env.DS_LONG_POLL_TIMEOUT)
          : undefined),
      redis: rest.redis ?? {
        url: REDIS_URL,
        keyPrefix: uniquePrefix(),
        offsetScheme: OFFSET_SCHEME,
      },
    })

    const track = <T extends (...args: Array<any>) => Promise<unknown>>(
      fn: T
    ): T =>
      ((...args: Parameters<T>) => {
        const promise = fn(...args)
        this.pendingWrites.add(promise)
        promise.then(
          () => this.pendingWrites.delete(promise),
          () => this.pendingWrites.delete(promise)
        )
        return promise
      }) as T
    this.store.create = track(this.store.create.bind(this.store))
    this.store.append = track(this.store.append.bind(this.store))

    const self = this as unknown as { handleRequest: RequestHandler }
    const handleRequest = self.handleRequest.bind(this)
    self.handleRequest = async (req, res) => {
      if (this.pendingWrites.size > 0) {
        await Promise.allSettled([...this.pendingWrites])
      }
      if (process.env.DS_TRACE) {
        const started = Date.now()
        const tag = `${req.method} ${req.url}`
        console.log(`[ds ${started % 100000}] -> ${tag}`)
        const write = res.write.bind(res)
        res.write = ((chunk: unknown, ...args: Array<unknown>) => {
          console.log(
            `[ds ${Date.now() % 100000}] ${tag} write: ${String(chunk).slice(0, 160).replace(/\n/g, `\\n`)}`
          )
          return (write as (...a: Array<unknown>) => boolean)(chunk, ...args)
        }) as typeof res.write
        res.on(`finish`, () => {
          console.log(
            `[ds ${Date.now() % 100000}] <- ${tag} ${res.statusCode} (+${Date.now() - started}ms)`
          )
        })
      }
      await handleRequest(req, res)
    }
  }

  override async stop(): Promise<void> {
    await this.store.clear()
    await super.stop()
  }
}

export { RedisStreamStore as FileBackedStreamStore }
export * from "../../src/index"
