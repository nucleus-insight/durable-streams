/**
 * HTTP server for durable streams testing.
 */

import { createServer } from "node:http"
import { deflateSync, gzipSync } from "node:zlib"
import {
  CURSOR_QUERY_PARAM,
  LIVE_QUERY_PARAM,
  OFFSET_QUERY_PARAM,
  PRODUCER_EPOCH_HEADER,
  PRODUCER_EXPECTED_SEQ_HEADER,
  PRODUCER_ID_HEADER,
  PRODUCER_RECEIVED_SEQ_HEADER,
  PRODUCER_SEQ_HEADER,
  SSE_CLOSED_FIELD,
  SSE_CURSOR_FIELD,
  SSE_OFFSET_FIELD,
  STREAM_CLOSED_HEADER,
  STREAM_CURSOR_HEADER,
  STREAM_EXPIRES_AT_HEADER,
  STREAM_OFFSET_HEADER,
  STREAM_SEQ_HEADER,
  STREAM_TTL_HEADER,
  STREAM_UP_TO_DATE_HEADER,
} from "@durable-streams/client"
import { RedisStreamStore } from "./redis-store"
import { formatMessages } from "./json"
import { generateResponseCursor } from "./cursor"
import { SubscriptionManager } from "./subscription-manager"
import { SubscriptionRoutes } from "./subscription-routes"
import { serverLog } from "./log"
import type { CursorOptions } from "./cursor"
import type { IncomingMessage, Server, ServerResponse } from "node:http"
import type { RedisServerOptions, Stream, StreamLifecycleEvent } from "./types"

const STREAM_SSE_DATA_ENCODING_HEADER = `Stream-SSE-Data-Encoding`

// SSE control event fields (Protocol Section 5.8)
const SSE_UP_TO_DATE_FIELD = `upToDate`

// Fork headers (request headers only — not set on responses)
const STREAM_FORKED_FROM_HEADER = `Stream-Forked-From`
const STREAM_FORK_OFFSET_HEADER = `Stream-Fork-Offset`
const STREAM_FORK_SUB_OFFSET_HEADER = `Stream-Fork-Sub-Offset`

/**
 * Encode data for SSE format.
 * Per SSE spec, each line in the payload needs its own "data:" prefix.
 * Line terminators in the payload (CR, LF, or CRLF) become separate data: lines.
 * This prevents CRLF injection attacks where malicious payloads could inject
 * fake SSE events using CR-only line terminators.
 *
 * Note: We don't add a space after "data:" because clients strip exactly one
 * leading space per the SSE spec. Adding one would cause data starting with
 * spaces to lose an extra space character.
 */
function encodeSSEData(payload: string): string {
  // Split on all SSE-valid line terminators: CRLF, CR, or LF
  // Order matters: \r\n must be matched before \r alone
  const lines = payload.split(/\r\n|\r|\n/)
  return lines.map((line) => `data:${line}`).join(`\n`) + `\n\n`
}

/**
 * Minimum response size to consider for compression.
 * Responses smaller than this won't benefit from compression.
 */
const COMPRESSION_THRESHOLD = 1024

/**
 * Determine the best compression encoding from Accept-Encoding header.
 * Returns 'gzip', 'deflate', or null if no compression should be used.
 */
function getCompressionEncoding(
  acceptEncoding: string | undefined
): `gzip` | `deflate` | null {
  if (!acceptEncoding) return null

  // Parse Accept-Encoding header (e.g., "gzip, deflate, br" or "gzip;q=1.0, deflate;q=0.5")
  const encodings = acceptEncoding
    .toLowerCase()
    .split(`,`)
    .map((e) => e.trim())

  // Prefer gzip over deflate (better compression, wider support)
  for (const encoding of encodings) {
    const name = encoding.split(`;`)[0]?.trim()
    if (name === `gzip`) return `gzip`
  }
  for (const encoding of encodings) {
    const name = encoding.split(`;`)[0]?.trim()
    if (name === `deflate`) return `deflate`
  }

  return null
}

/**
 * Compress data using the specified encoding.
 */
function compressData(
  data: Uint8Array,
  encoding: `gzip` | `deflate`
): Uint8Array {
  if (encoding === `gzip`) {
    return gzipSync(data)
  } else {
    return deflateSync(data)
  }
}

/**
 * HTTP server for durable streams backed by Redis.
 */
/**
 * Configuration for injected faults (for testing retry/resilience).
 * Supports various fault types beyond simple HTTP errors.
 */
interface InjectedFault {
  /** HTTP status code to return (if set, returns error response) */
  status?: number
  /** Number of times to trigger this fault (decremented on each use) */
  count: number
  /** Optional Retry-After header value (seconds) */
  retryAfter?: number
  /** Delay in milliseconds before responding */
  delayMs?: number
  /** Drop the connection after sending headers (simulates network failure) */
  dropConnection?: boolean
  /** Truncate response body to this many bytes */
  truncateBodyBytes?: number
  /** Probability of triggering fault (0-1, default 1.0 = always) */
  probability?: number
  /** Only match specific HTTP method (GET, POST, PUT, DELETE) */
  method?: string
  /** Corrupt the response body by flipping random bits */
  corruptBody?: boolean
  /** Add jitter to delay (random 0-jitterMs added to delayMs) */
  jitterMs?: number
  /** Inject an SSE event with custom type and data (for testing SSE parsing) */
  injectSseEvent?: {
    /** Event type (e.g., "unknown", "control", "data") */
    eventType: string
    /** Event data (will be sent as-is) */
    data: string
  }
}

export class DurableStreamRedisServer {
  readonly store: RedisStreamStore
  private readonly ownsStore: boolean
  private server: Server | null = null
  private options: Required<
    Omit<
      RedisServerOptions,
      | `redis`
      | `store`
      | `onStreamCreated`
      | `onStreamDeleted`
      | `compression`
      | `cursorIntervalSeconds`
      | `cursorEpoch`
      | `webhooks`
    >
  > & {
    onStreamCreated?: (event: StreamLifecycleEvent) => void | Promise<void>
    onStreamDeleted?: (event: StreamLifecycleEvent) => void | Promise<void>
    compression: boolean
    cursorOptions: CursorOptions
    webhooks: boolean
  }
  private _url: string | null = null
  private activeSSEResponses = new Set<ServerResponse>()
  private isShuttingDown = false
  /** Injected faults for testing retry/resilience */
  private injectedFaults = new Map<string, InjectedFault>()
  private subscriptionManager: SubscriptionManager | null = null
  private subscriptionRoutes: SubscriptionRoutes | null = null

  constructor(options: RedisServerOptions = {}) {
    if (options.store) {
      this.store = options.store
      this.ownsStore = false
    } else {
      this.store = new RedisStreamStore(options.redis)
      this.ownsStore = true
    }

    this.options = {
      port: options.port ?? 4437,
      host: options.host ?? `127.0.0.1`,
      longPollTimeout: options.longPollTimeout ?? 30_000,
      onStreamCreated: options.onStreamCreated,
      onStreamDeleted: options.onStreamDeleted,
      compression: options.compression ?? true,
      cursorOptions: {
        intervalSeconds: options.cursorIntervalSeconds,
        epoch: options.cursorEpoch,
      },
      webhooks: options.webhooks ?? false,
    }
  }

  /**
   * Start the server.
   */
  async start(): Promise<string> {
    if (this.server) {
      throw new Error(`Server already started`)
    }

    await this.store.connect()

    return new Promise((resolve, reject) => {
      this.server = createServer((req, res) => {
        this.handleRequest(req, res).catch((err) => {
          serverLog.error(`Request error:`, err)
          if (!res.headersSent) {
            res.writeHead(500, { "content-type": `text/plain` })
            res.end(`Internal server error`)
          }
        })
      })

      this.server.on(`error`, reject)

      this.server.listen(this.options.port, this.options.host, () => {
        const addr = this.server!.address()
        if (typeof addr === `string`) {
          this._url = addr
        } else if (addr) {
          this._url = `http://${this.options.host}:${addr.port}`
        }

        this.subscriptionManager = new SubscriptionManager({
          callbackBaseUrl: this._url!,
          streamStore: this.store,
          webhooksEnabled: this.options.webhooks,
        })
        this.subscriptionRoutes = new SubscriptionRoutes(
          this.subscriptionManager
        )
        resolve(this._url!)
      })
    })
  }

  /**
   * Stop the server.
   */
  async stop(): Promise<void> {
    if (!this.server) {
      return
    }

    // Mark as shutting down to stop SSE handlers
    this.isShuttingDown = true

    if (this.subscriptionManager) {
      this.subscriptionManager.shutdown()
      this.subscriptionManager = null
      this.subscriptionRoutes = null
    }

    // Cancel all pending long-polls and SSE waits to unblock connection handlers
    this.store.cancelAllWaits()

    // Force-close all active SSE connections
    for (const res of this.activeSSEResponses) {
      res.end()
    }
    this.activeSSEResponses.clear()

    return new Promise((resolve, reject) => {
      this.server!.close(async (err) => {
        if (err) {
          reject(err)
          return
        }

        try {
          if (this.ownsStore) {
            await this.store.close()
          }

          this.server = null
          this._url = null
          this.isShuttingDown = false
          resolve()
        } catch (closeErr) {
          reject(closeErr)
        }
      })
    })
  }

  /**
   * Get the server URL.
   */
  get url(): string {
    if (!this._url) {
      throw new Error(`Server not started`)
    }
    return this._url
  }

  /**
   * Clear all streams.
   */
  async clear(): Promise<void> {
    await this.store.clear()
  }

  /**
   * Inject an error to be returned on the next N requests to a path.
   * Used for testing retry/resilience behavior.
   * @deprecated Use injectFault for full fault injection capabilities
   */
  injectError(
    path: string,
    status: number,
    count: number = 1,
    retryAfter?: number
  ): void {
    this.injectedFaults.set(path, { status, count, retryAfter })
  }

  /**
   * Inject a fault to be triggered on the next N requests to a path.
   * Supports various fault types: delays, connection drops, body corruption, etc.
   */
  injectFault(
    path: string,
    fault: Omit<InjectedFault, `count`> & { count?: number }
  ): void {
    this.injectedFaults.set(path, { count: 1, ...fault })
  }

  /**
   * Clear all injected faults.
   */
  clearInjectedFaults(): void {
    this.injectedFaults.clear()
  }

  /**
   * Check if there's an injected fault for this path/method and consume it.
   * Returns the fault config if one should be triggered, null otherwise.
   */
  private consumeInjectedFault(
    path: string,
    method: string
  ): InjectedFault | null {
    const fault = this.injectedFaults.get(path)
    if (!fault) return null

    // Check method filter
    if (fault.method && fault.method.toUpperCase() !== method.toUpperCase()) {
      return null
    }

    // Check probability
    if (fault.probability !== undefined && Math.random() > fault.probability) {
      return null
    }

    fault.count--
    if (fault.count <= 0) {
      this.injectedFaults.delete(path)
    }

    return fault
  }

  /**
   * Apply delay from fault config (including jitter).
   */
  private async applyFaultDelay(fault: InjectedFault): Promise<void> {
    if (fault.delayMs !== undefined && fault.delayMs > 0) {
      const jitter = fault.jitterMs ? Math.random() * fault.jitterMs : 0
      await new Promise((resolve) =>
        setTimeout(resolve, fault.delayMs! + jitter)
      )
    }
  }

  /**
   * Apply body modifications from stored fault (truncation, corruption).
   * Returns modified body, or original if no modifications needed.
   */
  private applyFaultBodyModification(
    res: ServerResponse,
    body: Uint8Array
  ): Uint8Array {
    const fault = (res as ServerResponse & { _injectedFault?: InjectedFault })
      ._injectedFault
    if (!fault) return body

    let modified = body

    // Truncate body if configured
    if (
      fault.truncateBodyBytes !== undefined &&
      modified.length > fault.truncateBodyBytes
    ) {
      modified = modified.slice(0, fault.truncateBodyBytes)
    }

    // Corrupt body if configured - deterministically break JSON structure
    if (fault.corruptBody && modified.length > 0) {
      modified = new Uint8Array(modified) // Make a copy to avoid mutating original
      // Always corrupt the first byte (breaks JSON structure - the opening [ or {)
      // and add some random corruption for good measure
      modified[0] = 0x58 // 'X' - makes JSON syntactically invalid
      if (modified.length > 1) {
        modified[1] = 0x59 // 'Y'
      }
      // Also corrupt some bytes in the middle to catch edge cases
      const numCorrupt = Math.max(1, Math.floor(modified.length * 0.1))
      for (let i = 0; i < numCorrupt; i++) {
        const pos = Math.floor(Math.random() * modified.length)
        modified[pos] = 0x5a // 'Z' - valid UTF-8 but breaks JSON structure
      }
    }

    return modified
  }

  // ============================================================================
  // Request handling
  // ============================================================================

  private async handleRequest(
    req: IncomingMessage,
    res: ServerResponse
  ): Promise<void> {
    const url = new URL(req.url ?? `/`, `http://${req.headers.host}`)
    const path = url.pathname
    const method = req.method?.toUpperCase()

    // CORS headers for browser testing
    res.setHeader(`access-control-allow-origin`, `*`)
    res.setHeader(
      `access-control-allow-methods`,
      `GET, POST, PUT, DELETE, HEAD, OPTIONS`
    )
    res.setHeader(
      `access-control-allow-headers`,
      `content-type, authorization, If-None-Match, Stream-Seq, Stream-TTL, Stream-Expires-At, Stream-Closed, Producer-Id, Producer-Epoch, Producer-Seq, Stream-Forked-From, Stream-Fork-Offset, Stream-Fork-Sub-Offset`
    )
    res.setHeader(
      `access-control-expose-headers`,
      `Stream-Next-Offset, Stream-Cursor, Stream-Up-To-Date, Stream-Closed, Producer-Epoch, Producer-Seq, Producer-Expected-Seq, Producer-Received-Seq, etag, content-type, content-encoding, vary`
    )

    // Browser security headers (Protocol Section 10.7)
    res.setHeader(`x-content-type-options`, `nosniff`)
    res.setHeader(`cross-origin-resource-policy`, `cross-origin`)

    // Handle CORS preflight
    if (method === `OPTIONS`) {
      res.writeHead(204)
      res.end()
      return
    }

    // Handle test control endpoints (for error injection)
    if (path === `/_test/inject-error`) {
      await this.handleTestInjectError(method, req, res)
      return
    }

    // Check for injected faults (for testing retry/resilience)
    const fault = this.consumeInjectedFault(path, method ?? `GET`)
    if (fault) {
      // Apply delay if configured
      await this.applyFaultDelay(fault)

      // Drop connection if configured (simulates network failure)
      if (fault.dropConnection) {
        res.socket?.destroy()
        return
      }

      // If status is set, return an error response
      if (fault.status !== undefined) {
        const headers: Record<string, string> = {
          "content-type": `text/plain`,
        }
        if (fault.retryAfter !== undefined) {
          headers[`retry-after`] = fault.retryAfter.toString()
        }
        res.writeHead(fault.status, headers)
        res.end(`Injected error for testing`)
        return
      }

      // Store fault for response modification (truncation, corruption, SSE injection)
      if (
        fault.truncateBodyBytes !== undefined ||
        fault.corruptBody ||
        fault.injectSseEvent
      ) {
        ;(
          res as ServerResponse & { _injectedFault?: InjectedFault }
        )._injectedFault = fault
      }
    }

    if (this.subscriptionRoutes && method) {
      const handled = await this.subscriptionRoutes.handleRequest(
        method,
        path,
        req,
        res
      )
      if (handled) return
    }

    try {
      switch (method) {
        case `PUT`:
          await this.handleCreate(path, req, res)
          break
        case `HEAD`:
          await this.handleHead(path, res)
          break
        case `GET`:
          await this.handleRead(path, url, req, res)
          break
        case `POST`:
          await this.handleAppend(path, req, res)
          break
        case `DELETE`:
          await this.handleDelete(path, res)
          break
        default:
          res.writeHead(405, { "content-type": `text/plain` })
          res.end(`Method not allowed`)
      }
    } catch (err) {
      if (err instanceof Error) {
        if (err.message.includes(`active forks`)) {
          res.writeHead(409, { "content-type": `text/plain` })
          res.end(
            `stream was deleted but still has active forks — path cannot be reused until all forks are removed`
          )
        } else if (err.message.includes(`soft-deleted`)) {
          res.writeHead(410, { "content-type": `text/plain` })
          res.end(`Stream is gone`)
        } else if (err.message.includes(`not found`)) {
          res.writeHead(404, { "content-type": `text/plain` })
          res.end(`Stream not found`)
        } else if (
          err.message.includes(`already exists with different configuration`)
        ) {
          res.writeHead(409, { "content-type": `text/plain` })
          res.end(`Stream already exists with different configuration`)
        } else if (err.message.includes(`Sequence conflict`)) {
          res.writeHead(409, { "content-type": `text/plain` })
          res.end(`Sequence conflict`)
        } else if (err.message.includes(`Content-type mismatch`)) {
          res.writeHead(409, { "content-type": `text/plain` })
          res.end(`Content-type mismatch`)
        } else if (err.message.includes(`Invalid JSON`)) {
          res.writeHead(400, { "content-type": `text/plain` })
          res.end(`Invalid JSON`)
        } else if (err.message.includes(`Empty arrays are not allowed`)) {
          res.writeHead(400, { "content-type": `text/plain` })
          res.end(`Empty arrays are not allowed`)
        } else {
          throw err
        }
      } else {
        throw err
      }
    }
  }

  /**
   * Handle PUT - create stream
   */
  private async handleCreate(
    path: string,
    req: IncomingMessage,
    res: ServerResponse
  ): Promise<void> {
    let contentType = req.headers[`content-type`]

    // Parse fork headers (must come before content-type sanitization so
    // forks can fall through to the store's content-type inheritance)
    const forkedFromHeader = req.headers[
      STREAM_FORKED_FROM_HEADER.toLowerCase()
    ] as string | undefined
    const forkOffsetHeader = req.headers[
      STREAM_FORK_OFFSET_HEADER.toLowerCase()
    ] as string | undefined
    const forkSubOffsetHeaderRaw =
      req.headers[STREAM_FORK_SUB_OFFSET_HEADER.toLowerCase()]
    // Distinguish "header absent" from "header present but empty"
    const forkSubOffsetHeaderPresent = forkSubOffsetHeaderRaw !== undefined
    const forkSubOffsetHeader = Array.isArray(forkSubOffsetHeaderRaw)
      ? forkSubOffsetHeaderRaw[0]
      : forkSubOffsetHeaderRaw

    // Sanitize content-type: if empty or invalid, use default — but only
    // for non-fork creates. For forks, an omitted Content-Type means "inherit
    // from source", which is resolved by the store.
    if (
      !contentType ||
      contentType.trim() === `` ||
      !/^[\w-]+\/[\w-]+/.test(contentType)
    ) {
      contentType = forkedFromHeader ? undefined : `application/octet-stream`
    }

    const ttlHeader = req.headers[STREAM_TTL_HEADER.toLowerCase()] as
      | string
      | undefined
    const expiresAtHeader = req.headers[
      STREAM_EXPIRES_AT_HEADER.toLowerCase()
    ] as string | undefined

    // Parse Stream-Closed header
    const closedHeader = req.headers[STREAM_CLOSED_HEADER.toLowerCase()]
    const createClosed = closedHeader === `true`

    // Validate TTL and Expires-At headers
    if (ttlHeader && expiresAtHeader) {
      res.writeHead(400, { "content-type": `text/plain` })
      res.end(`Cannot specify both Stream-TTL and Stream-Expires-At`)
      return
    }

    let ttlSeconds: number | undefined
    if (ttlHeader) {
      // Strict TTL validation: must be a positive integer without leading zeros,
      // plus signs, decimals, whitespace, or non-decimal notation
      const ttlPattern = /^(0|[1-9]\d*)$/
      if (!ttlPattern.test(ttlHeader)) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Stream-TTL value`)
        return
      }

      ttlSeconds = parseInt(ttlHeader, 10)
      if (isNaN(ttlSeconds) || ttlSeconds < 0) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Stream-TTL value`)
        return
      }
    }

    // Validate Expires-At timestamp format (ISO 8601)
    if (expiresAtHeader) {
      const timestamp = new Date(expiresAtHeader)
      if (isNaN(timestamp.getTime())) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Stream-Expires-At timestamp`)
        return
      }
    }

    // Validate fork offset format if provided
    if (forkOffsetHeader) {
      const validOffsetPattern = /^\d+_\d+$/
      if (!validOffsetPattern.test(forkOffsetHeader)) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Stream-Fork-Offset format`)
        return
      }
    }

    // Validate sub-offset if header was present (including empty value)
    let forkSubOffset: number | undefined
    if (forkSubOffsetHeaderPresent) {
      if (!forkedFromHeader) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Stream-Fork-Sub-Offset requires Stream-Forked-From`)
        return
      }
      const subOffsetPattern = /^(0|[1-9]\d*)$/
      if (
        forkSubOffsetHeader === undefined ||
        !subOffsetPattern.test(forkSubOffsetHeader)
      ) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Stream-Fork-Sub-Offset format`)
        return
      }
      forkSubOffset = parseInt(forkSubOffsetHeader, 10)
    }

    // Read body if present
    const body = await this.readBody(req)

    const isNew = !(await this.store.has(path))

    let stream: Stream
    try {
      stream = await this.store.create(path, {
        contentType,
        ttlSeconds,
        expiresAt: expiresAtHeader,
        initialData: body.length > 0 ? body : undefined,
        closed: createClosed,
        forkedFrom: forkedFromHeader,
        forkOffset: forkOffsetHeader,
        forkSubOffset,
      })
    } catch (err) {
      if (err instanceof Error) {
        if (err.message.includes(`Source stream not found`)) {
          res.writeHead(404, { "content-type": `text/plain` })
          res.end(`Source stream not found`)
          return
        }
        if (err.message.includes(`Invalid fork sub-offset`)) {
          res.writeHead(400, { "content-type": `text/plain` })
          res.end(`Invalid fork sub-offset`)
          return
        }
        if (err.message.includes(`Invalid fork offset`)) {
          res.writeHead(400, { "content-type": `text/plain` })
          res.end(`Fork offset beyond source stream length`)
          return
        }
        if (err.message.includes(`soft-deleted`)) {
          res.writeHead(409, { "content-type": `text/plain` })
          res.end(`source stream was deleted but still has active forks`)
          return
        }
        if (err.message.includes(`Content type mismatch`)) {
          res.writeHead(409, { "content-type": `text/plain` })
          res.end(`Content type mismatch with source stream`)
          return
        }
      }
      throw err
    }

    const resolvedContentType =
      stream.contentType ?? contentType ?? `application/octet-stream`

    // Call lifecycle hook for new streams
    if (isNew && this.options.onStreamCreated) {
      await Promise.resolve(
        this.options.onStreamCreated({
          type: `created`,
          path,
          contentType: resolvedContentType,
          timestamp: Date.now(),
        })
      )
    }

    if (isNew && body.length > 0) {
      await this.notifyStreamAppend(path)
    }

    // Return 201 for new streams, 200 for idempotent creates
    const headers: Record<string, string> = {
      "content-type": resolvedContentType,
      [STREAM_OFFSET_HEADER]: stream.currentOffset,
    }

    // Add Location header for 201 Created responses
    if (isNew) {
      headers[`location`] = `${this._url}${path}`
    }

    // Include Stream-Closed header if created closed
    if (stream.closed) {
      headers[STREAM_CLOSED_HEADER] = `true`
    }

    res.writeHead(isNew ? 201 : 200, headers)
    res.end()
  }

  /**
   * Handle HEAD - get metadata
   */
  private async handleHead(path: string, res: ServerResponse): Promise<void> {
    const stream = await this.store.get(path)
    if (!stream) {
      res.writeHead(404, { "content-type": `text/plain` })
      res.end()
      return
    }

    // Check for soft-deleted streams
    if (stream.softDeleted) {
      res.writeHead(410, { "content-type": `text/plain` })
      res.end()
      return
    }

    const headers: Record<string, string> = {
      [STREAM_OFFSET_HEADER]: stream.currentOffset,
      // HEAD responses should not be cached to avoid stale tail offsets (Protocol Section 5.4)
      "cache-control": `no-store`,
    }

    if (stream.contentType) {
      headers[`content-type`] = stream.contentType
    }

    // Include Stream-Closed if stream is closed
    if (stream.closed) {
      headers[STREAM_CLOSED_HEADER] = `true`
    }

    // Include TTL/Expiry metadata
    if (stream.ttlSeconds !== undefined) {
      headers[STREAM_TTL_HEADER] = String(stream.ttlSeconds)
    }
    if (stream.expiresAt) {
      headers[STREAM_EXPIRES_AT_HEADER] = stream.expiresAt
    }

    // Generate ETag: {path}:-1:{offset}[:c] (includes closure status)
    // The :c suffix ensures ETag changes when a stream is closed, even without new data
    const closedSuffix = stream.closed ? `:c` : ``
    headers[`etag`] =
      `"${Buffer.from(path).toString(`base64`)}:-1:${stream.currentOffset}${closedSuffix}"`

    res.writeHead(200, headers)
    res.end()
  }

  /**
   * Handle GET - read data
   */
  private async handleRead(
    path: string,
    url: URL,
    req: IncomingMessage,
    res: ServerResponse
  ): Promise<void> {
    const stream = await this.store.get(path)
    if (!stream) {
      res.writeHead(404, { "content-type": `text/plain` })
      res.end(`Stream not found`)
      return
    }

    // Check for soft-deleted streams
    if (stream.softDeleted) {
      res.writeHead(410, { "content-type": `text/plain` })
      res.end(`Stream is gone`)
      return
    }

    const offset = url.searchParams.get(OFFSET_QUERY_PARAM) ?? undefined
    const live = url.searchParams.get(LIVE_QUERY_PARAM)
    const cursor = url.searchParams.get(CURSOR_QUERY_PARAM) ?? undefined

    // Validate offset parameter
    if (offset !== undefined) {
      // Reject empty offset
      if (offset === ``) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Empty offset parameter`)
        return
      }

      // Reject multiple offset parameters
      const allOffsets = url.searchParams.getAll(OFFSET_QUERY_PARAM)
      if (allOffsets.length > 1) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Multiple offset parameters not allowed`)
        return
      }

      // Validate offset format: must be "-1", "now", or match our offset format (digits_digits)
      // This prevents path traversal, injection attacks, and invalid characters
      const validOffsetPattern = /^(-1|now|\d+_\d+)$/
      if (!validOffsetPattern.test(offset)) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid offset format`)
        return
      }
    }

    // Require offset parameter for long-poll and SSE per protocol spec
    if ((live === `long-poll` || live === `sse`) && !offset) {
      res.writeHead(400, { "content-type": `text/plain` })
      res.end(
        `${live === `sse` ? `SSE` : `Long-poll`} requires offset parameter`
      )
      return
    }

    // Determine if this is a binary stream that needs base64 encoding in SSE mode
    let useBase64 = false
    if (live === `sse`) {
      const ct = stream.contentType?.toLowerCase().split(`;`)[0]?.trim() ?? ``
      const isTextCompatible =
        ct.startsWith(`text/`) || ct === `application/json`
      useBase64 = !isTextCompatible
    }

    // Handle SSE mode
    if (live === `sse`) {
      // For SSE with offset=now, convert to actual tail offset
      const sseOffset = offset === `now` ? stream.currentOffset : offset!
      await this.handleSSE(path, stream, sseOffset, cursor, useBase64, res)
      return
    }

    // For offset=now, convert to actual tail offset
    // This allows long-poll to immediately start waiting for new data
    const effectiveOffset = offset === `now` ? stream.currentOffset : offset

    // Handle catch-up mode offset=now: return empty response with tail offset
    // For long-poll mode, we fall through to wait for new data instead
    if (offset === `now` && live !== `long-poll`) {
      // Still a read: refresh the sliding TTL like any other GET
      await this.store.touchAccess(path)

      const headers: Record<string, string> = {
        [STREAM_OFFSET_HEADER]: stream.currentOffset,
        [STREAM_UP_TO_DATE_HEADER]: `true`,
        // Prevent caching - tail offset changes with each append
        [`cache-control`]: `no-store`,
      }

      if (stream.contentType) {
        headers[`content-type`] = stream.contentType
      }

      // Include Stream-Closed if stream is closed (client at tail, upToDate)
      if (stream.closed) {
        headers[STREAM_CLOSED_HEADER] = `true`
      }

      // No ETag for offset=now responses - Cache-Control: no-store makes ETag unnecessary
      // and some CDNs may behave unexpectedly with both headers

      // For JSON mode, return empty array; otherwise empty body
      const isJsonMode = stream.contentType?.includes(`application/json`)
      const responseBody = isJsonMode ? `[]` : ``

      res.writeHead(200, headers)
      res.end(responseBody)
      return
    }

    // Read current messages
    let { messages, upToDate } = await this.store.read(path, effectiveOffset)
    await this.store.touchAccess(path)

    // Only wait in long-poll if:
    // 1. long-poll mode is enabled
    // 2. Client provided an offset (not first request) OR used offset=now
    // 3. Client's offset matches current offset (already caught up)
    // 4. No new messages
    const clientIsCaughtUp =
      (effectiveOffset && effectiveOffset === stream.currentOffset) ||
      offset === `now`
    if (live === `long-poll` && clientIsCaughtUp && messages.length === 0) {
      // If stream is closed and client is at tail, return immediately (don't wait)
      if (stream.closed) {
        res.writeHead(204, {
          [STREAM_OFFSET_HEADER]: stream.currentOffset,
          [STREAM_UP_TO_DATE_HEADER]: `true`,
          [STREAM_CLOSED_HEADER]: `true`,
        })
        res.end()
        return
      }

      const result = await this.store.waitForMessages(
        path,
        effectiveOffset ?? stream.currentOffset,
        this.options.longPollTimeout
      )
      await this.store.touchAccess(path)

      // If stream was closed during wait, return immediately with Stream-Closed
      if (result.streamClosed) {
        const responseCursor = generateResponseCursor(
          cursor,
          this.options.cursorOptions
        )
        res.writeHead(204, {
          [STREAM_OFFSET_HEADER]: effectiveOffset ?? stream.currentOffset,
          [STREAM_UP_TO_DATE_HEADER]: `true`,
          [STREAM_CURSOR_HEADER]: responseCursor,
          [STREAM_CLOSED_HEADER]: `true`,
        })
        res.end()
        return
      }

      if (result.timedOut) {
        // Return 204 No Content on timeout (per Protocol Section 5.6)
        // Generate cursor for CDN cache collapsing (Protocol Section 8.1)
        const responseCursor = generateResponseCursor(
          cursor,
          this.options.cursorOptions
        )
        // Check if stream was closed during the wait
        const currentStream = await this.store.get(path)
        const timeoutHeaders: Record<string, string> = {
          [STREAM_OFFSET_HEADER]: effectiveOffset ?? stream.currentOffset,
          [STREAM_UP_TO_DATE_HEADER]: `true`,
          [STREAM_CURSOR_HEADER]: responseCursor,
        }
        if (currentStream?.closed) {
          timeoutHeaders[STREAM_CLOSED_HEADER] = `true`
        }
        res.writeHead(204, timeoutHeaders)
        res.end()
        return
      }

      messages = result.messages
      upToDate = true
    }

    // Build response
    const headers: Record<string, string> = {}

    if (stream.contentType) {
      headers[`content-type`] = stream.contentType
    }

    // Set offset header to the last message's offset, or current if no messages
    const lastMessage = messages[messages.length - 1]
    const responseOffset = lastMessage?.offset ?? stream.currentOffset
    headers[STREAM_OFFSET_HEADER] = responseOffset

    // Generate cursor for live mode responses (Protocol Section 8.1)
    if (live === `long-poll`) {
      headers[STREAM_CURSOR_HEADER] = generateResponseCursor(
        cursor,
        this.options.cursorOptions
      )
    }

    // Set up-to-date header
    if (upToDate) {
      headers[STREAM_UP_TO_DATE_HEADER] = `true`
    }

    // Include Stream-Closed when stream is closed AND client is at tail AND upToDate
    // Re-fetch stream to get current state (may have been closed during request)
    const currentStream = await this.store.get(path)
    const clientAtTail = responseOffset === currentStream?.currentOffset
    if (currentStream?.closed && clientAtTail && upToDate) {
      headers[STREAM_CLOSED_HEADER] = `true`
    }

    // Generate ETag: based on path, start offset, end offset, and closure status
    // The :c suffix ensures ETag changes when a stream is closed, even without new data
    const startOffset = offset ?? `-1`
    const closedSuffix =
      currentStream?.closed && clientAtTail && upToDate ? `:c` : ``
    const etag = `"${Buffer.from(path).toString(`base64`)}:${startOffset}:${responseOffset}${closedSuffix}"`
    headers[`etag`] = etag

    // Check If-None-Match for conditional GET (Protocol Section 8.1)
    const ifNoneMatch = req.headers[`if-none-match`]
    if (ifNoneMatch && ifNoneMatch === etag) {
      res.writeHead(304, { etag })
      res.end()
      return
    }

    // Format response (wraps JSON in array brackets)
    const responseData = formatMessages(stream.contentType, messages)

    // Apply compression if enabled and response is large enough
    let finalData: Uint8Array = responseData
    if (
      this.options.compression &&
      responseData.length >= COMPRESSION_THRESHOLD
    ) {
      const acceptEncoding = req.headers[`accept-encoding`]
      const compressionEncoding = getCompressionEncoding(acceptEncoding)
      if (compressionEncoding) {
        finalData = compressData(responseData, compressionEncoding)
        headers[`content-encoding`] = compressionEncoding
        // Add Vary header to indicate response varies by Accept-Encoding
        headers[`vary`] = `accept-encoding`
      }
    }

    // Apply fault body modifications (truncation, corruption) if configured
    finalData = this.applyFaultBodyModification(res, finalData)

    res.writeHead(200, headers)
    res.end(Buffer.from(finalData))
  }

  /**
   * Handle SSE (Server-Sent Events) mode
   */
  private async handleSSE(
    path: string,
    stream: Stream,
    initialOffset: string,
    cursor: string | undefined,
    useBase64: boolean,
    res: ServerResponse
  ): Promise<void> {
    // Track this SSE connection
    this.activeSSEResponses.add(res)

    // Set SSE headers (explicitly including security headers for clarity)
    const sseHeaders: Record<string, string> = {
      "content-type": `text/event-stream`,
      "cache-control": `no-cache`,
      connection: `keep-alive`,
      "access-control-allow-origin": `*`,
      "x-content-type-options": `nosniff`,
      "cross-origin-resource-policy": `cross-origin`,
    }

    // Add encoding header when base64 encoding is used for binary streams
    if (useBase64) {
      sseHeaders[STREAM_SSE_DATA_ENCODING_HEADER] = `base64`
    }

    res.writeHead(200, sseHeaders)

    // Check for injected SSE event (for testing SSE parsing)
    const fault = (res as ServerResponse & { _injectedFault?: InjectedFault })
      ._injectedFault
    if (fault?.injectSseEvent) {
      // Send the injected SSE event before normal stream
      res.write(`event: ${fault.injectSseEvent.eventType}\n`)
      res.write(`data: ${fault.injectSseEvent.data}\n\n`)
    }

    let currentOffset = initialOffset
    let isConnected = true
    const decoder = new TextDecoder()

    // Handle client disconnect
    res.on(`close`, () => {
      isConnected = false
      this.activeSSEResponses.delete(res)
    })

    // Get content type for formatting
    const isJsonStream = stream.contentType?.includes(`application/json`)

    const writeFrame = (frame: string): boolean => {
      if (!isConnected || res.writableEnded) return false
      res.write(frame)
      return true
    }

    // Send initial data and then wait for more
    // Note: isConnected and isShuttingDown can change asynchronously
    // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
    while (isConnected && !this.isShuttingDown) {
      // Snapshot the stream BEFORE reading: the store is asynchronous, so a
      // snapshot taken after the read could already include an append the
      // read did not return, and reporting that offset would skip the data.
      const currentStream = await this.store.get(path)
      if (!currentStream) break

      // Read current messages from offset
      const { messages, upToDate } = await this.store.read(path, currentOffset)
      await this.store.touchAccess(path)

      // Send the whole batch as ONE data event: a control event follows
      // every data event (Protocol Section 5.8), and per-message data
      // events sharing one control would make JSON catch-up unparseable
      // for clients that collect data up to the control boundary
      // (`[a][b]` is not a JSON value).
      let frame = ``
      if (messages.length > 0) {
        // Format data based on content type and encoding
        let dataPayload: string
        if (useBase64) {
          // Base64 encode binary data (Protocol Section 5.8)
          dataPayload = Buffer.concat(
            messages.map((message) => Buffer.from(message.data))
          ).toString(`base64`)
        } else if (isJsonStream) {
          // Use formatResponse to get properly formatted JSON (strips trailing commas)
          const jsonBytes = formatMessages(stream.contentType, messages)
          dataPayload = decoder.decode(jsonBytes)
        } else {
          dataPayload = decoder.decode(
            Buffer.concat(messages.map((message) => Buffer.from(message.data)))
          )
        }

        // Data event - encode multiline payloads per SSE spec
        // Each line in the payload needs its own "data:" prefix
        frame += `event: data\n` + encodeSSEData(dataPayload)

        currentOffset = messages[messages.length - 1]!.offset
      }

      // Compute offset the same way as HTTP GET: last message's offset, or
      // the stream's current offset as of the pre-read snapshot
      const controlOffset =
        messages[messages.length - 1]?.offset ?? currentStream.currentOffset

      // Check if stream is closed and client is at tail
      const streamIsClosed = currentStream.closed ?? false
      const clientAtTail = controlOffset === currentStream.currentOffset

      // Send control event with current offset/cursor (Protocol Section 5.8)
      // Generate cursor for CDN cache collapsing (Protocol Section 8.1)
      const responseCursor = generateResponseCursor(
        cursor,
        this.options.cursorOptions
      )
      const controlData: Record<string, string | boolean> = {
        [SSE_OFFSET_FIELD]: controlOffset,
      }

      if (streamIsClosed && clientAtTail) {
        // Final control event - stream is closed
        // streamCursor is omitted when streamClosed is true per protocol
        // upToDate is implied by streamClosed per protocol
        controlData[SSE_CLOSED_FIELD] = true
      } else {
        // Normal control event - include cursor
        controlData[SSE_CURSOR_FIELD] = responseCursor
        // Include upToDate flag when client has caught up to head
        if (upToDate) {
          controlData[SSE_UP_TO_DATE_FIELD] = true
        }
      }

      // Data and control events go out in one write so they share a chunk
      frame += `event: control\n` + encodeSSEData(JSON.stringify(controlData))
      if (!writeFrame(frame)) break

      // Close SSE connection after sending streamClosed
      if (streamIsClosed && clientAtTail) {
        break // Exit loop, connection will be closed
      }

      // Update currentOffset for next iteration (use controlOffset for consistency)
      currentOffset = controlOffset

      // If caught up, wait for new messages
      if (upToDate) {
        // Closed with the client at its tail: send the final control event
        if (
          currentStream.closed &&
          currentOffset === currentStream.currentOffset
        ) {
          const finalControlData: Record<string, string | boolean> = {
            [SSE_OFFSET_FIELD]: currentOffset,
            [SSE_CLOSED_FIELD]: true,
          }
          writeFrame(
            `event: control\n` + encodeSSEData(JSON.stringify(finalControlData))
          )
          break
        }

        const result = await this.store.waitForMessages(
          path,
          currentOffset,
          this.options.longPollTimeout
        )
        await this.store.touchAccess(path)

        // Check if we should exit after wait returns (values can change during await)
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
        if (this.isShuttingDown || !isConnected) break

        // Check if stream was closed during wait. If the close also appended
        // final data, let the next loop iteration deliver those messages
        // before emitting the streamClosed control event.
        if (result.streamClosed && result.messages.length === 0) {
          const finalControlData: Record<string, string | boolean> = {
            [SSE_OFFSET_FIELD]: currentOffset,
            [SSE_CLOSED_FIELD]: true,
          }
          writeFrame(
            `event: control\n` + encodeSSEData(JSON.stringify(finalControlData))
          )
          break
        }

        if (result.timedOut) {
          // Send keep-alive control event on timeout (Protocol Section 5.8)
          // Generate cursor for CDN cache collapsing (Protocol Section 8.1)
          const keepAliveCursor = generateResponseCursor(
            cursor,
            this.options.cursorOptions
          )

          // Closed during the wait with nothing new: send the final control
          // event. A close that also appended data is delivered by the next
          // iteration before the stream is reported closed.
          const streamAfterWait = await this.store.get(path)
          if (!streamAfterWait) break
          if (
            streamAfterWait.closed &&
            streamAfterWait.currentOffset === currentOffset
          ) {
            const closedControlData: Record<string, string | boolean> = {
              [SSE_OFFSET_FIELD]: currentOffset,
              [SSE_CLOSED_FIELD]: true,
            }
            writeFrame(
              `event: control\n` +
                encodeSSEData(JSON.stringify(closedControlData))
            )
            break
          }
          if (streamAfterWait.currentOffset !== currentOffset) continue

          const keepAliveData: Record<string, string | boolean> = {
            [SSE_OFFSET_FIELD]: currentOffset,
            [SSE_CURSOR_FIELD]: keepAliveCursor,
            [SSE_UP_TO_DATE_FIELD]: true, // Still caught up after timeout
          }
          // Single write for keep-alive control event
          if (
            !writeFrame(
              `event: control\n` + encodeSSEData(JSON.stringify(keepAliveData))
            )
          )
            break
        }
        // Loop will continue and read new messages
      }
    }

    this.activeSSEResponses.delete(res)
    if (!res.writableEnded) res.end()
  }

  /**
   * Handle POST - append data
   */
  private async handleAppend(
    path: string,
    req: IncomingMessage,
    res: ServerResponse
  ): Promise<void> {
    const contentType = req.headers[`content-type`]
    const seq = req.headers[STREAM_SEQ_HEADER.toLowerCase()] as
      | string
      | undefined

    // Parse Stream-Closed header
    const closedHeader = req.headers[STREAM_CLOSED_HEADER.toLowerCase()]
    const closeStream = closedHeader === `true`

    // Extract producer headers
    const producerId = req.headers[PRODUCER_ID_HEADER.toLowerCase()] as
      | string
      | undefined
    const producerEpochStr = req.headers[
      PRODUCER_EPOCH_HEADER.toLowerCase()
    ] as string | undefined
    const producerSeqStr = req.headers[PRODUCER_SEQ_HEADER.toLowerCase()] as
      | string
      | undefined

    // Validate producer headers - all three must be present together or none
    // Also reject empty producer ID (do this before reading body)
    const hasProducerHeaders =
      producerId !== undefined ||
      producerEpochStr !== undefined ||
      producerSeqStr !== undefined
    const hasAllProducerHeaders =
      producerId !== undefined &&
      producerEpochStr !== undefined &&
      producerSeqStr !== undefined

    if (hasProducerHeaders && !hasAllProducerHeaders) {
      res.writeHead(400, { "content-type": `text/plain` })
      res.end(
        `All producer headers (Producer-Id, Producer-Epoch, Producer-Seq) must be provided together`
      )
      return
    }

    if (hasAllProducerHeaders && producerId === ``) {
      res.writeHead(400, { "content-type": `text/plain` })
      res.end(`Invalid Producer-Id: must not be empty`)
      return
    }

    // Parse and validate producer epoch and seq as integers
    // Use strict digit-only validation to reject values like "1abc" or "1e3"
    const STRICT_INTEGER_REGEX = /^\d+$/
    let producerEpoch: number | undefined
    let producerSeq: number | undefined
    if (hasAllProducerHeaders) {
      if (!STRICT_INTEGER_REGEX.test(producerEpochStr)) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Producer-Epoch: must be a non-negative integer`)
        return
      }
      producerEpoch = Number(producerEpochStr)
      if (!Number.isSafeInteger(producerEpoch)) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Producer-Epoch: must be a non-negative integer`)
        return
      }

      if (!STRICT_INTEGER_REGEX.test(producerSeqStr)) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Producer-Seq: must be a non-negative integer`)
        return
      }
      producerSeq = Number(producerSeqStr)
      if (!Number.isSafeInteger(producerSeq)) {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid Producer-Seq: must be a non-negative integer`)
        return
      }
    }

    const body = await this.readBody(req)

    // Handle close-only request (empty body with Stream-Closed: true)
    // Note: Content-Type validation is skipped for close-only requests per protocol Section 5.2
    if (body.length === 0 && closeStream) {
      // Close-only with producer headers participates in producer sequencing
      if (hasAllProducerHeaders) {
        const closeResult = await this.store.closeStreamWithProducer(path, {
          producerId: producerId,
          producerEpoch: producerEpoch!,
          producerSeq: producerSeq!,
        })

        if (!closeResult) {
          res.writeHead(404, { "content-type": `text/plain` })
          res.end(`Stream not found`)
          return
        }

        // Handle producer validation results
        if (closeResult.producerResult?.status === `duplicate`) {
          res.writeHead(204, {
            [STREAM_OFFSET_HEADER]: closeResult.finalOffset,
            [STREAM_CLOSED_HEADER]: `true`,
            [PRODUCER_EPOCH_HEADER]: producerEpoch!.toString(),
            [PRODUCER_SEQ_HEADER]:
              closeResult.producerResult.lastSeq.toString(),
          })
          res.end()
          return
        }

        if (closeResult.producerResult?.status === `stale_epoch`) {
          res.writeHead(403, {
            "content-type": `text/plain`,
            [PRODUCER_EPOCH_HEADER]:
              closeResult.producerResult.currentEpoch.toString(),
          })
          res.end(`Stale producer epoch`)
          return
        }

        if (closeResult.producerResult?.status === `invalid_epoch_seq`) {
          res.writeHead(400, { "content-type": `text/plain` })
          res.end(`New epoch must start with sequence 0`)
          return
        }

        if (closeResult.producerResult?.status === `sequence_gap`) {
          res.writeHead(409, {
            "content-type": `text/plain`,
            [PRODUCER_EXPECTED_SEQ_HEADER]:
              closeResult.producerResult.expectedSeq.toString(),
            [PRODUCER_RECEIVED_SEQ_HEADER]:
              closeResult.producerResult.receivedSeq.toString(),
          })
          res.end(`Producer sequence gap`)
          return
        }

        // Stream already closed by a different producer - conflict
        if (closeResult.producerResult?.status === `stream_closed`) {
          const stream = await this.store.get(path)
          res.writeHead(409, {
            "content-type": `text/plain`,
            [STREAM_CLOSED_HEADER]: `true`,
            [STREAM_OFFSET_HEADER]: stream?.currentOffset ?? ``,
          })
          res.end(`Stream is closed`)
          return
        }

        // A close is a write: refresh the sliding TTL like any other POST.
        await this.store.touchAccess(path)

        res.writeHead(204, {
          [STREAM_OFFSET_HEADER]: closeResult.finalOffset,
          [STREAM_CLOSED_HEADER]: `true`,
          [PRODUCER_EPOCH_HEADER]: producerEpoch!.toString(),
          [PRODUCER_SEQ_HEADER]: producerSeq!.toString(),
        })
        res.end()
        return
      }

      // Close-only without producer headers (simple idempotent close)
      const closeResult = await this.store.closeStream(path)
      if (!closeResult) {
        res.writeHead(404, { "content-type": `text/plain` })
        res.end(`Stream not found`)
        return
      }

      // A close is a write: refresh the sliding TTL like any other POST.
      await this.store.touchAccess(path)

      res.writeHead(204, {
        [STREAM_OFFSET_HEADER]: closeResult.finalOffset,
        [STREAM_CLOSED_HEADER]: `true`,
      })
      res.end()
      return
    }

    // Empty body without Stream-Closed is an error
    if (body.length === 0) {
      res.writeHead(400, { "content-type": `text/plain` })
      res.end(`Empty body`)
      return
    }

    // Content-Type is required per protocol (for requests with body)
    if (!contentType) {
      res.writeHead(400, { "content-type": `text/plain` })
      res.end(`Content-Type header is required`)
      return
    }

    // Build append options (include close flag for append-and-close)
    const appendOptions = {
      seq,
      contentType,
      producerId,
      producerEpoch,
      producerSeq,
      close: closeStream,
    }

    let result
    if (producerId !== undefined) {
      result = await this.store.appendWithProducer(path, body, appendOptions)
    } else {
      result = await this.store.append(path, body, appendOptions)
    }
    await this.store.touchAccess(path)

    // Handle AppendResult with producer validation or streamClosed
    if (result && typeof result === `object` && `message` in result) {
      const { message, producerResult, streamClosed } = result as {
        message: { offset: string } | null
        producerResult?: {
          status: string
          lastSeq?: number
          currentEpoch?: number
          expectedSeq?: number
          receivedSeq?: number
        }
        streamClosed?: boolean
      }

      // Handle append to closed stream
      if (streamClosed && !message) {
        // Check if this is an idempotent producer duplicate (matching closing tuple)
        if (producerResult?.status === `duplicate`) {
          const stream = await this.store.get(path)
          res.writeHead(204, {
            [STREAM_OFFSET_HEADER]: stream?.currentOffset ?? ``,
            [STREAM_CLOSED_HEADER]: `true`,
            [PRODUCER_EPOCH_HEADER]: producerEpoch!.toString(),
            [PRODUCER_SEQ_HEADER]: producerResult.lastSeq!.toString(),
          })
          res.end()
          return
        }

        // Not a duplicate - stream was closed by different request, return 409
        const closedStream = await this.store.get(path)
        res.writeHead(409, {
          "content-type": `text/plain`,
          [STREAM_CLOSED_HEADER]: `true`,
          [STREAM_OFFSET_HEADER]: closedStream?.currentOffset ?? ``,
        })
        res.end(`Stream is closed`)
        return
      }

      if (!producerResult || producerResult.status === `accepted`) {
        // Success - return offset
        const responseHeaders: Record<string, string> = {
          [STREAM_OFFSET_HEADER]: message!.offset,
        }
        // Echo back the producer epoch and seq (highest accepted)
        if (producerEpoch !== undefined) {
          responseHeaders[PRODUCER_EPOCH_HEADER] = producerEpoch.toString()
        }
        if (producerSeq !== undefined) {
          responseHeaders[PRODUCER_SEQ_HEADER] = producerSeq.toString()
        }
        // Include Stream-Closed if stream was closed with this append
        if (streamClosed) {
          responseHeaders[STREAM_CLOSED_HEADER] = `true`
        }
        // Use 200 for producer appends (with headers), 204 for non-producer appends
        const statusCode = producerId !== undefined ? 200 : 204
        res.writeHead(statusCode, responseHeaders)
        res.end()

        await this.notifyStreamAppend(path)
        return
      }

      // Handle producer validation failures
      switch (producerResult.status) {
        case `duplicate`: {
          // 204 No Content for duplicates (idempotent success)
          // Return Producer-Seq as highest accepted (per PROTOCOL.md)
          const dupHeaders: Record<string, string> = {
            [PRODUCER_EPOCH_HEADER]: producerEpoch!.toString(),
            [PRODUCER_SEQ_HEADER]: producerResult.lastSeq!.toString(),
          }
          // Include Stream-Closed if the stream is now closed
          if (streamClosed) {
            dupHeaders[STREAM_CLOSED_HEADER] = `true`
          }
          res.writeHead(204, dupHeaders)
          res.end()
          return
        }

        case `stale_epoch`: {
          // 403 Forbidden for stale epochs (zombie fencing)
          res.writeHead(403, {
            "content-type": `text/plain`,
            [PRODUCER_EPOCH_HEADER]: producerResult.currentEpoch!.toString(),
          })
          res.end(`Stale producer epoch`)
          return
        }

        case `invalid_epoch_seq`:
          // 400 Bad Request for epoch increase with seq != 0
          res.writeHead(400, { "content-type": `text/plain` })
          res.end(`New epoch must start with sequence 0`)
          return

        case `sequence_gap`:
          // 409 Conflict for sequence gaps
          res.writeHead(409, {
            "content-type": `text/plain`,
            [PRODUCER_EXPECTED_SEQ_HEADER]:
              producerResult.expectedSeq!.toString(),
            [PRODUCER_RECEIVED_SEQ_HEADER]:
              producerResult.receivedSeq!.toString(),
          })
          res.end(`Producer sequence gap`)
          return
      }
    }

    // Standard append (no producer) - result is StreamMessage
    const message = result as { offset: string }
    const responseHeaders: Record<string, string> = {
      [STREAM_OFFSET_HEADER]: message.offset,
    }
    // Include Stream-Closed if stream was closed with this append
    if (closeStream) {
      responseHeaders[STREAM_CLOSED_HEADER] = `true`
    }
    res.writeHead(204, responseHeaders)
    res.end()

    await this.notifyStreamAppend(path)
  }

  private async notifyStreamAppend(path: string): Promise<void> {
    if (!this.subscriptionManager) return
    try {
      await this.subscriptionManager.onStreamAppend(path)
    } catch (err) {
      serverLog.error(`[server] subscription append hook failed:`, err)
    }
  }

  /**
   * Handle DELETE - delete stream
   */
  private async handleDelete(path: string, res: ServerResponse): Promise<void> {
    // Check for soft-deleted streams before attempting delete
    const existing = await this.store.get(path)
    if (existing?.softDeleted) {
      res.writeHead(410, { "content-type": `text/plain` })
      res.end(`Stream is gone`)
      return
    }

    const deleted = await this.store.delete(path)
    if (!deleted) {
      res.writeHead(404, { "content-type": `text/plain` })
      res.end(`Stream not found`)
      return
    }

    // Call lifecycle hook
    if (this.options.onStreamDeleted) {
      await Promise.resolve(
        this.options.onStreamDeleted({
          type: `deleted`,
          path,
          timestamp: Date.now(),
        })
      )
    }

    if (this.subscriptionManager) {
      this.subscriptionManager.onStreamDeleted(path)
    }

    res.writeHead(204)
    res.end()
  }

  /**
   * Handle test control endpoints for error injection.
   * POST /_test/inject-error - inject an error
   * DELETE /_test/inject-error - clear all injected errors
   */
  private async handleTestInjectError(
    method: string | undefined,
    req: IncomingMessage,
    res: ServerResponse
  ): Promise<void> {
    if (method === `POST`) {
      const body = await this.readBody(req)
      try {
        const config = JSON.parse(new TextDecoder().decode(body)) as {
          path: string
          // Legacy fields (still supported)
          status?: number
          count?: number
          retryAfter?: number
          // New fault injection fields
          delayMs?: number
          dropConnection?: boolean
          truncateBodyBytes?: number
          probability?: number
          method?: string
          corruptBody?: boolean
          jitterMs?: number
          // SSE event injection (for testing SSE parsing)
          injectSseEvent?: {
            eventType: string
            data: string
          }
        }

        if (!config.path) {
          res.writeHead(400, { "content-type": `text/plain` })
          res.end(`Missing required field: path`)
          return
        }

        // Must have at least one fault type specified
        const hasFaultType =
          config.status !== undefined ||
          config.delayMs !== undefined ||
          config.dropConnection ||
          config.truncateBodyBytes !== undefined ||
          config.corruptBody ||
          config.injectSseEvent !== undefined
        if (!hasFaultType) {
          res.writeHead(400, { "content-type": `text/plain` })
          res.end(
            `Must specify at least one fault type: status, delayMs, dropConnection, truncateBodyBytes, corruptBody, or injectSseEvent`
          )
          return
        }

        this.injectFault(config.path, {
          status: config.status,
          count: config.count ?? 1,
          retryAfter: config.retryAfter,
          delayMs: config.delayMs,
          dropConnection: config.dropConnection,
          truncateBodyBytes: config.truncateBodyBytes,
          probability: config.probability,
          method: config.method,
          corruptBody: config.corruptBody,
          jitterMs: config.jitterMs,
          injectSseEvent: config.injectSseEvent,
        })

        res.writeHead(200, { "content-type": `application/json` })
        res.end(JSON.stringify({ ok: true }))
      } catch {
        res.writeHead(400, { "content-type": `text/plain` })
        res.end(`Invalid JSON body`)
      }
    } else if (method === `DELETE`) {
      this.clearInjectedFaults()
      res.writeHead(200, { "content-type": `application/json` })
      res.end(JSON.stringify({ ok: true }))
    } else {
      res.writeHead(405, { "content-type": `text/plain` })
      res.end(`Method not allowed`)
    }
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  private readBody(req: IncomingMessage): Promise<Uint8Array> {
    return new Promise((resolve, reject) => {
      const chunks: Array<Buffer> = []

      req.on(`data`, (chunk: Buffer) => {
        chunks.push(chunk)
      })

      req.on(`end`, () => {
        const body = Buffer.concat(chunks)
        resolve(new Uint8Array(body))
      })

      req.on(`error`, reject)
    })
  }
}
