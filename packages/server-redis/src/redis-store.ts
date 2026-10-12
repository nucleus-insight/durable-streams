/**
 * Redis-backed stream storage.
 *
 * Metadata lives in a hash, message bytes in a Redis Stream whose entry ids
 * encode the protocol offset, and every mutation runs as a Lua script so
 * any number of server instances can share one Redis. Readers waiting for new
 * data are woken through a single pub/sub channel.
 */

import { createHash } from "node:crypto"
import { RESP_TYPES, createClient } from "redis"
import { serverLog } from "./log"
import { normalizeContentType, processJsonAppend } from "./json"
import {
  APPEND_SCRIPT,
  CLOSE_SCRIPT,
  CREATE_SCRIPT,
  DELETE_SCRIPT,
  SOFT_DELETE_SCRIPT,
  TOUCH_SCRIPT,
} from "./scripts"
import type { RedisClientType } from "redis"
import type {
  ProducerState,
  ProducerValidationResult,
  Stream,
  StreamMessage,
} from "./types"

export const ZERO_OFFSET = `0000000000000000_0000000000000000`
export const ZERO_ID = `0-0`

/**
 * How entry ids (and therefore offsets) are minted.
 * - `bytes`: id is the byte offset after the message (`<byte>-0`); offsets are
 *   byte-for-byte identical to the reference server (`0000000000000000_<byte>`)
 * - `time`: Redis-style `<milliseconds>-<sequence>` ids minted in Lua so they
 *   stay monotonic per stream even across clock steps and forks; offsets are
 *   `<milliseconds>_<sequence>`, each zero-padded to 16 digits
 * A prefix must only ever be used with one scheme.
 */
export type OffsetScheme = `bytes` | `time`

const DEFAULT_KEY_PREFIX = `ds:`
const DEFAULT_READ_LIMIT = 1000
const DEFAULT_COMMAND_TIMEOUT_MS = 5_000
const DEFAULT_CONNECT_TIMEOUT_MS = 5_000
const DEFAULT_CLIENT_NAME = `durable-streams`
const MAX_RECONNECT_DELAY_MS = 1_000

type RedisClient = RedisClientType

export interface RedisStreamStoreOptions {
  /**
   * Redis connection URL. Default: `process.env.REDIS_URL` or
   * `redis://127.0.0.1:6379`. Ignored when `client` is provided.
   */
  url?: string

  /**
   * An existing node-redis client. The store never closes a provided client,
   * but it does open (and close) one duplicate connection for pub/sub.
   */
  client?: RedisClient

  /**
   * Prefix for every key and the notification channel. Default: `ds:`.
   */
  keyPrefix?: string

  /**
   * Maximum number of messages returned by one read. Reads that hit the
   * limit report `upToDate: false` so clients page through the rest.
   * Default: 1000.
   */
  readLimit?: number

  /**
   * Offset scheme. Default: `bytes`.
   */
  offsetScheme?: OffsetScheme

  /**
   * Deadline for every non-blocking Redis command, including time spent
   * queued while the socket reconnects. Default: 5000 ms.
   */
  commandTimeoutMs?: number

  /**
   * Socket connect timeout for clients the store creates. Default: 5000 ms.
   */
  connectTimeoutMs?: number

  /**
   * `CLIENT SETNAME` value for clients the store creates, visible in
   * `CLIENT LIST`. Default: `durable-streams`.
   */
  clientName?: string
}

export interface AppendOptions {
  seq?: string
  contentType?: string
  producerId?: string
  producerEpoch?: number
  producerSeq?: number
  close?: boolean
}

export interface AppendResult {
  message: StreamMessage | null
  producerResult?: ProducerValidationResult
  streamClosed?: boolean
}

export interface CreateOptions {
  contentType?: string
  ttlSeconds?: number
  expiresAt?: string
  initialData?: Uint8Array
  closed?: boolean
  forkedFrom?: string
  forkOffset?: string
  forkSubOffset?: number
}

export interface ReadResult {
  messages: Array<StreamMessage>
  upToDate: boolean
}

export interface WaitResult {
  messages: Array<StreamMessage>
  timedOut: boolean
  streamClosed?: boolean
}

/**
 * Raw metadata hash as stored in Redis (every field is a string).
 */
interface MetaHash {
  path: string
  contentType?: string
  currentId: string
  lastSeq?: string
  ttlSeconds?: string
  expiresAt?: string
  expiresAtMs?: string
  createdAt: string
  lastAccessedAt: string
  closed?: string
  closedBy?: string
  forkedFrom?: string
  forkOffset?: string
  forkSubOffset?: string
  refCount?: string
  softDeleted?: string
}

/**
 * One notification's shared work: the metadata load issued after the
 * notification, and one read per distinct offset. Every waiter woken by the
 * same notification shares these instead of issuing its own commands.
 */
interface NotifyBatch {
  meta: Promise<MetaHash | undefined>
  reads: Map<string, Promise<ReadResult>>
}

interface Waiter {
  path: string
  wake: (batch?: NotifyBatch) => void
}

function pad16(value: string): string {
  return value.padStart(16, `0`)
}

/**
 * Convert a Redis entry id (`a-b`) to a protocol offset under the scheme.
 */
export function offsetFromId(scheme: OffsetScheme, id: string): string {
  const [first = `0`, second = `0`] = id.split(`-`)
  if (scheme === `bytes`) {
    return `${pad16(`0`)}_${pad16(first)}`
  }
  return `${pad16(first)}_${pad16(second)}`
}

/**
 * Convert a protocol offset to the Redis entry id it names under the scheme.
 * `undefined` and `-1` name the position before the first entry.
 */
export function idFromOffset(
  scheme: OffsetScheme,
  offset: string | undefined
): string {
  if (!offset || offset === `-1`) return ZERO_ID
  const [first = `0`, second = `0`] = offset.split(`_`)
  if (scheme === `bytes`) {
    return `${Number(second)}-0`
  }
  return `${Number(first)}-${Number(second)}`
}

function normalizeOffset(offset: string | undefined): string {
  if (!offset || offset === `-1`) return ZERO_OFFSET
  return offset
}

function isNoScriptError(err: unknown): boolean {
  return err instanceof Error && err.message.startsWith(`NOSCRIPT`)
}

function sha1(source: string): string {
  return createHash(`sha1`).update(source).digest(`hex`)
}

function binaryView(client: RedisClient) {
  return client.withTypeMapping({ [RESP_TYPES.BLOB_STRING]: Buffer })
}

export class RedisStreamStore {
  private readonly client: RedisClient
  private readonly binaryClient: ReturnType<typeof binaryView>
  private readonly ownsClient: boolean
  private subscriber: RedisClient | null = null
  private readonly prefix: string
  private readonly channel: string
  private readonly readLimit: number
  private readonly scheme: OffsetScheme
  private readonly scriptShas = new Map<string, string>()
  private readonly waiters = new Set<Waiter>()
  private readonly commandTimeoutMs: number
  private readonly clientName: string
  private connected = false

  constructor(options: RedisStreamStoreOptions = {}) {
    this.commandTimeoutMs =
      options.commandTimeoutMs ?? DEFAULT_COMMAND_TIMEOUT_MS
    this.clientName = options.clientName ?? DEFAULT_CLIENT_NAME
    if (options.client) {
      this.client = options.client
      this.ownsClient = false
    } else {
      this.client = createClient({
        url: options.url ?? process.env.REDIS_URL ?? `redis://127.0.0.1:6379`,
        name: this.clientName,
        socket: {
          connectTimeout:
            options.connectTimeoutMs ?? DEFAULT_CONNECT_TIMEOUT_MS,
          reconnectStrategy: (retries: number) =>
            Math.min(retries * 100, MAX_RECONNECT_DELAY_MS),
        },
      })
      this.ownsClient = true
    }
    this.client.on(`error`, (err: Error) => {
      serverLog.error(`[RedisStreamStore] client error:`, err)
    })
    this.binaryClient = binaryView(this.client)
    this.prefix = options.keyPrefix ?? DEFAULT_KEY_PREFIX
    this.channel = `${this.prefix}notify`
    this.readLimit = options.readLimit ?? DEFAULT_READ_LIMIT
    this.scheme = options.offsetScheme ?? `bytes`
  }

  /**
   * The shared client with the per-command deadline applied. The deadline
   * covers time spent in the offline queue, so an outage fails requests
   * instead of parking them.
   */
  private get cmd(): RedisClient {
    return this.client.withAbortSignal(
      AbortSignal.timeout(this.commandTimeoutMs)
    )
  }

  private get binaryCmd(): ReturnType<typeof binaryView> {
    return this.binaryClient.withAbortSignal(
      AbortSignal.timeout(this.commandTimeoutMs)
    )
  }

  get offsetScheme(): OffsetScheme {
    return this.scheme
  }

  private offsetOf(id: string): string {
    return offsetFromId(this.scheme, id)
  }

  private idOf(offset: string | undefined): string {
    return idFromOffset(this.scheme, offset)
  }

  /**
   * Open the Redis connections (idempotent).
   */
  async connect(): Promise<void> {
    if (this.connected) return
    if (!this.client.isOpen) {
      await this.client.connect()
    }
    const subscriber = this.client.duplicate({
      name: `${this.clientName}-notify`,
    })
    subscriber.on(`error`, (err: Error) => {
      serverLog.error(`[RedisStreamStore] subscriber error:`, err)
    })
    subscriber.on(`ready`, () => {
      // Notifications published while the subscriber was disconnected are
      // gone; every waiter re-reads so none sleeps through a missed append.
      if (this.connected) this.wakeAllWaiters()
    })
    await subscriber.connect()
    await subscriber.subscribe(this.channel, (path: string) => {
      this.notifyWaiters(path)
    })
    this.subscriber = subscriber
    this.connected = true
  }

  /**
   * Wake every pending wait and close the connections this store opened.
   */
  async close(): Promise<void> {
    this.cancelAllWaits()
    if (this.subscriber) {
      const subscriber = this.subscriber
      this.subscriber = null
      try {
        await subscriber.unsubscribe(this.channel)
      } catch {
        // The connection may already be gone; closing below is what matters.
      }
      await subscriber.close()
    }
    if (this.ownsClient && this.client.isOpen) {
      await this.client.close()
    }
    this.connected = false
  }

  // ===========================================================================
  // Keys and scripts
  // ===========================================================================

  private metaKey(path: string): string {
    return `${this.prefix}meta:${path}`
  }

  private logKey(path: string): string {
    return `${this.prefix}log:${path}`
  }

  private producersKey(path: string): string {
    return `${this.prefix}producers:${path}`
  }

  private streamKeys(path: string): [string, string, string] {
    return [this.metaKey(path), this.logKey(path), this.producersKey(path)]
  }

  private async runScript(
    source: string,
    keys: Array<string>,
    args: Array<string | Buffer>
  ): Promise<unknown> {
    let sha = this.scriptShas.get(source)
    if (!sha) {
      sha = sha1(source)
      this.scriptShas.set(source, sha)
    }
    const tail = [String(keys.length), ...keys, ...args]
    try {
      return await this.cmd.sendCommand([`EVALSHA`, sha, ...tail])
    } catch (err) {
      if (!isNoScriptError(err)) throw err
      return await this.cmd.sendCommand([`EVAL`, source, ...tail])
    }
  }

  // ===========================================================================
  // Metadata
  // ===========================================================================

  private async loadMeta(path: string): Promise<MetaHash | undefined> {
    const hash = await this.cmd.hGetAll(this.metaKey(path))
    if (!hash.currentId) return undefined
    return hash as unknown as MetaHash
  }

  private isExpired(meta: MetaHash): boolean {
    const now = Date.now()

    if (meta.expiresAt) {
      const expiryTime = new Date(meta.expiresAt).getTime()
      if (!Number.isFinite(expiryTime) || now >= expiryTime) {
        return true
      }
    }

    if (meta.ttlSeconds !== undefined) {
      const expiryTime =
        Number(meta.lastAccessedAt) + Number(meta.ttlSeconds) * 1000
      if (now >= expiryTime) {
        return true
      }
    }

    return false
  }

  /**
   * Load metadata, applying expiry: expired streams with forks are
   * soft-deleted and returned; expired streams without forks are deleted.
   */
  private async getMetaIfNotExpired(
    path: string,
    loaded?: Promise<MetaHash | undefined>
  ): Promise<MetaHash | undefined> {
    const meta = await (loaded ?? this.loadMeta(path))
    if (!meta) return undefined
    if (!this.isExpired(meta)) return meta

    if (Number(meta.refCount ?? 0) > 0) {
      if (meta.softDeleted !== `1`) {
        await this.runScript(SOFT_DELETE_SCRIPT, [this.metaKey(path)], [])
        meta.softDeleted = `1`
      }
      return meta
    }

    await this.delete(path)
    return undefined
  }

  private metaToStream(meta: MetaHash): Stream {
    const stream: Stream = {
      path: meta.path,
      contentType: meta.contentType,
      messages: [],
      currentOffset: this.offsetOf(meta.currentId),
      lastSeq: meta.lastSeq,
      ttlSeconds:
        meta.ttlSeconds !== undefined ? Number(meta.ttlSeconds) : undefined,
      expiresAt: meta.expiresAt,
      createdAt: Number(meta.createdAt),
      lastAccessedAt: Number(meta.lastAccessedAt),
      closed: meta.closed === `1`,
      closedBy: meta.closedBy
        ? (JSON.parse(meta.closedBy) as Stream[`closedBy`])
        : undefined,
      forkedFrom: meta.forkedFrom,
      forkOffset: meta.forkOffset,
      forkSubOffset:
        meta.forkSubOffset !== undefined
          ? Number(meta.forkSubOffset)
          : undefined,
      refCount: Number(meta.refCount ?? 0),
      softDeleted: meta.softDeleted === `1`,
    }
    return stream
  }

  // ===========================================================================
  // Lifecycle
  // ===========================================================================

  async get(path: string): Promise<Stream | undefined> {
    const meta = await this.getMetaIfNotExpired(path)
    return meta ? this.metaToStream(meta) : undefined
  }

  async has(path: string): Promise<boolean> {
    const meta = await this.getMetaIfNotExpired(path)
    return meta !== undefined && meta.softDeleted !== `1`
  }

  async getCurrentOffset(path: string): Promise<string | undefined> {
    const meta = await this.getMetaIfNotExpired(path)
    return meta ? this.offsetOf(meta.currentId) : undefined
  }

  async getProducerEpoch(
    path: string,
    producerId: string
  ): Promise<number | undefined> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) return undefined
    const raw = await this.cmd.hGet(this.producersKey(path), producerId)
    if (!raw) return undefined
    return (JSON.parse(raw) as ProducerState).epoch
  }

  /**
   * Refresh the sliding TTL. Called on reads and appends (not HEAD).
   */
  async touchAccess(path: string): Promise<void> {
    await this.runScript(TOUCH_SCRIPT, this.streamKeys(path), [
      String(Date.now()),
    ])
  }

  /**
   * Create a stream (idempotent when the configuration matches).
   * @throws Error on configuration conflict, missing or soft-deleted source,
   *         invalid fork offset or sub-offset, or invalid initial JSON
   */
  async create(path: string, options: CreateOptions = {}): Promise<Stream> {
    const existing = await this.loadMeta(path)
    if (existing) {
      if (this.isExpired(existing)) {
        await this.delete(path)
      } else if (existing.softDeleted === `1`) {
        throw new Error(
          `Stream has active forks — path cannot be reused until all forks are removed: ${path}`
        )
      } else {
        return this.matchExisting(path, existing, options)
      }
    }

    const isFork = !!options.forkedFrom
    let forkOffset = ZERO_OFFSET
    let contentType = options.contentType
    let sourceMeta: MetaHash | undefined
    let prefixData: Uint8Array = new Uint8Array(0)
    let ttlSeconds = options.ttlSeconds
    let expiresAt = options.expiresAt

    if (isFork) {
      sourceMeta = await this.loadMeta(options.forkedFrom!)
      if (!sourceMeta || this.isExpired(sourceMeta)) {
        throw new Error(`Source stream not found: ${options.forkedFrom}`)
      }
      if (sourceMeta.softDeleted === `1`) {
        throw new Error(`Source stream is soft-deleted: ${options.forkedFrom}`)
      }
      if (
        options.contentType &&
        options.contentType.trim() !== `` &&
        normalizeContentType(options.contentType) !==
          normalizeContentType(sourceMeta.contentType)
      ) {
        throw new Error(`Content type mismatch with source stream`)
      }
      if (!contentType || contentType.trim() === ``) {
        contentType = sourceMeta.contentType
      }

      const sourceOffset = this.offsetOf(sourceMeta.currentId)
      forkOffset = options.forkOffset
        ? normalizeOffset(options.forkOffset)
        : sourceOffset
      if (forkOffset < ZERO_OFFSET || forkOffset > sourceOffset) {
        throw new Error(`Invalid fork offset: ${options.forkOffset}`)
      }

      if (options.forkSubOffset && options.forkSubOffset > 0) {
        prefixData = await this.resolveForkSubOffset(
          options.forkedFrom!,
          forkOffset,
          options.forkSubOffset,
          normalizeContentType(sourceMeta.contentType) === `application/json`
        )
      }

      const resolved = this.resolveForkExpiry(options, sourceMeta)
      ttlSeconds = resolved.ttlSeconds
      expiresAt = resolved.expiresAt
    }

    let initialData: Uint8Array = new Uint8Array(0)
    if (options.initialData && options.initialData.length > 0) {
      initialData =
        normalizeContentType(contentType) === `application/json`
          ? processJsonAppend(options.initialData, true)
          : options.initialData
    }

    const expiresAtMs = expiresAt ? new Date(expiresAt).getTime() : undefined
    const sourceKeys = isFork
      ? this.streamKeys(options.forkedFrom!)
      : [``, ``, ``]
    const reply = (await this.runScript(
      CREATE_SCRIPT,
      [...this.streamKeys(path), ...sourceKeys],
      [
        String(Date.now()),
        path,
        contentType ?? ``,
        ttlSeconds !== undefined ? String(ttlSeconds) : ``,
        expiresAt ?? ``,
        expiresAtMs !== undefined && Number.isFinite(expiresAtMs)
          ? String(expiresAtMs)
          : ``,
        options.closed ? `1` : `0`,
        options.forkedFrom ?? ``,
        isFork ? this.idOf(forkOffset) : ZERO_ID,
        isFork ? forkOffset : ``,
        prefixData.length > 0 && options.forkSubOffset !== undefined
          ? String(options.forkSubOffset)
          : ``,
        Buffer.from(prefixData),
        Buffer.from(initialData),
        this.channel,
        this.scheme,
      ]
    )) as Array<string | number>

    switch (reply[0]) {
      case `ok`:
        break
      case `exists`: {
        const raced = await this.loadMeta(path)
        if (raced) return this.matchExisting(path, raced, options)
        throw new Error(
          `Stream already exists with different configuration: ${path}`
        )
      }
      case `source_not_found`:
        throw new Error(`Source stream not found: ${options.forkedFrom}`)
      case `source_soft_deleted`:
        throw new Error(`Source stream is soft-deleted: ${options.forkedFrom}`)
      case `invalid_fork_offset`:
        throw new Error(`Invalid fork offset: ${options.forkOffset}`)
      default:
        throw new Error(`Unexpected create result: ${String(reply[0])}`)
    }

    const created = await this.loadMeta(path)
    if (!created) {
      // A zero or past expiry removed the stream immediately after creation.
      return this.metaToStream({
        path,
        contentType,
        currentId: String(reply[1]),
        ttlSeconds: ttlSeconds !== undefined ? String(ttlSeconds) : undefined,
        expiresAt,
        createdAt: String(Date.now()),
        lastAccessedAt: String(Date.now()),
        closed: options.closed ? `1` : undefined,
        forkedFrom: options.forkedFrom,
        forkOffset: isFork ? forkOffset : undefined,
        refCount: `0`,
      })
    }
    return this.metaToStream(created)
  }

  private matchExisting(
    path: string,
    existing: MetaHash,
    options: CreateOptions
  ): Stream {
    const contentTypeMatches =
      (normalizeContentType(options.contentType) ||
        `application/octet-stream`) ===
      (normalizeContentType(existing.contentType) || `application/octet-stream`)
    const ttlMatches =
      options.ttlSeconds ===
      (existing.ttlSeconds !== undefined
        ? Number(existing.ttlSeconds)
        : undefined)
    const expiresMatches = options.expiresAt === existing.expiresAt
    const closedMatches =
      (options.closed ?? false) === (existing.closed === `1`)
    const forkedFromMatches =
      (options.forkedFrom ?? undefined) === existing.forkedFrom
    const forkOffsetMatches =
      options.forkOffset === undefined ||
      options.forkOffset === existing.forkOffset
    const forkSubOffsetMatches =
      (options.forkSubOffset ?? 0) === Number(existing.forkSubOffset ?? 0)

    if (
      contentTypeMatches &&
      ttlMatches &&
      expiresMatches &&
      closedMatches &&
      forkedFromMatches &&
      forkOffsetMatches &&
      forkSubOffsetMatches
    ) {
      return this.metaToStream(existing)
    }
    throw new Error(
      `Stream already exists with different configuration: ${path}`
    )
  }

  private resolveForkExpiry(
    opts: { ttlSeconds?: number; expiresAt?: string },
    sourceMeta: MetaHash
  ): { ttlSeconds?: number; expiresAt?: string } {
    if (opts.ttlSeconds !== undefined) {
      return { ttlSeconds: opts.ttlSeconds }
    }
    if (opts.expiresAt) {
      return { expiresAt: opts.expiresAt }
    }
    if (sourceMeta.ttlSeconds !== undefined) {
      return { ttlSeconds: Number(sourceMeta.ttlSeconds) }
    }
    if (sourceMeta.expiresAt) {
      return { expiresAt: sourceMeta.expiresAt }
    }
    return {}
  }

  /**
   * Resolve a fork sub-offset against the source: take a prefix of the first
   * source message past the fork offset (message count for JSON, bytes
   * otherwise) to materialize as the fork's first own message.
   */
  private async resolveForkSubOffset(
    sourcePath: string,
    forkOffset: string,
    subOffset: number,
    isJSON: boolean
  ): Promise<Uint8Array> {
    const sourceMeta = await this.loadMeta(sourcePath)
    if (!sourceMeta) {
      throw new Error(`Source stream not found: ${sourcePath}`)
    }
    const messages = await this.readForkedMessages(
      sourcePath,
      forkOffset,
      this.offsetOf(sourceMeta.currentId),
      1
    )
    if (messages.length === 0) {
      throw new Error(`Invalid fork sub-offset: no data past forkOffset`)
    }
    const first = messages[0]!
    if (isJSON) {
      const text = new TextDecoder().decode(first.data)
      const trimmed = text.endsWith(`,`) ? text.slice(0, -1) : text
      let values: Array<unknown>
      try {
        values = JSON.parse(`[${trimmed}]`)
      } catch {
        throw new Error(`Invalid fork sub-offset: source JSON is unparseable`)
      }
      if (subOffset > values.length) {
        throw new Error(
          `Invalid fork sub-offset: overshoots source message count`
        )
      }
      const prefix = values.slice(0, subOffset).map((v) => JSON.stringify(v))
      return new TextEncoder().encode(prefix.join(`,`) + `,`)
    }
    if (subOffset > first.data.length) {
      throw new Error(
        `Invalid fork sub-offset: overshoots source message length`
      )
    }
    return first.data.slice(0, subOffset)
  }

  /**
   * Delete a stream. Streams with forks are soft-deleted instead.
   * Returns true if the stream was found.
   */
  async delete(path: string): Promise<boolean> {
    const reply = (await this.runScript(
      DELETE_SCRIPT,
      [],
      [this.prefix, path, this.channel]
    )) as Array<string>
    return reply[0] === `ok`
  }

  // ===========================================================================
  // Writes
  // ===========================================================================

  /**
   * Append data to a stream.
   * @throws Error if the stream is missing, soft-deleted, has a different
   *         content type, rejects the Stream-Seq, or the JSON is invalid
   */
  async append(
    path: string,
    data: Uint8Array,
    options: AppendOptions = {}
  ): Promise<StreamMessage | AppendResult | null> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) {
      throw new Error(`Stream not found: ${path}`)
    }
    if (meta.softDeleted === `1`) {
      throw new Error(`Stream is soft-deleted: ${path}`)
    }

    if (options.contentType && meta.contentType) {
      const providedType = normalizeContentType(options.contentType)
      const streamType = normalizeContentType(meta.contentType)
      if (providedType !== streamType) {
        throw new Error(
          `Content-type mismatch: expected ${meta.contentType}, got ${options.contentType}`
        )
      }
    }

    let processedData = data
    if (normalizeContentType(meta.contentType) === `application/json`) {
      processedData = processJsonAppend(data, false)
    }

    const hasProducer =
      options.producerId !== undefined &&
      options.producerEpoch !== undefined &&
      options.producerSeq !== undefined
    const now = Date.now()

    const reply = (await this.runScript(APPEND_SCRIPT, this.streamKeys(path), [
      String(now),
      Buffer.from(processedData),
      options.seq ?? ``,
      hasProducer ? options.producerId! : ``,
      hasProducer ? String(options.producerEpoch) : ``,
      hasProducer ? String(options.producerSeq) : ``,
      options.close ? `1` : `0`,
      this.channel,
      path,
      this.scheme,
    ])) as Array<string | number>

    switch (reply[0]) {
      case `ok`: {
        const message: StreamMessage = {
          data: processedData,
          offset: this.offsetOf(String(reply[1])),
          timestamp: now,
        }
        let producerResult: ProducerValidationResult | undefined
        if (hasProducer) {
          producerResult = {
            status: `accepted`,
            isNew: reply[2] === 1,
            producerId: options.producerId!,
            proposedState: {
              epoch: options.producerEpoch!,
              lastSeq: options.producerSeq!,
              lastUpdated: now,
            },
          }
        }
        if (producerResult || options.close) {
          return { message, producerResult, streamClosed: options.close }
        }
        return message
      }
      case `not_found`:
        throw new Error(`Stream not found: ${path}`)
      case `soft_deleted`:
        throw new Error(`Stream is soft-deleted: ${path}`)
      case `closed`:
        return { message: null, streamClosed: true }
      case `closed_duplicate`:
        return {
          message: null,
          streamClosed: true,
          producerResult: { status: `duplicate`, lastSeq: Number(reply[2]) },
        }
      case `seq_conflict`:
        throw new Error(
          `Sequence conflict: ${options.seq} <= ${String(reply[1])}`
        )
      default:
        return { message: null, producerResult: this.producerError(reply) }
    }
  }

  private producerError(
    reply: Array<string | number>
  ): ProducerValidationResult {
    switch (reply[0]) {
      case `duplicate`:
        return { status: `duplicate`, lastSeq: Number(reply[1]) }
      case `stale_epoch`:
        return { status: `stale_epoch`, currentEpoch: Number(reply[1]) }
      case `invalid_epoch_seq`:
        return { status: `invalid_epoch_seq` }
      case `sequence_gap`:
        return {
          status: `sequence_gap`,
          expectedSeq: Number(reply[1]),
          receivedSeq: Number(reply[2]),
        }
      case `stream_closed`:
        return { status: `stream_closed` }
      default:
        throw new Error(`Unexpected script result: ${String(reply[0])}`)
    }
  }

  /**
   * Append with producer headers. Validation and append are already atomic
   * inside the Lua script, so this only normalizes the result shape.
   */
  async appendWithProducer(
    path: string,
    data: Uint8Array,
    options: AppendOptions
  ): Promise<AppendResult> {
    const result = await this.append(path, data, options)
    if (result && `message` in result) {
      return result
    }
    return { message: result }
  }

  /**
   * Close a stream without appending data.
   * @returns The final offset, or null if the stream does not exist
   */
  async closeStream(
    path: string
  ): Promise<{ finalOffset: string; alreadyClosed: boolean } | null> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) return null
    if (meta.softDeleted === `1`) {
      throw new Error(`Stream is soft-deleted: ${path}`)
    }

    const reply = (await this.runScript(CLOSE_SCRIPT, this.streamKeys(path), [
      String(Date.now()),
      ``,
      ``,
      ``,
      this.channel,
      path,
    ])) as Array<string | number>

    switch (reply[0]) {
      case `ok`:
        return {
          finalOffset: this.offsetOf(String(reply[1])),
          alreadyClosed: false,
        }
      case `already_closed`:
        return {
          finalOffset: this.offsetOf(String(reply[1])),
          alreadyClosed: true,
        }
      case `not_found`:
        return null
      case `soft_deleted`:
        throw new Error(`Stream is soft-deleted: ${path}`)
      default:
        throw new Error(`Unexpected close result: ${String(reply[0])}`)
    }
  }

  /**
   * Close a stream with producer headers (idempotent, sequenced).
   */
  async closeStreamWithProducer(
    path: string,
    options: { producerId: string; producerEpoch: number; producerSeq: number }
  ): Promise<{
    finalOffset: string
    alreadyClosed: boolean
    producerResult?: ProducerValidationResult
  } | null> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) return null
    if (meta.softDeleted === `1`) {
      throw new Error(`Stream is soft-deleted: ${path}`)
    }

    const reply = (await this.runScript(CLOSE_SCRIPT, this.streamKeys(path), [
      String(Date.now()),
      options.producerId,
      String(options.producerEpoch),
      String(options.producerSeq),
      this.channel,
      path,
    ])) as Array<string | number>

    switch (reply[0]) {
      case `ok`:
        return {
          finalOffset: this.offsetOf(String(reply[1])),
          alreadyClosed: false,
          producerResult: {
            status: `accepted`,
            isNew: false,
            producerId: options.producerId,
            proposedState: {
              epoch: options.producerEpoch,
              lastSeq: options.producerSeq,
              lastUpdated: Date.now(),
            },
          },
        }
      case `closed_duplicate`:
        return {
          finalOffset: this.offsetOf(String(reply[1])),
          alreadyClosed: true,
          producerResult: { status: `duplicate`, lastSeq: Number(reply[2]) },
        }
      case `stream_closed`:
        return {
          finalOffset: this.offsetOf(String(reply[1])),
          alreadyClosed: true,
          producerResult: { status: `stream_closed` },
        }
      case `not_found`:
        return null
      case `soft_deleted`:
        throw new Error(`Stream is soft-deleted: ${path}`)
      default:
        return {
          finalOffset: this.offsetOf(meta.currentId),
          alreadyClosed: false,
          producerResult: this.producerError(reply),
        }
    }
  }

  // ===========================================================================
  // Reads
  // ===========================================================================

  private async readLog(
    path: string,
    startOffset: string,
    capOffset: string | undefined,
    limit: number
  ): Promise<Array<StreamMessage>> {
    const end = capOffset === undefined ? `+` : this.idOf(capOffset)
    const rows = await this.binaryCmd.xRange(
      this.logKey(path),
      `(${this.idOf(startOffset)}`,
      end,
      { COUNT: limit }
    )
    return (rows ?? []).map((row) => {
      const id = row.id.toString()
      const data = row.message.d
      const timestamp = row.message.t
      return {
        data: new Uint8Array(data ?? Buffer.alloc(0)),
        offset: this.offsetOf(id),
        timestamp: timestamp ? Number(timestamp.toString()) : 0,
      }
    })
  }

  /**
   * Read messages inherited through a fork's source chain, capped at the
   * fork offset. Soft-deleted sources are read through on purpose.
   */
  private async readForkedMessages(
    sourcePath: string,
    startOffset: string,
    capOffset: string,
    limit: number
  ): Promise<Array<StreamMessage>> {
    const sourceMeta = await this.loadMeta(sourcePath)
    if (!sourceMeta) return []

    const messages: Array<StreamMessage> = []

    if (sourceMeta.forkedFrom && sourceMeta.forkOffset) {
      const sourceForkOffset = sourceMeta.forkOffset
      if (startOffset < sourceForkOffset) {
        const inherited = await this.readForkedMessages(
          sourceMeta.forkedFrom,
          startOffset,
          sourceForkOffset < capOffset ? sourceForkOffset : capOffset,
          limit
        )
        messages.push(...inherited)
        if (messages.length >= limit) return messages.slice(0, limit)
      }
    }

    const own = await this.readLog(
      sourcePath,
      startOffset,
      capOffset,
      limit - messages.length
    )
    messages.push(...own)
    return messages
  }

  /**
   * Read messages after the given offset.
   * @throws Error if the stream does not exist
   */
  async read(path: string, offset?: string): Promise<ReadResult> {
    const meta = await this.getMetaIfNotExpired(path)
    if (!meta) {
      throw new Error(`Stream not found: ${path}`)
    }
    return this.readFromMeta(path, meta, offset)
  }

  private async readFromMeta(
    path: string,
    meta: MetaHash,
    offset: string | undefined
  ): Promise<ReadResult> {
    const startOffset = normalizeOffset(offset)
    const currentOffset = this.offsetOf(meta.currentId)

    if (currentOffset === ZERO_OFFSET || startOffset >= currentOffset) {
      return { messages: [], upToDate: true }
    }

    const limit = this.readLimit
    const messages: Array<StreamMessage> = []

    if (meta.forkedFrom && meta.forkOffset) {
      const forkOffset = meta.forkOffset
      if (startOffset < forkOffset) {
        const inherited = await this.readForkedMessages(
          meta.forkedFrom,
          startOffset,
          forkOffset,
          limit
        )
        messages.push(...inherited)
      }
    }

    if (messages.length < limit) {
      const own = await this.readLog(
        path,
        startOffset,
        undefined,
        limit - messages.length
      )
      messages.push(...own)
    }

    return { messages, upToDate: messages.length < limit }
  }

  /**
   * Wait for messages after the offset (long-poll). Resolves as soon as a
   * message is appended or the stream is closed, or on timeout.
   * @throws Error if the stream does not exist
   */
  async waitForMessages(
    path: string,
    offset: string,
    timeoutMs: number
  ): Promise<WaitResult> {
    const state = {
      signalled: false,
      batch: undefined as NotifyBatch | undefined,
      wake: () => {},
    }
    const wasSignalled = (): boolean => state.signalled
    const waiter: Waiter = {
      path,
      wake: (batch) => {
        state.batch = batch
        state.signalled = true
        state.wake()
      },
    }
    // Register before the first read so an append that lands between the
    // read and the wait still wakes this call.
    this.waiters.add(waiter)

    const deadline = Date.now() + timeoutMs
    let first = true
    try {
      // Each pass clears the notified flag before snapshotting metadata, so a
      // notification that lands after the snapshot is kept and forces another
      // pass instead of being lost to a read that trusts stale metadata.
      for (;;) {
        // A batch holds work issued after the notification that woke this
        // waiter, so sharing it cannot hide the append it was woken for.
        const batch = state.batch
        state.batch = undefined
        state.signalled = false
        const meta = await this.getMetaIfNotExpired(path, batch?.meta)
        if (!meta) {
          if (first) throw new Error(`Stream not found: ${path}`)
          return { messages: [], timedOut: false, streamClosed: false }
        }

        if (
          first &&
          meta.forkedFrom &&
          meta.forkOffset &&
          offset < meta.forkOffset
        ) {
          const { messages } = await this.readFromMeta(path, meta, offset)
          return { messages, timedOut: false }
        }
        first = false

        const closed = meta.closed === `1`
        if (closed && offset === this.offsetOf(meta.currentId)) {
          return { messages: [], timedOut: false, streamClosed: true }
        }

        const { messages } = await this.sharedRead(batch, path, meta, offset)
        if (messages.length > 0) {
          return { messages, timedOut: false, streamClosed: closed }
        }
        if (closed) {
          return { messages: [], timedOut: false, streamClosed: true }
        }

        const remaining = deadline - Date.now()
        if (remaining <= 0) {
          return { messages: [], timedOut: true, streamClosed: false }
        }

        if (!wasSignalled()) {
          const outcome = await new Promise<`woken` | `timeout`>((resolve) => {
            const timer = setTimeout(() => resolve(`timeout`), remaining)
            state.wake = () => {
              clearTimeout(timer)
              resolve(`woken`)
            }
          })
          state.wake = () => {}
          if (outcome === `timeout`) {
            const latest = await this.loadMeta(path)
            return {
              messages: [],
              timedOut: true,
              streamClosed: latest?.closed === `1`,
            }
          }
        }

        if (!this.waiters.has(waiter)) {
          return { messages: [], timedOut: true, streamClosed: false }
        }
      }
    } finally {
      this.waiters.delete(waiter)
    }
  }

  private sharedRead(
    batch: NotifyBatch | undefined,
    path: string,
    meta: MetaHash,
    offset: string
  ): Promise<ReadResult> {
    if (!batch) return this.readFromMeta(path, meta, offset)
    let read = batch.reads.get(offset)
    if (!read) {
      read = this.readFromMeta(path, meta, offset)
      batch.reads.set(offset, read)
    }
    return read
  }

  private notifyWaiters(path: string): void {
    let batch: NotifyBatch | undefined
    for (const waiter of this.waiters) {
      if (waiter.path !== path) continue
      batch ??= { meta: this.loadMeta(path), reads: new Map() }
      waiter.wake(batch)
    }
    // A rejected shared load is surfaced by every waiter that awaits it.
    batch?.meta.catch(() => undefined)
  }

  private wakeAllWaiters(): void {
    for (const waiter of this.waiters) waiter.wake()
  }

  /**
   * Wake every pending wait (used during shutdown).
   */
  cancelAllWaits(): void {
    for (const waiter of this.waiters) {
      this.waiters.delete(waiter)
      waiter.wake()
    }
  }

  // ===========================================================================
  // Maintenance
  // ===========================================================================

  /**
   * List every stream path under this store's prefix.
   */
  async list(): Promise<Array<string>> {
    const prefix = this.metaKey(``)
    const paths: Array<string> = []
    for await (const keys of this.cmd.scanIterator({
      MATCH: `${prefix}*`,
      COUNT: 500,
    })) {
      for (const key of keys) {
        paths.push(key.slice(prefix.length))
      }
    }
    return paths
  }

  /**
   * Delete every key under this store's prefix and wake pending waits.
   */
  async clear(): Promise<void> {
    this.cancelAllWaits()
    for await (const keys of this.cmd.scanIterator({
      MATCH: `${this.prefix}*`,
      COUNT: 500,
    })) {
      if (keys.length > 0) {
        await this.cmd.del(keys)
      }
    }
  }
}
