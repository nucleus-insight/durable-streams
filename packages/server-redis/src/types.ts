/**
 * Types for the Redis-backed durable streams server.
 */

import type { RedisStreamStore, RedisStreamStoreOptions } from "./redis-store"

/**
 * A single message in a stream.
 */
export interface StreamMessage {
  /**
   * The raw bytes of the message.
   */
  data: Uint8Array

  /**
   * The offset after this message.
   * Format: "<read-seq>_<byte-offset>"
   */
  offset: string

  /**
   * Timestamp when the message was appended.
   */
  timestamp: number
}

/**
 * Stream metadata and data.
 */
export interface Stream {
  /**
   * The stream URL path (key).
   */
  path: string

  /**
   * Content type of the stream.
   */
  contentType?: string

  /**
   * Messages in the stream.
   */
  messages: Array<StreamMessage>

  /**
   * Current offset (next offset to write to).
   */
  currentOffset: string

  /**
   * Last sequence number for writer coordination.
   */
  lastSeq?: string

  /**
   * TTL in seconds.
   */
  ttlSeconds?: number

  /**
   * Absolute expiry time (ISO 8601).
   */
  expiresAt?: string

  /**
   * Timestamp when the stream was created.
   */
  createdAt: number

  /**
   * Timestamp of the last read or write (for TTL renewal).
   * Initialized to createdAt. Updated on GET reads and POST appends.
   * HEAD requests do NOT update this field.
   */
  lastAccessedAt: number

  /**
   * Producer states for idempotent writes.
   * Maps producer ID to their epoch and sequence state.
   */
  producers?: Map<string, ProducerState>

  /**
   * Whether the stream is closed (no further appends permitted).
   * Once set to true, this is permanent and durable.
   */
  closed?: boolean

  /**
   * The producer tuple that closed this stream (for idempotent close).
   * If set, duplicate close requests with this tuple return 204.
   */
  closedBy?: {
    producerId: string
    epoch: number
    seq: number
  }

  /**
   * Source stream path (set when this stream is a fork).
   */
  forkedFrom?: string

  /**
   * Divergence offset from the source stream.
   * Format: "0000000000000000_0000000000000000"
   */
  forkOffset?: string

  /**
   * User-supplied sub-offset value refining `forkOffset` (Section 4.2 of
   * PROTOCOL.md). Stored verbatim for idempotent re-creation matching:
   * bytes for non-JSON forks, flattened message count for JSON forks.
   * `undefined` and `0` are equivalent.
   */
  forkSubOffset?: number

  /**
   * Number of forks referencing this stream.
   * Defaults to 0.
   */
  refCount: number

  /**
   * Whether this stream is logically deleted but retained for fork readers.
   */
  softDeleted?: boolean
}

/**
 * Event data for stream lifecycle hooks.
 */
export interface StreamLifecycleEvent {
  /**
   * Type of event.
   */
  type: `created` | `deleted`

  /**
   * Stream path.
   */
  path: string

  /**
   * Content type (only for 'created' events).
   */
  contentType?: string

  /**
   * Timestamp of the event.
   */
  timestamp: number
}

/**
 * Hook function called when a stream is created or deleted.
 */
export type StreamLifecycleHook = (
  event: StreamLifecycleEvent
) => void | Promise<void>

/**
 * Options for creating the server.
 */
export interface RedisServerOptions {
  /**
   * Port to listen on. Default: 0 (auto-assign).
   */
  port?: number

  /**
   * Host to bind to. Default: "127.0.0.1".
   */
  host?: string

  /**
   * Default long-poll timeout in milliseconds.
   * Default: 30000 (30 seconds).
   */
  longPollTimeout?: number

  /**
   * Redis connection and key options.
   * Ignored when `store` is provided.
   */
  redis?: RedisStreamStoreOptions

  /**
   * A pre-built store to share between servers (e.g. several server
   * instances in one process). The server does not close a provided store.
   */
  store?: RedisStreamStore

  /**
   * Hook called when a stream is created.
   */
  onStreamCreated?: StreamLifecycleHook

  /**
   * Hook called when a stream is deleted.
   */
  onStreamDeleted?: StreamLifecycleHook

  /**
   * Enable gzip/deflate compression for responses.
   * Default: true.
   */
  compression?: boolean

  /**
   * Interval in seconds for cursor calculation.
   * Used for CDN cache collapsing to prevent infinite cache loops.
   * Default: 20 seconds.
   */
  cursorIntervalSeconds?: number

  /**
   * Epoch timestamp for cursor interval calculation.
   * Default: October 9, 2024 00:00:00 UTC.
   */
  cursorEpoch?: Date

  /**
   * Enable webhook subscriptions.
   * Pull-wake subscription routes are always mounted, but type=webhook creates
   * are rejected unless this is true.
   * Default: false.
   */
  webhooks?: boolean
}

/**
 * Producer state for idempotent writes.
 * Tracks epoch and sequence number per producer ID for deduplication.
 */
export interface ProducerState {
  /**
   * Current epoch for this producer.
   * Client-declared, server-validated monotonically increasing.
   */
  epoch: number

  /**
   * Last sequence number received in this epoch.
   */
  lastSeq: number

  /**
   * Timestamp when this producer state was last updated.
   * Used for TTL-based cleanup.
   */
  lastUpdated: number
}

/**
 * Result of producer validation for append operations.
 * For 'accepted' status, includes proposedState to commit after successful append.
 */
export type ProducerValidationResult =
  | {
      status: `accepted`
      isNew: boolean
      /** State to commit after successful append (deferred mutation) */
      proposedState: ProducerState
      producerId: string
    }
  | { status: `duplicate`; lastSeq: number }
  | { status: `stale_epoch`; currentEpoch: number }
  | { status: `invalid_epoch_seq` }
  | { status: `sequence_gap`; expectedSeq: number; receivedSeq: number }
  | { status: `stream_closed` }
