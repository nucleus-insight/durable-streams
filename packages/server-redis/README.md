# @durable-streams/server-redis

Redis-backed Node.js server implementation for the Durable Streams protocol.

This package is a copy of `@durable-streams/server` with the in-memory and
file-backed stores replaced by a single Redis store. It passes the full
`@durable-streams/server-conformance-tests` suite, including forks, idempotent
producers, long-poll, SSE, and subscriptions.

## Why Redis

- Any number of server instances can share one Redis. Every mutation runs as a
  Lua script inside Redis, so offsets, producer sequencing, close, and fork
  reference counts stay consistent without coordination between instances.
- Streams survive server restarts and deploys.
- No local disk, no sidecar: only a Redis (or Valkey) connection.

## Installation

```bash
npm install @durable-streams/server-redis redis
```

Requires Redis 6.2 or newer (exclusive `XRANGE` bounds, `PEXPIREAT`).
Valkey 8 works. Redis Cluster is not supported because fork deletion touches
keys of several streams in one script.

## Quick Start

```typescript
import { DurableStreamRedisServer } from "@durable-streams/server-redis"

const server = new DurableStreamRedisServer({
  port: 4437,
  host: "127.0.0.1",
  redis: { url: "redis://127.0.0.1:6379", keyPrefix: "ds:" },
})

await server.start()
```

Several servers (in one process or many) can share the same Redis and prefix.
A long-poll or SSE reader on one instance is woken by an append on another.

## Options

```typescript
interface RedisServerOptions {
  port?: number // Default: 4437
  host?: string // Default: "127.0.0.1"
  longPollTimeout?: number // Default: 30000 (ms)
  redis?: RedisStreamStoreOptions // Connection and key options
  store?: RedisStreamStore // Share a store between servers; not closed by the server
  onStreamCreated?: StreamLifecycleHook
  onStreamDeleted?: StreamLifecycleHook
  compression?: boolean // Default: true
  cursorIntervalSeconds?: number // Default: 20
  cursorEpoch?: Date // Default: 2024-10-09T00:00:00Z
  webhooks?: boolean // Default: false
}

interface RedisStreamStoreOptions {
  url?: string // Default: process.env.REDIS_URL ?? "redis://127.0.0.1:6379"
  client?: RedisClientType // Use an existing node-redis client (never closed by the store)
  keyPrefix?: string // Default: "ds:"
  readLimit?: number // Max messages per read; default 1000
  offsetScheme?: "bytes" | "time" // Default: "bytes"
}
```

### Offset schemes

Both schemes pass the full conformance suite. A key prefix must only ever be
used with one scheme.

- `bytes` (default): the entry id is the byte offset after the message
  (`<byte>-0`, counting a 4-byte length prefix and a trailing newline), and the
  offset is `0000000000000000_<byte, 16 digits>`. Offsets are identical to the
  reference, Caddy and Rivet servers, so stored client offsets survive a
  switch between those servers and this one.
- `time`: the entry id is a Redis-style `<milliseconds>-<sequence>` pair minted
  in Lua from the appending server's clock, never below the stream's current
  id (so clock steps and forks stay monotonic), and the offset is
  `<milliseconds, 16 digits>_<sequence, 16 digits>`. Offsets carry wall-clock
  time, which can help debugging, but they are not interchangeable with other
  servers.

## Production settings

```typescript
const store = new RedisStreamStore({
  url: process.env.REDIS_URL, // rediss:// for TLS
  keyPrefix: `ds:${process.env.APP_ENV}:`, // one prefix per environment, never shared across schemes
  offsetScheme: "bytes", // keep the default unless offsets never leave this deployment
  commandTimeoutMs: 5000, // every non-blocking command, queued time included
  connectTimeoutMs: 5000,
  clientName: "core-durable-streams", // shows in CLIENT LIST; the notify connection is suffixed -notify
  readLimit: 1000, // messages per response; clients page with Stream-Next-Offset
})
```

- **Connections per process**: one command connection plus one pub/sub
  connection, regardless of how many readers are waiting.
- **Reconnects**: clients the store creates retry with a linear backoff capped
  at one second. When the pub/sub connection comes back, every waiting reader
  re-reads once, so an append published during the outage is delivered on
  reconnect rather than at the long-poll timeout.
- **Timeouts**: a command that cannot complete within `commandTimeoutMs`
  rejects, including commands held in node-redis's offline queue during an
  outage, so requests fail fast instead of piling up.
- **Shared reads**: all readers woken by one notification share a single
  metadata load and a single read per distinct offset.
- **Expiry**: set `Stream-TTL` or `Stream-Expires-At` per stream at creation;
  Redis collects idle streams without any sweeper.

## Using the store without the HTTP server

`RedisStreamStore` is usable on its own, for example to append from a
workflow while another process serves readers:

```typescript
import { RedisStreamStore } from "@durable-streams/server-redis"

const store = new RedisStreamStore({ url: process.env.REDIS_URL })
await store.connect()

await store.create("/v1/stream/run-42", { contentType: "application/json" })
await store.append("/v1/stream/run-42", new TextEncoder().encode(`{"step":1}`))
await store.closeStream("/v1/stream/run-42")

const { messages } = await store.read("/v1/stream/run-42")
await store.close()
```

## Storage layout

All keys share the configured prefix (`ds:` by default):

| Key                   | Type            | Contents                                                                            |
| --------------------- | --------------- | ----------------------------------------------------------------------------------- |
| `ds:meta:{path}`      | hash            | content type, current byte offset, TTL, closed flag, fork metadata, reference count |
| `ds:log:{path}`       | stream          | one entry per message; entry id minted per offset scheme (see above)                |
| `ds:producers:{path}` | hash            | producer id to `{epoch, lastSeq, lastUpdated}`                                      |
| `ds:notify`           | pub/sub channel | path of every stream that changed                                                   |

Because the entry id maps one-to-one to the protocol offset, "messages after
offset X" is a single `XRANGE (id +` call with no scanning.

Expiry (`Stream-TTL` and `Stream-Expires-At`) is applied with `PEXPIRE` and
`PEXPIREAT` on all three keys and re-armed on every read and write, so idle
streams are collected by Redis itself. A stream that has forks is persisted
until its last fork is deleted.

## Live readers

Each store opens one extra connection subscribed to the notification channel.
Every append, close, create-with-data, and delete publishes the stream path;
waiting long-poll and SSE handlers re-read from Redis when their path is
published. Waiters register before their first read, so an append that lands
between the read and the wait is not missed.

## Differences from `@durable-streams/server`

- Store methods are asynchronous.
- No `dataDir` or in-memory mode; `redis` or `store` replaces them.
- `clear()` deletes every key under the prefix.
- Producer state is kept for the life of the stream (the reference server
  drops producer entries after seven days).
- The `__registry__` hook and subscriptions work unchanged, but subscription
  state is still held in process memory, as in the reference server.

## Testing

Tests need a reachable Redis. Set `REDIS_URL` (default
`redis://127.0.0.1:6379`), then run from the monorepo root:

```bash
# Server conformance suite (both offset schemes), compression, store tests
pnpm vitest run --project server-redis

# Every client-side suite in the monorepo (TypeScript client, state, Yjs,
# TanStack AI transport, AI SDK transport) with the in-process reference
# server replaced by this one
pnpm vitest run --config vitest.server-redis-clients.config.ts

# Cross-language client conformance runner, TypeScript adapter
pnpm --filter @durable-streams/client-conformance-tests exec \
  tsx --tsconfig tsconfig.server-redis.json src/cli.ts --run ts
```

`test/support/reference-shim.ts` is the drop-in used by the last two
commands. `DS_OFFSET_SCHEME=time` runs them with time-based offsets;
`DS_TRACE=1` logs every request and SSE frame.

## License

Apache-2.0
