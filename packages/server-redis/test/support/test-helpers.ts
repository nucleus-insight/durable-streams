/**
 * Test helper utilities for server integration tests.
 */

/**
 * Encode a string to Uint8Array.
 */
export function encode(text: string): Uint8Array {
  return new TextEncoder().encode(text)
}

/**
 * Decode a Uint8Array to string.
 */
export function decode(data: Uint8Array): string {
  return new TextDecoder().decode(data)
}

/**
 * Sleep for a specified number of milliseconds.
 */
export function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms))
}

/**
 * Create a deferred promise that can be resolved/rejected externally.
 */
export function deferred<T>(): {
  promise: Promise<T>
  resolve: (value: T) => void
  reject: (error: Error) => void
} {
  let resolve!: (value: T) => void
  let reject!: (error: Error) => void

  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })

  return { promise, resolve, reject }
}

/**
 * Redis URL for tests. Defaults to a local Redis; override with REDIS_URL.
 */
export const TEST_REDIS_URL: string =
  process.env.REDIS_URL ?? `redis://127.0.0.1:6379`

/**
 * Unique key prefix per test run so parallel runs do not share keys.
 */
export function uniquePrefix(label: string): string {
  return `dstest:${label}:${Date.now().toString(36)}:${Math.random().toString(36).slice(2, 8)}:`
}
