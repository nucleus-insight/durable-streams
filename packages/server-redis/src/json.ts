/**
 * JSON-mode helpers shared by the Redis store and the HTTP server.
 */

import type { StreamMessage } from "./types"

/**
 * Normalize content-type by extracting the media type (before any semicolon).
 * Handles cases like "application/json; charset=utf-8".
 */
export function normalizeContentType(contentType: string | undefined): string {
  if (!contentType) return ``
  return contentType.split(`;`)[0]!.trim().toLowerCase()
}

/**
 * Process JSON data for append in JSON mode.
 * - Validates JSON
 * - Extracts array elements if data is an array
 * - Always appends trailing comma for easy concatenation
 * @param isInitialCreate - If true, empty arrays are allowed (creates empty stream)
 * @throws Error if JSON is invalid or array is empty (for non-create operations)
 */
export function processJsonAppend(
  data: Uint8Array,
  isInitialCreate = false
): Uint8Array {
  const text = new TextDecoder().decode(data)

  // Validate JSON
  let parsed: unknown
  try {
    parsed = JSON.parse(text)
  } catch {
    throw new Error(`Invalid JSON`)
  }

  // If it's an array, extract elements and join with commas
  let result: string
  if (Array.isArray(parsed)) {
    if (parsed.length === 0) {
      // Empty arrays are valid for PUT (creates empty stream)
      // but invalid for POST (no-op append, likely a bug)
      if (isInitialCreate) {
        return new Uint8Array(0) // Return empty data for empty stream
      }
      throw new Error(`Empty arrays are not allowed`)
    }
    const elements = parsed.map((item) => JSON.stringify(item))
    result = elements.join(`,`) + `,`
  } else {
    // Single value - re-serialize to normalize whitespace (single-line JSON)
    result = JSON.stringify(parsed) + `,`
  }

  return new TextEncoder().encode(result)
}

/**
 * Format JSON mode response by wrapping in array brackets.
 * Strips trailing comma before wrapping.
 */
export function formatJsonResponse(data: Uint8Array): Uint8Array {
  if (data.length === 0) {
    return new TextEncoder().encode(`[]`)
  }

  let text = new TextDecoder().decode(data)
  // Strip trailing comma if present
  text = text.trimEnd()
  if (text.endsWith(`,`)) {
    text = text.slice(0, -1)
  }

  const wrapped = `[${text}]`
  return new TextEncoder().encode(wrapped)
}

function decodeStoredJsonMessage(data: Uint8Array): string {
  let text = new TextDecoder().decode(data).trimEnd()
  if (text.endsWith(`,`)) {
    text = text.slice(0, -1)
  }
  return text
}

function enrichJsonValueWithOffset(parsed: unknown, offset: string): string {
  if (!parsed || typeof parsed !== `object` || Array.isArray(parsed)) {
    return JSON.stringify(parsed)
  }

  const candidate = parsed as {
    headers?: Record<string, unknown>
  }
  const headers = candidate.headers

  if (!headers || typeof headers !== `object`) {
    return JSON.stringify(parsed)
  }

  const isStateChange = typeof headers.operation === `string`
  const isStateControl = typeof headers.control === `string`
  if (!isStateChange && !isStateControl) {
    return JSON.stringify(parsed)
  }

  return JSON.stringify({
    ...candidate,
    headers: {
      ...headers,
      offset,
    },
  })
}

export function formatJsonMessages(messages: Array<StreamMessage>): Uint8Array {
  if (messages.length === 0) {
    return new TextEncoder().encode(`[]`)
  }

  const items = messages.flatMap((message) => {
    const rawFragment = decodeStoredJsonMessage(message.data)
    const parsed = JSON.parse(`[${rawFragment}]`) as Array<unknown>
    return parsed.map((value) =>
      enrichJsonValueWithOffset(value, message.offset)
    )
  })

  return new TextEncoder().encode(`[${items.join(`,`)}]`)
}

/**
 * Format messages for a response body.
 * JSON streams are wrapped in array brackets; other streams are concatenated.
 */
export function formatMessages(
  contentType: string | undefined,
  messages: Array<StreamMessage>
): Uint8Array {
  if (normalizeContentType(contentType) === `application/json`) {
    return formatJsonMessages(messages)
  }

  const totalSize = messages.reduce((sum, m) => sum + m.data.length, 0)
  const concatenated = new Uint8Array(totalSize)
  let offset = 0
  for (const msg of messages) {
    concatenated.set(msg.data, offset)
    offset += msg.data.length
  }

  return concatenated
}
