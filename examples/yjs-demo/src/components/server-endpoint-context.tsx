import { createContext, useContext, useMemo } from "react"
import type { ReactNode } from "react"

// ============================================================================
// Context
// ============================================================================

interface ServerEndpointContextValue {
  serverEndpoint: string
  dsEndpoint: string
  yjsHeaders: Record<string, string>
  dsHeaders: Record<string, string>
}

const ServerEndpointContext = createContext<ServerEndpointContextValue | null>(
  null
)

export function useServerEndpoint(): ServerEndpointContextValue {
  const context = useContext(ServerEndpointContext)
  if (!context) {
    throw new Error(
      `useServerEndpoint must be used within ServerEndpointProvider`
    )
  }
  return context
}

// ============================================================================
// Provider
// ============================================================================

/**
 * Get the server endpoint URL.
 *
 * Uses VITE_YJS_URL or VITE_SERVER_URL environment variable or falls back to the
 * local Yjs server (port 4438).
 */
function getServerEndpoint(): string {
  if (import.meta.env.VITE_YJS_URL) {
    return import.meta.env.VITE_YJS_URL
  }

  if (import.meta.env.VITE_SERVER_URL) {
    return import.meta.env.VITE_SERVER_URL
  }

  // Vite dev server: talk to the local Yjs server directly. Otherwise assume
  // a reverse proxy exposes both servers on the page origin.
  if (import.meta.env.DEV) {
    return `http://localhost:4438/v1/yjs`
  }
  return `${window.location.origin}/v1/yjs`
}

function getDsEndpoint(): string {
  if (import.meta.env.VITE_DS_URL) {
    return import.meta.env.VITE_DS_URL
  }

  if (import.meta.env.DEV) {
    return `http://localhost:4437/v1/stream`
  }
  return `${window.location.origin}/v1/stream`
}

function getYjsHeaders(): Record<string, string> {
  const token = import.meta.env.VITE_YJS_TOKEN
  if (token) {
    return { Authorization: `Bearer ${token}` }
  }
  return {}
}

function getDsHeaders(): Record<string, string> {
  const token = import.meta.env.VITE_DS_TOKEN
  if (token) {
    return { Authorization: `Bearer ${token}` }
  }
  return {}
}

export function ServerEndpointProvider({ children }: { children: ReactNode }) {
  const serverEndpoint = getServerEndpoint()
  const dsEndpoint = getDsEndpoint()
  const yjsHeaders = useMemo(() => getYjsHeaders(), [])
  const dsHeaders = useMemo(() => getDsHeaders(), [])

  const value = useMemo<ServerEndpointContextValue>(
    () => ({
      serverEndpoint,
      dsEndpoint,
      yjsHeaders,
      dsHeaders,
    }),
    [serverEndpoint, dsEndpoint, yjsHeaders, dsHeaders]
  )

  return (
    <ServerEndpointContext.Provider value={value}>
      {children}
    </ServerEndpointContext.Provider>
  )
}
