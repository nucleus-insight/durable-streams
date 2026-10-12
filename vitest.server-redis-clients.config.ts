import { defineConfig, defineProject } from "vitest/config"
import path from "node:path"

// Runs the client-side suites with `@durable-streams/server` aliased to the
// Redis-backed server, so every in-process reference server becomes a Redis one.
const alias = {
  "@durable-streams/client": path.resolve(__dirname, "./packages/client/src"),
  "@durable-streams/cli": path.resolve(__dirname, "./packages/cli/src"),
  "@durable-streams/server": path.resolve(
    __dirname,
    "./packages/server-redis/test/support/reference-shim.ts"
  ),
  "@durable-streams/server-redis": path.resolve(
    __dirname,
    "./packages/server-redis/src"
  ),
  "@durable-streams/state": path.resolve(__dirname, "./packages/state/src"),
  "@durable-streams/proxy": path.resolve(__dirname, "./packages/proxy/src"),
  "@durable-streams/server-conformance-tests": path.resolve(
    __dirname,
    "./packages/server-conformance-tests/src"
  ),
  "y-durable-streams": path.resolve(
    __dirname,
    "./packages/y-durable-streams/src"
  ),
}

const project = (name: string, include: Array<string>) =>
  defineProject({
    test: {
      name,
      include,
      exclude: ["**/node_modules/**"],
      testTimeout: 30000,
      hookTimeout: 30000,
    },
    resolve: { alias },
  })

export default defineConfig({
  test: {
    projects: [
      project("client", ["packages/client/test/**/*.test.ts"]),
      project("state", ["packages/state/test/**/*.test.ts"]),
      project("y-durable-streams", [
        "packages/y-durable-streams/test/**/*.test.ts",
      ]),
      project("tanstack-transport", [
        "packages/tanstack-ai-transport/test/**/*.test.ts",
      ]),
      project("aisdk-transport", [
        "packages/aisdk-transport/test/**/*.test.ts",
      ]),
    ],
  },
})
