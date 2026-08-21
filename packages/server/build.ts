// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { build } from "bun";
import fs from "fs";

fs.rmSync("./dist", { recursive: true, force: true });
fs.mkdirSync("./dist");

await build({
   // package_load_worker.ts is bundled as a SEPARATE entrypoint so it
   // can be loaded by `new Worker(...)` at runtime. It must NOT be
   // inlined into server.mjs (workers can't share module state with
   // the parent process — they get their own JS realm).
   entrypoints: [
      "./src/server.ts",
      "./src/instrumentation.ts",
      "./src/package_load/package_load_worker.ts",
   ],
   outdir: "./dist",
   target: "node",
   format: "esm",
   external: [
      "@malloydata/db-duckdb",
      // The DuckDB engine binding (used by the storage layer). Externalized,
      // not bundled, because its loader require()s platform-specific native
      // .node files for every OS via a runtime switch; the bundler can't
      // statically resolve the ones absent on the build host. Left as a
      // runtime require resolved from node_modules, like the Malloy drivers.
      "@duckdb/node-api",
      "@duckdb/node-bindings",
      "@malloydata/malloy",
      // Externalized alongside malloy itself: malloy loads its own copy of the
      // tag parser at runtime, so bundling a second one ships two independent
      // copies of the same Tag implementation in every entrypoint.
      "@malloydata/malloy-tag",
      "@malloydata/malloy-sql",
      "@malloydata/render",
      "@malloydata/db-bigquery",
      "@malloydata/db-mysql",
      "@malloydata/db-postgres",
      "@malloydata/db-snowflake",
      "@malloydata/db-trino",
      "@malloydata/db-databricks",
      "@google-cloud/storage",
      "@azure/identity",
      "@azure/storage-blob",
   ],
});

// `dist/app` is what the server actually serves the SPA from at runtime (see
// ROOT in server.ts) — nothing reads ../app/dist directly. So this copy is
// load-bearing for the shipped image, not a convenience.
//
// The SPA is built by a separate step, which `build:server-only` does not run.
// That does NOT mean its callers don't need the UI: the Dockerfile builds the
// app in its own cache layer and then relies on this copy to place it. Absent a
// bundle we therefore fail loudly by default, so an app build that stops
// happening breaks the build instead of quietly publishing a UI-less server.
//
// SKIP_APP_BUNDLE only declares that absence is acceptable — the harness builds
// on checkouts with no SPA and drives the REST API. Note the flag does not
// suppress the copy: `dist` is wiped above, so skipping whenever it was merely
// requested would strip an already-built SPA out of an existing bundle and leave
// the next `bun run start` serving a UI that 404s for no visible reason.
if (process.env.SKIP_APP_BUNDLE === "1" && !fs.existsSync("../app/dist")) {
   console.log(
      "SKIP_APP_BUNDLE=1 and ../app/dist is absent: building the server without the app bundle",
   );
} else {
   fs.cpSync("../app/dist", "./dist/app", { recursive: true });
}

// `dist/mcp-apps` is where the MCP server reads the MCP Apps widgets from at
// runtime (see mcp/ui_resources.ts resolveWidgetDir), so this copy is what puts
// them in the shipped bundle.
//
// A warning rather than a hard failure, which is the difference from the SPA
// above: a Publisher with no widget bundle is fully functional, it just does not
// offer inline rendering, and it is built to advertise nothing in that case
// rather than a widget it cannot serve. So a missing bundle must not break a
// server build, but it should be visible, because the usual cause is that the
// widget build step stopped running.
if (fs.existsSync("../mcp-apps/dist")) {
   fs.cpSync("../mcp-apps/dist", "./dist/mcp-apps", { recursive: true });
} else {
   console.warn(
      "WARNING: ../mcp-apps/dist is absent, so this server bundle ships without " +
         "the MCP Apps widgets and will not offer inline result rendering. " +
         "Run `bun run build:mcp-apps` first to include them.",
   );
}

// Copy hand-authored vanilla-JS runtime served at /sdk/publisher.js.
fs.cpSync("./src/runtime", "./dist/runtime", { recursive: true });

// Ship a default publisher.config.json inside the bundle so that
// `npx @malloy-publisher/server` works with zero args (uses
// DuckDB-only samples). config.ts looks for this file next to
// server.mjs.
//
// IMPORTANT: keep `packages/server/src/default-publisher.config.json`
// in sync with `packages/server/publisher.config.json`. The two files
// have intentionally identical content — the committed one drives the
// dev workflow (`bun run start`, integration tests), the bundled one
// is the fallback for npx users with no config of their own. A drift
// here surfaces as "npx users see different samples than dev users."
fs.copyFileSync(
   "./src/default-publisher.config.json",
   "./dist/default-publisher.config.json",
);

// Rename ESM outputs to .mjs so both Node and Bun can execute them
fs.renameSync("./dist/server.js", "./dist/server.mjs");
fs.renameSync("./dist/instrumentation.js", "./dist/instrumentation.mjs");
// Bun emits package_load_worker into its source-relative subdir;
// flatten so package_load_pool.ts's `resolveWorkerScript()` finds it
// as a sibling of server.mjs. The path layout match is intentional —
// keep these two in sync or the worker pool throws at boot (the
// in-process fallback was removed; missing worker = no service).
if (fs.existsSync("./dist/package_load/package_load_worker.js")) {
   fs.renameSync(
      "./dist/package_load/package_load_worker.js",
      "./dist/package_load_worker.mjs",
   );
   try {
      fs.rmdirSync("./dist/package_load");
   } catch {
      /* directory may be non-empty if Bun produced sourcemaps; leave it */
   }
} else if (fs.existsSync("./dist/package_load_worker.js")) {
   fs.renameSync(
      "./dist/package_load_worker.js",
      "./dist/package_load_worker.mjs",
   );
}

// Add shebang to server.mjs for npx/bunx compatibility
const serverJsPath = "./dist/server.mjs";
const serverJsContent = fs.readFileSync(serverJsPath, "utf8");
const shebangContent = "#!/usr/bin/env node\n" + serverJsContent;
fs.writeFileSync(serverJsPath, shebangContent);

// Make the file executable
fs.chmodSync(serverJsPath, "755");
