#!/usr/bin/env node

/**
 * Pre-download ("bake") the DuckDB extensions the server loads at runtime so
 * they are on disk before any query runs.
 *
 * DuckDB fetches extensions on first use from extensions.duckdb.org and caches
 * them under ~/.duckdb/extensions/v<version>/<platform>/. Some hosts cannot
 * reach that CDN (notably the macOS GitHub Actions fleet), so an on-demand
 * fetch at query time fails and takes the query (or a test) down. Running this
 * as part of `bun run build` populates the cache via the same @duckdb/node-api
 * engine the runtime uses, so later INSTALL/LOAD calls are served from disk.
 *
 * Runs in the same place build does -- local `bun run build` and the Docker
 * build both invoke it -- so dev, CI, and the image stay in sync.
 *
 * Each extension is baked independently: a failure (offline build, transient
 * CDN error) is logged and skipped, never aborting the others or the build.
 */

import { DuckDBInstance } from "@duckdb/node-api";

// Extensions the connection layer (packages/server/src/service/connection.ts)
// and storage manager INSTALL/LOAD for cloud attach, the per-package sandbox,
// and the materialization catalog.
const EXTENSIONS = ["httpfs", "ducklake", "aws", "postgres"];

async function main() {
  const instance = await DuckDBInstance.create(":memory:");
  const connection = await instance.connect();

  for (const ext of EXTENSIONS) {
    try {
      await connection.run(`INSTALL ${ext}; LOAD ${ext};`);
      console.log(`baked DuckDB extension: ${ext}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(`skipped DuckDB extension "${ext}": ${message}`);
    }
  }

  connection.closeSync();
  instance.closeSync();
}

main().catch((err) => {
  // Never fail the build on a bake error -- the extensions are an optimization,
  // not a correctness requirement (the runtime can still fetch on demand where
  // the network allows).
  console.warn(
    `DuckDB extension bake skipped: ${err instanceof Error ? err.message : String(err)}`,
  );
});
