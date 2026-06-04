#!/usr/bin/env node

/**
 * Single source of truth for the DuckDB version the publisher builds against.
 *
 * The query engine is `@duckdb/node-api`, pulled in by `@malloydata/db-duckdb`
 * (and used directly by the storage layer). DuckDB resolves/downloads
 * extensions under ~/.duckdb/extensions/v<version>/<platform>/, so the DuckDB
 * CLI we install at build time must be the same version as that engine -- or
 * the engine looks in a directory the CLI never wrote to and re-fetches every
 * extension from the network at query time (which fails on hosts that cannot
 * reach extensions.duckdb.org).
 *
 * Rather than store the version in multiple places, every build script and the
 * sync checker resolve it here from bun.lock. Run as a CLI it prints the bare
 * version (for shell substitution / Docker build-args).
 */

import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";

export const REPO_ROOT = path.resolve(
  path.dirname(fileURLToPath(import.meta.url)),
  "..",
);

/**
 * Normalize a DuckDB package version to the value used in the extension
 * directory and CDN path: major.minor.patch with any pre-release/build suffix
 * stripped (e.g. "1.5.3-r.2" -> "1.5.3").
 */
export function toReleaseVersion(raw) {
  const match = String(raw).match(/(\d+)\.(\d+)\.(\d+)/);
  if (!match) {
    throw new Error(`Could not parse a DuckDB version from "${raw}"`);
  }
  return `${match[1]}.${match[2]}.${match[3]}`;
}

/**
 * Resolve the full resolved spec of `@duckdb/node-api` from bun.lock, including
 * any pre-release/build suffix (e.g. "1.4.4-r.1"). This is the exact value an
 * npm dependency pin must carry.
 *
 * The source of truth is the version `@malloydata/db-duckdb` (the query engine)
 * resolves to, NOT a top-level entry -- our own server pin also produces a
 * top-level `"@duckdb/node-api@..."` key, and pinning to that would just
 * reflect our pin back at us. bun records a dependency's own resolution under a
 * nested key like `"@malloydata/db-duckdb/@duckdb/node-api"`; prefer that. Only
 * when there is no nested entry (engine already deduped to a single copy) do we
 * fall back to the plain top-level entry.
 *
 * bun.lock is JSONC; match the spec strings directly rather than parsing it.
 */
export function resolveDuckDBSpec(repoRoot = REPO_ROOT) {
  const lockPath = path.join(repoRoot, "bun.lock");
  if (!fs.existsSync(lockPath)) {
    throw new Error(`bun.lock not found at ${lockPath}; run "bun install".`);
  }
  const lock = fs.readFileSync(lockPath, "utf8");

  // The version Malloy actually runs on (nested under @malloydata/db-duckdb).
  const malloy = lock.match(
    /"@malloydata\/db-duckdb\/@duckdb\/node-api":\s*\["@duckdb\/node-api@(\d+\.\d+\.\d+[^"]*)"/,
  );
  if (malloy) {
    return malloy[1];
  }

  // Deduped to a single copy: the plain top-level entry is Malloy's version.
  const plain = lock.match(
    /"@duckdb\/node-api":\s*\["@duckdb\/node-api@(\d+\.\d+\.\d+[^"]*)"/,
  );
  if (plain) {
    return plain[1];
  }

  throw new Error(
    '@duckdb/node-api not found in bun.lock. Run "bun install" first, or ' +
      "confirm @malloydata/db-duckdb is a dependency.",
  );
}

/**
 * Resolve the DuckDB release version (major.minor.patch, suffix stripped) used
 * by the extension directory and CDN path. This is what the CLI install and
 * the Dockerfile need.
 */
export function resolveDuckDBVersion(repoRoot = REPO_ROOT) {
  return toReleaseVersion(resolveDuckDBSpec(repoRoot));
}

// When run directly, print the bare version (for `$(...)` / --build-arg).
if (import.meta.url === `file://${process.argv[1]}`) {
  try {
    process.stdout.write(resolveDuckDBVersion());
  } catch (err) {
    console.error(err instanceof Error ? err.message : String(err));
    process.exit(1);
  }
}
