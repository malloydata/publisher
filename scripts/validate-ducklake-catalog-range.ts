#!/usr/bin/env bun

/**
 * DuckLake catalog-format version-contract check (run under Bun).
 *
 * Derives the supported DuckLake catalog-format RANGE (1.0 <= format <= the
 * pinned engine's max) purely from the pinned `@duckdb/node-api` version and
 * the migration matrix in `packages/server/src/ducklake_version.ts`, and FAILS
 * THE BUILD when the pinned engine has no matrix row — i.e. a Malloy bump moved
 * the DuckDB engine to a minor line whose max DuckLake catalog format has not
 * been verified and recorded yet.
 *
 * This never enumerates an allow-list of catalog versions: the range is
 * derived, and the only thing a human maintains is the single per-engine
 * `maxFormat` fact in the matrix. Companion to `validate-duckdb-sync`, which
 * checks the engine/CLI/extension versions themselves.
 *
 * Usage:
 *   bun scripts/validate-ducklake-catalog-range.ts           # check; exit 1 on drift
 *   bun scripts/validate-ducklake-catalog-range.ts --print   # print the derived range
 */

import { resolveDuckDBSpec, resolveDuckDBVersion } from "./duckdb-version.js";
import {
  catalogFormatRangeForEngine,
  ENGINE_MAX_FORMAT,
} from "../packages/server/src/ducklake_version";

const PRINT = process.argv.includes("--print");

function fail(message: string): never {
  console.error(`✗ ${message}`);
  process.exit(1);
}

const spec = resolveDuckDBSpec();
const version = resolveDuckDBVersion();
const range = catalogFormatRangeForEngine(version);

if (!range) {
  const known = ENGINE_MAX_FORMAT.map(
    (r) => `${r.engine.major}.${r.engine.minor} → max ${r.maxFormat}`,
  ).join(", ");
  fail(
    `DuckLake catalog-format contract drift: the pinned DuckDB engine ` +
      `@duckdb/node-api ${spec} (v${version}) has no row in ENGINE_MAX_FORMAT ` +
      `(known engine lines: ${known || "none"}).\n` +
      `  A Malloy bump moved the DuckDB engine to a new minor line. Verify ` +
      `the DuckLake extension bundled with this engine, then add a row to ` +
      `ENGINE_MAX_FORMAT in packages/server/src/ducklake_version.ts mapping ` +
      `engine ${version.replace(/\.\d+$/, "")} to the maximum DuckLake catalog ` +
      `format it attaches WITHOUT migration.`,
  );
}

if (PRINT) {
  process.stdout.write(`${range.min}..${range.max}`);
} else {
  console.log(
    `DuckDB query engine (@duckdb/node-api): ${spec} (v${version})\n`,
  );
  console.log(
    `  ✓ Supported DuckLake catalog-format range: ${range.min} ≤ format ≤ ${range.max} ` +
      `(engine line v${range.engineVersion})`,
  );
  console.log(
    "\n✓ DuckLake catalog-format contract holds for the pinned engine.",
  );
}
