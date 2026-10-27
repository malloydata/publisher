// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   DEFAULT_MAX_CONCURRENT_QUERIES,
   DEFAULT_MAX_QUERY_ROWS,
   DEFAULT_MAX_RESPONSE_BYTES,
   DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES,
   DEFAULT_POSTGRES_POOL_MAX,
   DEFAULT_QUERY_ROW_LIMIT,
   DEFAULT_QUERY_TIMEOUT_MS,
} from "../../constants";
import { setting } from "../setting";
import { boolean, integer, number } from "../types";

/** The configuration design's `query`, `postgres`, `memory`, `packageLoad` table. */
export const query = [
   setting({
      key: "query.maxRows",
      type: integer({ min: 0 }),
      default: DEFAULT_MAX_QUERY_ROWS,
      description:
         "Row cap on ad-hoc connection SQL; 0 leaves only the byte cap.",
      notes: "Maximum rows returned per query on every query surface (`/connections/.../sqlQuery`, model query, notebook cell, MCP `executeQuery`). Forwarded to the connector / Malloy `runnable.run` as the effective row limit; queries that exceed the cap fail with HTTP 413. Set to `0` to disable. A caller-supplied `rowLimit` smaller than the cap is preserved.",
      aliases: { env: ["PUBLISHER_MAX_QUERY_ROWS"] },
   }),
   setting({
      key: "query.maxResponseBytes",
      type: integer({ min: 0 }),
      default: DEFAULT_MAX_RESPONSE_BYTES,
      description:
         "Byte cap on ad-hoc connection SQL responses; 0 leaves only the row cap.",
      notes: "Maximum JSON-serialized response size, for ad-hoc SQL, model queries and notebook cells. Streaming-capable connections (Postgres, DuckDB) enforce mid-stream and abort the driver immediately; non-streaming connections enforce post-buffer. Exceeding the cap fails with HTTP 413. Set to `0` to disable. See the note below the table.",
      defaultText: "`50000000` (50 MB)",
      aliases: { env: ["PUBLISHER_MAX_RESPONSE_BYTES"] },
   }),
   setting({
      key: "query.defaultRowLimit",
      type: integer({ min: 1 }),
      default: DEFAULT_QUERY_ROW_LIMIT,
      description: "Row limit of a model query that sets none of its own.",
      notes: "Default `LIMIT` applied to model queries that don't include their own. Always \u2264 `PUBLISHER_MAX_QUERY_ROWS`. `0` is rejected.",
      aliases: { env: ["PUBLISHER_DEFAULT_QUERY_ROW_LIMIT"] },
   }),
   setting({
      key: "query.timeoutMs",
      type: integer({ min: 0 }),
      default: DEFAULT_QUERY_TIMEOUT_MS,
      description: "Per-query wall-clock timeout in ms; 0 turns it off.",
      notes: "Wall-clock timeout per query (all surfaces). Wired to the underlying SDK via `AbortSignal`; queries that exceed the budget are aborted and return HTTP 504. Set to `0` to disable.",
      defaultText: "`300000` (5 min)",
      aliases: { env: ["PUBLISHER_QUERY_TIMEOUT_MS"] },
   }),
   setting({
      key: "query.maxConcurrent",
      type: integer({ min: 0 }),
      default: DEFAULT_MAX_CONCURRENT_QUERIES,
      description: "Queries this process runs at once; 0 turns the cap off.",
      notes: "Per-pod cap on simultaneous in-flight work (HTTP + MCP share the same slot pool). Covers queries and the two authoring surfaces that also reach the database: `compile`, which resolves source schemas against the connection, and `sqlSource`, which runs a live introspection. When the cap is reached, new requests fail fast with HTTP 503 (or the MCP-error equivalent). Size it for authoring traffic as well as query traffic; set to `0` to disable.",
      aliases: { env: ["PUBLISHER_MAX_CONCURRENT_QUERIES"] },
   }),
   setting({
      key: "query.metadataEnabled",
      type: boolean(),
      default: false,
      description: "Attach per-query metadata to every statement.",
      notes: "`on` attaches per-query metadata to every statement, as a leading SQL comment where the connection has no native tag; `off` attaches none. Only `on` and `off` are read. See [query-metadata.md](query-metadata.md).",
      aliases: {
         env: ["PUBLISHER_QUERY_METADATA"],
         today: { reads: "on-off" },
      },
   }),
   setting({
      key: "postgres.poolMax",
      type: integer({ min: 1 }),
      default: DEFAULT_POSTGRES_POOL_MAX,
      description: "Open sessions per plain Postgres connection.",
      notes: "Per-process cap on open database sessions for each plain (non-proxied) Postgres connection. Each query uses its own session and closes it; when all are busy a query waits up to 30 s for one, then fails with HTTP 502. Across a fleet the ceiling is roughly this value times the pods serving the environment, so size it against the role's `CONNECTION LIMIT`. Must be a positive integer.",
      aliases: { env: ["PUBLISHER_POSTGRES_POOL_MAX"] },
   }),
   setting({
      key: "memory.maxBytes",
      type: integer({ min: 0 }),
      default: 0,
      description:
         "Process RSS ceiling for the memory governor, in bytes; 0 turns the governor off.",
      notes: "Enables the RSS-based memory governor. When set, the governor samples process RSS every `PUBLISHER_MEMORY_CHECK_INTERVAL_MS` ms and rejects new package loads and queries with HTTP 503 once RSS crosses `PUBLISHER_MEMORY_HIGH_WATER_FRACTION \u00d7 PUBLISHER_MAX_MEMORY_BYTES`, until it drops below `PUBLISHER_MEMORY_LOW_WATER_FRACTION \u00d7`. Unset or `0` disables.",
      defaultText: "`0` (the governor is off)",
      aliases: { env: ["PUBLISHER_MAX_MEMORY_BYTES"] },
   }),
   setting({
      key: "memory.highWaterFraction",
      type: number({ above: 0, below: 1 }),
      default: 0.8,
      description:
         "Share of memory.maxBytes at which new package loads are refused.",
      notes: "High-water mark (fraction of `PUBLISHER_MAX_MEMORY_BYTES`). Must be in `(0, 1)` and strictly above the low-water mark.",
      aliases: { env: ["PUBLISHER_MEMORY_HIGH_WATER_FRACTION"] },
   }),
   setting({
      key: "memory.lowWaterFraction",
      type: number({ above: 0, below: 1 }),
      default: 0.7,
      description:
         "Share of memory.maxBytes at which loads are admitted again; below the high-water mark.",
      notes: "Low-water mark (fraction of `PUBLISHER_MAX_MEMORY_BYTES`). Hysteresis: back-pressure clears when RSS dips below this value.",
      aliases: { env: ["PUBLISHER_MEMORY_LOW_WATER_FRACTION"] },
   }),
   setting({
      key: "memory.checkIntervalMs",
      type: integer({ min: 100 }),
      default: 5000,
      description: "How often the memory governor samples RSS, in ms.",
      notes: "RSS sampling interval (ms). Minimum 100.",
      aliases: { env: ["PUBLISHER_MEMORY_CHECK_INTERVAL_MS"] },
   }),
   setting({
      key: "memory.backpressureEnabled",
      type: boolean(),
      default: true,
      description:
         "Refuse loads above the high-water mark; off only samples and reports.",
      notes: "Set to `false` to disable the 503 behavior while keeping RSS monitoring \u2014 useful for a metrics-only rollout before enabling enforcement.",
      aliases: { env: ["PUBLISHER_MEMORY_BACKPRESSURE"] },
   }),
   setting({
      key: "packageLoad.workers",
      type: integer({ min: 1 }),
      default: 1,
      description: "Package-load worker threads.",
      notes: "Worker processes for package compilation. Must be \u2265 1.",
      aliases: { env: ["PACKAGE_LOAD_WORKERS"] },
   }),
   setting({
      key: "packageLoad.jobTimeoutMs",
      type: integer({ min: 1 }),
      default: 120_000,
      description: "Compile budget of one package load, in ms.",
      notes: "Bounds a package-load job's compile. A job may run to twice it before its worker is recycled, because the work it does for the main thread (render-tag results, pre-aggregation companions and the build plan) comes on top. The worker starts none of that work after three quarters of the timeout, and leaves what remains to the main thread.",
      defaultText: "`120000` (2 min)",
      aliases: { env: ["PACKAGE_LOAD_JOB_TIMEOUT_MS"] },
   }),
   setting({
      key: "packageLoad.schemaCacheBytes",
      type: integer({ min: 0 }),
      default: DEFAULT_PACKAGE_LOAD_SCHEMA_CACHE_BYTES,
      description:
         "Serialized schema each package-load worker caches across loads, in bytes; 0 turns it off.",
      notes: "Serialized size of the table and SQL schemas each package-load worker keeps across loads, so a model's repeated and later requests for the same table skip the round trip to the main thread. Least recently used entries go first. Scoped per environment and per connection instance, so editing a connection drops its entries; a request that asks to refresh a schema is fetched again, and the result replaces the entry. Heap use is a few times the serialized size. `0` disables it; empty takes the default, and a value that is not a byte count stops the server from starting.",
      defaultText: "`16777216` (16 MiB)",
      aliases: { env: ["PACKAGE_LOAD_SCHEMA_CACHE_BYTES"] },
   }),
];
