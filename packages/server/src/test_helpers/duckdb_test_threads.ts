// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Test-only preload (bunfig.toml): cap the thread pool of every DuckDB instance
 * a test opens without its own `threads` setting.
 *
 * DuckDB starts a pool of about one thread per core for each instance. Specs
 * across a serial `bun test ./src` leave hundreds of instances open (about 650
 * measured locally, ~12,000 threads at 18 cores), and the process runs out of
 * threads: DuckDB's "thread constructor failed: Resource temporarily
 * unavailable" escapes native code as Bun's "panic: A C++ exception occurred",
 * which killed macOS CI runs mid-suite. Tests need no parallelism inside one
 * instance, so two threads each keeps the whole run far below any limit.
 * Override with PUBLISHER_TEST_DUCKDB_THREADS. Production never loads this.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";

const cap = process.env.PUBLISHER_TEST_DUCKDB_THREADS ?? "2";

type WithInstanceOptions = {
   buildInstanceOptions(opts?: unknown): Record<string, string>;
};
const proto = DuckDBConnection.prototype as unknown as WithInstanceOptions;
const buildInstanceOptions = proto.buildInstanceOptions;
proto.buildInstanceOptions = function (
   this: WithInstanceOptions,
   opts?: unknown,
): Record<string, string> {
   const options = buildInstanceOptions.call(this, opts);
   if (options["threads"] === undefined) options["threads"] = cap;
   return options;
};
