// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Test-only preload (bunfig.toml): keep the DuckDB instances a serial
 * `bun test ./src` opens from exhausting the process's threads.
 *
 * DuckDB starts a pool of about one thread per core for each instance. Specs
 * across a serial run used to leave hundreds of instances open (about 650
 * measured locally, ~12,000 threads at 18 cores), and the process ran out of
 * threads: DuckDB's "thread constructor failed: Resource temporarily
 * unavailable" escapes native code as Bun's "panic: A C++ exception occurred",
 * which killed macOS CI runs mid-suite. Two measures:
 *
 * 1. Each instance a test opens without its own `threads` setting gets two
 *    threads (override with PUBLISHER_TEST_DUCKDB_THREADS). Production caps
 *    instances too, at PUBLISHER_DUCKDB_THREADS (duckdb_instance_sharing.ts);
 *    this lower cap applies first.
 * 2. After each test, an instance whose directory a spec has already removed
 *    is closed. Most leaked instances are the per-package `duckdb` sandboxes
 *    of environments a spec built in a temp directory, removed on teardown,
 *    and never closed; nothing can use them once the directory is gone. Bun
 *    runs a preload's `afterAll` once per run, not once per file, so the
 *    directory is the signal that a spec is done with an instance. This
 *    bounds open instances (about 650 at the end of a run became 5) more than
 *    threads: a closed instance keeps its pool until its node-api connections
 *    are collected (db-duckdb's note in `detachInstance`), so the cap in (1)
 *    is what keeps the run alive.
 *
 * Production never loads this.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import { afterEach } from "bun:test";
import * as fs from "fs";
import * as path from "path";

const cap = process.env.PUBLISHER_TEST_DUCKDB_THREADS ?? "2";

type ActiveDB = (typeof DuckDBConnection.activeDBs)[string];
type Internals = {
   shareKey: string;
   normalized?: { databasePath?: string; workingDirectory?: string };
   init(): Promise<void>;
   buildInstanceOptions(opts?: unknown): Record<string, string>;
};
const proto = DuckDBConnection.prototype as unknown as Internals;

const buildInstanceOptions = proto.buildInstanceOptions;
proto.buildInstanceOptions = function (
   this: Internals,
   opts?: unknown,
): Record<string, string> {
   const options = buildInstanceOptions.call(this, opts);
   if (options["threads"] === undefined) options["threads"] = cap;
   return options;
};

/** The directory each open instance belongs to: its file's, or its root. */
const directoryOf = new WeakMap<ActiveDB, string>();
const init = proto.init;
proto.init = function (this: Internals): Promise<void> {
   const { databasePath, workingDirectory } = this.normalized ?? {};
   const directory =
      databasePath &&
      databasePath !== ":memory:" &&
      path.isAbsolute(databasePath)
         ? path.dirname(databasePath)
         : workingDirectory;
   return init.call(this).then(() => {
      const activeDB = DuckDBConnection.activeDBs[this.shareKey];
      if (activeDB && directory && !directoryOf.has(activeDB)) {
         directoryOf.set(activeDB, directory);
      }
   });
};

afterEach(() => {
   for (const [key, activeDB] of Object.entries(DuckDBConnection.activeDBs)) {
      const directory = directoryOf.get(activeDB);
      if (!directory || fs.existsSync(directory)) continue;
      try {
         activeDB.instance.closeSync();
      } catch {
         // Best effort: nothing can reach the instance either way.
      }
      delete DuckDBConnection.activeDBs[key];
   }
});
