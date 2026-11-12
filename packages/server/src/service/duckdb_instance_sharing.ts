// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * One DuckDB instance per database file, per process.
 *
 * DuckDB does not support two instances of one database file in one process:
 * Windows refuses the second open ("being used by another process"), and on
 * Linux and macOS it is unsupported (a second instance can read stale data or
 * corrupt writes). `@malloydata/db-duckdb` already shares an instance between
 * connections whose configuration is identical, keyed by a share key that
 * digests every option, including `setupSQL`.
 *
 * A connection edit builds a new connection generation while the old one
 * drains (environment.ts, RETIRED_CONNECTION_DRAIN_MS), and a re-derived build
 * plan opens the edited connection at once. An edit to `setupSQL` or the
 * attached databases (the only DuckDB fields Publisher's API exposes) changes
 * the share key, so the new generation used to open a second instance of the
 * same file.
 *
 * This module closes that gap: when a connection is about to open a file an
 * open instance already holds, and the two differ only in settings DuckDB
 * applies per session (`setupSQL`, run by each connection in `setupOnce`), the
 * new connection is pointed at the open instance. When the instances would
 * differ in a setting DuckDB fixes when it opens the file (access mode,
 * threads, memory limit, extensions, security policy, directories), the new
 * connection is refused with {@link DuckDBInstanceSettingsChangedError}
 * instead of opening a second instance: those settings cannot change while the
 * file is open, so the change applies after a restart. Publisher's API cannot
 * change them; only an edit to publisher.config.json under a running server
 * can.
 *
 * Load-pool workers are worker threads that fetch schemas through the main
 * thread's connections (package_load_pool.ts), so they never open these files.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import * as fs from "fs";
import * as path from "path";

/** A connection's settings, as `@malloydata/db-duckdb` normalized them. */
interface NormalizedDuckDBConfig {
   databasePath: string;
   setupSQL?: string;
   effectiveShareable?: boolean;
   [option: string]: unknown;
}

/** What this module reads and wraps on a `DuckDBConnection`. */
interface DuckDBConnectionInternals {
   normalized: NormalizedDuckDBConfig;
   shareKey: string;
   setupError?: Error;
   init(): Promise<void>;
   detachInstance(): void;
}

type ActiveDB = (typeof DuckDBConnection.activeDBs)[string];

/** The open instance holding one file, and the settings it was opened with. */
interface OpenFile {
   instanceKey: string;
   activeDB: ActiveDB;
}

const openFiles = new Map<string, OpenFile>();
let installed = false;

/**
 * Raised instead of opening a second instance of a file an open instance holds
 * with different open-time settings.
 */
export class DuckDBInstanceSettingsChangedError extends Error {
   constructor(file: string) {
      super(
         `DuckDB database file "${file}" is already open with different ` +
            `open-time settings (access mode, threads, memory limit, ` +
            `extensions, security policy or directories). DuckDB fixes those ` +
            `when it opens a file, and two instances of one file are not ` +
            `supported, so the change applies after a restart.`,
      );
      this.name = "DuckDBInstanceSettingsChangedError";
   }
}

/**
 * The file a DuckDB connection opens, in one canonical spelling per file, or
 * undefined when it opens no file (in-memory, MotherDuck). Symlinks are
 * resolved where the file or its directory exists, and the path is case-folded
 * on Windows, whose file system is case-insensitive.
 */
export function canonicalDuckDBFile(
   databasePath: string | undefined,
   platform: NodeJS.Platform = process.platform,
): string | undefined {
   if (
      !databasePath ||
      databasePath === ":memory:" ||
      databasePath.startsWith("md:") ||
      databasePath.startsWith("motherduck:")
   ) {
      return undefined;
   }
   let resolved = path.resolve(databasePath);
   try {
      resolved = fs.realpathSync.native(resolved);
   } catch {
      // A file not created yet: resolve its directory instead.
      try {
         resolved = path.join(
            fs.realpathSync.native(path.dirname(resolved)),
            path.basename(resolved),
         );
      } catch {
         // Neither exists yet; the resolved path is the best spelling.
      }
   }
   return platform === "win32" ? resolved.toLowerCase() : resolved;
}

/**
 * The settings DuckDB fixes when it opens a file: every normalized option
 * except the ones each connection applies to its own session.
 */
export function instanceKeyOf(normalized: NormalizedDuckDBConfig): string {
   const { setupSQL: _setupSQL, ...instanceSettings } = normalized;
   return JSON.stringify(
      Object.keys(instanceSettings)
         .sort()
         .map((key) => [key, instanceSettings[key]]),
   );
}

/** Whether `open` still names a live instance in Malloy's bookkeeping. */
function isLive(open: OpenFile): boolean {
   if (open.activeDB.connections.length === 0) return false;
   return Object.values(DuckDBConnection.activeDBs).includes(open.activeDB);
}

/**
 * Point every later connection to a file an open instance already holds at
 * that instance. Idempotent; installed once per process (connection.ts).
 */
export function installDuckDBInstanceSharing(): void {
   if (installed) return;
   installed = true;
   const proto =
      DuckDBConnection.prototype as unknown as DuckDBConnectionInternals;
   const init = proto.init;
   const detachInstance = proto.detachInstance;

   proto.init = function (this: DuckDBConnectionInternals): Promise<void> {
      const normalized = this.normalized;
      // Shareable mode opens a private in-memory primary and ATTACHes the file
      // lazily, releasing it on idle; it holds no instance of the file.
      const file = normalized.effectiveShareable
         ? undefined
         : canonicalDuckDBFile(normalized.databasePath);
      if (!file) return init.call(this);

      const instanceKey = instanceKeyOf(normalized);
      const open = openFiles.get(file);
      if (open && isLive(open)) {
         if (open.instanceKey !== instanceKey) {
            this.setupError = new DuckDBInstanceSettingsChangedError(file);
            return Promise.resolve();
         }
         // Malloy's init reads activeDBs synchronously, before its first
         // await, so this alias is seen and the open instance is reused.
         DuckDBConnection.activeDBs[this.shareKey] ??= open.activeDB;
      }
      return init.call(this).then(() => {
         const activeDB = DuckDBConnection.activeDBs[this.shareKey];
         if (activeDB && !this.setupError) {
            openFiles.set(file, { instanceKey, activeDB });
         }
      });
   };

   proto.detachInstance = function (this: DuckDBConnectionInternals): void {
      const activeDB = DuckDBConnection.activeDBs[this.shareKey];
      detachInstance.call(this);
      if (!activeDB || activeDB.connections.length > 0) return;
      // The last sharer closed the instance: drop every alias to it, or a
      // later connection would reuse a closed instance.
      for (const [key, value] of Object.entries(DuckDBConnection.activeDBs)) {
         if (value === activeDB) delete DuckDBConnection.activeDBs[key];
      }
      for (const [file, open] of openFiles) {
         if (open.activeDB === activeDB) openFiles.delete(file);
      }
   };
}

/** Test seam: the files this process holds open through shared instances. */
export function openDuckDBFilesForTesting(): string[] {
   return [...openFiles.entries()]
      .filter(([, open]) => isLive(open))
      .map(([file]) => file);
}
