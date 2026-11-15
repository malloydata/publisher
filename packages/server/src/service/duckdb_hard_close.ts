// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * Hard close: release a retired DuckDB connection's native database at once,
 * not whenever a JS garbage collection happens to finalize it.
 *
 * `@malloydata/db-duckdb`'s `close()` only decrements a refcount
 * (`detachInstance` → `closeSync`). The node-api `Connection` it held keeps
 * its C++ `ClientContext`, and with it the whole database: its thread pool,
 * its buffers, an attached DuckLake and that lake's Postgres metadata
 * connection. All of it lives until a GC finalizes the JS handle, so churn
 * (an environment deleted and created, a package unloaded and reloaded) grows
 * native memory and threads by the instance between collections: a sandbox
 * loop measured 2.7 GiB and about 700 threads within 30 s.
 *
 * The fix is `disconnectSync()` on that handle after `close()`, as build
 * sessions already do (createIsolatedBuildSession). db-duckdb cannot do it
 * unconditionally: upstream reverted it (PR #2793) because work still running
 * on the handle crashed (SIGSEGV, `bad_weak_ptr`). So this module disconnects
 * only a handle it can prove idle:
 *
 * - Closed. After `close()` the wrapper is poisoned: `setup()` throws and its
 *   handle is nulled, so no new native work can start on it. A model or served
 *   copy memoized against the wrapper fails cleanly ("is closed"), exactly as
 *   it does after today's refcount close.
 * - Drained. Every native use goes through `runDuckDBQuery` or
 *   `runSQLStream`, which this module counts per wrapper. The disconnect runs
 *   when the count is zero: at once, or when the last call in flight settles.
 *   A stream its reader abandons (never finished, never returned) never
 *   settles, so that handle is left to the GC, which is today's behaviour.
 *
 * Anything that cannot be proven (db-duckdb's internals changed shape, the
 * shareable mode that never detaches, a handle nobody captured) falls back to
 * the refcount close. Nothing here throws.
 *
 * Callers say which connections Publisher owns ({@link ownDuckDBConnection})
 * and release them through {@link closeMalloyConfig} or
 * {@link hardCloseOwnedDuckDBConnections}; a connection nobody owns is never
 * hard-closed.
 */

import { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection, MalloyConfig } from "@malloydata/malloy";
import { logger } from "../logger";

/** The node-api handle, as far as this module touches it. */
interface NativeConnection {
   disconnectSync(): void;
}

/** What this module reads and wraps on a `DuckDBConnection`. */
interface Internals {
   connection: NativeConnection | null;
   closed: boolean;
   runDuckDBQuery(sql: string): Promise<unknown>;
   runSQLStream(sql: string, options?: unknown): AsyncIterableIterator<unknown>;
   detachInstance(): void;
}

/** Native calls running per wrapper. */
const inFlight = new WeakMap<object, number>();
/** The disconnect waiting for a wrapper's calls to drain. */
const onDrained = new WeakMap<object, () => void>();
/** The handle a wrapper held when it last detached (closed or idled). */
const detachedHandle = new WeakMap<object, NativeConnection>();
/** The DuckDB connections each owner (a MalloyConfig) created. */
const ownedBy = new WeakMap<object, Set<DuckDBConnection>>();

let installed = false;
let available = false;

/** Counters for tests and diagnostics. */
const stats = { disconnected: 0, deferred: 0, skipped: 0, failed: 0 };

export function hardCloseStats(): Readonly<typeof stats> {
   return { ...stats };
}

/**
 * Whether db-duckdb still has the internals this module wraps. Checked once
 * at install; a mismatch leaves every close a refcount close.
 */
export function dbDuckDBHardCloseInternalsMatch(): boolean {
   const proto = DuckDBConnection.prototype as unknown as Partial<
      Record<keyof Internals, unknown>
   >;
   return (
      typeof proto.runDuckDBQuery === "function" &&
      typeof proto.runSQLStream === "function" &&
      typeof proto.detachInstance === "function"
   );
}

function enter(connection: object): void {
   inFlight.set(connection, (inFlight.get(connection) ?? 0) + 1);
}

function exit(connection: object): void {
   const count = (inFlight.get(connection) ?? 1) - 1;
   inFlight.set(connection, count);
   if (count > 0) return;
   const drained = onDrained.get(connection);
   if (drained) {
      onDrained.delete(connection);
      drained();
   }
}

async function* countedStream(
   connection: object,
   inner: AsyncIterableIterator<unknown>,
): AsyncIterableIterator<unknown> {
   enter(connection);
   try {
      yield* inner;
   } finally {
      exit(connection);
   }
}

/**
 * Count native work per wrapper, and remember each wrapper's handle as it
 * detaches. Idempotent; installed once per process (connection.ts).
 */
export function installDuckDBHardClose(): void {
   if (installed) return;
   installed = true;
   if (!dbDuckDBHardCloseInternalsMatch()) {
      logger.warn(
         "DuckDB hard close is off (retired instances are freed at GC): @malloydata/db-duckdb's internals changed shape",
      );
      return;
   }
   const proto = DuckDBConnection.prototype as unknown as Internals;
   const runDuckDBQuery = proto.runDuckDBQuery;
   const runSQLStream = proto.runSQLStream;
   const detachInstance = proto.detachInstance;

   proto.runDuckDBQuery = async function (this: Internals, sql: string) {
      enter(this);
      try {
         return await runDuckDBQuery.call(this, sql);
      } finally {
         exit(this);
      }
   };

   // Counted from the first `next()` until the stream finishes, throws or is
   // returned early; `yield*` forwards `return()` to the inner stream.
   proto.runSQLStream = function (
      this: Internals,
      sql: string,
      options?: unknown,
   ): AsyncIterableIterator<unknown> {
      return countedStream(this, runSQLStream.call(this, sql, options));
   };

   // `close()` nulls the handle right after detaching, so this is the last
   // point it can be read.
   proto.detachInstance = function (this: Internals): void {
      if (this.connection) detachedHandle.set(this, this.connection);
      detachInstance.call(this);
   };
   available = true;
}

/**
 * Record that `owner` (the MalloyConfig whose lookup created it) owns
 * `connection`, so releasing the owner hard-closes it. A non-DuckDB
 * connection is ignored.
 */
export function ownDuckDBConnection(
   owner: object,
   connection: Connection,
): void {
   if (!(connection instanceof DuckDBConnection)) return;
   let owned = ownedBy.get(owner);
   if (!owned) {
      owned = new Set();
      ownedBy.set(owner, owned);
   }
   owned.add(connection);
}

/**
 * Disconnect a CLOSED connection's native handle once no call on it is in
 * flight: now, or when the last one settles. Never throws; anything it cannot
 * prove safe is left to the refcount close.
 */
export function hardCloseWhenIdle(connection: DuckDBConnection): void {
   if (!available) return;
   const internals = connection as unknown as Internals;
   const handle = detachedHandle.get(connection);
   // Not closed: new work could still start on it. No handle: nothing was
   // captured (shareable mode, an open that never settled) or it is done.
   if (!internals.closed || !handle) {
      stats.skipped++;
      return;
   }
   detachedHandle.delete(connection);
   const disconnect = () => {
      try {
         handle.disconnectSync();
         stats.disconnected++;
      } catch (error) {
         stats.failed++;
         logger.warn("Failed to disconnect a retired DuckDB connection", {
            connection: connection.name,
            error: error instanceof Error ? error.message : String(error),
         });
      }
   };
   if ((inFlight.get(connection) ?? 0) === 0) {
      disconnect();
      return;
   }
   stats.deferred++;
   onDrained.set(connection, disconnect);
}

/**
 * Hard-close every DuckDB connection `owner` owns. Call it after the owner's
 * connections are closed; one still open is skipped.
 */
export function hardCloseOwnedDuckDBConnections(owner: object): void {
   const owned = ownedBy.get(owner);
   if (!owned) return;
   ownedBy.delete(owner);
   for (const connection of owned) hardCloseWhenIdle(connection);
}

/**
 * `config.shutdown("close")`, then hard-close the DuckDB connections it owns.
 * The release path for a package's connections.
 */
export async function closeMalloyConfig(config: MalloyConfig): Promise<void> {
   try {
      await config.shutdown("close");
   } finally {
      hardCloseOwnedDuckDBConnections(config);
   }
}
