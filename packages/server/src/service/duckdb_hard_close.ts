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
 * The fix is `disconnectSync()` on that handle after `close()`, which build
 * sessions use too (createIsolatedBuildSession). db-duckdb cannot do it
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
/** Owners already released: a connection they create later is never freed. */
const releasedOwners = new WeakSet<object>();
/**
 * Closed connections whose hard close found no handle yet: an open still in
 * flight when the close timed out (duckdb_instance_sharing.ts), which detaches
 * its handle only once it settles.
 */
const awaitingHandle = new WeakSet<object>();
/**
 * Notes a deferred connection collected before its calls drained (a stream its
 * reader abandoned): its handle was left to the GC after all.
 */
const neverDrained = new FinalizationRegistry<string>((name) => {
   logger.debug(
      "A retired DuckDB connection was collected with a call still in flight; its native database was freed by the GC, not the hard close",
      { connection: name },
   );
});

let installed = false;
let available = false;

/**
 * Where a connection's hard close stands: skipped (still open, or no handle
 * yet), deferred (waiting for its calls to drain), disconnected, or failed.
 */
export type HardCloseOutcome =
   | "skipped"
   | "deferred"
   | "disconnected"
   | "failed";

const outcomes = new WeakMap<object, HardCloseOutcome>();

/**
 * The latest outcome of `connection`'s hard close, or undefined if none was
 * requested. Per connection, so a reader is never confused by another
 * connection retired meanwhile.
 */
export function hardCloseOutcome(
   connection: DuckDBConnection,
): HardCloseOutcome | undefined {
   return outcomes.get(connection);
}

/**
 * Every member of db-duckdb's `DuckDBConnection` and `DuckDBCommon`
 * prototypes, as of the version this module was written against. The in-flight
 * count is only a proof if `runDuckDBQuery` and `runSQLStream` are the sole
 * paths to the native handle, so a release that adds a member (an Arrow read,
 * a prepared statement) could open a path the count misses and race a
 * disconnect. Any difference turns the hard close off; duckdb_hard_close.spec.ts
 * fails on the upgrade, to be re-audited and the lists updated.
 */
const DUCKDB_CONNECTION_MEMBERS = [
   "applyFinalBaseline",
   "attachIfShareable",
   "buildInstanceOptions",
   "close",
   "constructor",
   "detachInstance",
   "detachShareableFile",
   "getDigest",
   "idle",
   "init",
   "loadBaselineExtensions",
   "loadExtension",
   "runDuckDBQuery",
   "runSQLStream",
   "setup",
   "setupOnce",
   "shouldApplyEnableExternalAccessAtOpenTime",
];
const DUCKDB_COMMON_MEMBERS = [
   "canPersist",
   "canStream",
   "constructor",
   "dialectName",
   "estimateQueryCost",
   "fetchSelectSchema",
   "fetchTableSchema",
   "fillStructDefFromTypeMap",
   "isPool",
   "manifestTemporaryTable",
   "readQueryOptions",
   "runRawSQL",
   "runSQL",
   "schemaFromQuery",
   "test",
];

function sameMembers(proto: object, expected: readonly string[]): boolean {
   const actual = Object.getOwnPropertyNames(proto).sort();
   return (
      actual.length === expected.length &&
      actual.every((name, i) => name === expected[i])
   );
}

/**
 * Whether db-duckdb's connection classes have exactly the members this module
 * was audited against. Checked once at install; a mismatch leaves every close
 * a refcount close. duckdb_statement_gate.ts relies on the same audit, assuming
 * `setup()` runs before its gate and setup statements go through `runDuckDBQuery`.
 */
export function dbDuckDBHardCloseInternalsMatch(): boolean {
   const proto = DuckDBConnection.prototype;
   const common = Object.getPrototypeOf(proto) as object | null;
   return (
      common !== null &&
      sameMembers(proto, DUCKDB_CONNECTION_MEMBERS) &&
      sameMembers(common, DUCKDB_COMMON_MEMBERS)
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
   if (releasedOwners.has(owner)) {
      // A lookup on a generation already released (a model still holding
      // it). Nothing will release it again, so this one is freed at GC.
      logger.debug(
         "A DuckDB connection was created on a released connection generation; it is freed at GC, not hard-closed",
         { connection: connection.name },
      );
   }
   let owned = ownedBy.get(owner);
   if (!owned) {
      owned = new Set();
      ownedBy.set(owner, owned);
   }
   owned.add(connection);
}

/**
 * Forget that `owner` owns `connection`: it was closed and hard-closed on its
 * own (a DuckLake connection reopened without its catalog), so the owner's
 * release has nothing left to do for it.
 */
export function disownDuckDBConnection(
   owner: object,
   connection: Connection,
): void {
   if (!(connection instanceof DuckDBConnection)) return;
   ownedBy.get(owner)?.delete(connection);
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
      if (internals.closed) awaitingHandle.add(connection);
      // A repeat request (a second release of the same connection) keeps
      // what the first one recorded.
      if (!outcomes.has(connection)) outcomes.set(connection, "skipped");
      return;
   }
   awaitingHandle.delete(connection);
   detachedHandle.delete(connection);
   const token = {};
   const disconnect = () => {
      neverDrained.unregister(token);
      try {
         handle.disconnectSync();
         outcomes.set(connection, "disconnected");
      } catch (error) {
         outcomes.set(connection, "failed");
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
   outcomes.set(connection, "deferred");
   neverDrained.register(connection, connection.name, token);
   onDrained.set(connection, disconnect);
}

/**
 * Retry a hard close that found no handle because the connection's open was
 * still in flight; called once a deferred close has detached it. A connection
 * nobody asked to hard-close is left alone.
 */
export function hardCloseIfAwaitingHandle(connection: DuckDBConnection): void {
   if (awaitingHandle.has(connection)) hardCloseWhenIdle(connection);
}

/**
 * Hard-close every DuckDB connection `owner` owns. Call it after the owner's
 * connections are closed; one still open is skipped.
 */
export function hardCloseOwnedDuckDBConnections(owner: object): void {
   const owned = ownedBy.get(owner);
   releasedOwners.add(owner);
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
