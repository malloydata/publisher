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
import { getDuckDBThreads } from "../config";
import { logger } from "../logger";
import { hardCloseIfAwaitingHandle } from "./duckdb_hard_close";
import {
   differsOnlyInLockedSetup,
   isInstanceLocked,
   markInstanceLocked,
   openTimeLockdownOptions,
   preloadExcel,
   relativeReadEntries,
   runAsOperatorSetup,
} from "./duckdb_lockdown";

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
   connection: { run(sql: string): Promise<unknown> } | null;
   shareKey: string;
   setupError?: Error;
   connecting?: Promise<void>;
   init(): Promise<void>;
   close(): Promise<void>;
   detachInstance(): void;
   buildInstanceOptions(opts?: unknown): Record<string, string>;
   setupOnce(): Promise<void>;
   applyFinalBaseline(): Promise<void>;
   runDuckDBQuery(sql: string): Promise<unknown>;
   connectionSetupRan?: boolean;
}

type ActiveDB = (typeof DuckDBConnection.activeDBs)[string];

/** The open instance holding one file, and the settings it was opened with. */
interface OpenFile {
   instanceKey: string;
   activeDB: ActiveDB;
}

const openFiles = new Map<string, OpenFile>();
/**
 * A first open of a file in flight. A second connection to the same file waits
 * for it rather than opening the file again: both would otherwise find no open
 * instance and each create one.
 */
const pendingOpens = new Map<string, Promise<void>>();
/**
 * The databases Publisher ATTACHed on each instance, by the connection name
 * that attached them. ATTACHed catalogs (and the secrets they use) belong to
 * the instance, not to a session, so a later generation of the same connection
 * on the same instance must replace what an earlier one attached (see
 * {@link recordedAttachments}).
 */
const attachmentsOn = new WeakMap<ActiveDB, Map<string, AttachmentRecord>>();
/** The attach run in progress per instance and connection name. */
const attachRuns = new WeakMap<ActiveDB, Map<string, Promise<unknown>>>();
/** Connection names that have recorded an attachment on any instance. */
const namesWithAttachments = new Set<string>();
let installed = false;

/** How long a close waits for its connection's open before closing anyway. */
const CLOSE_WAIT_MS = 10_000;
let closeWaitMs = CLOSE_WAIT_MS;
/**
 * How long an open waits for a locked instance of the same file to close when
 * the two differ only in their setup (duckdb_lockdown.ts): a connection edit's
 * new generation waits for the old one, which drains within
 * RETIRED_CONNECTION_DRAIN_MS (environment.ts, 30 s), with margin.
 */
const LOCKED_REOPEN_WAIT_MS = 45_000;
let lockedReopenWaitMs = LOCKED_REOPEN_WAIT_MS;

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
 * that instance, and cap each new instance's thread pool. Idempotent;
 * installed once per process (connection.ts).
 */
export function installDuckDBInstanceSharing(): void {
   if (installed) return;
   installed = true;
   const proto =
      DuckDBConnection.prototype as unknown as DuckDBConnectionInternals;
   const init = proto.init;
   const close = proto.close;
   const detachInstance = proto.detachInstance;
   const buildInstanceOptions = proto.buildInstanceOptions;
   const setupOnce = proto.setupOnce;
   const applyFinalBaseline = proto.applyFinalBaseline;
   // This patches `@malloydata/db-duckdb` internals. If a release reshapes
   // them, sharing and the thread cap are skipped (the second-instance
   // limitation and one-per-core pools return) rather than guessed at;
   // duckdb_instance_sharing.spec.ts fails on the upgrade.
   if (!dbDuckDBInternalsMatch()) {
      logger.warn(
         "DuckDB instance sharing and the thread cap are off: @malloydata/db-duckdb's internals changed shape",
      );
      return;
   }

   // DuckDB starts one thread per core per instance, and a process holds many
   // instances: cap each pool unless the connection sets its own `threads`
   // (config.ts, getDuckDBThreads). The shareable-mode `:memory:` primary
   // passes through here too.
   // Every instance also opens under the open-time lockdown
   // (duckdb_lockdown.ts): the host's persistent secrets never read, and
   // none writable.
   proto.buildInstanceOptions = function (
      this: DuckDBConnectionInternals,
      opts?: unknown,
   ): Record<string, string> {
      const restricted =
         this.normalized.lockConfiguration === true ||
         (this.normalized.securityPolicy ?? "none") !== "none";
      return {
         ...withThreadCap(buildInstanceOptions.call(this, opts)),
         ...openTimeLockdownOptions(restricted),
      };
   };

   // An instance Malloy locks (the sandboxed and local tiers) turns external
   // access off before any Publisher code runs on it, after which no extension
   // can load; `excel` is not built in (nor are `json` and `icu` everywhere),
   // so they are loaded here, on the fresh connection, or `.xlsx` reads, and
   // Malloy's own baseline, would break.
   const initThenPreload = function (
      this: DuckDBConnectionInternals,
   ): Promise<void> {
      return init.call(this).then(async () => {
         const restricted =
            this.normalized.lockConfiguration === true ||
            (this.normalized.securityPolicy ?? "none") !== "none";
         const raw = this.connection;
         if (restricted && raw && !this.setupError && !isInstanceLocked(this)) {
            // Per-session settings a later joiner of the locked instance can
            // no longer set; it inherits these instance defaults instead.
            await raw.run("SET GLOBAL TimeZone='UTC'").catch(() => {});
            const workingDirectory = this.normalized.workingDirectory;
            if (typeof workingDirectory === "string") {
               await raw
                  .run(
                     `SET GLOBAL file_search_path='${workingDirectory.replace(/'/g, "''")}'`,
                  )
                  .catch(() => {});
            }
            const files =
               typeof workingDirectory === "string" &&
               Array.isArray(this.normalized.allowedDirectories)
                  ? relativeReadEntries(workingDirectory).files
                  : [];
            if (files.length > 0) {
               // Refused on an instance a sharer already restricted, which
               // already holds the same list.
               await raw
                  .run(
                     `SET allowed_paths=[${files
                        .map((file) => `'${file.replace(/'/g, "''")}'`)
                        .join(",")}]`,
                  )
                  .catch(() => {});
            }
            // Malloy's baseline extensions too: a build that does not link
            // them in (Windows) loads them from disk, which external access,
            // off before Malloy's baseline loads them, would refuse.
            // And `ducklake`, for a catalog kept in a local file under the
            // root (an env connection's setupSQL may attach one). Not on an
            // in-memory instance with no setupSQL (the package sandbox): no
            // setup of its own can attach a catalog, and no connection with
            // one shares it (setupSQL is in the share key). Loading it from
            // disk checks its signature on a burst of short-lived hashing
            // threads, about one per core, on every sandbox a package opens.
            const attachesCatalog =
               this.normalized.databasePath !== ":memory:" ||
               (this.normalized.setupSQL ?? "").trim() !== "";
            for (const extension of [
               "json",
               "icu",
               ...(attachesCatalog ? ["ducklake"] : []),
            ]) {
               await raw.run(`LOAD ${extension}`).catch(() => {});
            }
            await preloadExcel((sql) => raw.run(sql));
         }
      });
   };

   // A connection joining an instance that is already locked (same share key,
   // so the same baseline and post-open setup) skips Malloy's baseline: every
   // SET in it would be refused, and the instance holds its effects, the
   // per-session TimeZone and file_search_path included (set globally before
   // the lock). Its own setupSQL still runs, as on any shared instance: each
   // connection runs its own (see the header), confined like the first.
   proto.setupOnce = async function (
      this: DuckDBConnectionInternals,
   ): Promise<void> {
      if (isInstanceLocked(this)) {
         // Malloy's own split (duckdb_connection.ts, splitSetupSQL).
         const setupSQL = this.normalized.setupSQL ?? "";
         await runAsOperatorSetup(async () => {
            for (const statement of setupSQL
               .split(";\n")
               .map((part) => part.trim())
               .filter((part) => part !== "")) {
               await this.runDuckDBQuery(statement);
            }
         });
         this.connectionSetupRan = true;
         return;
      }
      // Malloy's baseline and the operator's setupSQL, before any caller
      // statement: not held to the one-statement rule (duckdb_lockdown.ts).
      await runAsOperatorSetup(() => setupOnce.call(this));
      if (this.normalized.lockConfiguration === true) markInstanceLocked(this);
   };

   // A confined connection's allowlist also takes its root's cwd-anchored
   // entries, so its relative reads pass (duckdb_lockdown.ts,
   // relativeReadEntries). Malloy sets the allowlist from the normalized
   // options, which key the instance, so the entries ride along for this
   // baseline only and the options are left as they were.
   proto.applyFinalBaseline = async function (
      this: DuckDBConnectionInternals,
   ): Promise<void> {
      const { allowedDirectories, workingDirectory } = this.normalized;
      if (
         !Array.isArray(allowedDirectories) ||
         typeof workingDirectory !== "string"
      ) {
         return applyFinalBaseline.call(this);
      }
      const extra = relativeReadEntries(workingDirectory).directories;
      if (extra.length === 0) return applyFinalBaseline.call(this);
      const normalized = this.normalized;
      this.normalized = {
         ...normalized,
         allowedDirectories: [...allowedDirectories, ...extra],
      };
      try {
         await applyFinalBaseline.call(this);
      } finally {
         this.normalized = normalized;
      }
   };

   proto.init = function (this: DuckDBConnectionInternals): Promise<void> {
      const normalized = this.normalized;
      const init = initThenPreload;
      // Shareable mode opens a private in-memory primary and ATTACHes the file
      // lazily, releasing it on idle; it holds no instance of the file.
      if (normalized.effectiveShareable) return init.call(this);
      const file = canonicalDuckDBFile(normalized.databasePath);
      if (!file) {
         // No file (in-memory, MotherDuck): db-duckdb still shares one
         // instance between connections with the same share key, so a joining
         // connection needs the same pin as a file's.
         const cached = DuckDBConnection.activeDBs[this.shareKey];
         if (!cached) return init.call(this);
         return pinnedWhile(cached, () => init.call(this)).then(() => {});
      }

      const pending = pendingOpens.get(file);
      if (pending) {
         // Another connection is opening this file right now: wait for it,
         // then join its instance (or be refused) as any later open would.
         return pending.then(
            () => proto.init.call(this),
            () => proto.init.call(this),
         );
      }

      const instanceKey = instanceKeyOf(normalized);
      let open = openFiles.get(file);
      if (open && !isLive(open)) {
         // Closed since (or by closeAllInstances): forget it, so a stale entry
         // never pins a closed instance or refuses a fresh open.
         openFiles.delete(file);
         open = undefined;
      }
      if (open && open.instanceKey !== instanceKey) {
         if (differsOnlyInLockedSetup(open.instanceKey, instanceKey)) {
            // A locked instance cannot take a different setup (its catalog
            // ATTACH, its secrets), and two instances of one file are not
            // supported: wait for the generation holding it to drain, then
            // open the file afresh with this connection's setup.
            const held = open;
            return releasedWithin(held, lockedReopenWaitMs).then((released) => {
               if (!released) {
                  this.setupError = new DuckDBInstanceSettingsChangedError(
                     file,
                  );
                  return;
               }
               return proto.init.call(this);
            });
         }
         this.setupError = new DuckDBInstanceSettingsChangedError(file);
         return Promise.resolve();
      }
      if (open) {
         // Malloy's init reads activeDBs synchronously, before its first
         // await, so this alias is seen and the open instance is reused.
         DuckDBConnection.activeDBs[this.shareKey] ??= open.activeDB;
      }
      const opened = (
         open
            ? pinnedWhile(open.activeDB, () => init.call(this))
            : init.call(this).then(() => true)
      ).then((live) => {
         if (!live) return;
         const activeDB = DuckDBConnection.activeDBs[this.shareKey];
         if (activeDB && !this.setupError) {
            openFiles.set(file, { instanceKey, activeDB });
         }
      });
      if (!open) {
         // A first open: hold later opens of this file until it settles.
         const settled = opened.finally(() => {
            if (pendingOpens.get(file) === settled) pendingOpens.delete(file);
         });
         pendingOpens.set(file, settled);
         return settled;
      }
      return opened;
   };

   // db-duckdb starts `init()` in the constructor and its `close()` does not
   // wait for it, so a connection closed before its first query would detach
   // nothing, and the instance its `init()` then registers would stay open
   // with a connection nobody detaches: a leaked instance, and a file lock
   // held until the process exits. The wait is bounded: a connect that hangs
   // (a remote database that never answers) must not hold an environment
   // delete or a shutdown with it. Past the bound the connection closes now,
   // and closes again once its connect settles, to detach what it registered.
   proto.close = async function (
      this: DuckDBConnectionInternals,
   ): Promise<void> {
      const connecting = this.connecting;
      if (connecting && !(await settlesWithin(connecting, closeWaitMs))) {
         logger.warn(
            "Closing a DuckDB connection whose open has not settled; its instance is released when it does",
            { waitedMs: closeWaitMs },
         );
         // A hard close requested meanwhile found no handle to free; the
         // deferred close detaches it, so retry then (duckdb_hard_close.ts).
         const closeLater = () =>
            close
               .call(this)
               .then(() =>
                  hardCloseIfAwaitingHandle(
                     this as unknown as DuckDBConnection,
                  ),
               );
         void connecting.then(closeLater, closeLater);
      }
      return close.call(this);
   };

   proto.detachInstance = function (this: DuckDBConnectionInternals): void {
      const activeDB = DuckDBConnection.activeDBs[this.shareKey];
      detachInstance.call(this);
      if (!activeDB || activeDB.connections.length > 0) return;
      dropAliases(activeDB);
   };
}

/**
 * Run `connect`, a connection's `init()` joining `shared`, while a placeholder
 * in `shared`'s connection list holds the instance open. Malloy adds the
 * joining connection to that list only after `instance.connect()` resolves,
 * so without the placeholder the last other connection detaching meanwhile
 * closes the instance under the pending connect: DuckDB frees the database
 * the connect is using, and the process segfaults. Resolves whether the
 * instance is still open afterwards; when the connect failed and nothing else
 * holds it, it is released, as the last detach would have.
 */
function pinnedWhile(
   shared: ActiveDB,
   connect: () => Promise<void>,
): Promise<boolean> {
   const pin = {} as ActiveDB["connections"][number];
   shared.connections.push(pin);
   const unpin = (): boolean => {
      shared.connections = shared.connections.filter((c) => c !== pin);
      if (shared.connections.length > 0) return true;
      releaseInstance(shared);
      return false;
   };
   return connect().then(unpin, (error: unknown) => {
      unpin();
      throw error;
   });
}

/**
 * Forget an instance the last sharer closed: drop every alias to it, or a
 * later connection would reuse a closed instance.
 */
function dropAliases(activeDB: ActiveDB): void {
   for (const [key, value] of Object.entries(DuckDBConnection.activeDBs)) {
      if (value === activeDB) delete DuckDBConnection.activeDBs[key];
   }
   for (const [file, open] of openFiles) {
      if (open.activeDB === activeDB) openFiles.delete(file);
   }
}

/** Close an instance nothing holds any more, and forget it. */
function releaseInstance(activeDB: ActiveDB): void {
   try {
      activeDB.instance.closeSync();
   } catch {
      // Best effort, as db-duckdb's own cleanup: a close that fails leaves
      // nothing this module can recover.
   }
   dropAliases(activeDB);
}

/**
 * `options` with `threads` set to the configured cap (PUBLISHER_DUCKDB_THREADS)
 * when the connection set none; unchanged under `off`.
 */
export function withThreadCap(
   options: Record<string, string>,
): Record<string, string> {
   const threads = getDuckDBThreads();
   if (options["threads"] === undefined && threads !== undefined) {
      options["threads"] = threads;
   }
   return options;
}

/**
 * Whether `@malloydata/db-duckdb` still has the internals this module patches:
 * an instance cache keyed by share key, and the per-connection `init` and
 * `detachInstance` it wraps.
 */
export function dbDuckDBInternalsMatch(): boolean {
   const proto = DuckDBConnection.prototype as unknown as Partial<
      Record<keyof DuckDBConnectionInternals, unknown>
   >;
   const activeDBs = (DuckDBConnection as unknown as { activeDBs?: unknown })
      .activeDBs;
   return (
      typeof proto.init === "function" &&
      typeof proto.close === "function" &&
      typeof proto.detachInstance === "function" &&
      typeof proto.buildInstanceOptions === "function" &&
      typeof proto.setupOnce === "function" &&
      typeof proto.applyFinalBaseline === "function" &&
      typeof activeDBs === "object" &&
      activeDBs !== null
   );
}

/** The shared instance a connection is on, if it holds one. */
function activeDBOf(connection: unknown): ActiveDB | undefined {
   const shareKey = (connection as Partial<DuckDBConnectionInternals>).shareKey;
   return shareKey ? DuckDBConnection.activeDBs[shareKey] : undefined;
}

/** One database a connection ATTACHed, as {@link AttachmentRecord} holds it. */
export interface RecordedAttachment {
   /** A digest of the settings it was attached with. */
   digest: string;
   /** Its `type`, which names the secret its attach created. */
   type: string;
}

/** What one connection has ATTACHed on an instance. */
export interface AttachmentRecord {
   /**
    * The connection generation (environment config build) that wrote it. An
    * older generation, still draining, never rewrites a newer one's record:
    * it would put back the settings an edit replaced.
    */
   generation: number;
   /** By attached name. */
   databases: ReadonlyMap<string, RecordedAttachment>;
}

/**
 * What a generation of `connectionName` ATTACHed on the instance `connection`
 * is on, or undefined when none has attached anything there in this process.
 *
 * A connection edit keeps the instance (the file stays open, and an edit to
 * the attached databases alone does not even change the share key), so its
 * attachments outlive the generation that made them. A new generation
 * re-ATTACHes each database whose settings changed and DETACHes each one it
 * no longer declares; skipping an already-attached name would keep serving
 * the old credentials, project or host after an edit, and keep a removed
 * attachment queryable, until a restart.
 */
export function recordedAttachments(
   connection: unknown,
   connectionName: string,
): AttachmentRecord | undefined {
   const activeDB = activeDBOf(connection);
   if (!activeDB) return undefined;
   return attachmentsOn.get(activeDB)?.get(connectionName);
}

/**
 * Whether any generation of `connectionName` has ATTACHed a database in this
 * process, so a generation that declares none still has some to detach.
 */
export function mayHoldAttachments(connectionName: string): boolean {
   return namesWithAttachments.has(connectionName);
}

/** Record what `connectionName` has ATTACHed on its instance. */
export function recordAttachments(
   connection: unknown,
   connectionName: string,
   record: AttachmentRecord,
): void {
   const activeDB = activeDBOf(connection);
   if (!activeDB) return;
   let byConnection = attachmentsOn.get(activeDB);
   if (!byConnection) {
      byConnection = new Map();
      attachmentsOn.set(activeDB, byConnection);
   }
   byConnection.set(connectionName, {
      generation: record.generation,
      databases: new Map(record.databases),
   });
   if (record.databases.size > 0) namesWithAttachments.add(connectionName);
}

/**
 * Run `attach` once no other attach run of `connectionName` on the instance
 * `connection` is in progress. Two generations attaching at once would each
 * read the record before the other wrote it, and the slower ATTACH would win
 * with whichever settings it carried. The connection must be open (its
 * instance known); one that holds no shared instance runs at once.
 */
export function serializeAttachRun<T>(
   connection: unknown,
   connectionName: string,
   attach: () => Promise<T>,
): Promise<T> {
   const activeDB = activeDBOf(connection);
   if (!activeDB) return attach();
   let runs = attachRuns.get(activeDB);
   if (!runs) {
      runs = new Map();
      attachRuns.set(activeDB, runs);
   }
   const before = runs.get(connectionName) ?? Promise.resolve();
   const run = before.then(attach, attach);
   const settled = run.catch(() => {});
   runs.set(connectionName, settled);
   void settled.then(() => {
      if (runs.get(connectionName) === settled) runs.delete(connectionName);
   });
   return run;
}

/** Test seam: the files this process holds open through shared instances. */
export function openDuckDBFilesForTesting(): string[] {
   for (const [file, open] of openFiles) {
      if (!isLive(open)) openFiles.delete(file);
   }
   return [...openFiles.keys()];
}

/** Test seam: shorten how long a close waits for an open; undefined resets. */
export function setDuckDBCloseWaitMsForTesting(ms: number | undefined): void {
   closeWaitMs = ms ?? CLOSE_WAIT_MS;
}

/** Test seam: shorten how long a locked reopen waits; undefined resets. */
export function setDuckDBLockedReopenWaitMsForTesting(
   ms: number | undefined,
): void {
   lockedReopenWaitMs = ms ?? LOCKED_REOPEN_WAIT_MS;
}

/** Whether the instance `open` names is released within `ms`. */
async function releasedWithin(open: OpenFile, ms: number): Promise<boolean> {
   const deadline = Date.now() + ms;
   while (isLive(open)) {
      if (Date.now() >= deadline) return false;
      await new Promise<void>((resolve) => {
         const timer = setTimeout(resolve, 100);
         (timer as { unref?: () => void }).unref?.();
      });
   }
   return true;
}

/** Whether `promise` settles, either way, within `ms`. */
function settlesWithin(
   promise: Promise<unknown>,
   ms: number,
): Promise<boolean> {
   let timer: ReturnType<typeof setTimeout> | undefined;
   const timeout = new Promise<boolean>((resolve) => {
      timer = setTimeout(resolve, ms, false);
      (timer as { unref?: () => void }).unref?.();
   });
   return Promise.race([
      promise.then(
         () => true,
         () => true,
      ),
      timeout,
   ]).finally(() => clearTimeout(timer));
}
