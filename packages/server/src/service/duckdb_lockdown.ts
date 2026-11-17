// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The lockdown every DuckDB instance Publisher creates runs under.
 *
 * Caller SQL reaches DuckDB (a raw `sqlQuery`, on the package sandbox too), and
 * so does model text (`duckdb.sql("…")`, `duckdb.table('…')`). With DuckDB's
 * defaults that SQL could read any file the process can, sign requests with the
 * host's ambient cloud credentials, reach the cloud metadata server, load
 * community extensions and write persistent secrets. Two policies close that,
 * by what an instance legitimately needs:
 *
 * - **No network** ({@link sandboxedDuckDBOptions}, {@link localLockedDuckDBOptions}).
 *   The package sandbox runs under `@malloydata/db-duckdb`'s own
 *   `securityPolicy: "sandboxed"`: no network, no extension install, no
 *   `setupSQL`, a locked configuration, neutralized secrets and a filesystem
 *   confined to the package directory. An environment `duckdb` connection with
 *   only `setupSQL` (which the sandbox refuses) gets the same confinement
 *   spelled as options: an allowlist, external access off, then its
 *   `setupSQL`, then the lock. Windows has no sandboxed policy (Malloy refuses
 *   it off POSIX), so the sandbox falls back to `"local"` there: no network
 *   and a locked configuration, but no directory confinement.
 *
 * - **Networked readers** ({@link networkedDuckDBOptions},
 *   {@link lockDownDuckDBSession}). DuckLake catalogs, attached databases,
 *   storage destinations, MotherDuck and build sessions first do their own
 *   setup (their scoped secret, the ATTACH of their own catalog, their
 *   extensions), so the lock follows that setup: the env-seeded S3 settings
 *   cleared, extensions frozen, `allowed_directories` set to exactly the
 *   connection's data paths and its temp directory, external access off
 *   (allowlisted paths still pass), then `lock_configuration`.
 *
 * Every instance also opens with {@link OPEN_TIME_LOCKDOWN}: persistent
 * secrets off (it must be off before any lock, or DuckDB's secret manager
 * wedges). The global S3 configuration is an httpfs option, so it cannot be
 * set at open: every httpfs load Publisher makes turns it off and blanks the
 * env-seeded keys at once ({@link clearAmbientS3Settings}), and the sandboxed
 * and local tiers never load httpfs.
 *
 * The lockdown lives in the connection's NORMALIZED options, never in
 * `setupSQL`, because both instance caches key on those options:
 * `@malloydata/db-duckdb`'s share key and Publisher's own file-sharing key
 * (duckdb_instance_sharing.ts, which ignores `setupSQL`). The sandboxed and
 * local tiers carry it as `securityPolicy`/`allowedDirectories`/
 * `lockConfiguration`; a networked instance carries a private temp directory
 * named by a digest of its tier and its setup, so a locked instance is only
 * ever joined by a connection whose setup it already holds, and instances
 * with different allowlists or policies never share. (`setupSQL` stays out of
 * that digest: each connection runs its own, on a locked instance too.)
 */

import { DuckDBConnection as DuckDBConnectionClass } from "@malloydata/db-duckdb";
import type { DuckDBConnection } from "@malloydata/db-duckdb";
import { createHash } from "crypto";
import * as fs from "fs";
import * as os from "os";
import * as path from "path";
import {
   getDuckDBMemoryLimit,
   getDuckDBTempDirectory,
   getExtensionFetchPolicy,
} from "../config";
import { logger } from "../logger";

/**
 * DuckDB options every Publisher instance opens with (applied in
 * duckdb_instance_sharing.ts's `buildInstanceOptions` patch).
 */
export const OPEN_TIME_LOCKDOWN: Readonly<Record<string, string>> = {
   // Before any lock: a lock taken with persistent secrets on wedges the
   // secret manager. Publisher never writes a persistent secret.
   allow_persistent_secrets: "false",
};

/** The prefix of a networked instance's private temp directory. */
const LOCKED_TEMP_PREFIX = "locked-";

/** Where Publisher's private DuckDB temp directories live. */
export function lockdownTempRoot(): string {
   return path.join(
      getDuckDBTempDirectory() ?? os.tmpdir(),
      "publisher-duckdb",
   );
}

function digest(...parts: string[]): string {
   const hash = createHash("sha256");
   for (const part of parts) {
      hash.update(part);
      hash.update("\0");
   }
   return hash.digest("hex").slice(0, 24);
}

/**
 * A private directory under {@link lockdownTempRoot}, created, and in its
 * canonical spelling (the allowlist compares canonical paths).
 */
function privateTempDirectory(name: string): string {
   const dir = path.join(lockdownTempRoot(), name);
   fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
   return fs.realpathSync.native(dir);
}

function canonicalDirectory(dir: string): string {
   try {
      return fs.realpathSync.native(dir);
   } catch {
      return path.resolve(dir);
   }
}

/**
 * The cwd-anchored allowlist entries a confined connection needs for its
 * RELATIVE reads. DuckDB resolves `duckdb.table('data/x.csv')` through
 * `file_search_path` (the connection's root), but checks the allowlist against
 * the path as written, relative to the process's working directory. So each
 * top-level entry of `root` is allowed as `<cwd>/<entry>`, except a name that
 * already exists under the cwd: allowing it would open a real file or
 * directory outside the root, so a relative read of that name stays refused
 * (an absolute path still reads it). A top-level directory goes in
 * `allowed_directories`; a top-level file in `allowed_paths`, since DuckDB
 * reads a directory entry as a prefix ending in `/`.
 *
 * Computed when the instance opens, from what `root` holds then. Never part
 * of the options: they key the instance, and an entry added to the directory
 * must not make a connection's next generation look like another instance.
 */
export function relativeReadEntries(root: string): {
   directories: string[];
   files: string[];
} {
   const none = { directories: [], files: [] };
   const cwd = canonicalDirectory(process.cwd());
   const canonicalRoot = canonicalDirectory(root);
   if (cwd === canonicalRoot) return none;
   let names: string[];
   try {
      names = fs.readdirSync(canonicalRoot);
   } catch {
      return none;
   }
   const directories: string[] = [];
   const files: string[] = [];
   for (const name of names) {
      const anchored = path.join(cwd, name);
      try {
         fs.lstatSync(anchored);
         continue; // Exists under the cwd (a dangling link included).
      } catch {
         // Free: allowed below.
      }
      let isFile = false;
      try {
         isFile = fs.statSync(path.join(canonicalRoot, name)).isFile();
      } catch {
         continue;
      }
      (isFile ? files : directories).push(anchored);
   }
   return { directories, files };
}

function memoryLimitOption(): { memoryLimit?: string } {
   const memoryLimit = getDuckDBMemoryLimit();
   return memoryLimit === undefined ? {} : { memoryLimit };
}

let warnedNonPosix = false;

/** Whether this host can run Malloy's `sandboxed` policy. */
export function supportsSandboxedPolicy(): boolean {
   return path.sep === "/";
}

/**
 * Options for a DuckDB connection with no network whose data is `root`: the
 * package sandbox. Malloy's `sandboxed` policy on POSIX; `local` (no network,
 * no directory confinement) on Windows, where `sandboxed` is refused.
 */
export function sandboxedDuckDBOptions(root: string): Record<string, unknown> {
   // Named by the package's top-level entries too: the relative-read entries
   // are taken when the instance opens, so a reload that adds one opens a
   // fresh in-memory instance rather than joining one that lacks it.
   const relative = relativeReadEntries(root);
   const temp = privateTempDirectory(
      `sandbox-${digest(
         canonicalDirectory(root),
         ...relative.directories,
         "files:",
         ...relative.files,
      )}`,
   );
   const base = {
      workingDirectory: root,
      tempDirectory: temp,
      ...memoryLimitOption(),
   };
   if (!supportsSandboxedPolicy()) {
      if (!warnedNonPosix) {
         warnedNonPosix = true;
         logger.warn(
            "DuckDB sandboxed policy is POSIX-only: package sandboxes run under the " +
               "'local' policy here (no network, but no directory confinement)",
         );
      }
      return { ...base, securityPolicy: "local" };
   }
   return {
      ...base,
      securityPolicy: "sandboxed",
      allowedDirectories: [root, temp],
   };
}

/**
 * Options for an environment `duckdb` connection with no network that runs
 * `setupSQL`, which the sandboxed policy refuses: the same confinement as
 * options Malloy applies before the `setupSQL` (an allowlist of `root` and a
 * private temp directory, external access off, extensions frozen), then
 * `lock_configuration`. The `setupSQL` runs confined: it cannot reach the
 * network or a file outside `root`, nor load an extension not already loaded.
 */
export function localLockedDuckDBOptions(
   root: string,
): Record<string, unknown> {
   const temp = privateTempDirectory(
      `${LOCKED_TEMP_PREFIX}local-${digest(canonicalDirectory(root))}`,
   );
   return {
      workingDirectory: root,
      tempDirectory: temp,
      allowedDirectories: [root, temp],
      enableExternalAccess: false,
      lockConfiguration: true,
      autoloadKnownExtensions: false,
      autoinstallKnownExtensions: false,
      allowCommunityExtensions: false,
      allowUnsignedExtensions: false,
      ...memoryLimitOption(),
   };
}

/** The kinds of networked instance, which name its private temp directory. */
export type NetworkedDuckDBKind =
   | "ducklake"
   | "attached"
   | "motherduck"
   | "destination";

/**
 * Options that carry a networked instance's lockdown into both share keys: a
 * private temp directory named by a digest of the instance's kind and its
 * setup (`setupIdentity`: whatever its post-open setup attaches and with which
 * credentials, as a stable string, never logged). Two connections share a
 * locked instance only when the instance already holds the setup each needs.
 */
export function networkedDuckDBOptions(
   kind: NetworkedDuckDBKind,
   setupIdentity: string,
): Record<string, unknown> {
   return {
      tempDirectory: privateTempDirectory(
         `${LOCKED_TEMP_PREFIX}${kind}-${digest(kind, setupIdentity)}`,
      ),
   };
}

/**
 * Whether two Publisher instance keys (duckdb_instance_sharing.ts
 * `instanceKeyOf`) differ only in a networked instance's private temp
 * directory: the same file, opened for a connection whose setup changed.
 */
export function differsOnlyInLockedSetup(keyA: string, keyB: string): boolean {
   // The private temp directory appears as `tempDirectory` and, on the local
   // tier, in `allowedDirectories` (and anything derived under it): each
   // spelling of it is replaced by one placeholder before comparing.
   const strip = (key: string): string | undefined => {
      let parsed: unknown;
      try {
         parsed = JSON.parse(key);
      } catch {
         return undefined;
      }
      let locked = false;
      const lockedRoot = (value: string): boolean => {
         if (!path.isAbsolute(value)) return false;
         for (
            let dir = value;
            dir !== path.dirname(dir);
            dir = path.dirname(dir)
         ) {
            if (path.basename(dir).startsWith(LOCKED_TEMP_PREFIX)) return true;
         }
         return false;
      };
      const replace = (value: unknown): unknown => {
         if (typeof value === "string" && lockedRoot(value)) {
            locked = true;
            return "<locked-setup>";
         }
         return Array.isArray(value) ? value.map(replace) : value;
      };
      const rest = replace(parsed);
      return locked ? JSON.stringify(rest) : undefined;
   };
   const a = strip(keyA);
   return a !== undefined && a === strip(keyB);
}

// --- The post-setup lock -----------------------------------------------------

/** Paths each networked connection's own setup says it reads. */
const allowedByConnection = new WeakMap<object, Set<string>>();

/**
 * Record that `connection`'s setup reads `prefix` (a data path, a bucket
 * prefix, a directory), so {@link lockDownDuckDBSession} allows it. Called by
 * the attach that creates the scoped secret or the catalog.
 */
export function allowForLockdown(connection: object, prefix: string): void {
   let allowed = allowedByConnection.get(connection);
   if (!allowed) {
      allowed = new Set();
      allowedByConnection.set(connection, allowed);
   }
   allowed.add(prefix);
}

/** The prefixes recorded for `connection` ({@link allowForLockdown}). */
export function allowedForLockdown(connection: object): string[] {
   return [...(allowedByConnection.get(connection) ?? [])];
}

function sqlString(value: string): string {
   return `'${value.replace(/'/g, "''")}'`;
}

function rowsOf(result: unknown): Record<string, unknown>[] {
   if (Array.isArray(result)) return result as Record<string, unknown>[];
   const rows = (result as { rows?: unknown })?.rows;
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

async function setting(
   connection: DuckDBConnection,
   name: string,
): Promise<unknown> {
   const rows = rowsOf(
      await connection.runSQL(
         `SELECT current_setting(${sqlString(name)}) AS v`,
      ),
   );
   return rows[0]?.v;
}

function isTrue(value: unknown): boolean {
   return value === true || value === "true";
}

/**
 * Native instances whose configuration is locked; a lock is never lifted.
 * Keyed on the instance, not the connection: a later connection joining a
 * locked instance (its share key equal, so its setup already held there)
 * must neither re-run its setup nor issue a `SET`.
 */
const lockedInstances = new WeakSet<object>();

/** The native instance `connection` is on, once it is opened or joined. */
export function duckDBInstanceOf(connection: unknown): object | undefined {
   const shareKey = (connection as { shareKey?: string }).shareKey;
   if (!shareKey) return undefined;
   return DuckDBConnectionClass.activeDBs[shareKey]?.instance as
      | object
      | undefined;
}

/** Record that `connection`'s instance is locked. */
export function markInstanceLocked(connection: unknown): void {
   const instance = duckDBInstanceOf(connection);
   if (instance) lockedInstances.add(instance);
}

/** Whether `connection`'s instance is known to be locked. */
export function isInstanceLocked(connection: unknown): boolean {
   const instance = duckDBInstanceOf(connection);
   return instance !== undefined && lockedInstances.has(instance);
}

/**
 * Whether Publisher must leave `connection`'s configuration alone: Malloy
 * locks it from its options ({@link isLockedByOptions}), or its instance is
 * already locked (a later generation joining a locked instance, whose setup
 * that instance already holds).
 */
export function isConfigurationFrozen(connection: unknown): boolean {
   return isLockedByOptions(connection) || isInstanceLocked(connection);
}

/**
 * Whether Malloy itself locks `connection` (the sandboxed and local tiers),
 * so Publisher must not issue a `SET` on it: its options already carry the
 * limits Publisher would otherwise set.
 */
export function isLockedByOptions(connection: unknown): boolean {
   const normalized = (connection as { normalized?: Record<string, unknown> })
      .normalized;
   return normalized?.lockConfiguration === true;
}

async function loadedExtensions(
   connection: DuckDBConnection,
): Promise<Set<string>> {
   const rows = rowsOf(
      await connection.runSQL(
         "SELECT extension_name FROM duckdb_extensions() WHERE loaded",
      ),
   );
   return new Set(rows.map((row) => String(row.extension_name)));
}

/**
 * Blank the S3 settings httpfs seeds from the AWS_* environment. The global S3
 * configuration is off (so nothing is signed with them), but the values would
 * still read back through `current_setting`. Run after any httpfs load and
 * before a lock; a no-op while httpfs is not loaded.
 */
export async function clearAmbientS3Settings(
   connection: DuckDBConnection,
   { httpfsLoaded = false }: { httpfsLoaded?: boolean } = {},
): Promise<void> {
   if (!httpfsLoaded && !(await loadedExtensions(connection)).has("httpfs")) {
      return;
   }
   // GLOBAL: the instance's value, which every connection on it reads.
   await connection.runSQL("SET GLOBAL enable_global_s3_configuration=false");
   for (const name of [
      "s3_access_key_id",
      "s3_secret_access_key",
      "s3_session_token",
   ]) {
      await connection.runSQL(`SET GLOBAL ${name}=''`);
   }
}

/**
 * Load `excel` while extensions can still load, so `.xlsx` reads keep working
 * once autoload and external access are off (fetching it first under
 * EXTENSION_FETCH_POLICY=on-demand, as autoload would have). Best effort: a
 * host without the extension reads no `.xlsx` either way.
 */
export async function preloadExcel(
   run: (sql: string) => Promise<unknown>,
): Promise<void> {
   try {
      try {
         await run("LOAD excel");
      } catch (error) {
         if (getExtensionFetchPolicy() !== "on-demand") throw error;
         await run("INSTALL excel");
         await run("LOAD excel");
      }
   } catch (error) {
      logger.debug("DuckDB excel extension not preloaded", {
         error: error instanceof Error ? error.message : String(error),
      });
   }
}

/** A directory or URL prefix as an allowlist entry: one trailing slash. */
function asAllowEntry(prefix: string): string {
   if (/^[a-zA-Z][a-zA-Z0-9+.-]*:\/\//.test(prefix)) {
      return prefix.endsWith("/") ? prefix : `${prefix}/`;
   }
   return canonicalDirectory(prefix);
}

/**
 * Lock a networked instance once its own setup is done: what its setup
 * reads stays reachable, nothing else does.
 *
 * In order: the env-seeded S3 settings blanked; `excel` preloaded; extension
 * auto-install and auto-load off (persistent secrets are off from open);
 * `allowed_directories` set to the recorded prefixes ({@link allowForLockdown}),
 * `allow`, and the instance's temp directory; external access off; then
 * `lock_configuration` (unless `lockConfiguration: false`, for a server-owned
 * session whose own steps still set DuckDB options and which runs no caller or
 * author SQL in DuckDB). Idempotent: an instance already locked is verified
 * and left alone. Throws (fails closed) when a step is refused.
 */
export async function lockDownDuckDBSession(
   connection: DuckDBConnection,
   {
      allow = [],
      paths = [],
      lockConfiguration = true,
   }: {
      /** Directories and URL prefixes, beyond those its setup recorded. */
      allow?: string[];
      /** Single files (its own database file and WAL), as `allowed_paths`. */
      paths?: string[];
      lockConfiguration?: boolean;
   } = {},
): Promise<void> {
   if (isInstanceLocked(connection)) return;
   if (isTrue(await setting(connection, "lock_configuration"))) {
      if (isTrue(await setting(connection, "enable_external_access"))) {
         throw new Error(
            "DuckDB instance is locked with external access on; refusing to use it",
         );
      }
      markInstanceLocked(connection);
      return;
   }
   if (!isTrue(await setting(connection, "enable_external_access"))) {
      // External access is already off: the allowlist is fixed. Only the
      // configuration lock is left to take.
      if (lockConfiguration) {
         await pinSessionDefaultsGlobally(connection);
         await connection.runSQL("SET lock_configuration=true");
         markInstanceLocked(connection);
      }
      return;
   }
   await clearAmbientS3Settings(connection);
   await preloadExcel((sql) => connection.runSQL(sql));
   const temp = await setting(connection, "temp_directory");
   const entries = new Set<string>();
   for (const prefix of [...allowedForLockdown(connection), ...allow]) {
      if (prefix) entries.add(asAllowEntry(prefix));
   }
   if (typeof temp === "string" && temp !== "") {
      entries.add(asAllowEntry(temp));
   }
   // Persistent secrets are already off: every instance opens that way
   // (OPEN_TIME_LOCKDOWN), and DuckDB refuses the SET once a secret exists.
   await connection.runSQL("SET autoinstall_known_extensions=false");
   await connection.runSQL("SET autoload_known_extensions=false");
   await connection.runSQL(
      `SET allowed_directories=[${[...entries].map(sqlString).join(",")}]`,
   );
   if (paths.length > 0) {
      await connection.runSQL(
         `SET allowed_paths=[${paths.map(sqlString).join(",")}]`,
      );
   }
   await connection.runSQL("SET enable_external_access=false");
   if (lockConfiguration) {
      await pinSessionDefaultsGlobally(connection);
      await connection.runSQL("SET lock_configuration=true");
      markInstanceLocked(connection);
   }
}

/**
 * Make this session's `TimeZone` (Malloy's UTC) and `file_search_path` the
 * instance's defaults before a lock. Both are per-session settings, and a
 * connection that later joins the locked instance can no longer set them: it
 * inherits the instance's global value instead.
 */
async function pinSessionDefaultsGlobally(
   connection: DuckDBConnection,
): Promise<void> {
   await connection.runSQL("SET GLOBAL TimeZone='UTC'");
   const searchPath = await setting(connection, "file_search_path");
   if (typeof searchPath === "string" && searchPath !== "") {
      await connection.runSQL(
         `SET GLOBAL file_search_path=${sqlString(searchPath)}`,
      );
   }
}

/** Allowlist entries for reads through an attached `s3` or `gcs` secret. */
export function cloudStorageAllowPrefixes(type: string): string[] {
   switch (type) {
      case "s3":
         return ["s3://", "s3a://", "s3n://"];
      case "gcs":
         return ["gs://", "gcs://"];
      default:
         return [];
   }
}

/**
 * The directory of an Azure attachment's configured URL (a SAS URL's or a
 * service principal's file URL, its glob or file name and query dropped): the
 * prefix its reads stay under.
 */
export function azureAllowPrefix(url: string | undefined): string | undefined {
   if (!url) return undefined;
   const query = url.indexOf("?");
   let base = query >= 0 ? url.slice(0, query) : url;
   const lastSlash = base.lastIndexOf("/");
   const last = base.slice(lastSlash + 1);
   if (last.includes("*") || /\.[A-Za-z0-9]+$/.test(last)) {
      base = base.slice(0, lastSlash);
   }
   base = base.replace(/\/\*\*$/, "").replace(/\/+$/, "");
   return base.includes("://") ? `${base}/` : undefined;
}

/** A stable identity string for a setup description (sorted keys). */
export function setupIdentity(value: unknown): string {
   return JSON.stringify(value, (_key, inner) =>
      inner && typeof inner === "object" && !Array.isArray(inner)
         ? Object.fromEntries(
              Object.entries(inner as Record<string, unknown>).sort(
                 ([a], [b]) => (a < b ? -1 : a > b ? 1 : 0),
              ),
           )
         : inner,
   );
}
