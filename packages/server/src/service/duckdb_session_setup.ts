// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * What every Publisher-owned DuckDB session is set up with before it runs a
 * statement of its own: its resource limits, its extension-management pins,
 * the extensions it installs and loads, the string-literal and secret-name
 * escaping its attach statements are built with, and the rows its reads return.
 *
 * A leaf: it imports no connection module, so the connection-type registry
 * (service/dialects/) and connection.ts can both use it.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { Connection } from "@malloydata/malloy";
import {
   getDuckDBMemoryLimit,
   getDuckDBTempDirectory,
   getExtensionFetchPolicy,
} from "../config";
import { logger } from "../logger";
import {
   clearAmbientS3Settings,
   isConfigurationFrozen,
} from "./duckdb_lockdown";

// Sessions whose extension PRAGMAs are already pinned, so the lookup funnel
// (run on every resolve) issues the SETs once per connection. The DuckLake
// tier pins with `alwaysDisableAutoinstall` inside its attach, before the
// funnel sees the connection, so the stronger setting is recorded first and
// never re-evaluated down to policy.
const extensionSessionPinned = new WeakSet<Connection>();

/**
 * Sessions that already carry their resource limits, mapped to their spill
 * directory, so the SETs are not re-issued while a caller that owns a
 * directory can still re-point one set from the global default.
 */
const sessionLimitsApplied = new WeakMap<
   DuckDBConnection,
   string | undefined
>();

/**
 * Bound ONE DuckDB session's memory, and name where it spills.
 *
 * Every Publisher-owned session gets this: DuckDB sizes `memory_limit` per
 * INSTANCE from the container, and Publisher runs several in one process, so
 * unbounded they overcommit in SUM and the kernel kills the process. Both
 * settings are opt-in; see {@link getDuckDBMemoryLimit} for why the limit is
 * absolute.
 *
 * `tempDirectory` is a directory the session owns (a build's disposable
 * working directory), so its spill cannot outlive or collide with another
 * build. It overrides the configured default even when that default was
 * already applied, because an attach may carry the session through the funnel
 * first, without knowing the caller's directory.
 *
 * Failures are not swallowed: a rejected value is an operator error in a
 * resource bound, and continuing would run unbounded while config says
 * otherwise.
 */
export async function applySessionResourceLimits(
   connection: DuckDBConnection,
   { tempDirectory }: { tempDirectory?: string } = {},
): Promise<void> {
   // A locked instance (duckdb_lockdown.ts) takes no SET: Malloy's locked
   // tiers carry the memory limit and temp directory as options, and a
   // networked instance was bounded before its lock.
   if (isConfigurationFrozen(connection)) return;
   const memoryLimit = getDuckDBMemoryLimit();
   // A networked instance's own private temp directory (its options, which
   // key its instance) is where it spills, unless the session names one.
   const ownTemp = (
      connection as unknown as { normalized?: { tempDirectory?: string } }
   ).normalized?.tempDirectory;
   const temp =
      tempDirectory ?? (ownTemp ? undefined : getDuckDBTempDirectory());
   if (sessionLimitsApplied.has(connection)) {
      const applied = sessionLimitsApplied.get(connection);
      // Only a session-owned directory revises an earlier decision; the memory
      // bound is the same either way.
      if (tempDirectory === undefined || tempDirectory === applied) {
         return;
      }
      await connection.runSQL(
         `SET temp_directory = '${escapeSQL(tempDirectory)}'`,
      );
      sessionLimitsApplied.set(connection, tempDirectory);
      return;
   }
   if (temp !== undefined) {
      // Before `memory_limit`: a limit low enough to force spill must never be in
      // effect while the directory is still DuckDB's default.
      await connection.runSQL(`SET temp_directory = '${escapeSQL(temp)}'`);
   }
   if (memoryLimit !== undefined) {
      await connection.runSQL(`SET memory_limit = '${escapeSQL(memoryLimit)}'`);
   }
   sessionLimitsApplied.set(connection, temp);
   logger.debug("Applied DuckDB session resource limits", {
      memoryLimit: memoryLimit ?? "<duckdb default>",
      tempDirectory: temp ?? "<duckdb default>",
   });
}

/**
 * Pin the extension-management PRAGMAs on a Publisher-owned DuckDB session.
 * Publisher installs what it needs explicitly ({@link installAndLoadExtension}),
 * so DuckDB's implicit auto-install is never needed.
 *
 * Auto-load stays on so locally-present extensions lazy-load. Implicit
 * auto-install is turned off when `alwaysDisableAutoinstall` is set (the
 * DuckLake tier needs only its curated set) or under
 * `EXTENSION_FETCH_POLICY=local-only` (air-gapped: no session may reach the
 * network). Under `on-demand` a generic session keeps DuckDB's defaults.
 *
 * Called for every Publisher-owned session (lookup funnel, package sandbox,
 * attach entry points). A failed SET under `local-only` fails closed (throws),
 * since that mode promises no network access; otherwise the pin is
 * defense-in-depth, so it logs and continues.
 */
export async function applyExtensionSessionSettings(
   connection: DuckDBConnection,
   {
      alwaysDisableAutoinstall = false,
   }: { alwaysDisableAutoinstall?: boolean } = {},
): Promise<void> {
   // Before the early return below: every Publisher-owned session reaches this
   // funnel, so this is where "every session is bounded" is enforced.
   await applySessionResourceLimits(connection);
   const policy = getExtensionFetchPolicy();
   const disableAutoinstall =
      alwaysDisableAutoinstall || policy === "local-only";
   if (!disableAutoinstall || extensionSessionPinned.has(connection)) {
      return;
   }
   // A locked instance has auto-install off already (duckdb_lockdown.ts).
   if (isConfigurationFrozen(connection)) {
      extensionSessionPinned.add(connection);
      return;
   }
   try {
      await connection.runSQL("SET autoinstall_known_extensions=false;");
      // DuckDB's default, set explicitly so a baked extension still loads.
      await connection.runSQL("SET autoload_known_extensions=true;");
      extensionSessionPinned.add(connection);
   } catch (error) {
      const detail = error instanceof Error ? error.message : String(error);
      if (policy === "local-only") {
         throw new Error(
            `Failed to disable DuckDB implicit extension auto-install under ` +
               `EXTENSION_FETCH_POLICY=local-only; refusing to open a session that ` +
               `could still fetch from the network. Underlying error: ${detail}`,
         );
      }
      logger.warn("Failed to pin DuckDB extension session settings", {
         error: detail,
      });
   }
}

export async function installAndLoadExtension(
   connection: DuckDBConnection,
   extensionName: string,
   fromCommunity = false,
): Promise<void> {
   const policy = getExtensionFetchPolicy();

   // `local-only` never fetches: it relies on the extension being baked into
   // the image, and a missing one fails the LOAD below loudly.
   if (policy === "on-demand") {
      try {
         // Plain INSTALL only fetches a missing extension. Not FORCE: it
         // re-fetches even a baked extension, drifting the binary past the
         // offline-verified build.
         const installCommand = fromCommunity
            ? `INSTALL ${extensionName} FROM community;`
            : `INSTALL ${extensionName};`;
         await connection.runSQL(installCommand);
         logger.info(`${extensionName} extension installed`);
      } catch (error) {
         logger.info(
            `${extensionName} extension already installed or install skipped`,
            { error },
         );
      }
   }

   try {
      await connection.runSQL(`LOAD ${extensionName};`);
      logger.info(`${extensionName} extension loaded`);
      if (extensionName === "httpfs") {
         // httpfs seeds the S3 settings from the AWS_* environment; blank them
         // so no SQL on this session reads them back (duckdb_lockdown.ts).
         await clearAmbientS3Settings(connection, { httpfsLoaded: true });
      }
   } catch (error) {
      if (policy === "local-only") {
         const detail = error instanceof Error ? error.message : String(error);
         logger.warn("DuckDB extension unavailable under local-only policy", {
            extension: extensionName,
            policy,
            error: detail,
         });
         throw new Error(
            `DuckDB extension '${extensionName}' is not available locally and ` +
               `EXTENSION_FETCH_POLICY=local-only forbids fetching it. Bake the ` +
               `'${extensionName}' extension into the image (or a pre-populated ` +
               `~/.duckdb/extensions directory), or set ` +
               `EXTENSION_FETCH_POLICY=on-demand to allow fetching missing ` +
               `extensions on first use. Underlying LOAD error: ${detail}`,
         );
      }
      throw error;
   }
}

export function sanitizeSecretName(name: string): string {
   return `secret_${name.replace(/[^a-zA-Z0-9_]/g, "_")}`;
}

export function escapeSQL(value: string): string {
   return value.replace(/'/g, "''");
}

/** Rows out of a DuckDBConnection.runSQL result, in either shape it returns. */
export function runSQLRows(result: unknown): Record<string, unknown>[] {
   if (Array.isArray(result)) return result as Record<string, unknown>[];
   const rows = (result as { rows?: unknown }).rows;
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}
