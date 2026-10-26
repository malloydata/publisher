// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import "@malloydata/db-bigquery";
import type { BigQueryConnection } from "@malloydata/db-bigquery";
import "@malloydata/db-databricks";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import "@malloydata/db-duckdb/native";
import "@malloydata/db-mysql";
import type { MySQLConnection } from "@malloydata/db-mysql";
import "@malloydata/db-postgres";
import {
   PooledPostgresConnection,
   type PostgresConnection,
   type PostgresSSLConfig,
} from "@malloydata/db-postgres";
// Registers the "publisher" connection type (proxies SQL to a remote Publisher);
// lookupConnection resolves it through the registry like any other type.
import "@malloydata/db-publisher";
import {
   buildPoolOptions,
   SnowflakeConnection,
} from "@malloydata/db-snowflake";
import "@malloydata/db-sqlserver";
import type { SQLServerConnection } from "@malloydata/db-sqlserver";
import "@malloydata/db-trino";
import type { TrinoConnection } from "@malloydata/db-trino";
import {
   Connection,
   contextOverlay,
   MalloyConfig,
   TableSourceDef,
} from "@malloydata/malloy";
import type { LookupConnection } from "@malloydata/malloy/connection";
import { AxiosError } from "axios";
import { createHash } from "crypto";
import { mkdtempSync, readFileSync, writeFileSync } from "fs";
import fs from "fs/promises";
import os from "os";
import path from "path";
import type { PoolConfig } from "pg";
import tls from "tls";
import { components } from "../api";
import {
   getDuckDBMemoryLimit,
   getDuckLakeRowGroupSizeBytes,
   getDuckLakeTargetFileSizeBytes,
   getDuckDBTempDirectory,
   getExtensionFetchPolicy,
   getPostgresPoolMax,
} from "../config";
import {
   catalogFormatRangeForEngine,
   isCatalogFormatInRange,
} from "../ducklake_version";
import {
   ConnectionNotFoundError,
   ConnectionPoolExhaustedError,
   TableNotFoundError,
   UnconfiguredConnectionError,
   UnsupportedCatalogFormatError,
} from "../errors";
import { logAxiosError, logger } from "../logger";
import { redactConnectionSecretShapes, redactPgSecrets } from "../pg_helpers";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   safeJoinUnderRoot,
} from "../path_safety";
import {
   assembleEnvironmentConnections,
   buildPostgresConnectionString,
   CoreConnectionEntry,
   EnvironmentConnectionMetadata,
   normalizeSnowflakePrivateKey,
   parseServiceAccountKey,
   postgresStatementTimeoutMs,
   PROXIED_SSLMODES,
} from "./connection_config";
import { gcpImpersonationOverlay } from "./gcp_impersonation";
import { assertGoogleCredentialsIsNotADirectory } from "./google_credentials";
import {
   CloudStorageCredentials,
   DEFAULT_S3_CREDENTIAL_CHAIN,
   resolveCloudStorageCredentials,
} from "./gcs_s3_utils";
import { openProxy, type ProxyEndpoint } from "./proxy";
import { quoteIdentifier } from "./quoting";

type AttachedDatabase = components["schemas"]["AttachedDatabase"];
type ApiConnection = components["schemas"]["Connection"];
type ApiConnectionAttributes = components["schemas"]["ConnectionAttributes"];
type ApiConnectionStatus = components["schemas"]["ConnectionStatus"];
type PublisherDuckDBOptions = {
   name: string;
   databasePath?: string;
   workingDirectory?: string;
   motherDuckToken?: string;
};

// The API connection plus the per-type objects that carry passwords and
// connection strings.
export type InternalConnection = ApiConnection & {
   postgresConnection?: components["schemas"]["PostgresConnection"];
   bigqueryConnection?: components["schemas"]["BigqueryConnection"];
   snowflakeConnection?: components["schemas"]["SnowflakeConnection"];
   trinoConnection?: components["schemas"]["TrinoConnection"];
   databricksConnection?: components["schemas"]["DatabricksConnection"];
   mysqlConnection?: components["schemas"]["MysqlConnection"];
   sqlserverConnection?: components["schemas"]["SqlserverConnection"];
   duckdbConnection?: components["schemas"]["DuckdbConnection"];
   ducklakeConnection?: components["schemas"]["DucklakeConnection"];
};

// Sessions whose extension PRAGMAs are already pinned, so the lookup funnel
// (run on every resolve) issues the SETs once per connection. The DuckLake
// tier pins with `alwaysDisableAutoinstall` inside its attach, before the
// funnel sees the connection, so the stronger setting is recorded first and
// never re-evaluated down to policy.
const extensionSessionPinned = new WeakSet<Connection>();

/**
 * Bound the Parquet row group DuckLake buffers before flushing, when configured.
 *
 * DuckLake has only a CATALOG option for this (no session equivalent):
 * `set_option` writes it to `ducklake_metadata`, where every writer of the lake
 * sees it. That is why it is skipped on a read-only attach.
 *
 * Failures are swallowed: a catalog that predates the option, or one this
 * deployment may not re-configure, must still attach; this is a memory
 * optimisation, not a correctness requirement.
 */
export async function applyDuckLakeRowGroupBound(
   connection: DuckDBConnection,
   dbName: string,
): Promise<void> {
   const bytes = getDuckLakeRowGroupSizeBytes();
   if (bytes === undefined) {
      return;
   }
   try {
      // DuckDB refuses ROW_GROUP_SIZE_BYTES (Binder Error) while insertion order
      // is preserved. This SET is GLOBAL to the DuckDB instance; it stays
      // contained because a build gets its own instance from
      // `createIsolatedBuildSession` and a read-only attach never reaches here.
      await connection.runSQL("SET preserve_insertion_order=false");
      await connection.runSQL(
         `CALL ${quoteIdentifier(dbName, "duckdb")}.set_option('parquet_row_group_size_bytes', '${escapeSQL(bytes)}')`,
      );
      logger.info(`DuckLake row group bound applied to ${dbName}: ${bytes}`);
   } catch (error) {
      logger.warn(
         `Could not set the DuckLake row group bound on ${dbName}; the lake keeps ` +
            `its existing value: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

/**
 * Bound how large a Parquet file DuckLake writes before rotating, when configured.
 *
 * Same catalog-option mechanics, read-only skip and swallowed failure as
 * {@link applyDuckLakeRowGroupBound}, without its `preserve_insertion_order`
 * requirement; the two may be applied in either order.
 *
 * It bounds a separate term: writing to object storage, DuckDB holds each
 * multipart part until the file completes, so the row group bounds the
 * per-column buffer and this bounds the file. Setting only one keeps paying the
 * other.
 */
export async function applyDuckLakeTargetFileSize(
   connection: DuckDBConnection,
   dbName: string,
): Promise<void> {
   const bytes = getDuckLakeTargetFileSizeBytes();
   if (bytes === undefined) {
      return;
   }
   try {
      await connection.runSQL(
         `CALL ${quoteIdentifier(dbName, "duckdb")}.set_option('target_file_size', '${escapeSQL(bytes)}')`,
      );
      logger.info(`DuckLake target file size applied to ${dbName}: ${bytes}`);
   } catch (error) {
      logger.warn(
         `Could not set the DuckLake target file size on ${dbName}; the lake keeps ` +
            `its existing value: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

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
   const memoryLimit = getDuckDBMemoryLimit();
   const temp = tempDirectory ?? getDuckDBTempDirectory();
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

async function isDatabaseAttached(
   connection: DuckDBConnection,
   dbName: string,
): Promise<boolean> {
   try {
      const existingDatabases = await connection.runSQL("SHOW DATABASES");
      const rows = Array.isArray(existingDatabases)
         ? existingDatabases
         : existingDatabases.rows || [];

      logger.debug("connection.duckdb.databases.queried", {
         count: rows.length,
      });

      return rows.some((row: Record<string, unknown>) =>
         Object.values(row).some(
            (value) => typeof value === "string" && value === dbName,
         ),
      );
   } catch (error) {
      // Redact in case the error text carries a connection string.
      logger.warn("Failed to check existing databases", {
         error: redactPgSecrets(
            error instanceof Error ? error.message : String(error),
         ),
      });
      return false;
   }
}

function sanitizeSecretName(name: string): string {
   return `secret_${name.replace(/[^a-zA-Z0-9_]/g, "_")}`;
}

export function escapeSQL(value: string): string {
   return value.replace(/'/g, "''");
}

function handleAlreadyAttachedError(error: unknown, dbName: string): void {
   if (error instanceof Error && error.message.includes("already exists")) {
      logger.info(`Database ${dbName} is already attached, skipping`);
   } else {
      throw error;
   }
}

interface BigQueryServiceAccount {
   projectId: string;
   serviceAccountJson: string;
   clientEmail: string;
}

/**
 * Validate a BigQuery connection's service account key and resolve its
 * project. The DuckDB BIGQUERY secret authenticates only with a key, so the
 * read-only attach and the build-time federation both require one.
 */
function parseBigQueryServiceAccount(
   name: string | undefined,
   config: components["schemas"]["BigqueryConnection"],
): BigQueryServiceAccount {
   const keyData = parseServiceAccountKey(config.serviceAccountKeyJson);
   if (keyData) {
      return {
         projectId: (keyData.project_id || config.defaultProjectId) as string,
         serviceAccountJson: config.serviceAccountKeyJson as string,
         clientEmail: keyData.client_email as string,
      };
   }
   throw new Error(
      `BigQuery project_id and service account key required for: ${name}`,
   );
}

/** Load the bigquery extension and create the secret scoped to the project. */
async function createBigQuerySecret(
   connection: DuckDBConnection,
   name: string | undefined,
   { projectId, serviceAccountJson }: BigQueryServiceAccount,
): Promise<string> {
   await installAndLoadExtension(connection, "bigquery", true);

   const secretName = sanitizeSecretName(`bigquery_${name}`);
   const escapedJson = escapeSQL(serviceAccountJson);
   await connection.runSQL(`
      CREATE OR REPLACE SECRET ${secretName} (
         TYPE BIGQUERY,
         SCOPE 'bq://${projectId}',
         SERVICE_ACCOUNT_JSON '${escapedJson}'
      );
   `);
   return secretName;
}

/** Each named Snowflake field must be non-empty. */
function assertSnowflakeFieldsPresent(
   name: string | undefined,
   fields: Record<string, string | null | undefined>,
): void {
   for (const [field, value] of Object.entries(fields)) {
      if (!value) {
         throw new Error(`Snowflake ${field} is required for: ${name}`);
      }
   }
}

/** The SQL-escaped identity fields every Snowflake secret is built from. */
function escapeSnowflakeIdentity(
   config: components["schemas"]["SnowflakeConnection"],
) {
   return {
      account: escapeSQL(config.account || ""),
      user: escapeSQL(config.username || ""),
      database: config.database ? escapeSQL(config.database) : undefined,
      warehouse: config.warehouse ? escapeSQL(config.warehouse) : undefined,
      schema: config.schema ? escapeSQL(config.schema) : undefined,
      role: config.role ? escapeSQL(config.role) : undefined,
   };
}

async function attachBigQuery(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.bigqueryConnection) {
      throw new Error(
         `BigQuery connection configuration missing for: ${attachedDb.name}`,
      );
   }

   const key = parseBigQueryServiceAccount(
      attachedDb.name,
      attachedDb.bigqueryConnection,
   );
   logger.info(`Using service account: ${key.clientEmail}`);
   const { projectId } = key;
   const secretName = await createBigQuerySecret(
      connection,
      attachedDb.name,
      key,
   );
   logger.info(
      `Created BigQuery secret: ${secretName} for project: ${projectId}`,
   );

   const attachCommand = `ATTACH 'project=${projectId}' AS ${attachedDb.name} (TYPE bigquery, READ_ONLY);`;
   await connection.runSQL(attachCommand);
   logger.info(`Successfully attached BigQuery database: ${attachedDb.name}`);
}

async function attachSnowflake(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.snowflakeConnection) {
      throw new Error(
         `Snowflake connection configuration missing for: ${attachedDb.name}`,
      );
   }

   const config = attachedDb.snowflakeConnection;

   assertSnowflakeFieldsPresent(attachedDb.name, {
      account: config.account,
      username: config.username,
      password: config.password,
   });

   await installAndLoadExtension(connection, "snowflake", true);

   try {
      const version = await connection.runSQL("SELECT snowflake_version();");
      logger.info(`Snowflake ADBC driver verified with version:`, version.rows);
   } catch (error) {
      throw new Error(
         `Snowflake ADBC driver verification failed: ${error instanceof Error ? error.message : String(error)}`,
      );
   }

   const params = {
      ...escapeSnowflakeIdentity(config),
      password: escapeSQL(config.password || ""),
   };

   // Optional fields are emitted only when set: an absent one would otherwise
   // interpolate the literal 'undefined', which Snowflake reads as a real
   // database or warehouse name. ROLE and SCHEMA are carried so the attached
   // database queries under the configured role, as the passthrough build does.
   const secretLines = [
      `      TYPE snowflake`,
      `      ACCOUNT '${params.account}'`,
      `      USER '${params.user}'`,
      `      PASSWORD '${params.password}'`,
      ...(params.database ? [`      DATABASE '${params.database}'`] : []),
      ...(params.warehouse ? [`      WAREHOUSE '${params.warehouse}'`] : []),
      ...(params.schema ? [`      SCHEMA '${params.schema}'`] : []),
      ...(params.role ? [`      ROLE '${params.role}'`] : []),
   ];
   const secretString = `CREATE OR REPLACE SECRET ${attachedDb.name}_secret (\n${secretLines.join(",\n")}\n   );`;

   await connection.runSQL(secretString);

   const testresult = await connection.runSQL(
      `SELECT * FROM snowflake_query('SELECT 1', '${attachedDb.name}_secret');`,
   );
   logger.info(`Testing Snowflake connection:`, testresult.rows);

   const attachCommand = `ATTACH '${attachedDb.name}' AS ${attachedDb.name} (TYPE snowflake, SECRET ${attachedDb.name}_secret, READ_ONLY);`;
   await connection.runSQL(attachCommand);
   logger.info(`Successfully attached Snowflake database: ${attachedDb.name}`);
}

// A server switch that sets statement_timeout: `-cstatement_timeout=N` or
// `--statement_timeout=N` (Postgres reads `-` in a switch name as `_`). The
// two-token `-c statement_timeout=N` form is handled where tokens are walked.
const STATEMENT_TIMEOUT_SWITCH = /^(?:-c|--)statement[_-]timeout=/i;

/**
 * libpq `options` with every statement_timeout switch removed and the
 * connection's own appended, so the string carries exactly one. Other switches
 * are kept in order. Splits on unescaped whitespace, as the server does.
 */
function mergeStatementTimeoutOption(
   existing: string,
   ms: number,
): { value: string; replaced: boolean } {
   const tokens = existing.split(/(?<!\\)\s+/).filter((t) => t !== "");
   const kept: string[] = [];
   let replaced = false;
   for (let i = 0; i < tokens.length; i++) {
      if (
         tokens[i] === "-c" &&
         /^statement[_-]timeout=/i.test(tokens[i + 1] ?? "")
      ) {
         i++;
         replaced = true;
      } else if (STATEMENT_TIMEOUT_SWITCH.test(tokens[i])) {
         replaced = true;
      } else {
         kept.push(tokens[i]);
      }
   }
   kept.push("-c", `statement_timeout=${ms}`);
   return { value: kept.join(" "), replaced };
}

/**
 * The key/value pairs of a libpq keyword/value conninfo string, with each pair's
 * span so it can be replaced in place. Values are unquoted and unescaped.
 * Stops at the first token that is not a `key=value` pair.
 */
function conninfoPairs(
   conninfo: string,
): { key: string; value: string; start: number; end: number }[] {
   const pairs: { key: string; value: string; start: number; end: number }[] =
      [];
   // ASCII only, as libpq: a Unicode space inside a password is password data.
   const isSpace = (c: string | undefined) =>
      c !== undefined && /[ \t\n\v\f\r]/.test(c);
   let i = 0;
   while (i < conninfo.length) {
      while (isSpace(conninfo[i])) i++;
      if (i >= conninfo.length) break;
      const start = i;
      let key = "";
      while (
         i < conninfo.length &&
         conninfo[i] !== "=" &&
         !isSpace(conninfo[i])
      ) {
         key += conninfo[i++];
      }
      while (isSpace(conninfo[i])) i++;
      if (conninfo[i] !== "=") break;
      i++;
      while (isSpace(conninfo[i])) i++;
      let value = "";
      if (conninfo[i] === "'") {
         i++;
         while (i < conninfo.length && conninfo[i] !== "'") {
            if (conninfo[i] === "\\" && i + 1 < conninfo.length) i++;
            value += conninfo[i++];
         }
         i++;
      } else {
         while (i < conninfo.length && !isSpace(conninfo[i])) {
            if (conninfo[i] === "\\" && i + 1 < conninfo.length) i++;
            value += conninfo[i++];
         }
      }
      pairs.push({ key, value, start, end: Math.min(i, conninfo.length) });
   }
   return pairs;
}

function decodeUriComponentOrRaw(value: string): string {
   try {
      return decodeURIComponent(value);
   } catch {
      return value;
   }
}

/** libpq has no `no-verify`; "encrypt, don't verify" is `require`. The one spelling every site maps to. */
const LIBPQ_NO_VERIFY_SSLMODE = "require";

const isUriConnectionString = (connectionString: string) =>
   /^postgres(ql)?:\/\//i.test(connectionString);

/** Applies `fn` to a URI's query parameters; the `?` is dropped when none remain. */
function mapUriQuery(uri: string, fn: (params: string[]) => string[]): string {
   const queryStart = uri.indexOf("?");
   const base = queryStart === -1 ? uri : uri.slice(0, queryStart);
   const params = fn(
      queryStart === -1 ? [] : uri.slice(queryStart + 1).split("&"),
   );
   return params.length ? `${base}?${params.join("&")}` : base;
}

/**
 * Makes a raw connectionString's sslmode one libpq accepts: `no-verify` becomes
 * `require`, and a string with no sslmode gets `require` when the deployment's
 * PGSSLMODE is `no-verify`, because libpq reads that variable itself and rejects it.
 * A string naming a libpq `service`, or a deployment `PGSERVICE`, is left alone: its
 * file's sslmode outranks the variable, and an appended `require` would override a
 * stricter one (a service file with no sslmode then fails closed under `no-verify`).
 * In a URI, node-pg-only params libpq rejects are normalized too: `uselibpqcompat` is
 * dropped, and `ssl` yields to an `sslmode` (node-pg's precedence) or is respelled.
 */
function libpqSslmodeInRawString(connectionString: string): string {
   const envNoVerify =
      process.env.PGSSLMODE?.toLowerCase() === "no-verify" &&
      !process.env.PGSERVICE;
   const rewritten = `sslmode=${LIBPQ_NO_VERIFY_SSLMODE}`;
   if (isUriConnectionString(connectionString)) {
      return mapUriQuery(connectionString, (params) => {
         const keys = params.map((param) =>
            decodeUriComponentOrRaw(param.split("=")[0]),
         );
         const hasSslmode = keys.includes("sslmode");
         // node-pg turns ssl=false into verified TLS when a cert param is present.
         const hasCert = keys.some((key) =>
            ["sslrootcert", "sslcert", "sslkey"].includes(key),
         );
         let seen = false;
         const mapped = params.flatMap((param, i) => {
            const [, ...rest] = param.split("=");
            const value = decodeUriComponentOrRaw(rest.join("="));
            if (keys[i] === "service") seen = true;
            if (keys[i] === "uselibpqcompat") return [];
            if (keys[i] === "ssl") {
               if (hasSslmode) return [];
               if (value === "no-verify") {
                  seen = true;
                  return [rewritten];
               }
               if (value === "1") return ["ssl=true"];
               if (value === "0") {
                  if (hasCert) return [];
                  seen = true;
                  return ["sslmode=disable"];
               }
               return [param];
            }
            if (keys[i] !== "sslmode") return [param];
            seen = true;
            return [value === "no-verify" ? rewritten : param];
         });
         return !seen && envNoVerify ? [...mapped, rewritten] : mapped;
      });
   }
   const pairs = conninfoPairs(connectionString);
   let result = connectionString;
   for (const pair of [...pairs].reverse()) {
      if (pair.key === "sslmode" && pair.value === "no-verify") {
         result =
            result.slice(0, pair.start) + rewritten + result.slice(pair.end);
      }
   }
   return !pairs.some(
      (pair) => pair.key === "sslmode" || pair.key === "service",
   ) && envNoVerify
      ? `${result} ${rewritten}`
      : result;
}

/**
 * A raw connectionString with `sslmode=no-verify` rewritten to `require` (node-pg
 * spells "encrypt, don't verify" that way; libpq rejects it) and the connection's
 * statement timeout added, in whichever of libpq's two forms the string is
 * written: a URI takes the timeout as an `options` query parameter, a
 * keyword/value string as an `options` pair.
 *
 * libpq honors only the last `options`, so the timeout is merged into it
 * (adding another would drop the string's other server settings). A
 * statement_timeout the string already sets is replaced with a warning: the
 * explicit field wins, as on the query driver.
 */
function libpqConnectionString(
   name: string,
   rawConnectionString: string,
   pg: components["schemas"]["PostgresConnection"],
): string {
   const connectionString = libpqSslmodeInRawString(rawConnectionString);
   const ms = postgresStatementTimeoutMs(name, pg);
   if (ms === undefined) return connectionString;
   const merged = (existing: string | undefined): string => {
      if (existing === undefined) return `-c statement_timeout=${ms}`;
      const { value, replaced } = mergeStatementTimeoutOption(existing, ms);
      if (replaced) {
         logger.warn(
            `Connection '${name}' sets statementTimeoutMilliseconds and its connectionString's ` +
               `options also set statement_timeout; statementTimeoutMilliseconds applies.`,
         );
      }
      return value;
   };

   if (isUriConnectionString(connectionString)) {
      const isOptions = (param: string) =>
         decodeUriComponentOrRaw(param.split("=", 1)[0]) === "options";
      return mapUriQuery(connectionString, (params) => {
         const last = params.map(isOptions).lastIndexOf(true);
         if (last === -1) {
            return [
               ...params,
               `options=${encodeURIComponent(merged(undefined))}`,
            ];
         }
         const raw = params[last].slice(params[last].indexOf("=") + 1);
         const out = [...params];
         out[last] =
            `options=${encodeURIComponent(merged(decodeUriComponentOrRaw(raw)))}`;
         return out;
      });
   }

   const existing = conninfoPairs(connectionString)
      .filter((pair) => pair.key === "options")
      .pop();
   if (!existing) {
      return `${connectionString} ${pgConninfoPair("options", merged(undefined))}`;
   }
   return (
      connectionString.slice(0, existing.start) +
      pgConninfoPair("options", merged(existing.value)) +
      connectionString.slice(existing.end)
   );
}

/**
 * The libpq conninfo a DuckDB `TYPE postgres` attach opens a postgres connection
 * with.
 *
 * `applyConnectionOptions` decides whether the connection's own `sslmode` and
 * `statementTimeoutMilliseconds` reach the string: false for a DuckLake
 * catalog, true for an attached database and a federated source. When false or
 * unset, TLS comes from the deployment PGSSLMODE. A raw connectionString keeps
 * its own settings, but `sslmode=no-verify` (or an inherited
 * PGSSLMODE=no-verify) becomes `require`, which libpq accepts.
 *
 * Exported for tests.
 */
export function buildPgConnectionString(
   pg: components["schemas"]["PostgresConnection"],
   {
      name,
      applyConnectionOptions,
   }: { name: string; applyConnectionOptions: boolean },
): string {
   if (pg.connectionString) {
      if (!applyConnectionOptions) {
         return libpqSslmodeInRawString(pg.connectionString);
      }
      if (pg.sslmode != null) {
         logger.warn(
            `Connection '${name}' sets both postgresConnection.sslmode and ` +
               `connectionString; sslmode is ignored and the connectionString's own sslmode applies.`,
         );
      }
      return libpqConnectionString(name, pg.connectionString, pg);
   }

   const parts: string[] = [];
   if (pg.host) parts.push(`host=${pg.host}`);
   if (pg.port) parts.push(`port=${pg.port}`);
   if (pg.databaseName) parts.push(`dbname=${pg.databaseName}`);
   if (pg.userName) parts.push(`user=${pg.userName}`);
   if (pg.password) parts.push(`password=${pg.password}`);

   if (applyConnectionOptions) {
      parts.push(...libpqStatementTimeoutParts(name, pg));
      if (pg.sslmode != null) {
         parts.push(
            ...libpqSslmodeParts(
               `Connection '${name}'`,
               pg.sslmode,
               defaultProxiedAttachTrust(),
            ),
         );
         return parts.join(" ");
      }
   }

   const pgSSLMode = process.env.PGSSLMODE;

   if (pgSSLMode) {
      const mapping: Record<string, string> = {
         "no-verify": LIBPQ_NO_VERIFY_SSLMODE,
         disable: "disable",
         allow: "allow",
         prefer: "prefer",
         require: "require",
         "verify-ca": "verify-ca",
         "verify-full": "verify-full",
      };

      const sslmode = mapping[pgSSLMode.toLowerCase()];
      if (sslmode) parts.push(`sslmode=${sslmode}`);
   }

   return parts.join(" ");
}

async function attachPostgres(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.postgresConnection) {
      throw new Error(
         `PostgreSQL connection configuration missing for: ${attachedDb.name}`,
      );
   }
   // Before any SQL is built: an empty alias emits `AS ""`, which DuckDB
   // rejects at a position after the DSN literal, and that error truncates the
   // redactor's anchor away.
   if (!attachedDb.name) {
      throw new Error("Attached database name is required");
   }

   await installAndLoadExtension(connection, "postgres");

   const config = attachedDb.postgresConnection;
   const attachString: string = buildPgConnectionString(config, {
      name: attachedDb.name,
      applyConnectionOptions: true,
   });

   const attachCommand = `ATTACH '${escapeSQL(attachString)}' AS ${quoteIdentifier(attachedDb.name, "duckdb")} (TYPE postgres, READ_ONLY);`;
   await connection.runSQL(attachCommand);
   logger.info(`Successfully attached PostgreSQL database: ${attachedDb.name}`);
}

/** Rows out of a DuckDBConnection.runSQL result, in either shape it returns. */
function runSQLRows(result: unknown): Record<string, unknown>[] {
   if (Array.isArray(result)) return result as Record<string, unknown>[];
   const rows = (result as { rows?: unknown }).rows;
   return Array.isArray(rows) ? (rows as Record<string, unknown>[]) : [];
}

/**
 * Range-preflight for a DuckLake ATTACH (preflightDuckLakeCatalogFormat). Reads
 * the catalog's format version from `ducklake_metadata` through a plain postgres
 * ATTACH (not the DuckLake extension) and checks it against the running
 * engine's range (`ducklake_version.ts`). Out of range throws
 * {@link UnsupportedCatalogFormatError} (HTTP 422) naming the format, the range
 * and where to migrate, instead of an opaque 500 from deep inside DuckDB.
 *
 * Any failure of the preflight itself logs and returns, so the real ATTACH
 * stays the source of truth for unrelated errors.
 *
 * The temp attach name is unique per call so a failed DETACH cannot wedge a
 * fixed name for every later preflight, and concurrent lookups cannot
 * cross-DETACH each other.
 *
 * `metadataSchema` must be the configured `catalog.metadataSchema`: a read in
 * the wrong schema misses silently and the range check stops protecting the
 * catalogs that set it.
 *
 * Returns whether the catalog exists. With a `metadataSchema` it is looked up:
 * no `ducklake_metadata` table there is `absent`, the normal state before the
 * first read-write attach creates it. `unknown` is anything the preflight could
 * not settle, left to the real ATTACH.
 */
export type DuckLakeCatalogPresence = "present" | "absent" | "unknown";

let ducklakePreflightSeq = 0;
async function preflightDuckLakeCatalogFormat(
   connection: DuckDBConnection,
   dbName: string,
   pgConnString: string,
   metadataSchema?: string,
): Promise<DuckLakeCatalogPresence> {
   const tempDb = `${dbName}_fmt_preflight_${++ducklakePreflightSeq}`;
   const tempDbRef = quoteIdentifier(tempDb, "duckdb");
   // Quoted although every name the config validator admits resolves unquoted:
   // this preflight fails soft, so a miss would silently disable range checks.
   // Quoting keeps that correct if the validator is widened. Safe because the
   // validator admits no quote character.
   const metadataRef = metadataSchema
      ? `${tempDbRef}."${metadataSchema}".ducklake_metadata`
      : `${tempDbRef}.ducklake_metadata`;
   let catalogFormat: string | undefined;
   try {
      await connection.runSQL(
         `ATTACH '${escapeSQL(pgConnString)}' AS ${tempDbRef} (TYPE postgres, READ_ONLY);`,
      );
      if (metadataSchema !== undefined) {
         const found = await connection.runSQL(
            `SELECT count(*) AS n FROM duckdb_tables() ` +
               `WHERE database_name = '${escapeSQL(tempDb)}' ` +
               `AND schema_name = '${escapeSQL(metadataSchema)}' ` +
               `AND table_name = 'ducklake_metadata';`,
         );
         if (Number(runSQLRows(found)[0]?.n ?? 0) === 0) {
            return "absent";
         }
      }
      const result = await connection.runSQL(
         `SELECT value FROM ${metadataRef} WHERE key = 'version' LIMIT 1;`,
      );
      const value = runSQLRows(result)[0]?.value;
      catalogFormat = typeof value === "string" ? value : undefined;
   } catch (error) {
      const message = redactPgSecrets(
         error instanceof Error ? error.message : String(error),
      );
      logger.warn(
         "DuckLake catalog-format preflight read failed; falling back to ATTACH",
         { dbName, error: message },
      );
      return "unknown";
   } finally {
      try {
         await connection.runSQL(`DETACH ${tempDbRef};`);
      } catch {
         // The ATTACH may have failed, so there may be nothing to detach.
      }
   }

   if (!catalogFormat) {
      // Table present but no version row, or unreadable value: let ATTACH decide.
      return "present";
   }

   // An engine with no matrix row skips the check; the CI version-contract
   // check guarantees the pinned engine is covered.
   let engineVersion = "";
   try {
      const versionResult = await connection.runSQL("SELECT version() AS v;");
      engineVersion = String(runSQLRows(versionResult)[0]?.v ?? "");
   } catch (error) {
      logger.warn(
         "DuckLake catalog-format preflight: could not read engine version; skipping",
         {
            dbName,
            error: error instanceof Error ? error.message : String(error),
         },
      );
      return "present";
   }

   const range = catalogFormatRangeForEngine(engineVersion);
   if (!range) {
      logger.warn(
         "DuckLake catalog-format preflight: engine not covered by the format matrix; skipping",
         { dbName, engineVersion },
      );
      return "present";
   }

   if (!isCatalogFormatInRange(catalogFormat, range)) {
      logger.warn(
         "DuckLake catalog format outside supported range; rejecting",
         {
            dbName,
            catalogFormat,
            supportedMin: range.min,
            supportedMax: range.max,
            engineVersion: range.engineVersion,
         },
      );
      throw new UnsupportedCatalogFormatError(
         `DuckLake catalog '${dbName}' has format version ${catalogFormat}, ` +
            `which is outside the range this Publisher supports ` +
            `(${range.min} ≤ format ≤ ${range.max}). The DuckLake ` +
            `extension bundled with DuckDB v${range.engineVersion} attaches only ` +
            `catalogs in that range without migration. Migrate the catalog to a ` +
            `supported format (see the DuckLake catalog-migration docs at ` +
            `https://ducklake.select/docs), or run a Publisher built against a ` +
            `DuckDB engine whose supported range includes ${catalogFormat}.`,
      );
   }

   logger.info(
      `DuckLake catalog '${dbName}' format ${catalogFormat} is within supported range ` +
         `${range.min}..${range.max} (engine v${range.engineVersion})`,
   );
   return "present";
}

/**
 * Attach a DuckLake catalog. `readOnly: true` is the live user-facing path;
 * `readOnly: false` is the build variant, used only on a transient build
 * session so a materialization can write into the catalog. Neither passes
 * `AUTOMATIC_MIGRATION`: catalog-format migration is a coordinated fleet
 * cutover, never a side effect of an attach.
 *
 * `skipIfAbsent` is for a caller that only looks: when the preflight finds no
 * catalog it returns `false` without attaching. Otherwise returns `true`.
 */
async function attachDuckLakeWithMode(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
   options: {
      readOnly: boolean;
      skipIfAbsent?: boolean;
      /**
       * First-attach settings, which DuckLake fixes in the catalog when it creates
       * it and ignores on a catalog that already exists. Only a catalog create
       * (the admin API's POST /catalogs) sets them.
       */
      encrypted?: boolean;
      dataInliningRowLimit?: number;
   },
): Promise<boolean> {
   // Never implicitly auto-install, whatever the policy: the DuckLake stack is
   // installed explicitly below.
   await applyExtensionSessionSettings(connection, {
      alwaysDisableAutoinstall: true,
   });
   await installAndLoadExtension(connection, "ducklake");
   await installAndLoadExtension(connection, "postgres");
   await installAndLoadExtension(connection, "aws");
   await installAndLoadExtension(connection, "httpfs");
   if (!ducklakeConfig.catalog?.postgresConnection) {
      throw new Error(
         `PostgreSQL connection configuration is required for DuckLake catalog: ${dbName}`,
      );
   }

   const hasS3 = !!ducklakeConfig.storage?.s3Connection;
   const hasGCS = !!ducklakeConfig.storage?.gcsConnection;

   if (hasS3) {
      await attachCloudStorage(connection, {
         name: `${dbName}_storage`,
         type: "s3",
         s3Connection: ducklakeConfig.storage!.s3Connection,
      });
   } else if (hasGCS) {
      await attachCloudStorage(connection, {
         name: `${dbName}_storage`,
         type: "gcs",
         gcsConnection: ducklakeConfig.storage!.gcsConnection,
      });
   }

   const pg = ducklakeConfig.catalog.postgresConnection;
   const pgConnString: string = buildPgConnectionString(pg, {
      name: dbName,
      applyConnectionOptions: false,
   });
   const mode = options.readOnly ? "READ_ONLY" : "READ_WRITE";
   // These three attach diagnostics stay at debug: `redactPgSecrets` removes the
   // password and URI userinfo but not the catalog host or the DATA_PATH, which
   // would disclose the bucket and prefix to every log reader at info. The mode
   // and outcome are logged at info below.
   logger.debug(`pgConnString: ${redactPgSecrets(pgConnString)}`);
   const escapedPgConnString = escapeSQL(pgConnString);
   logger.debug(
      `Final escaped connection string: ${redactPgSecrets(escapedPgConnString)}`,
   );
   // The data path: given, every attach uses it (and a read-write attach may
   // create the catalog there); omitted, the catalog's recorded path is used and
   // no attach may create one (see `dataPathClauses`).
   const dataPath = ducklakeConfig.storage?.bucketUrl;
   // The schema holding the `ducklake_*` tables; absent keeps DuckLake's default.
   // Validated to a plain identifier at config load, because it reaches a quoted
   // literal here and an identifier position in the preflight.
   const metadataSchema = ducklakeConfig.catalog.metadataSchema;
   const presence = await preflightDuckLakeCatalogFormat(
      connection,
      dbName,
      pgConnString,
      metadataSchema,
   );
   if (presence === "absent") {
      if (options.skipIfAbsent) {
         return false;
      }
      // Normal before the first read-write attach, but also what a mistyped
      // schema looks like, and no attach mode says so (a read-write attach with
      // a data path creates an empty catalog; the others fail without naming the
      // schema).
      logger.warn(
         "No DuckLake catalog found in the configured metadata schema. With a " +
            "configured data path this is expected before the first build, which " +
            "creates it; without one, every attach fails until the catalog is " +
            "created (the admin API's POST /catalogs). Otherwise check the schema " +
            "name against the configuration.",
         { dbName, metadataSchema },
      );
   }
   // Read-write omits the flag: DuckLake's default is writable.
   const readOnlyClause = options.readOnly ? ", READ_ONLY true" : "";
   const metadataSchemaClause = metadataSchema
      ? `, METADATA_SCHEMA '${escapeSQL(metadataSchema)}'`
      : "";
   // Every clause but the data path's leads with ", ", and that one may be
   // empty, so a leading separator is dropped rather than emitted.
   const attachClauses = (
      dataPathClauses(dataPath, options.readOnly) +
      readOnlyClause +
      metadataSchemaClause +
      firstAttachClauses(options)
   ).replace(/^, /, "");
   const attachCommand = `ATTACH OR REPLACE 'ducklake:postgres:${escapedPgConnString}' AS ${quoteIdentifier(dbName, "duckdb")} (${attachClauses});`;
   logger.debug(
      `Attaching DuckLake database using command: ${redactPgSecrets(attachCommand)}`,
   );
   try {
      await connection.runSQL(attachCommand);
      logger.info(
         `Successfully attached DuckLake database in ${mode} mode: ${dbName}`,
      );
      if (!options.readOnly) {
         await applyDuckLakeRowGroupBound(connection, dbName);
         await applyDuckLakeTargetFileSize(connection, dbName);
      }
   } catch (error) {
      if (
         error instanceof Error &&
         (error.message.includes("already exists") ||
            error.message.includes("already attached"))
      ) {
         logger.info(
            `DuckLake database ${dbName} is already attached, skipping`,
         );
      } else {
         throw error;
      }
   }
   return true;
}

/**
 * The settings a catalog attached as `alias` recorded in its `ducklake_metadata`
 * table (`data_path`, `encrypted`, `version`, `data_inlining_row_limit`,
 * `delete_older_than`, …), keyed by name. The metadata tables live in the
 * catalog's metadata schema when one is configured, and in its default schema
 * otherwise.
 */
export async function recordedCatalogSettings(
   connection: DuckDBConnection,
   alias: string,
   metadataSchema: string | undefined,
): Promise<Record<string, string>> {
   const metadataCatalog = [
      `__ducklake_metadata_${alias}`,
      ...(metadataSchema ? [metadataSchema] : []),
   ]
      .map((part) => quoteIdentifier(part, "duckdb"))
      .join(".");
   const rows = runSQLRows(
      await connection.runSQL(
         `SELECT key, value FROM ${metadataCatalog}.ducklake_metadata WHERE scope IS NULL`,
      ),
   );
   const settings: Record<string, string> = {};
   for (const row of rows) {
      if (typeof row.key === "string" && row.value != null) {
         settings[row.key] = String(row.value);
      }
   }
   return settings;
}

/**
 * The data-path half of a DuckLake ATTACH. A configured path is the one every
 * attach uses (`OVERRIDE_DATA_PATH`, so a catalog first created elsewhere still
 * writes under it), and a read-write attach may create the catalog there. With
 * none, DuckLake uses the path the catalog recorded at its creation, and a
 * read-write attach passes `CREATE_IF_NOT_EXISTS false`, so it refuses a missing
 * catalog ("Existing DuckLake ... does not exist") rather than creating one
 * with no path; a read-only attach never creates either way.
 */
export function dataPathClauses(
   dataPath: string | undefined,
   readOnly: boolean,
): string {
   if (dataPath) {
      return `DATA_PATH '${escapeSQL(dataPath)}', OVERRIDE_DATA_PATH true`;
   }
   return readOnly ? "" : "CREATE_IF_NOT_EXISTS false";
}

/**
 * ATTACH options a catalog takes only at its creation. Integers and booleans
 * only, so nothing here is spliced from text.
 */
export function firstAttachClauses(options: {
   encrypted?: boolean;
   dataInliningRowLimit?: number;
}): string {
   let clauses = "";
   if (options.encrypted === true) {
      clauses += ", ENCRYPTED true";
   }
   if (
      options.dataInliningRowLimit !== undefined &&
      Number.isInteger(options.dataInliningRowLimit) &&
      options.dataInliningRowLimit >= 0
   ) {
      clauses += `, DATA_INLINING_ROW_LIMIT ${options.dataInliningRowLimit}`;
   }
   return clauses;
}

/**
 * The attach that creates a catalog (the admin API's POST /catalogs): read-write,
 * with encryption, which DuckLake records in the catalog at its creation. The
 * inlining limit is an ATTACH option for this session only; the create records it
 * in the catalog with `set_option`.
 */
export async function attachDuckLakeForCreate(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
   settings: { encrypted: boolean; dataInliningRowLimit?: number },
): Promise<void> {
   await attachDuckLakeWithMode(connection, dbName, ducklakeConfig, {
      readOnly: false,
      encrypted: settings.encrypted,
      dataInliningRowLimit: settings.dataInliningRowLimit,
   });
}

async function attachDuckLake(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
): Promise<void> {
   // Read-only unless the connection says otherwise in so many words: absent,
   // null and anything but `false` keep the pre-existing read-only attach.
   await attachDuckLakeWithMode(connection, dbName, ducklakeConfig, {
      readOnly: ducklakeConfig.readOnly !== false,
   });
}

/**
 * After a managed DuckLake attach, make the catalog's `main` the session's
 * search path: DuckDB never searches an attached catalog for an unqualified
 * table name, and `<name>.table('t')` compiles to the bare name. The alias is
 * quoted as an identifier inside the string literal the setting takes.
 */
export function managedSearchPathSql(dbName: string): string {
   const alias = `"${dbName.replace(/"/g, '""')}"`;
   return `SET search_path = '${`${alias}.main`.replace(/'/g, "''")}'`;
}

async function setManagedSearchPath(
   connection: DuckDBConnection,
   dbName: string,
): Promise<void> {
   await connection.runSQL(managedSearchPathSql(dbName));
}

/**
 * Read-only DuckLake attach on a caller-owned session, for an operator path that
 * only looks (listing a storage destination's tables).
 */
export async function attachDuckLakeReadOnly(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
): Promise<void> {
   await attachDuckLake(connection, dbName, ducklakeConfig);
}

/**
 * {@link attachDuckLakeReadOnly}, unless the catalog does not exist yet: then it
 * attaches nothing and returns `false`. A catalog without a `metadataSchema` is
 * never found absent (the preflight cannot look it up); its attach decides.
 */
export async function attachDuckLakeReadOnlyIfPresent(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
): Promise<boolean> {
   return attachDuckLakeWithMode(connection, dbName, ducklakeConfig, {
      readOnly: true,
      skipIfAbsent: true,
   });
}

/**
 * Read-write DuckLake attach for a materialization build. Least privilege: only
 * ever used on a transient build session, never the serve connection.
 */
export async function attachDuckLakeReadWrite(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
): Promise<void> {
   await attachDuckLakeWithMode(connection, dbName, ducklakeConfig, {
      readOnly: false,
   });
}

/** A source warehouse type that exposes a native DuckDB query-passthrough. */
export type FederatedSourceType = "bigquery" | "snowflake" | "postgres";

/**
 * The result of federating a source connection onto a build session: the
 * `handle` a passthrough table function needs to reach it —
 *  - bigquery: the resolved project id (`bigquery_query('<project>', '<sql>')`),
 *  - snowflake: the created secret name (`snowflake_query('<sql>', '<secret>')`),
 *  - postgres: the ATTACH alias (`postgres_query('<alias>', '<sql>')`).
 */
export interface FederatedHandle {
   handle: string;
   sourceType: FederatedSourceType;
   /**
    * Releases what the federation holds outside the DuckDB session (the SSH
    * tunnel of a proxied Postgres source), which disposing the session does not
    * close. The build session calls this in its `finally`.
    */
   close?: () => Promise<void>;
}

/**
 * The source connection's configuration needed to federate it onto a build
 * session. `name` is a caller-supplied, already-safe base for the secret/alias
 * identifiers; the type-specific config carries the credentials.
 */
export interface FederationConfig {
   name: string;
   bigqueryConnection?: components["schemas"]["BigqueryConnection"];
   snowflakeConnection?: components["schemas"]["SnowflakeConnection"];
   postgresConnection?: components["schemas"]["PostgresConnection"];
   /**
    * The connection's proxy. A build must tunnel a proxied Postgres source as
    * the query path does, or the passthrough dials the database host the
    * bastion exists to keep unreachable.
    */
   proxy?: components["schemas"]["ConnectionProxy"];
}

/** Injection seam for tests: how a proxied federation opens its tunnel. */
export interface FederationDeps {
   openProxy?: typeof openProxy;
}

/**
 * Establish a source warehouse's credentials on a build-scoped DuckDB session so
 * a native passthrough (`bigquery_query`/`snowflake_query`/`postgres_query`)
 * can push the compiled SELECT to it. Runs only on the transient build session:
 * source credentials are never set on the serve connection nor persisted.
 * BigQuery and Snowflake need only a SECRET; Postgres needs an ATTACH whose
 * alias the function references.
 *
 * @returns the {@link FederatedHandle} the caller passes to `wrapPassthrough`.
 * @throws for a source type without a native passthrough, or missing/invalid
 *   credentials, so a build fails loud rather than producing a wrong table.
 */
export async function federateSourceForPassthrough(
   connection: DuckDBConnection,
   sourceType: FederatedSourceType,
   config: FederationConfig,
   deps: FederationDeps = {},
): Promise<FederatedHandle> {
   switch (sourceType) {
      case "bigquery":
         return federateBigQuery(connection, config);
      case "snowflake":
         return federateSnowflake(connection, config);
      case "postgres":
         return federatePostgres(connection, config, deps);
      default: {
         // Exhaustiveness guard: a new FederatedSourceType must add a branch.
         const exhaustive: never = sourceType;
         throw new Error(
            `No native query-passthrough for source type '${String(exhaustive)}'`,
         );
      }
   }
}

async function federateBigQuery(
   connection: DuckDBConnection,
   config: FederationConfig,
): Promise<FederatedHandle> {
   const bq = config.bigqueryConnection;
   if (!bq) {
      throw new Error(
         `BigQuery connection configuration missing for: ${config.name}`,
      );
   }

   // A build federating from a BigQuery source connection may carry
   // impersonation legitimately (for the serve path). The DuckDB BIGQUERY secret
   // takes a key, not a token, so name that limitation rather than fall through
   // to the generic "service account key required".
   if (bq.impersonateServiceAccount) {
      throw new Error(
         `BigQuery connection '${config.name}' uses impersonateServiceAccount, ` +
            `which cannot federate through DuckDB: the DuckDB BIGQUERY secret ` +
            `authenticates with a service account key, not a token. Federated ` +
            `builds from this source require serviceAccountKeyJson.`,
      );
   }
   const key = parseBigQueryServiceAccount(config.name, bq);
   const { projectId } = key;
   await createBigQuerySecret(connection, config.name, key);
   logger.info(
      `Federated BigQuery source for passthrough: project ${projectId}`,
   );
   // bigquery_query finds the secret scoped to bq://<project>, so the project id
   // is the handle.
   return { handle: projectId, sourceType: "bigquery" };
}

async function federateSnowflake(
   connection: DuckDBConnection,
   config: FederationConfig,
): Promise<FederatedHandle> {
   const sf = config.snowflakeConnection;
   if (!sf) {
      throw new Error(
         `Snowflake connection configuration missing for: ${config.name}`,
      );
   }
   assertSnowflakeFieldsPresent(config.name, {
      account: sf.account,
      username: sf.username,
   });
   // Key pair or password, as a live Snowflake connection accepts; requiring a
   // password would make key-pair connections (Snowflake's preferred
   // programmatic auth) unbuildable.
   const usesKeyPair = !!sf.privateKey;
   if (!usesKeyPair && !sf.password) {
      throw new Error(
         `Snowflake privateKey or password is required for: ${config.name}`,
      );
   }

   await installAndLoadExtension(connection, "snowflake", true);

   const params = {
      ...escapeSnowflakeIdentity(sf),
      password: sf.password ? escapeSQL(sf.password) : undefined,
      // Normalized here because this config is an unnormalized clone (the live
      // path normalizes at its own call site): a single-line PEM makes Go's
      // pem.Decode return nil, and the extension wants PKCS#8 where a user may
      // have pasted PKCS#1.
      privateKey: sf.privateKey
         ? escapeSQL(normalizeSnowflakePrivateKey(sf.privateKey))
         : undefined,
      privateKeyPass: sf.privateKeyPass
         ? escapeSQL(sf.privateKeyPass)
         : undefined,
   };
   const secretName = sanitizeSecretName(`snowflake_${config.name}`);
   // Optional fields are emitted only when supplied, or the literal 'undefined'
   // would be used as a db/warehouse name.
   //
   // ROLE and SCHEMA are part of the connection's identity (its digest).
   // Dropping them would run the build under the user's default role while live
   // queries use the configured one: a build that fails on permissions, or
   // reads under wider ones.
   const secretLines = [
      `   TYPE snowflake`,
      `   ACCOUNT '${params.account}'`,
      `   USER '${params.user}'`,
      // PRIVATE_KEY_PASSWORD, not the documented PRIVATE_KEY_PASSPHRASE, which
      // is only a backward-compatible alias for it.
      ...(usesKeyPair
         ? [
              `   AUTH_TYPE 'key_pair'`,
              `   PRIVATE_KEY '${params.privateKey}'`,
              ...(params.privateKeyPass
                 ? [`   PRIVATE_KEY_PASSWORD '${params.privateKeyPass}'`]
                 : []),
           ]
         : [`   PASSWORD '${params.password}'`]),
      ...(params.database ? [`   DATABASE '${params.database}'`] : []),
      ...(params.warehouse ? [`   WAREHOUSE '${params.warehouse}'`] : []),
      ...(params.schema ? [`   SCHEMA '${params.schema}'`] : []),
      ...(params.role ? [`   ROLE '${params.role}'`] : []),
   ];
   await connection.runSQL(
      `CREATE OR REPLACE SECRET ${secretName} (\n${secretLines.join(",\n")}\n);`,
   );
   logger.info(`Federated Snowflake source for passthrough: ${secretName}`);
   // snowflake_query(sql, secret): the secret name is the handle.
   return { handle: secretName, sourceType: "snowflake" };
}

async function federatePostgres(
   connection: DuckDBConnection,
   config: FederationConfig,
   deps: FederationDeps = {},
): Promise<FederatedHandle> {
   const pg = config.postgresConnection;
   if (!pg) {
      throw new Error(
         `PostgreSQL connection configuration missing for: ${config.name}`,
      );
   }

   await installAndLoadExtension(connection, "postgres");

   // The raw name is the postgres_query handle; the ATTACH identifier is quoted
   // (a hyphenated name would otherwise be a parser error).
   const alias = config.name;

   // A proxied connection is reached only through the SSH tunnel, as on the
   // query path (buildProxiedPostgresConnection). The tunnel is build-scoped,
   // closed by the handle's `close`. It is opened inside the try so any throw
   // before the handle is returned closes it here; otherwise the listener is
   // stranded for the life of the process.
   let endpoint: ProxyEndpoint | undefined;
   try {
      let attachString: string;
      if (config.proxy) {
         if (!pg.host || !pg.port) {
            // validateConnectionShape requires both; guard so the tunnel target
            // is never undefined.
            throw new Error(
               `Connection proxy on '${config.name}' requires explicit host and port on the postgres connection.`,
            );
         }
         endpoint = await (deps.openProxy ?? openProxy)(config.proxy, {
            host: pg.host,
            port: pg.port,
         });
         attachString = buildProxiedPgAttachString(config.name, pg, endpoint);
      } else {
         attachString = buildPgConnectionString(pg, {
            name: config.name,
            applyConnectionOptions: true,
         });
      }
      logger.info(
         `Federating Postgres source for passthrough as alias '${alias}'${endpoint ? " through its SSH proxy" : ""}: ${redactPgSecrets(attachString)}`,
      );
      // OR REPLACE only for idempotency within a session. Isolation between
      // builds and tenants comes from each build's private DuckDB instance
      // (createIsolatedBuildSession), which also scopes the bigquery/snowflake
      // secrets.
      await connection.runSQL(
         `ATTACH OR REPLACE '${escapeSQL(attachString)}' AS ${quoteIdentifier(alias, "duckdb")} (TYPE postgres, READ_ONLY);`,
      );
   } catch (e) {
      // The federation error propagates; a tunnel that fails to close outlives
      // the build, so it is logged rather than lost.
      await endpoint
         ?.close()
         .catch((closeErr) =>
            logger.warn(
               `Failed to close the SSH proxy opened for Postgres source '${config.name}' after its federation failed: ${String(closeErr)}`,
            ),
         );
      throw e;
   }
   return {
      handle: alias,
      sourceType: "postgres",
      ...(endpoint ? { close: () => endpoint!.close() } : {}),
   };
}

/**
 * One libpq conninfo keyword/value pair. libpq takes a bare value only when it
 * has no whitespace, quote or backslash; any other value is single-quoted with
 * `\` and `'` backslash-escaped, so a password containing a space is not read
 * as a second keyword. Exported for tests.
 */
export function pgConninfoPair(key: string, value: string): string {
   if (value !== "" && !/[\s'\\]/.test(value)) return `${key}=${value}`;
   return `${key}='${value.replace(/([\\'])/g, "\\$1")}'`;
}

/**
 * The CA material a proxied storage build hands libpq. Injected so a test needs
 * neither the process environment nor a file on disk.
 */
export interface ProxiedAttachTrust {
   /** The deployment's trusted CA bundle (NODE_EXTRA_CA_CERTS), when set. */
   caBundle?: string;
   /**
    * Path of a bundle holding the runtime's ambient trust anchors — its bundled
    * roots plus `caBundle` — which is the set the query path verifies
    * `verify-full` against.
    */
   ambientBundle: () => string;
}

export function defaultProxiedAttachTrust(): ProxiedAttachTrust {
   return {
      caBundle: process.env.NODE_EXTRA_CA_CERTS || undefined,
      ambientBundle: ambientTrustBundlePath,
   };
}

const ambientTrustBundles = new Map<string | undefined, string>();
let ambientTrustDir: string | undefined;

/**
 * Writes the runtime's ambient trust anchors (`tls.rootCertificates` plus
 * NODE_EXTRA_CA_CERTS) to one PEM file for libpq's `sslrootcert`, which takes
 * exactly one file.
 *
 * Written by this process into a directory it created (`mkdtemp`, 0700), never
 * reused from a path that merely exists: a file at a predictable name would let
 * whoever wrote it choose the CAs a `verify-full` build trusts. An unreadable
 * NODE_EXTRA_CA_CERTS is skipped with a warning, as both runtimes do at
 * startup, so the build trusts what the query path does. Exported for tests.
 */
export function ambientTrustBundlePath(): string {
   const extra = process.env.NODE_EXTRA_CA_CERTS || undefined;
   const cached = ambientTrustBundles.get(extra);
   if (cached) return cached;
   let extraPem: string | undefined;
   if (extra) {
      try {
         extraPem = readFileSync(extra, "utf8").trim();
      } catch (err) {
         logger.warn(
            `Ignoring NODE_EXTRA_CA_CERTS for the storage build's trust bundle: ${extra} could not be read (${err instanceof Error ? err.message : String(err)}).`,
         );
      }
   }
   const pem =
      [...tls.rootCertificates, ...(extraPem ? [extraPem] : [])].join("\n") +
      "\n";
   const digest = createHash("sha256").update(pem).digest("hex").slice(0, 16);
   ambientTrustDir ??= mkdtempSync(
      path.join(os.tmpdir(), "publisher-ambient-ca-"),
   );
   const file = path.join(ambientTrustDir, `${digest}.pem`);
   writeFileSync(file, pem, { mode: 0o600 });
   ambientTrustBundles.set(extra, file);
   return file;
}

type ProxiedSslmode = (typeof PROXIED_SSLMODES)[number];

/**
 * libpq keyword/value string for a Postgres source reached through the SSH
 * tunnel. libpq dials `hostaddr` (the tunnel's local endpoint) but sends `host`
 * (the database's own name) as SNI and checks the certificate against it, so
 * every `sslmode`, `verify-full` included, keeps its meaning through the
 * tunnel. TLS maps as on the query path (resolveProxiedTls):
 *
 *  - unset / `no-verify` → `require`: encrypt without verifying, so a force-SSL
 *    target is not rejected for plaintext;
 *  - `disable` → `disable`;
 *  - `verify-ca` → against the NODE_EXTRA_CA_CERTS bundle;
 *  - `verify-full` → against the ambient trust anchors plus NODE_EXTRA_CA_CERTS,
 *    so a publicly-trusted target builds as it queries.
 *
 * Exported for tests.
 */
export function buildProxiedPgAttachString(
   name: string,
   pg: components["schemas"]["PostgresConnection"],
   endpoint: ProxyEndpoint,
   trust: ProxiedAttachTrust = defaultProxiedAttachTrust(),
): string {
   if (!pg.host) {
      // validateConnectionShape requires host; guard so the certificate is
      // never checked against an empty name.
      throw new Error(
         `Connection proxy on '${name}' requires an explicit host on the postgres connection.`,
      );
   }
   const parts = [
      pgConninfoPair("host", pg.host),
      pgConninfoPair("hostaddr", endpoint.host),
      pgConninfoPair("port", String(endpoint.port)),
   ];
   if (pg.databaseName) parts.push(pgConninfoPair("dbname", pg.databaseName));
   if (pg.userName) parts.push(pgConninfoPair("user", pg.userName));
   if (pg.password) parts.push(pgConninfoPair("password", pg.password));
   parts.push(
      ...libpqSslmodeParts(
         `Connection proxy on '${name}'`,
         pg.sslmode ?? "no-verify",
         trust,
      ),
   );
   parts.push(...libpqStatementTimeoutParts(name, pg));
   return parts.join(" ");
}

/**
 * The libpq conninfo pairs for an explicit sslmode, shared by the proxied and
 * the direct DuckDB attach so a mode means the same thing on both. `subject` is
 * the phrase an error leads with.
 */
function libpqSslmodeParts(
   subject: string,
   mode: ProxiedSslmode,
   trust: ProxiedAttachTrust,
): string[] {
   // Typed against PROXIED_SSLMODES, so a mode added there without a case here
   // is a compile error.
   switch (mode) {
      case "disable":
         return ["sslmode=disable"];
      case "no-verify":
         return [`sslmode=${LIBPQ_NO_VERIFY_SSLMODE}`];
      case "verify-ca": {
         if (!trust.caBundle) {
            throw new Error(
               `${subject} uses sslmode 'verify-ca' but no trusted CA bundle is available (NODE_EXTRA_CA_CERTS is unset).`,
            );
         }
         return [
            "sslmode=verify-ca",
            pgConninfoPair("sslrootcert", trust.caBundle),
         ];
      }
      case "verify-full":
         return [
            "sslmode=verify-full",
            pgConninfoPair("sslrootcert", trust.ambientBundle()),
         ];
      default: {
         const unhandled: never = mode;
         throw new Error(
            `${subject} has unsupported sslmode '${String(unhandled)}' (expected ${PROXIED_SSLMODES.join(" | ")}).`,
         );
      }
   }
}

/**
 * The libpq conninfo pair that applies a connection's statement timeout, as a
 * server option on every session the attach opens. Empty when it sets none.
 */
function libpqStatementTimeoutParts(
   name: string,
   pg: components["schemas"]["PostgresConnection"],
): string[] {
   const ms = postgresStatementTimeoutMs(name, pg);
   return ms === undefined
      ? []
      : [pgConninfoPair("options", `-c statement_timeout=${ms}`)];
}

/** A connection whose object-storage secret can be re-resolved without a reattach. */
interface RenewableStorageSecret {
   setStorageSecretRenewer(renew: () => Promise<void>): void;
}

/**
 * Whether a failed statement should re-resolve the storage credential and retry.
 * Exported so the spec tests this rule rather than a copy of it.
 */
export function shouldRenewStorageSecret(opts: {
   error: unknown;
   hasRenewer: boolean;
   alreadyRenewing: boolean;
}): boolean {
   return (
      opts.hasRenewer &&
      !opts.alreadyRenewing &&
      isExpiredCredentialError(opts.error)
   );
}

/**
 * Whether object storage refused an expired credential, as opposed to a wrong
 * one. Matched narrowly (`ExpiredToken`, S3's code): retrying a never-valid key
 * would hide a misconfiguration behind a silent second attempt.
 */
export function isExpiredCredentialError(error: unknown): boolean {
   const message =
      error instanceof Error
         ? error.message
         : typeof error === "string"
           ? error
           : "";
   return message.includes("ExpiredToken");
}

/**
 * The CREATE OR REPLACE SECRET for a GCS or S3 storage root. Separate from
 * `attachCloudStorage` so the statement can be asserted without a live DuckDB.
 */
export function buildCloudStorageSecretSQL(
   secretName: string,
   credentials: CloudStorageCredentials,
): string {
   const isGCS = credentials.type === "gcs";
   const escapedKeyId = escapeSQL(credentials.accessKeyId);
   const escapedSecret = escapeSQL(credentials.secretAccessKey);

   let createSecretCommand: string;

   if (isGCS) {
      createSecretCommand = `
         CREATE OR REPLACE SECRET ${secretName} (
            TYPE gcs,
            KEY_ID '${escapedKeyId}',
            SECRET '${escapedSecret}'
         );
      `;
   } else {
      // The schema carries no `default:` (a generated client would not apply
      // it), so the fallback lives here. Warned because a bucket in another
      // region then fails as "no such bucket", which reads as a wrong name.
      const region = credentials.region || "us-east-1";
      if (!credentials.region) {
         logger.warn(
            "S3 connection has no region; defaulting to us-east-1. A bucket in " +
               "another region will not be found.",
            { secretName },
         );
      }

      if (credentials.provider === "credential_chain") {
         // CHAIN is required: omitted, DuckDB resolves against `config` alone,
         // which cannot work in a container.
         const chain = escapeSQL(
            credentials.chain?.trim() || DEFAULT_S3_CREDENTIAL_CHAIN,
         );
         // A chain secret stores the credentials it resolved, so a temporary
         // credential (web-identity assume-role: an hour by default) expires
         // while a serve-path secret lives on. `auto` is the only value that
         // arms refresh (the aws extension stores `refresh_info` only for it;
         // any other value silently does nothing). Not caller-configurable: a
         // frozen snapshot of an expiring credential has no use.
         const clauses = [
            "TYPE s3",
            "PROVIDER credential_chain",
            `CHAIN '${chain}'`,
            "REFRESH 'auto'",
            `REGION '${region}'`,
         ];
         // Additive: an S3-compatible endpoint behind a host role is a real
         // combination.
         if (credentials.endpoint) {
            clauses.push(
               `ENDPOINT '${escapeSQL(credentials.endpoint)}'`,
               "URL_STYLE 'path'",
            );
         }
         createSecretCommand = `
            CREATE OR REPLACE SECRET ${secretName} (
               ${clauses.join(",\n               ")}
            );
         `;
      } else if (credentials.endpoint) {
         const escapedEndpoint = escapeSQL(credentials.endpoint);
         createSecretCommand = `
            CREATE OR REPLACE SECRET ${secretName} (
               TYPE s3,
               KEY_ID '${escapedKeyId}',
               SECRET '${escapedSecret}',
               REGION '${region}',
               ENDPOINT '${escapedEndpoint}',
               URL_STYLE 'path'
            );
         `;
      } else if (credentials.sessionToken) {
         const escapedToken = escapeSQL(credentials.sessionToken);
         createSecretCommand = `
            CREATE OR REPLACE SECRET ${secretName} (
               TYPE s3,
               KEY_ID '${escapedKeyId}',
               SECRET '${escapedSecret}',
               REGION '${region}',
               SESSION_TOKEN '${escapedToken}'
            );
         `;
      } else {
         createSecretCommand = `
            CREATE OR REPLACE SECRET ${secretName} (
               TYPE s3,
               KEY_ID '${escapedKeyId}',
               SECRET '${escapedSecret}',
               REGION '${region}'
            );
         `;
      }
   }

   return createSecretCommand;
}

async function attachCloudStorage(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   const storageType = attachedDb.type?.toUpperCase() || "";
   const credentials = resolveCloudStorageCredentials(attachedDb);

   await installAndLoadExtension(connection, "httpfs");
   // `credential_chain` is implemented by the `aws` extension and resolves when
   // the secret is created, so load it first (the generic attach path does not
   // otherwise). `aws` is baked into the image, so local-only policy holds.
   if (credentials.provider === "credential_chain") {
      await installAndLoadExtension(connection, "aws");
   }

   const secretName = sanitizeSecretName(
      `${attachedDb.type}_${attachedDb.name}`,
   );
   const createSecretCommand = buildCloudStorageSecretSQL(
      secretName,
      credentials,
   );

   if (await doesSecretExistInDuckDB(connection, secretName)) {
      // Force-refresh attachments using this storage.
      await connection.runSQL(`DETACH ${attachedDb.name};`).catch(() => {});
   }
   await connection.runSQL(createSecretCommand);

   // Only a chain secret goes stale; a renewer for a key pair would only retry a
   // wrong key. This backs up `REFRESH 'auto'`, which does not reach every read
   // path: reads through an attached DuckLake catalog have surfaced ExpiredToken
   // from an armed secret. Structural check, because only some of the
   // connection types reaching here can renew.
   const renewable = connection as unknown as Partial<RenewableStorageSecret>;
   if (
      credentials.provider === "credential_chain" &&
      typeof renewable.setStorageSecretRenewer === "function"
   ) {
      renewable.setStorageSecretRenewer(async () => {
         // Replaced in place on the live connection; no in-flight query is
         // disturbed.
         await connection.runSQL(
            buildCloudStorageSecretSQL(secretName, credentials),
         );
      });
   }

   logger.info(`Created ${storageType} secret: ${secretName}`);
   logger.info(`${storageType} connection configured for: ${attachedDb.name}`);
}

async function attachAzureStorage(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.azureConnection) {
      throw new Error(
         `Azure connection configuration missing for: ${attachedDb.name}`,
      );
   }

   const config = attachedDb.azureConnection;

   // Extensions are loaded once in attachDatabasesToDuckDB.
   const secretName = sanitizeSecretName(`azure_${attachedDb.name}`);

   let createSecretCommand: string;

   if (config.authType === "service_principal") {
      if (
         !config.tenantId ||
         !config.clientId ||
         !config.clientSecret ||
         !config.accountName
      ) {
         throw new Error(
            `Azure SPN auth requires tenantId, clientId, clientSecret, and accountName for: ${attachedDb.name}`,
         );
      }

      const escapedTenantId = escapeSQL(config.tenantId);
      const escapedClientId = escapeSQL(config.clientId);
      const escapedClientSecret = escapeSQL(config.clientSecret);
      const escapedAccountName = escapeSQL(config.accountName);

      createSecretCommand = `
         CREATE OR REPLACE SECRET ${secretName} (
            TYPE azure,
            PROVIDER service_principal,
            TENANT_ID '${escapedTenantId}',
            CLIENT_ID '${escapedClientId}',
            CLIENT_SECRET '${escapedClientSecret}',
            ACCOUNT_NAME '${escapedAccountName}'
         );
      `;
   } else if (config.authType === "sas_token") {
      if (!config.sasUrl) {
         throw new Error(
            `Azure SAS token auth requires sasUrl for: ${attachedDb.name}`,
         );
      }

      // httpfs reads the HTTPS SAS URL directly; no secret is needed.
      logger.info(
         `Azure SAS token configured for: ${attachedDb.name} (no secret needed, using direct URL)`,
      );
      return;
   } else {
      throw new Error(
         `Unsupported Azure auth type: ${config.authType} for: ${attachedDb.name}`,
      );
   }

   if (await doesSecretExistInDuckDB(connection, secretName)) {
      await connection.runSQL(`DETACH ${attachedDb.name};`).catch(() => {});
   }
   await connection.runSQL(createSecretCommand);

   logger.info(`Created Azure secret: ${secretName}`);
   logger.info(`Azure ADLS connection configured for: ${attachedDb.name}`);
}

async function doesSecretExistInDuckDB(
   connection: DuckDBConnection,
   secretName: string,
): Promise<boolean> {
   const escapedSecretName = escapeSQL(secretName);
   const result = await connection.runSQL(`
     SELECT COUNT(*) AS count
     FROM duckdb_secrets()
     WHERE name = '${escapedSecretName}';
   `);
   const rows = result.rows;
   return Number(rows?.[0]?.count ?? 0) > 0;
}

async function attachDatabasesToDuckDB(
   duckdbConnection: DuckDBConnection,
   attachedDatabases: AttachedDatabase[],
): Promise<void> {
   const attachHandlers = {
      bigquery: attachBigQuery,
      snowflake: attachSnowflake,
      postgres: attachPostgres,
      gcs: attachCloudStorage,
      s3: attachCloudStorage,
      azure: attachAzureStorage,
   };

   // Generic attach session: autoinstall follows the deployment policy.
   await applyExtensionSessionSettings(duckdbConnection);

   const hasAzure = attachedDatabases.some((db) => db.type === "azure");
   if (hasAzure) {
      await installAndLoadExtension(duckdbConnection, "azure");
      await installAndLoadExtension(duckdbConnection, "httpfs");
   }

   for (const attachedDb of attachedDatabases) {
      try {
         if (
            await isDatabaseAttached(duckdbConnection, attachedDb.name || "")
         ) {
            logger.info(
               `Database ${attachedDb.name} is already attached, skipping`,
            );
            continue;
         }

         const handler =
            attachHandlers[attachedDb.type as keyof typeof attachHandlers];
         if (!handler) {
            throw new Error(`Unsupported database type: ${attachedDb.type}`);
         }

         try {
            await handler(duckdbConnection, attachedDb);
         } catch (attachError) {
            handleAlreadyAttachedError(attachError, attachedDb.name || "");
         }
      } catch (error) {
         // Attach errors echo the connection string (DuckDB embeds the DSN).
         logger.error(`Failed to attach database ${attachedDb.name}`, {
            error: redactPgSecrets(
               error instanceof Error ? error.message : String(error),
            ),
         });
         throw new Error(
            `Failed to attach database ${attachedDb.name}: ${(error as Error).message}`,
         );
      }
   }
}

type ApiAzureConnection = components["schemas"]["AzureConnection"];

/**
 * Builds the actual Azure URL for a blob given an AzureConnection config.
 * Strips any glob pattern from the base URL and appends the blob name.
 */
function buildAzureFileUrl(
   azureConn: ApiAzureConnection,
   blobName: string,
): string {
   if (azureConn.authType === "sas_token" && azureConn.sasUrl) {
      const qIdx = azureConn.sasUrl.indexOf("?");
      const baseUrl =
         qIdx >= 0 ? azureConn.sasUrl.substring(0, qIdx) : azureConn.sasUrl;
      const token = qIdx >= 0 ? azureConn.sasUrl.substring(qIdx) : "";
      // A single-file URL has its filename replaced rather than appended to.
      if (/\.(parquet|csv|json|jsonl|ndjson)$/i.test(baseUrl)) {
         const dir = baseUrl.replace(/\/[^/]+$/, "");
         return `${dir}/${blobName}${token}`;
      }
      const cleanBase = baseUrl.replace(/\/\*[^/]*$/, "").replace(/\/+$/, "");
      return `${cleanBase}/${blobName}${token}`;
   } else if (azureConn.authType === "service_principal" && azureConn.fileUrl) {
      const url = azureConn.fileUrl;
      if (/\.(parquet|csv|json|jsonl|ndjson)$/i.test(url)) {
         return url.replace(/\/[^/]+$/, `/${blobName}`);
      }
      const base = url.replace(/\*[^/]*$/, "").replace(/\/+$/, "");
      return `${base}/${blobName}`;
   }
   throw new Error(
      `Cannot build Azure file URL: missing sasUrl or fileUrl in config`,
   );
}

/**
 * Resolves Azure attached-database table paths: `azure_schema.blob_name` in
 * fetchTableSchema becomes the real Azure URL for the azure/httpfs extension.
 */
class AzureDuckDBConnection extends DuckDBConnection {
   private azureDatabases: AttachedDatabase[];

   constructor(
      options: PublisherDuckDBOptions,
      azureDatabases: AttachedDatabase[],
   ) {
      super(options);
      this.azureDatabases = azureDatabases;
   }

   async fetchTableSchema(
      tableKey: string,
      tablePath: string,
   ): Promise<TableSourceDef> {
      const dotIdx = tablePath.indexOf(".");
      if (dotIdx > 0) {
         const schemaName = tablePath.substring(0, dotIdx);
         const blobName = tablePath.substring(dotIdx + 1);

         const azureDb = this.azureDatabases.find(
            (db) =>
               db.type === "azure" &&
               db.name === schemaName &&
               db.azureConnection,
         );

         if (azureDb) {
            const azureUrl = buildAzureFileUrl(
               azureDb.azureConnection!,
               blobName,
            );
            logger.debug("Resolved Azure table path", {
               original: tablePath,
               resolved: azureUrl,
            });
            const result = await super.fetchTableSchema(tableKey, azureUrl);
            if (!result) {
               throw new TableNotFoundError(
                  `Azure file not found: ${azureUrl}`,
               );
            }
            return result;
         }
      }

      const result = await super.fetchTableSchema(tableKey, tablePath);
      if (!result) {
         throw new TableNotFoundError(`Table ${tablePath} not found`);
      }
      return result;
   }
}

class DuckLakeConnection extends DuckDBConnection {
   private connectionName: string;
   private storageSecretRenewer?: () => Promise<void>;
   private renewingStorageSecret = false;

   /** @see RenewableStorageSecret */
   setStorageSecretRenewer(renew: () => Promise<void>): void {
      this.storageSecretRenewer = renew;
   }

   /**
    * Runs SQL, re-resolving an expired object-storage credential once and retrying.
    *
    * The serve secret lives as long as the connection while a chain secret's
    * resolved credentials last about an hour. `REFRESH 'auto'` is the first
    * defence; this is the second. Without it a serve connection fails every read
    * with ExpiredToken after an hour while builds (fresh sessions) keep working.
    *
    * Reactive because resolving costs an STS round trip: per query that is a
    * call per read, and a timer needs a TTL nobody reports. The error is the
    * authoritative signal. Only once: a second failure is not an expiry.
    */
   async runSQL(
      sql: string,
      options?: Parameters<DuckDBConnection["runSQL"]>[1],
   ): Promise<Awaited<ReturnType<DuckDBConnection["runSQL"]>>> {
      try {
         return await super.runSQL(sql, options);
      } catch (error) {
         // The renewal's own statement runs through this method; the flag stops
         // a failure there from recursing.
         const renew = this.storageSecretRenewer;
         if (
            !shouldRenewStorageSecret({
               error,
               hasRenewer: !!renew,
               alreadyRenewing: this.renewingStorageSecret,
            }) ||
            !renew
         ) {
            throw error;
         }
         logger.info(
            "Object-storage credential expired; re-resolving and retrying once",
            { connection: this.connectionName },
         );
         this.renewingStorageSecret = true;
         try {
            await renew();
         } finally {
            this.renewingStorageSecret = false;
         }
         return await super.runSQL(sql, options);
      }
   }

   constructor(options: PublisherDuckDBOptions) {
      super(options);

      if (!options.databasePath?.endsWith("_ducklake.duckdb")) {
         throw new Error(
            `DuckLakeConnection should only be used for DuckLake connections. ` +
               `Expected database path ending with '_ducklake.duckdb', got: ${options.databasePath}`,
         );
      }

      this.connectionName = options.name;
   }

   async fetchTableSchema(
      tableKey: string,
      tablePath: string,
   ): Promise<TableSourceDef> {
      // A one- or two-part path is relative to this catalog.
      const parts = tablePath.split(".");
      if (
         !tablePath.startsWith(this.connectionName) &&
         (parts.length === 1 || parts.length === 2)
      ) {
         const prefixedPath = `${this.connectionName}.${tablePath}`;
         logger.debug("Prefixing DuckLake table path", {
            original: tablePath,
            prefixed: prefixedPath,
            connectionName: this.connectionName,
         });
         const result = await super.fetchTableSchema(tableKey, prefixedPath);
         if (!result) {
            throw new TableNotFoundError(
               `Table ${prefixedPath} not found in connection ${this.connectionName}`,
            );
         }
         return result;
      }
      const result = await super.fetchTableSchema(tableKey, tablePath);
      if (!result) {
         throw new TableNotFoundError(
            `Table ${tablePath} not found in connection ${this.connectionName}`,
         );
      }
      return result;
   }
}

export async function deleteDuckLakeConnectionFile(
   connectionName: string,
   environmentPath: string,
): Promise<void> {
   assertSafePackageName(connectionName);
   assertSafeEnvironmentPath(environmentPath);
   const ducklakePath = safeJoinUnderRoot(
      environmentPath,
      `${connectionName}_ducklake.duckdb`,
   );
   try {
      await fs.access(ducklakePath);
      await fs.rm(ducklakePath);
      logger.info(
         `Removed DuckLake connection file ${connectionName}_ducklake.duckdb from ${environmentPath}`,
      );
   } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
         logger.debug(
            `DuckLake connection file ${connectionName}_ducklake.duckdb does not exist, skipping deletion`,
         );
      } else {
         logger.error(
            `Failed to remove DuckLake connection file ${connectionName}_ducklake.duckdb from ${environmentPath}`,
            { error },
         );
      }
   }
}

/**
 * A config that resolves `allowed` connection names and nothing else, delegating
 * those to `base`.
 *
 * Used for the materialization serve-shape compile, which may resolve only the
 * destinations its bindings name: never a user connection, nor a destination it
 * has no binding for. Deriving the set from the bindings keeps the reachable
 * surface minimal by construction.
 */
export function restrictMalloyConfigToConnections(
   base: MalloyConfig,
   allowed: ReadonlySet<string>,
): MalloyConfig {
   const restricted = new MalloyConfig({ connections: {} });
   restricted.wrapConnections(() => ({
      lookupConnection: async (name?: string) => {
         if (!name || !allowed.has(name)) {
            throw new ConnectionNotFoundError(
               `Connection ${name ?? "(default)"} is not available to a materialization serve shape`,
            );
         }
         return base.connections.lookupConnection(name);
      },
   }));
   return restricted;
}

export type EnvironmentMalloyConfig = {
   malloyConfig: MalloyConfig;
   apiConnections: InternalConnection[];
   // Releases both core-managed connections and Publisher wrapper-managed
   // DuckDB connections captured by wrapConnections.
   releaseConnections: () => Promise<void>;
};

function entryToDuckDBOptions(
   name: string,
   entry: CoreConnectionEntry,
   workingDirectory?: string,
): PublisherDuckDBOptions {
   const { is: _is, ...rest } = entry;
   if (workingDirectory !== undefined) {
      rest.workingDirectory = workingDirectory;
   }
   return { ...removeUndefined(rest), name };
}

/**
 * Drop keys whose value is `undefined` or `null`.
 *
 * The null guard for connectors Publisher builds itself, outside Malloy's
 * registry (which strips nulls on its own path). A surviving `null` reaches
 * `makeDigest`, which reads `.length` off each part, and throws on the first
 * digest; the package-load worker takes that digest, so the whole package fails
 * to load ("import reference failure") while /compile still succeeds.
 *
 * Nulls come from an explicit `null` in publisher.config.json or a client
 * serializing unset optionals over POST /connections, and persist through the
 * repository's JSON round trip. An explicit null and an omitted field produce
 * the same digest.
 */
function removeUndefined<T extends object>(
   value: T,
): { [K in keyof T]?: Exclude<T[K], null> } {
   return Object.fromEntries(
      Object.entries(value).filter(
         ([, fieldValue]) => fieldValue !== undefined && fieldValue !== null,
      ),
   ) as { [K in keyof T]?: Exclude<T[K], null> };
}

function buildSnowflakePrivateKeyConnection(
   metadata: EnvironmentConnectionMetadata,
): SnowflakeConnection {
   const name = metadata.apiConnection.name!;
   const snowflake = metadata.apiConnection.snowflakeConnection;
   if (!snowflake?.privateKey) {
      throw new Error(
         `Snowflake private key is required for connection ${name}`,
      );
   }
   if (!snowflake.account) {
      throw new Error(`Snowflake account is required for connection ${name}`);
   }
   if (!snowflake.username) {
      throw new Error(`Snowflake username is required for connection ${name}`);
   }
   if (!snowflake.warehouse) {
      throw new Error(`Snowflake warehouse is required for connection ${name}`);
   }

   return new SnowflakeConnection(name, {
      connOptions: {
         account: snowflake.account,
         username: snowflake.username,
         privateKey: normalizeSnowflakePrivateKey(snowflake.privateKey),
         authenticator: "SNOWFLAKE_JWT",
         warehouse: snowflake.warehouse,
         ...removeUndefined({
            password: snowflake.password,
            privateKeyPass: snowflake.privateKeyPass,
            database: snowflake.database,
            schema: snowflake.schema,
            role: snowflake.role,
         }),
      },
      timeoutMs: snowflake.responseTimeoutMilliseconds,
      // Same pool sizing as the registry path; server-owned, not in the API.
      poolOptions: buildPoolOptions({ poolMin: 1, poolMax: 20 }),
   });
}

// TLS mode for a proxied postgres connection, as pg connectionString query
// params. The tunnel ends at 127.0.0.1, so no hostname check is possible here:
// `verify-ca` checks the chain against NODE_EXTRA_CA_CERTS without the hostname,
// `no-verify` encrypts without checking, `disable` uses no TLS. `verify-full`
// needs an `ssl` object with `servername` (see resolveProxiedTls).
export function buildProxiedSslQuery(name: string, sslmode?: string): string {
   const caBundle = process.env.NODE_EXTRA_CA_CERTS;
   // Default to encrypting so a force-SSL target (common on RDS) is not
   // rejected for plaintext.
   const mode = sslmode ?? "no-verify";
   switch (mode) {
      case "disable":
         // Explicit, or pg falls back to the deployment's PGSSLMODE, which
         // would verify and fail the 127.0.0.1 hostname check.
         return "?sslmode=disable";
      case "no-verify":
         return "?sslmode=no-verify";
      case "verify-ca":
         // Backstop; validateConnectionShape checks the file is readable at
         // config load.
         if (!caBundle) {
            throw new Error(
               `Connection proxy on '${name}' uses sslmode 'verify-ca' but no trusted CA bundle is available (NODE_EXTRA_CA_CERTS is unset). Add the CA bundle to the image or use sslmode 'no-verify'.`,
            );
         }
         return `?uselibpqcompat=true&sslmode=verify-ca&sslrootcert=${encodeURIComponent(caBundle)}`;
      default:
         throw new Error(
            `Connection proxy on '${name}' has unsupported sslmode '${mode}' (expected disable | no-verify | verify-ca; verify-full is applied via the ssl object in resolveProxiedTls, not this query).`,
         );
   }
}

// A proxied connection's TLS mode as a connectionString query suffix, or for
// `verify-full` a raw `ssl` object: verifying the hostname through a tunnel
// needs `servername` set to the real host. pg keeps our `servername` because
// the socket targets an IP literal. Verification uses Node's bundled roots plus
// NODE_EXTRA_CA_CERTS, not the OS store (unless --use-system-ca).
export function resolveProxiedTls(
   name: string,
   host: string | undefined,
   sslmode?: string,
): { query: string; ssl?: PostgresSSLConfig } {
   if (sslmode === "verify-full") {
      if (!host) {
         // validateConnectionShape requires host; guard so servername is never
         // undefined.
         throw new Error(
            `Connection proxy on '${name}' uses sslmode 'verify-full' but has no host to verify against.`,
         );
      }
      return { query: "", ssl: { servername: host, rejectUnauthorized: true } };
   }
   return { query: buildProxiedSslQuery(name, sslmode) };
}

function buildProxiedPostgresConnection(
   metadata: EnvironmentConnectionMetadata,
   endpoint: ProxyEndpoint,
): PooledPostgresConnection {
   const name = metadata.apiConnection.name!;
   const pg = metadata.apiConnection.postgresConnection;
   if (!pg) {
      throw new Error(
         `Proxied connection '${name}' has type 'postgres' but no postgresConnection config.`,
      );
   }
   // Our own connectionString to the local tunnel endpoint; a tenant
   // connectionString cannot be tunneled and is rejected at config load.
   const enc = encodeURIComponent;
   const auth = pg.userName
      ? `${enc(pg.userName)}${pg.password ? `:${enc(pg.password)}` : ""}@`
      : "";
   const db = pg.databaseName ? `/${enc(pg.databaseName)}` : "";
   const { query, ssl } = resolveProxiedTls(name, pg.host, pg.sslmode);
   const connectionString = `postgresql://${auth}${endpoint.host}:${endpoint.port}${db}${query}`;
   const setupSQL = statementTimeoutSetupSQL(name, pg);
   return new PooledPostgresConnection({
      name,
      connectionString,
      ...(ssl ? { ssl } : {}),
      ...(setupSQL ? { setupSQL } : {}),
      // Pool sizing mirrors buildSnowflakePrivateKeyConnection.
      poolMin: 1,
      poolMax: 20,
   });
}

// One query per session, so session state (SET, SET ROLE, an open BEGIN) from
// arbitrary SQL never reaches the next caller.
const POSTGRES_POOL_MAX_USES = 1;

// pg-pool's error for a caller that waited connectionTimeoutMillis for a free
// session. A connect that itself times out fails with a different message.
const PG_POOL_WAIT_TIMEOUT_MESSAGE = "timeout exceeded when trying to connect";

/**
 * `PooledPostgresConnection` whose pool options reach `pg.Pool`. The base
 * `buildClientConfig` drops `poolMin`/`poolMax`, so pg-pool's default size
 * applies regardless of what the constructor is given.
 */
export class EnvironmentPooledPostgresConnection extends PooledPostgresConnection {
   // Per-process cap on open sessions (PUBLISHER_POSTGRES_POOL_MAX). pg-pool
   // applies the timeout to waiting for a free session as well as to connecting.
   protected poolLimits(): { max: number; connectionTimeoutMillis: number } {
      return { max: getPostgresPoolMax(), connectionTimeoutMillis: 30_000 };
   }

   // getPool() passes this result to `new Pool(...)`, so it is a PoolConfig.
   protected buildClientConfig(
      cfg: Parameters<PooledPostgresConnection["buildClientConfig"]>[0],
   ): PoolConfig {
      const base = super.buildClientConfig(cfg);
      return {
         ...base,
         ...this.poolLimits(),
         maxUses: POSTGRES_POOL_MAX_USES,
         // Identifies these sessions in the database's pg_stat_activity.
         application_name: "malloy-publisher",
      };
   }

   // The base class runs every pool checkout through withTlsHint, so this is
   // where a query that gave up waiting for a free session is reclassified.
   protected async withTlsHint<T>(op: () => Promise<T>): Promise<T> {
      try {
         return await super.withTlsHint(op);
      } catch (err) {
         if (
            err instanceof Error &&
            err.message === PG_POOL_WAIT_TIMEOUT_MESSAGE
         ) {
            const { max, connectionTimeoutMillis } = this.poolLimits();
            const waitSeconds = connectionTimeoutMillis / 1000;
            logger.warn("Postgres connection pool exhausted", {
               connection: this.name,
               poolMax: max,
               waitSeconds,
               remedy:
                  "fewer concurrent queries on this connection, or raise PUBLISHER_POSTGRES_POOL_MAX",
            });
            throw new ConnectionPoolExhaustedError(
               `Connection '${this.name}' has no free database session: this server opens at most ${max} at a time for it, and none came free within ${waitSeconds} s. Retry once fewer queries are running on this connection.`,
            );
         }
         throw err;
      }
   }
}

function buildEnvironmentPostgresConnection(
   metadata: EnvironmentConnectionMetadata,
): EnvironmentPooledPostgresConnection {
   const name = metadata.apiConnection.name!;
   const pg = metadata.apiConnection.postgresConnection;
   if (!pg) {
      throw new Error(
         `Connection '${name}' has type 'postgres' but no postgresConnection config.`,
      );
   }
   // buildPostgresConnectionString applies the connection's sslmode, else the
   // deployment's PGSSLMODE, and returns undefined when there is nothing to
   // apply, in which case pg falls back to the individual fields.
   const setupSQL = statementTimeoutSetupSQL(name, pg);
   return new EnvironmentPooledPostgresConnection({
      name,
      connectionString: buildPostgresConnectionString(pg, {
         applySslmode: true,
      }),
      host: pg.host,
      port: pg.port,
      username: pg.userName,
      password: pg.password,
      databaseName: pg.databaseName,
      ...(setupSQL ? { setupSQL } : {}),
   });
}

/**
 * The per-session SQL that applies a connection's statement timeout, or
 * undefined when it sets none. Run as the driver's setupSQL, so it executes on
 * every session - each pool acquire and each fresh client - rather than being
 * sent as a startup parameter, which a connection pooler can refuse.
 */
function statementTimeoutSetupSQL(
   name: string,
   pg: components["schemas"]["PostgresConnection"],
): string | undefined {
   const ms = postgresStatementTimeoutMs(name, pg);
   return ms === undefined ? undefined : `SET statement_timeout = ${ms}`;
}

function buildDuckLakeConnection(
   metadata: EnvironmentConnectionMetadata,
   entry: CoreConnectionEntry,
): DuckLakeConnection {
   return new DuckLakeConnection(
      entryToDuckDBOptions(
         metadata.apiConnection.name!,
         entry,
         metadata.workingDirectory,
      ),
   );
}

function buildAzureDuckDBConnection(
   metadata: EnvironmentConnectionMetadata,
   entry: CoreConnectionEntry,
): AzureDuckDBConnection {
   return new AzureDuckDBConnection(
      entryToDuckDBOptions(
         metadata.apiConnection.name!,
         entry,
         metadata.workingDirectory,
      ),
      metadata.attachedDatabases,
   );
}

function getMetadataForLookup(
   metadata: Map<string, EnvironmentConnectionMetadata>,
   name?: string,
): EnvironmentConnectionMetadata | undefined {
   return name ? metadata.get(name) : undefined;
}

/**
 * Make a connection's digest the caller-supplied API `fingerprint`, verbatim.
 *
 * The fingerprint is the caller-declared data identity (secret-free, stable
 * across credential rotation). It replaces `getDigest()` everywhere a digest is
 * read: build planning, the package-load worker's metadata RPC, and Malloy's
 * serve-time manifest resolution.
 *
 * Overriding it on the resolved instance, inside the one `lookupConnection`
 * wrapper, keeps build-time and serve-time ids identical; per-call-site
 * application would risk divergence and a silent live-serving fallback. The
 * value is an opaque token: never re-hashed, logged or parsed.
 */
function applyConnectionFingerprint(
   connection: Connection,
   metadata: EnvironmentConnectionMetadata | undefined,
): Connection {
   const fingerprint = metadata?.apiConnection.fingerprint;
   if (fingerprint) {
      connection.getDigest = () => fingerprint;
   }
   return connection;
}

function isDuckDBConnection(
   connection: Connection,
): connection is DuckDBConnection {
   return connection instanceof DuckDBConnection;
}

/**
 * @param isUpdateConnectionRequest Forces a DuckLake re-ATTACH even when the
 *   catalog is already attached, so a generation replacing one after a config
 *   change rebinds to the new catalog DSN and storage secrets rather than the
 *   prior attach state. Only the DuckLake branch reads it.
 */
export function buildEnvironmentMalloyConfig(
   connections: ApiConnection[] = [],
   environmentPath: string = "",
   isUpdateConnectionRequest: boolean = false,
): EnvironmentMalloyConfig {
   const assembled = assembleEnvironmentConnections(
      connections,
      environmentPath,
   );
   // Promises are cached so concurrent lookups share one build and never leak a
   // losing instance's pool. One cache per branch keeps the concrete subtype.
   const duckLakeCache = new Map<string, Promise<DuckLakeConnection>>();
   const snowflakeJwtCache = new Map<string, Promise<SnowflakeConnection>>();
   const azureDuckDBCache = new Map<string, Promise<AzureDuckDBConnection>>();
   const proxyConnectionCache = new Map<
      string,
      Promise<PooledPostgresConnection>
   >();
   const postgresConnectionCache = new Map<
      string,
      Promise<EnvironmentPooledPostgresConnection>
   >();
   const proxyEndpoints = new Map<string, ProxyEndpoint>();
   const attachPromises = new WeakMap<Connection, Promise<void>>();

   const malloyConfig = new MalloyConfig(assembled.pojo, {
      config: contextOverlay({ rootDirectory: environmentPath }),
      // Resolves bigquery `authClient: {gcpImpersonation}` references (see
      // gcp_impersonation.ts); consulted only when a connection carries one.
      gcpImpersonation: gcpImpersonationOverlay(),
   });

   async function attachOnce(
      connection: Connection,
      metadata: EnvironmentConnectionMetadata,
   ): Promise<void> {
      if (
         metadata.attachedDatabases.length === 0 ||
         !isDuckDBConnection(connection)
      ) {
         return;
      }

      let attachPromise = attachPromises.get(connection);
      if (!attachPromise) {
         // One ATTACH run per connection object per config generation.
         attachPromise = attachDatabasesToDuckDB(
            connection,
            metadata.attachedDatabases,
         );
         // Evict a rejected run so a later lookup can retry: every attach handler
         // reaches the network, and a cached failure would make one transient
         // blip permanent until the environment is rebuilt.
         attachPromise.catch(() => {
            if (attachPromises.get(connection) === attachPromise) {
               attachPromises.delete(connection);
            }
         });
         attachPromises.set(connection, attachPromise);
      }
      await attachPromise;
   }

   malloyConfig.wrapConnections(
      (base: LookupConnection<Connection>): LookupConnection<Connection> => {
         const resolveConnection = async (
            name: string | undefined,
            metadata: EnvironmentConnectionMetadata | undefined,
         ): Promise<Connection> => {
            if (metadata?.isDuckLake) {
               let connectionPromise = duckLakeCache.get(name!);
               if (!connectionPromise) {
                  const entry = assembled.pojo.connections[name!];
                  connectionPromise = Promise.resolve(
                     buildDuckLakeConnection(metadata, entry),
                  );
                  duckLakeCache.set(name!, connectionPromise);
               }
               const connection = await connectionPromise;
               if (
                  isUpdateConnectionRequest ||
                  !(await isDatabaseAttached(connection, name!))
               ) {
                  await attachDuckLake(
                     connection,
                     name!,
                     metadata.apiConnection.ducklakeConnection!,
                  );
                  if (metadata.apiConnection.managed === true) {
                     await setManagedSearchPath(connection, name!);
                  }
               }
               return connection;
            }

            if (metadata?.proxy) {
               let connectionPromise = proxyConnectionCache.get(name!);
               if (!connectionPromise) {
                  const connType = metadata.apiConnection.type;
                  if (connType !== "postgres") {
                     throw new Error(
                        `SSH proxy is not yet supported for connection type '${connType}'. ` +
                           `Only 'postgres' is implemented.`,
                     );
                  }
                  // Defense in depth: validateConnectionShape already requires
                  // host and port at config load.
                  const pgConfig = metadata.apiConnection.postgresConnection;
                  if (!pgConfig?.host || !pgConfig?.port) {
                     throw new Error(
                        `SSH proxy for connection '${name}' requires explicit host and ` +
                           `port on the postgres connection; the connectionString form ` +
                           `is not supported with a proxy.`,
                     );
                  }
                  connectionPromise = (async () => {
                     const endpoint = await openProxy(metadata.proxy!, {
                        host: pgConfig.host!,
                        port: pgConfig.port!,
                     });
                     try {
                        const connection = buildProxiedPostgresConnection(
                           metadata,
                           endpoint,
                        );
                        // Registered only after the build succeeds, so a failed
                        // build leaves no orphaned tunnel in proxyEndpoints.
                        proxyEndpoints.set(name!, endpoint);
                        return connection;
                     } catch (err) {
                        await endpoint.close().catch(() => {});
                        throw err;
                     }
                  })();
                  // Evict a rejected build so a transient SSH failure is retried.
                  connectionPromise.catch(() => {
                     if (
                        proxyConnectionCache.get(name!) === connectionPromise
                     ) {
                        proxyConnectionCache.delete(name!);
                     }
                  });
                  proxyConnectionCache.set(name!, connectionPromise);
               }
               return connectionPromise;
            }

            if (metadata?.hasSnowflakePrivateKey) {
               let connectionPromise = snowflakeJwtCache.get(name!);
               if (!connectionPromise) {
                  connectionPromise = Promise.resolve(
                     buildSnowflakePrivateKeyConnection(metadata),
                  );
                  snowflakeJwtCache.set(name!, connectionPromise);
               }
               return connectionPromise;
            }

            if (metadata?.hasAzureAttachment) {
               let connectionPromise = azureDuckDBCache.get(name!);
               if (!connectionPromise) {
                  const entry = assembled.pojo.connections[name!];
                  connectionPromise = Promise.resolve(
                     buildAzureDuckDBConnection(metadata, entry),
                  );
                  azureDuckDBCache.set(name!, connectionPromise);
               }
               const connection = await connectionPromise;
               await attachOnce(connection, metadata);
               return connection;
            }

            // Plain (non-proxied) Postgres, which would otherwise fall through
            // to the registry's unpooled PostgresConnection.
            if (metadata?.apiConnection.type === "postgres") {
               let connectionPromise = postgresConnectionCache.get(name!);
               if (!connectionPromise) {
                  connectionPromise = Promise.resolve(
                     buildEnvironmentPostgresConnection(metadata),
                  );
                  postgresConnectionCache.set(name!, connectionPromise);
                  // Evict a rejected build so a later lookup can retry.
                  connectionPromise.catch(() => {
                     if (
                        postgresConnectionCache.get(name!) === connectionPromise
                     ) {
                        postgresConnectionCache.delete(name!);
                     }
                  });
               }
               const connection = await connectionPromise;
               await attachOnce(connection, metadata);
               return connection;
            }

            const connection = await base.lookupConnection(name);
            if (metadata) {
               await attachOnce(connection, metadata);
            }
            return connection;
         };

         return {
            lookupConnection: async (name?: string): Promise<Connection> => {
               const metadata = getMetadataForLookup(assembled.metadata, name);
               let connection: Connection;
               try {
                  connection = await resolveConnection(name, metadata);
               } catch (error) {
                  // A cached model can name a connection deleted since it
                  // loaded. Raise the typed error so the query answers 424
                  // CONNECTION_NOT_FOUND rather than a 400. Checked only after
                  // the lookup fails, so names Malloy resolves still resolve.
                  if (
                     name !== undefined &&
                     !Object.hasOwn(assembled.pojo.connections, name)
                  ) {
                     throw new UnconfiguredConnectionError(name, {
                        cause: error,
                     });
                  }
                  throw error;
               }
               // Every resolution branch exits here, so the extension pin and
               // the fingerprint apply whichever branch produced the
               // connection, including one that does not attach. (The
               // per-package sandbox is pinned in package.ts.)
               if (isDuckDBConnection(connection)) {
                  await applyExtensionSessionSettings(connection);
               }
               return applyConnectionFingerprint(connection, metadata);
            },
         };
      },
   );

   return {
      malloyConfig,
      apiConnections: assembled.apiConnections,
      releaseConnections: async () => {
         const wrapperPromises: Promise<Connection>[] = [
            ...duckLakeCache.values(),
            ...snowflakeJwtCache.values(),
            ...azureDuckDBCache.values(),
            ...proxyConnectionCache.values(),
            ...postgresConnectionCache.values(),
         ];
         const closeResults = await Promise.allSettled([
            malloyConfig.shutdown("close"),
            ...wrapperPromises.map(async (promise) => {
               const connection = await promise;
               await connection.close();
            }),
         ]);
         // Tunnels close only after the builds above settle: an in-flight
         // openProxy registers its endpoint inside that promise, and the tunnel
         // must stay up while the DB connections drain.
         const endpointResults = await Promise.allSettled(
            [...proxyEndpoints.values()].map((ep) => ep.close()),
         );
         duckLakeCache.clear();
         snowflakeJwtCache.clear();
         azureDuckDBCache.clear();
         proxyConnectionCache.clear();
         postgresConnectionCache.clear();
         proxyEndpoints.clear();

         const failures = [...closeResults, ...endpointResults].filter(
            (result): result is PromiseRejectedResult =>
               result.status === "rejected",
         );
         // Logged individually: callers that just log the AggregateError would
         // lose per-branch detail.
         for (const failure of failures) {
            logger.error("Failed to release environment connection", {
               error: failure.reason,
            });
         }
         if (failures.length > 0) {
            throw new AggregateError(
               failures.map((failure) => failure.reason),
               "Failed to release one or more environment connections",
            );
         }
      },
   };
}

export async function createEnvironmentConnections(
   connections: ApiConnection[] = [],
   environmentPath: string = "",
   isUpdateConnectionRequest: boolean = false,
): Promise<{
   malloyConnections: Map<string, Connection>;
   apiConnections: InternalConnection[];
   releaseConnections: () => Promise<void>;
}> {
   const connectionMap = new Map<string, Connection>();
   const environmentConfig = buildEnvironmentMalloyConfig(
      connections,
      environmentPath,
      isUpdateConnectionRequest,
   );

   for (const connection of environmentConfig.apiConnections) {
      if (!connection.name) continue;
      logger.info(`Adding connection ${connection.name}`, {
         type: connection.type,
      });
      const malloyConnection =
         await environmentConfig.malloyConfig.connections.lookupConnection(
            connection.name,
         );
      connection.attributes = getConnectionAttributes(malloyConnection);
      connectionMap.set(connection.name, malloyConnection);
   }

   return {
      malloyConnections: connectionMap,
      apiConnections: environmentConfig.apiConnections,
      releaseConnections: environmentConfig.releaseConnections,
   };
}

function getConnectionAttributes(
   connection: Connection,
): ApiConnectionAttributes {
   let canStream = false;
   try {
      canStream = connection.canStream();
   } catch {
      // pass
   }
   return {
      dialectName: connection.dialectName,
      isPool: connection.isPool(),
      canPersist: connection.canPersist(),
      canStream: canStream,
   };
}

// Most attached-database probes below only confirm the attach or secret exists;
// they do not exercise its credentials.
async function testDuckDBConnection(
   duckdbConnection: DuckDBConnection,
   connectionConfig: InternalConnection,
): Promise<void> {
   try {
      await duckdbConnection.runSQL("SELECT 1 AS test");
      logger.info(
         `DuckDB base connection test passed for: ${connectionConfig.name}`,
      );
   } catch (error) {
      throw new Error(
         `DuckDB base connection test failed: ${(error as Error).message}`,
      );
   }

   const attachedDatabases =
      connectionConfig.duckdbConnection?.attachedDatabases;
   if (!attachedDatabases || attachedDatabases.length === 0) {
      return;
   }

   const failedAttachments: string[] = [];

   for (const attachedDb of attachedDatabases) {
      if (!attachedDb.name) {
         continue;
      }

      try {
         switch (attachedDb.type) {
            case "postgres": {
               await duckdbConnection.runSQL(
                  `SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '${attachedDb.name}' LIMIT 1`,
               );
               logger.info(
                  `Attached Postgres database test passed: ${attachedDb.name}`,
               );
               break;
            }
            case "bigquery": {
               await duckdbConnection.runSQL(
                  `SELECT database_name FROM duckdb_databases() WHERE database_name = '${attachedDb.name}'`,
               );
               logger.info(
                  `Attached BigQuery database test passed: ${attachedDb.name}`,
               );
               break;
            }
            case "snowflake": {
               await duckdbConnection.runSQL(
                  `SELECT database_name FROM duckdb_databases() WHERE database_name = '${attachedDb.name}'`,
               );
               logger.info(
                  `Attached Snowflake database test passed: ${attachedDb.name}`,
               );
               break;
            }
            case "gcs": {
               await duckdbConnection.runSQL(
                  `SELECT name FROM duckdb_secrets() WHERE name LIKE '%${attachedDb.name}%' LIMIT 1`,
               );
               logger.info(
                  `Cloud storage credentials test passed: ${attachedDb.name}`,
               );
               break;
            }
            case "s3": {
               await duckdbConnection.runSQL(
                  `SELECT name FROM duckdb_secrets() WHERE name LIKE '%${attachedDb.name}%' LIMIT 1`,
               );
               logger.info(
                  `Cloud storage credentials test passed: ${attachedDb.name}`,
               );
               break;
            }
            case "azure": {
               const azureConfig = attachedDb.azureConnection;
               if (azureConfig?.authType === "sas_token") {
                  // SAS token is embedded in the URL — no DuckDB secret is created.
                  if (!azureConfig.sasUrl) {
                     throw new Error(
                        `Azure SAS token URL is missing for: ${attachedDb.name}`,
                     );
                  }
                  logger.info(
                     `Azure SAS token URL present for: ${attachedDb.name}`,
                  );
               } else {
                  await duckdbConnection.runSQL(
                     `SELECT name FROM duckdb_secrets() WHERE name LIKE '%${attachedDb.name}%' LIMIT 1`,
                  );
                  logger.info(
                     `Azure SPN credentials test passed: ${attachedDb.name}`,
                  );
               }
               break;
            }
            default: {
               logger.warn(
                  `Unknown attached database type: ${attachedDb.type}`,
               );
            }
         }
      } catch (error) {
         const errorMessage = `Attached database '${attachedDb.name}' (${attachedDb.type}) test failed: ${(error as Error).message}`;
         // A probe after a half-succeeded attach can carry the DSN, so redact
         // (the rethrow is redacted at the testConnectionConfig boundary).
         logger.error(redactPgSecrets(errorMessage));
         failedAttachments.push(errorMessage);
      }
   }

   if (failedAttachments.length > 0) {
      throw new Error(
         `DuckDB connection test failed for attached databases:\n${failedAttachments.join("\n")}`,
      );
   }
}

/**
 * Redacts a connection-test failure before it reaches the caller or the log.
 *
 * Two passes, since neither suffices alone. The value pass removes the exact
 * credential strings this request supplied, found by key pattern rather than a
 * field list. The shape pass catches what that cannot: a credential echoed
 * re-encoded, or a secret from somewhere other than this config.
 *
 * Credentials shorter than four characters are left to the shape pass:
 * removing every occurrence of a two-character string would shred the prose.
 */
export function redactTestFailure(message: string, config: unknown): string {
   const secrets = new Set<string>();
   collectConfigSecrets(config, secrets, new WeakSet());
   let redacted = message;
   for (const secret of secrets) {
      redacted = redacted.split(secret).join("***");
      // A driver that echoes the offending SQL shows a quote-carrying secret in
      // its escaped form, so the raw value alone would not match.
      const escaped = escapeSQL(secret);
      if (escaped !== secret) redacted = redacted.split(escaped).join("***");
   }
   return redactConnectionSecretShapes(redacted);
}

/**
 * Connection-config keys whose string values are credentials to remove whole.
 *
 * Excludes `connectionString` and `sasUrl` on purpose: their host and port are
 * what tells "wrong password" from "wrong host". The shape pass masks the
 * password or signature in place and leaves the rest readable.
 */
const CONFIG_SECRET_KEY =
   /pass(word)?|secret|private_?key|service_?account|access_?key|token|peaka_?key/i;

/** Collects the credential strings a connection config carries. */
function collectConfigSecrets(
   value: unknown,
   out: Set<string>,
   seen: WeakSet<object>,
): void {
   if (value === null || typeof value !== "object") return;
   // Cycle-guarded: a hydrated config can hold a back-reference, and a
   // RangeError would replace the failure being redacted.
   if (seen.has(value)) return;
   seen.add(value);
   if (Array.isArray(value)) {
      for (const v of value) collectConfigSecrets(v, out, seen);
      return;
   }
   for (const [key, v] of Object.entries(value)) {
      if (
         typeof v === "string" &&
         v.length >= 4 &&
         CONFIG_SECRET_KEY.test(key)
      ) {
         out.add(v);
      } else {
         collectConfigSecrets(v, out, seen);
      }
   }
}

export async function testConnectionConfig(
   connectionConfig: ApiConnection,
): Promise<ApiConnectionStatus> {
   let environmentConfig: EnvironmentMalloyConfig | null = null;
   let testRoot: string | null = null;
   try {
      if (!connectionConfig.name) {
         throw new Error("Connection name is required");
      }

      // Only duckdb/ducklake derive a filename from the name. Defense in depth:
      // the throwaway directory below already contains any write.
      if (
         connectionConfig.type === "duckdb" ||
         connectionConfig.type === "ducklake"
      ) {
         assertSafePackageName(connectionConfig.name);
      }

      // A BigQuery connection with no inline key authenticates through
      // GOOGLE_APPLICATION_CREDENTIALS, whose directory case google-auth
      // reports as a file that does not exist.
      if (
         connectionConfig.type === "bigquery" &&
         !connectionConfig.bigqueryConnection?.serviceAccountKeyJson
      ) {
         assertGoogleCredentialsIsNotADirectory();
      }

      // A fresh temp root (removed in the finally): DuckDB needs a non-empty
      // workingDirectory and opens `<name>.duckdb` there, so the test never
      // touches an operator's own database and concurrent tests of one name
      // cannot clobber each other.
      testRoot = await fs.mkdtemp(
         path.join(os.tmpdir(), "publisher-conn-test-"),
      );
      environmentConfig = buildEnvironmentMalloyConfig(
         [connectionConfig],
         testRoot,
      );
      const connection =
         await environmentConfig.malloyConfig.connections.lookupConnection(
            connectionConfig.name,
         );

      if (connectionConfig.type === "duckdb") {
         await testDuckDBConnection(
            connection as DuckDBConnection,
            connectionConfig as InternalConnection,
         );
      } else if (connectionConfig.type === "ducklake") {
         const duckConn = connection as DuckDBConnection;
         const attached = await isDatabaseAttached(
            duckConn,
            connectionConfig.name as string,
         );
         if (!attached) {
            throw new Error(
               `DuckLake connection test failed: Error attaching database '${connectionConfig.name}'`,
            );
         }
         await duckConn.runSQL(
            `SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '${connectionConfig.name}' LIMIT 1`,
         );

         logger.info(
            `DuckLake connection test passed: ${connectionConfig.name}`,
         );
      } else {
         await (
            connection as
               | PostgresConnection
               | BigQueryConnection
               | SnowflakeConnection
               | TrinoConnection
               | MySQLConnection
               | SQLServerConnection
         ).test();
      }

      return {
         status: "ok",
         errorMessage: "",
      };
   } catch (error) {
      if (error instanceof AxiosError) {
         logAxiosError(error);
      } else {
         // Redacted like the response, stack kept. Operator logs are read more
         // widely than the caller's response, so this exposure is the wider one.
         logger.error("Connection test failed", {
            error: redactTestFailure(
               error instanceof Error
                  ? (error.stack ?? error.message)
                  : String(error),
               connectionConfig,
            ),
         });
      }

      return {
         status: "failed",
         // Drivers echo their config (DuckDB DSNs, SSH private keys and
         // passphrases, warehouse tokens or key JSON), and this message goes to
         // the REST response, which clients log. This is the connection test's
         // only redaction point: failures resolve here, never reaching the
         // controller's catch.
         errorMessage: redactTestFailure(
            error instanceof Error ? error.message : String(error),
            connectionConfig,
         ),
      };
   } finally {
      if (environmentConfig) {
         try {
            await environmentConfig.releaseConnections();
         } catch (closeError) {
            logger.warn("Error releasing temporary connection test config", {
               error: closeError,
            });
         }
      }

      // Wrapped so a cleanup failure never overrides the response.
      if (testRoot) {
         try {
            await fs.rm(testRoot, { recursive: true, force: true });
         } catch (cleanupError) {
            logger.warn("Error cleaning up connection test directory", {
               error:
                  cleanupError instanceof Error
                     ? cleanupError.message
                     : String(cleanupError),
            });
         }
      }
   }
}
