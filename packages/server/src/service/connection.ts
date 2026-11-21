// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { externalEnv } from "../settings";
import "@malloydata/db-bigquery";
import type { BigQueryConnection } from "@malloydata/db-bigquery";
import "@malloydata/db-databricks";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { errMessage } from "../utils";
import {
   installDuckDBInstanceSharing,
   mayHoldAttachments,
   recordAttachments,
   recordedAttachments,
   type RecordedAttachment,
   serializeAttachRun,
} from "./duckdb_instance_sharing";
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
import type { SnowflakeConnection } from "@malloydata/db-snowflake";
import "@malloydata/db-sqlserver";
import type { SQLServerConnection } from "@malloydata/db-sqlserver";
import "@malloydata/db-trino";
import type { TrinoConnection } from "@malloydata/db-trino";
import {
   Connection,
   contextOverlay,
   MalloyConfig,
   type QueryRecord,
   TableSourceDef,
} from "@malloydata/malloy";
import type { LookupConnection } from "@malloydata/malloy/connection";
import { AxiosError } from "axios";
import { AsyncLocalStorage } from "async_hooks";
import { createHash } from "crypto";
import fs from "fs/promises";
import os from "os";
import path from "path";
import type { PoolConfig } from "pg";
import { components } from "../api";
import {
   ConnectionOrigin,
   getDuckLakeRowGroupSizeBytes,
   getDuckLakeTargetFileSizeBytes,
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
   EnvironmentConnectionMetadata,
   removeUndefined,
} from "./connection_config";
import { connectionDialectOf, type CoreConnectionEntry } from "./dialects";
import {
   applyExtensionSessionSettings,
   escapeSQL,
   installAndLoadExtension,
   sanitizeSecretName,
} from "./duckdb_session_setup";
import {
   buildPgConnectionString,
   buildPostgresConnectionString,
   createPgAttachSecret,
   postgresStatementTimeoutMs,
} from "./postgres_conninfo";
import { buildSnowflakeConnection } from "./snowflake_connection";
import { guardBigQueryTruncation } from "./bigquery_truncation";
import { gcpImpersonationOverlay } from "./gcp_impersonation";
import { assertGoogleCredentialsIsNotADirectory } from "./google_credentials";
import {
   CloudStorageCredentials,
   checkS3Endpoint,
   DEFAULT_S3_CREDENTIAL_CHAIN,
   resolveCloudStorageCredentials,
} from "./gcs_s3_utils";
import { recordConnectionType } from "./connection_type";
import {
   allowForLockdown,
   azureAllowPrefix,
   cloudStorageAllowPrefixes,
   duckDBInstanceHasOtherHolders,
   duckDBInstanceId,
   duckDBInstanceOf,
   isConfigurationFrozen,
   installDuckDBStatementLockdown,
   isInstanceLocked,
   isLockedByOptions,
   lockDownDuckDBSession,
   relativeReadEntries,
   runAsInternalStatement,
   runExclusiveOnDuckDBInstance,
   runInternalSQL,
} from "./duckdb_lockdown";
import {
   DUCKLAKE_ATTACH_FAILED,
   DUCKLAKE_CATALOG_UNAVAILABLE,
   DuckLakeCatalogUnavailableError,
} from "./ducklake_catalog_unavailable";
import { installDuckDBStatementGate } from "./duckdb_statement_gate";
import {
   disownDuckDBConnection,
   hardCloseOwnedDuckDBConnections,
   hardCloseWhenIdle,
   installDuckDBHardClose,
   ownDuckDBConnection,
} from "./duckdb_hard_close";
import { openProxy, type ProxyEndpoint } from "./proxy";
import { quoteIdentifier } from "./quoting";

// One DuckDB instance per database file (duckdb_instance_sharing.ts).
installDuckDBInstanceSharing();
// One statement at a time per DuckDB connection (duckdb_statement_gate.ts),
// installed before the hard close so that counts queued statements.
installDuckDBStatementGate();
// Retired DuckDB connections free their native database at once
// (duckdb_hard_close.ts).
installDuckDBHardClose();
// Statements a locked instance refuses, checked before any of the above
// (duckdb_lockdown.ts).
installDuckDBStatementLockdown();

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

function handleAlreadyAttachedError(error: unknown, dbName: string): void {
   if (
      error instanceof Error &&
      error.message.includes("already exists") &&
      // A DuckLake catalog create that lost a race attached nothing.
      !isDuckLakeInitRace(error)
   ) {
      logger.info(`Database ${dbName} is already attached, skipping`);
   } else {
      throw error;
   }
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
   catalog: { path: string; secret?: string },
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
         `ATTACH '${escapeSQL(catalog.path)}' AS ${tempDbRef} (TYPE postgres, READ_ONLY${catalog.secret ? `, SECRET ${catalog.secret}` : ""});`,
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

/** How many times an attach that lost a catalog-create race is tried in all. */
export const DUCKLAKE_INIT_RACE_ATTEMPTS = 3;

/**
 * Whether `error` is a DuckLake attach that lost a race to create the same
 * catalog. DuckLake creates its metadata tables with a plain CREATE TABLE, so
 * when two sessions (two builds, on one server or two) attach a catalog that
 * does not exist yet, both create it and Postgres refuses the slower one: a
 * duplicate key on `pg_type`, or the relation already existing.
 */
export function isDuckLakeInitRace(error: unknown): boolean {
   const message = errMessage(error);
   return (
      message.includes("Failed to initialize DuckLake") &&
      (message.includes("already exists") ||
         message.includes("duplicate key value"))
   );
}

/**
 * Whether a database is attached under `alias`, compared as DuckDB resolves
 * it: case-insensitively. An `ATTACH OR REPLACE … AS "lake"` over `"Lake"`
 * keeps the stored name `Lake`, so an exact match would miss it.
 */
async function isAliasAttached(
   connection: DuckDBConnection,
   alias: string,
): Promise<boolean> {
   try {
      const rows = runSQLRows(
         await runInternalSQL(
            connection,
            `SELECT 1 FROM duckdb_databases() WHERE lower(database_name) = lower('${escapeSQL(alias)}')`,
         ),
      );
      return rows.length > 0;
   } catch {
      return false;
   }
}

/**
 * Whether `alias` is attached on `connection`'s instance; undefined when the
 * check itself failed, which says nothing about the catalog.
 */
async function aliasAttachedOnInstance(
   connection: DuckDBConnection,
   alias: string,
): Promise<boolean | undefined> {
   try {
      const rows = runSQLRows(
         await runInternalSQL(
            connection,
            `SELECT 1 FROM duckdb_databases() WHERE lower(database_name) = lower('${escapeSQL(alias)}')`,
         ),
      );
      return rows.length > 0;
   } catch {
      return undefined;
   }
}

/**
 * Run a DuckLake ATTACH, trying again, up to `attempts` tries in all, when it
 * lost a race to create the catalog. Postgres reports the duplicate only once
 * the other create has committed, so the catalog exists by then and the next
 * attach opens it as any attach of an existing catalog does. Any other failure,
 * and the last try's, is thrown.
 */
async function runDuckLakeAttach(
   connection: DuckDBConnection,
   dbName: string,
   attachCommand: string,
   attempts: number,
): Promise<void> {
   for (let attempt = 1; ; attempt++) {
      try {
         await connection.runSQL(attachCommand);
         return;
      } catch (error) {
         if (!isDuckLakeInitRace(error) || attempt >= attempts) {
            throw error;
         }
         logger.info(
            "Another session created the DuckLake catalog first; attaching it again",
            { dbName, attempt },
         );
         await new Promise((resolve) => setTimeout(resolve, 25 * attempt));
      }
   }
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
       * Who authored the config, which the S3 credential-chain policy turns on:
       * the operator's own storage destinations and catalogs pass
       * `"storage-destination"`; an environment connection is author-supplied.
       */
      origin?: ConnectionOrigin;
      /**
       * A catalog served as its own tables (the connection's `managed`). Attached
       * read-only, its storage secret reaches only the `main` schema's files.
       */
      managed?: boolean;
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
      // DuckDB signs an S3 read that no secret matches with a legacy key seeded
      // from the AWS_* environment, which would bypass the scope on the secret
      // created below. Off first, before anything here can reach S3, and GLOBAL:
      // a plain SET is session-local, while the attach and the secret live on
      // the instance, so a later connection object on an already-attached
      // catalog would keep the fallback. The instance is this catalog's own
      // (keyed on its database file), so nothing else is reached.
      await connection.runSQL(
         "SET GLOBAL enable_global_s3_configuration=false",
      );
   }
   // The storage credential, and the policy gate on it, before the ATTACH: a
   // refused credential attaches nothing. The secret itself is created after
   // the ATTACH, once its scope can be read from the catalog (below).
   const storage: AttachedDatabase | undefined = hasS3
      ? {
           name: `${dbName}_storage`,
           type: "s3",
           s3Connection: ducklakeConfig.storage!.s3Connection,
        }
      : hasGCS
        ? {
             name: `${dbName}_storage`,
             type: "gcs",
             gcsConnection: ducklakeConfig.storage!.gcsConnection,
          }
        : undefined;
   if (storage) {
      resolveCloudStorageCredentials(storage, options.origin ?? "connection");
   }

   const pg = ducklakeConfig.catalog.postgresConnection;
   const pgConnString: string = buildPgConnectionString(pg, {
      name: dbName,
      applyConnectionOptions: false,
   });
   const mode = options.readOnly ? "READ_ONLY" : "READ_WRITE";
   // The catalog's user and password reach DuckDB as a secret, never in the
   // attach path.
   const catalog = await createPgAttachSecret(connection, dbName, pgConnString);
   // These three attach diagnostics stay at debug: `redactPgSecrets` removes the
   // password and URI userinfo but not the catalog host or the DATA_PATH, which
   // would disclose the bucket and prefix to every log reader at info. The mode
   // and outcome are logged at info below.
   logger.debug(`pgConnString: ${redactPgSecrets(catalog.path)}`);
   const escapedPgConnString = escapeSQL(catalog.path);
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
   const preflight = await preflightDuckLakeCatalogFormat(
      connection,
      dbName,
      catalog,
      metadataSchema,
   );
   if (preflight === "absent") {
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
   const metadataSecretClause = catalog.secret
      ? `, METADATA_PARAMETERS MAP {'SECRET': '${catalog.secret}'}`
      : "";
   // Every clause but the data path's leads with ", ", and that one may be
   // empty, so a leading separator is dropped rather than emitted.
   const attachClauses = (
      dataPathClauses(dataPath, options.readOnly) +
      readOnlyClause +
      metadataSchemaClause +
      metadataSecretClause +
      firstAttachClauses(options)
   ).replace(/^, /, "");
   const attachCommand = `ATTACH OR REPLACE 'ducklake:postgres:${escapedPgConnString}' AS ${quoteIdentifier(dbName, "duckdb")} (${attachClauses});`;
   logger.debug(
      `Attaching DuckLake database using command: ${redactPgSecrets(attachCommand)}`,
   );
   try {
      // A create (the admin API's, the only caller with first-attach settings)
      // that lost is not tried again: the retry would open the other creator's
      // catalog, with that creator's settings and data path, and the create
      // would report it as its own. It fails, and the caller answers that the
      // catalog exists.
      const creating =
         options.encrypted !== undefined ||
         options.dataInliningRowLimit !== undefined;
      await runDuckLakeAttach(
         connection,
         dbName,
         attachCommand,
         creating ? 1 : DUCKLAKE_INIT_RACE_ATTEMPTS,
      );
      // Origin and instance tell apart two attaches logged in one
      // millisecond: a connection's and a destination's, or two on one
      // instance (which single-flighting should make impossible).
      logger.info(
         `Successfully attached DuckLake database in ${mode} mode: ${dbName}`,
         {
            origin: options.origin ?? "connection",
            instance: duckDBInstanceId(connection),
         },
      );
      if (!options.readOnly) {
         await applyDuckLakeRowGroupBound(connection, dbName);
         await applyDuckLakeTargetFileSize(connection, dbName);
      }
   } catch (error) {
      // Skipped only when the alias really is attached. A catalog create that
      // lost a race also says "already exists", and skipping that one left
      // nothing attached: the build's next statement failed with "Schema with
      // name <alias> does not exist".
      if (
         error instanceof Error &&
         (error.message.includes("already exists") ||
            error.message.includes("already attached")) &&
         !isDuckLakeInitRace(error) &&
         (await isAliasAttached(connection, dbName))
      ) {
         logger.info(
            `DuckLake database ${dbName} is already attached, skipping`,
         );
      } else {
         throw error;
      }
   }
   if (!storage) {
      // No storage secret: the catalog's data path is what its lock allows.
      const root =
         dataPath ||
         (await recordedCatalogSettings(connection, dbName, metadataSchema)
            .then((settings) => settings.data_path)
            .catch(() => undefined));
      if (root) allowForLockdown(connection, root);
   }
   if (storage) {
      // The storage secret, scoped to the catalog's prefix. After the ATTACH,
      // which reaches the catalog database and no data file, so a catalog whose
      // config states no data path is scoped to the one it recorded, read from
      // the attached catalog's own metadata rather than a second reader in the
      // preflight. No scope, no attach: an unscoped secret would reach the whole
      // bucket, and a catalog left attached without a secret would look attached
      // to the next lookup and never be retried.
      try {
         const scope = await attachedDuckLakeScope(connection, dbName, {
            configuredDataPath: dataPath,
            metadataSchema,
            readOnly: options.readOnly,
            // Only a connection view is narrowed: an operator's destination
            // serves every schema, whatever flags its entry carries.
            managed:
               options.managed === true &&
               options.origin !== "storage-destination",
         });
         if (!scope) {
            throw new Error(
               `No storage secret could be scoped for DuckLake catalog '${dbName}': ` +
                  `the config states no data path, and the catalog records no data ` +
                  `path (or, for a managed view, no path for its main schema) to ` +
                  `scope it to. The catalog was not attached.`,
            );
         }
         await attachCloudStorage(connection, storage, options.origin, scope);
         // The lock allows exactly what the secret is scoped to.
         allowForLockdown(connection, scope);
      } catch (error) {
         // Once the instance is locked the catalog is the locker's, attached
         // and scoped, and nothing could ATTACH it again: detaching it would
         // leave every later lookup on the instance without it.
         if (isInstanceLocked(connection)) {
            logger.error(
               "DuckLake storage secret failed after its instance was locked; the catalog is left attached",
               {
                  code: DUCKLAKE_CATALOG_UNAVAILABLE,
                  connection: dbName,
                  error: redactPgSecrets(errMessage(error)),
               },
            );
         } else {
            logger.warn(
               "DuckLake storage secret failed; detaching the catalog so the next lookup attaches it again",
               {
                  code: DUCKLAKE_ATTACH_FAILED,
                  connection: dbName,
                  origin: options.origin ?? "connection",
                  instance: duckDBInstanceId(connection),
                  error: redactPgSecrets(errMessage(error)),
               },
            );
            await connection
               .runSQL(`DETACH ${quoteIdentifier(dbName, "duckdb")};`)
               .catch(() => {});
         }
         throw error;
      }
   }
   return true;
}

/**
 * The scope of an attached catalog's storage secret ({@link storageSecretScope}),
 * reading from the catalog's metadata whatever the config does not state: the
 * recorded data path when the config has none, and the `main` schema's path for
 * a managed read-only view. Runs on the attached catalog, so nothing here is a
 * guess the preflight could fail softly on.
 */
async function attachedDuckLakeScope(
   connection: DuckDBConnection,
   dbName: string,
   input: {
      configuredDataPath?: string;
      metadataSchema?: string;
      readOnly: boolean;
      managed: boolean;
   },
): Promise<string | undefined> {
   const recordedDataPath = input.configuredDataPath
      ? undefined
      : (
           await recordedCatalogSettings(
              connection,
              dbName,
              input.metadataSchema,
           )
        ).data_path;
   const mainSchemaPath =
      input.managed && input.readOnly
         ? await recordedSchemaPath(
              connection,
              dbName,
              input.metadataSchema,
              "main",
           )
         : undefined;
   return storageSecretScope({
      configuredDataPath: input.configuredDataPath,
      recordedDataPath,
      mainSchemaPath,
      readOnly: input.readOnly,
      managed: input.managed,
   });
}

/**
 * The metadata catalog of a DuckLake catalog attached as `alias`: DuckLake
 * exposes it as `__ducklake_metadata_<alias>`, with the `ducklake_*` tables in
 * the catalog's metadata schema when one is configured, and in its default
 * schema otherwise.
 */
function metadataCatalogRef(
   alias: string,
   metadataSchema: string | undefined,
): string {
   return [
      `__ducklake_metadata_${alias}`,
      ...(metadataSchema ? [metadataSchema] : []),
   ]
      .map((part) => quoteIdentifier(part, "duckdb"))
      .join(".");
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
   const rows = runSQLRows(
      await runInternalSQL(
         connection,
         `SELECT key, value FROM ${metadataCatalogRef(alias, metadataSchema)}.ducklake_metadata WHERE scope IS NULL`,
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
 * The path a catalog attached as `alias` records for one of its schemas in
 * `ducklake_schema`, and whether it is relative to the data path, or undefined
 * when the schema is not recorded. A path of `''` is what a catalog migrated
 * from DuckLake 0.2 records for `main`: its files sit directly under the data
 * path. A NULL `path_is_relative` reads as relative, the recorded default.
 */
export async function recordedSchemaPath(
   connection: DuckDBConnection,
   alias: string,
   metadataSchema: string | undefined,
   schemaName: string,
): Promise<{ path: string; relative: boolean } | undefined> {
   const row = runSQLRows(
      await runInternalSQL(
         connection,
         `SELECT path, path_is_relative FROM ${metadataCatalogRef(alias, metadataSchema)}.ducklake_schema ` +
            `WHERE schema_name = '${escapeSQL(schemaName)}' AND end_snapshot IS NULL LIMIT 1`,
      ),
   )[0];
   if (!row || typeof row.path !== "string") return undefined;
   return { path: row.path, relative: row.path_is_relative !== false };
}

/**
 * One key of a DuckLake table's partition spec: a column, and the transform
 * DuckLake records for it (`identity`, `year`, `month`, `day`, `hour`, or
 * `bucket(N)`).
 */
export interface PartitionKey {
   column: string;
   transform: string;
}

/**
 * The partition spec `schemaName.tableName` has now, in the catalog attached as
 * `alias`, in key order. Empty when the table is not partitioned, including a
 * table whose spec was reset (DuckLake records that as a spec with no keys), and
 * when no such table exists.
 */
export async function tablePartitionKeys(
   connection: DuckDBConnection,
   alias: string,
   metadataSchema: string | undefined,
   schemaName: string,
   tableName: string,
): Promise<PartitionKey[]> {
   const meta = metadataCatalogRef(alias, metadataSchema);
   const rows = runSQLRows(
      await runInternalSQL(
         connection,
         `SELECT c.column_name, pc.transform
            FROM ${meta}.ducklake_table t
            JOIN ${meta}.ducklake_schema s
              ON s.schema_id = t.schema_id AND s.end_snapshot IS NULL
            JOIN ${meta}.ducklake_partition_info pi
              ON pi.table_id = t.table_id AND pi.end_snapshot IS NULL
            JOIN ${meta}.ducklake_partition_column pc
              ON pc.partition_id = pi.partition_id AND pc.table_id = t.table_id
            JOIN ${meta}.ducklake_column c
              ON c.table_id = t.table_id AND c.column_id = pc.column_id AND c.end_snapshot IS NULL
           WHERE s.schema_name = '${escapeSQL(schemaName)}'
             AND t.table_name = '${escapeSQL(tableName)}'
             AND t.end_snapshot IS NULL
           ORDER BY pc.partition_key_index`,
      ),
   );
   return rows.map((row) => ({
      column: String(row.column_name),
      transform: String(row.transform),
   }));
}

/** `url` without its trailing slashes. A loop, not `/\/+$/`, which backtracks on a long run of them. */
export function stripTrailingSlashes(url: string): string {
   let end = url.length;
   while (end > 0 && url[end - 1] === "/") end--;
   return url.slice(0, end);
}

/** `url` with its trailing slashes replaced by exactly one. */
function oneTrailingSlash(url: string): string {
   return `${stripTrailingSlashes(url)}/`;
}

/**
 * Whether a URL names a scheme but no bucket (`s3://`, `gs:///`). As a scope it
 * would become `s3:/`, which DuckDB matches against every S3 URL, so such a
 * root is refused. A path with no scheme is not a URL and is left alone.
 */
function isBucketlessUrl(url: string): boolean {
   const at = url.indexOf("://");
   return at >= 0 && stripTrailingSlashes(url.slice(at + 3)) === "";
}

/**
 * The URL prefix a DuckLake catalog's storage secret is scoped to, with exactly
 * one trailing slash so `…/org_a/` never matches `…/org_ab`. The configured data
 * path when the config states one, else the data path the catalog recorded,
 * which the preflight read. A managed catalog attached read-only is the
 * user-facing view of its own tables, so its secret reaches only the `main`
 * schema's files: the schema path the catalog recorded, under the data path when
 * relative. Undefined when nothing states a path, when a managed view's main
 * path is unknown, or when the root names no bucket: the caller then refuses
 * the attach rather than creating an unscoped secret.
 */
export function storageSecretScope(input: {
   configuredDataPath?: string;
   recordedDataPath?: string;
   mainSchemaPath?: { path: string; relative: boolean };
   readOnly: boolean;
   managed: boolean;
}): string | undefined {
   const root = input.configuredDataPath || input.recordedDataPath;
   if (!root || isBucketlessUrl(root)) return undefined;
   if (input.managed && input.readOnly) {
      const main = input.mainSchemaPath;
      if (!main) return undefined;
      if (!main.relative) {
         return isBucketlessUrl(main.path)
            ? undefined
            : oneTrailingSlash(main.path);
      }
      // An empty relative path is the data path itself (a catalog migrated from
      // DuckLake 0.2 records it for `main`).
      let start = 0;
      while (start < main.path.length && main.path[start] === "/") start++;
      return oneTrailingSlash(
         `${oneTrailingSlash(root)}${main.path.slice(start)}`,
      );
   }
   return oneTrailingSlash(root);
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
      origin: "storage-destination",
      encrypted: settings.encrypted,
      dataInliningRowLimit: settings.dataInliningRowLimit,
   });
}

async function attachDuckLake(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
   options: { origin?: ConnectionOrigin; managed?: boolean } = {},
): Promise<void> {
   // Read-only unless the connection says otherwise in so many words: absent,
   // null and anything but `false` keep the pre-existing read-only attach.
   await attachDuckLakeWithMode(connection, dbName, ducklakeConfig, {
      readOnly: ducklakeConfig.readOnly !== false,
      origin: options.origin,
      managed: options.managed,
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
   await attachDuckLake(connection, dbName, ducklakeConfig, {
      origin: "storage-destination",
   });
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
      origin: "storage-destination",
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
      origin: "storage-destination",
   });
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
   const scopeClause = credentials.scope
      ? `SCOPE '${escapeSQL(credentials.scope)}'`
      : "";

   let createSecretCommand: string;

   if (isGCS) {
      createSecretCommand = `
         CREATE OR REPLACE SECRET ${secretName} (
            TYPE gcs,
            KEY_ID '${escapedKeyId}',
            SECRET '${escapedSecret}'${scopeClause ? `,\n            ${scopeClause}` : ""}
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

      let clauses: string[];
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
         clauses = [
            "TYPE s3",
            "PROVIDER credential_chain",
            `CHAIN '${chain}'`,
            "REFRESH 'auto'",
            `REGION '${escapeSQL(region)}'`,
         ];
      } else {
         clauses = [
            "TYPE s3",
            `KEY_ID '${escapedKeyId}'`,
            `SECRET '${escapedSecret}'`,
            `REGION '${escapeSQL(region)}'`,
         ];
      }
      // Additive on both arms: an S3-compatible endpoint behind a host role is a
      // real combination, and so is one with temporary keys.
      if (credentials.endpoint) {
         const { hostAndPath, useSsl } = checkS3Endpoint(
            credentials.endpoint,
            credentials.provider,
         );
         clauses.push(
            `ENDPOINT '${escapeSQL(hostAndPath)}'`,
            "URL_STYLE 'path'",
         );
         // `true` is never written: it is DuckDB's default, so a secret for an
         // https or bare endpoint is what a bare one always was.
         if (!useSsl) {
            clauses.push("USE_SSL false");
         }
      }
      if (
         credentials.provider !== "credential_chain" &&
         credentials.sessionToken
      ) {
         clauses.push(`SESSION_TOKEN '${escapeSQL(credentials.sessionToken)}'`);
      }
      if (scopeClause) {
         clauses.push(scopeClause);
      }
      createSecretCommand = `
            CREATE OR REPLACE SECRET ${secretName} (
               ${clauses.join(",\n               ")}
            );
         `;
   }

   return createSecretCommand;
}

async function attachCloudStorage(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
   origin: ConnectionOrigin = "connection",
   scope?: string,
): Promise<void> {
   const storageType = attachedDb.type?.toUpperCase() || "";
   // The scope rides on the credentials so the renewer re-emits it.
   const credentials = {
      ...resolveCloudStorageCredentials(attachedDb, origin),
      scope,
   };

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
      await connection
         .runSQL(`DETACH ${quoteIdentifier(attachedDb.name ?? "", "duckdb")};`)
         .catch(() => {});
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
      const renewOn = async (target: DuckDBConnection) => {
         // Replaced in place on the live connection; no in-flight query is
         // disturbed. Admitted past a locked instance's secret gate for this
         // one statement only (duckdb_lockdown.ts).
         const sql = buildCloudStorageSecretSQL(secretName, credentials);
         await runAsInternalStatement(sql, () => target.runSQL(sql));
      };
      renewable.setStorageSecretRenewer(() => renewOn(connection));
      // The secret is the instance's: a later connection joining the locked
      // instance, which runs no attach of its own, renews it the same way.
      const instance = duckDBInstanceOf(connection);
      if (instance) instanceSecretRenewers.set(instance, renewOn);
   }

   if (scope === undefined) {
      // An attached database names no bucket: its reads are any URL of its
      // scheme the secret reaches.
      for (const prefix of cloudStorageAllowPrefixes(attachedDb.type ?? "")) {
         allowForLockdown(connection, prefix);
      }
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
   const allowPrefix = azureAllowPrefix(
      config.authType === "sas_token" ? config.sasUrl : config.fileUrl,
   );
   if (allowPrefix) allowForLockdown(connection, allowPrefix);

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
      await connection
         .runSQL(`DETACH ${quoteIdentifier(attachedDb.name ?? "", "duckdb")};`)
         .catch(() => {});
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
   const result = await runInternalSQL(
      connection,
      `SELECT COUNT(*) AS count FROM duckdb_secrets() WHERE name = '${escapedSecretName}';`,
   );
   const rows = result.rows;
   return Number(rows?.[0]?.count ?? 0) > 0;
}

/**
 * A digest of the settings one database is attached with. Keys are sorted, so
 * the same settings digest the same whatever order they arrive in: an echoed
 * update has its withheld credentials put back after the other fields
 * (mergeConnectionUpdate), and a JSON column may store keys in its own order.
 * An order-sensitive digest would re-attach every such database on an edit
 * that did not touch it, failing queries in flight against it.
 */
export function attachmentDigest(attachedDb: AttachedDatabase): string {
   const canonical = JSON.stringify(attachedDb, (_key, value: unknown) =>
      value !== null && typeof value === "object" && !Array.isArray(value)
         ? Object.fromEntries(
              Object.entries(value as Record<string, unknown>).sort(
                 ([a], [b]) => (a < b ? -1 : a > b ? 1 : 0),
              ),
           )
         : value,
   );
   return createHash("sha256").update(canonical).digest("hex");
}

/**
 * @param connectionName The Publisher connection the databases belong to. An
 *   earlier generation of it on the same DuckDB instance may have attached
 *   some already (duckdb_instance_sharing.ts, `recordedAttachments`): one whose
 *   settings changed is re-attached, and one no longer declared is detached.
 * @param generation The environment config build this connection belongs to.
 *   A draining older generation never undoes what a newer one attached.
 */
async function attachDatabasesToDuckDB(
   duckdbConnection: DuckDBConnection,
   attachedDatabases: AttachedDatabase[],
   connectionName: string,
   generation: number,
): Promise<void> {
   // The object-storage types attach here; a database type attaches by its
   // registry entry.
   const storageAttachHandlers = {
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

   // The extensions above opened the connection, so its instance is known.
   await serializeAttachRun(duckdbConnection, connectionName, async () => {
      const previous = recordedAttachments(duckdbConnection, connectionName);
      if (previous && previous.generation > generation) {
         // A newer generation owns this connection's attachments here now.
         logger.info(
            `Connection ${connectionName} was edited since; leaving its attached databases to the newer connection`,
         );
         return;
      }
      // What the instance holds for this connection, kept current as each
      // database is replaced, so a run that fails partway leaves a record a
      // retry can finish from.
      const held = new Map<string, RecordedAttachment>(previous?.databases);
      const declared = new Set(attachedDatabases.map((db) => db.name || ""));
      try {
         for (const [name, recorded] of previous?.databases ?? []) {
            if (declared.has(name)) continue;
            // An earlier generation attached it and this one no longer
            // declares it.
            try {
               await detachAttachment(duckdbConnection, name, recorded.type);
               held.delete(name);
            } catch (error) {
               // Kept in the record, so the next generation tries again.
               logger.warn(`Failed to detach removed database ${name}`, {
                  error: redactPgSecrets(errMessage(error)),
               });
            }
         }

         for (const attachedDb of attachedDatabases) {
            const name = attachedDb.name || "";
            const digest = attachmentDigest(attachedDb);
            try {
               const recorded = held.get(name);
               if (recorded !== undefined && recorded.digest !== digest) {
                  // Attached by an earlier generation with other settings:
                  // replace it, or the edit would not take effect until a
                  // restart.
                  await detachAttachment(duckdbConnection, name, recorded.type);
                  held.delete(name);
               } else if (await isDatabaseAttached(duckdbConnection, name)) {
                  held.set(
                     name,
                     recorded ?? { digest, type: attachedDb.type ?? "" },
                  );
                  logger.info(
                     `Database ${attachedDb.name} is already attached, skipping`,
                  );
                  continue;
               }

               const handler =
                  connectionDialectOf(attachedDb.type)?.attached?.attach ??
                  storageAttachHandlers[
                     attachedDb.type as keyof typeof storageAttachHandlers
                  ];
               if (!handler) {
                  throw new Error(
                     `Unsupported database type: ${attachedDb.type}`,
                  );
               }

               try {
                  await handler(duckdbConnection, attachedDb);
               } catch (attachError) {
                  handleAlreadyAttachedError(
                     attachError,
                     attachedDb.name || "",
                  );
               }
               held.set(name, { digest, type: attachedDb.type ?? "" });
            } catch (error) {
               // Attach errors echo the connection string (DuckDB embeds the
               // DSN).
               logger.error(`Failed to attach database ${attachedDb.name}`, {
                  error: redactPgSecrets(errMessage(error)),
               });
               throw new Error(
                  `Failed to attach database ${attachedDb.name}: ${(error as Error).message}`,
               );
            }
         }
      } finally {
         // Whatever the instance holds for this connection now, so the next
         // generation, or a retry of this one, replaces it.
         recordAttachments(duckdbConnection, connectionName, {
            generation,
            databases: held,
         });
      }
   });
}

/**
 * Remove one database an earlier generation attached: the catalog, and the
 * secret its attach created, which is instance-wide too and is all a `gcs`,
 * `s3` or `azure` entry creates. Leaving the secret would keep a removed
 * entry's credentials in force on every later query of the instance.
 */
async function detachAttachment(
   duckdbConnection: DuckDBConnection,
   name: string,
   type: string,
): Promise<void> {
   await duckdbConnection.runSQL(
      `DETACH DATABASE IF EXISTS ${quoteIdentifier(name, "duckdb")};`,
   );
   const attached = connectionDialectOf(type)?.attached;
   const secret = attached
      ? attached.secretName?.(name)
      : type === "gcs" || type === "s3" || type === "azure"
        ? sanitizeSecretName(`${type}_${name}`)
        : undefined;
   if (secret !== undefined) {
      await duckdbConnection.runSQL(`DROP SECRET IF EXISTS ${secret};`);
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

/**
 * The DuckLake connection whose storage-secret renewal the current async chain
 * is running, so a statement the renewal itself issues does not renew again.
 */
const renewingStorageSecretOf = new AsyncLocalStorage<object>();

class DuckLakeConnection extends DuckDBConnection {
   private connectionName: string;
   private storageSecretRenewer?: () => Promise<void>;
   /**
    * The renewal in flight, shared by every statement that failed on the
    * expired credential meanwhile: the statement gate hands the connection to
    * the next statement as soon as one fails, so a second can fail before the
    * first's renewal has run, and it must wait for that renewal, not give up.
    * Resolves to whether the renewal succeeded; it never rejects.
    */
   private storageSecretRenewal?: Promise<boolean>;
   /**
    * How many renewals have completed. A statement that started before the
    * latest one completed ran on the old credential, so it runs again on the
    * new one rather than renewing a credential that is already fresh.
    */
   private storageSecretRenewals = 0;

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
      const renewalsBefore = this.storageSecretRenewals;
      try {
         return await super.runSQL(sql, options);
      } catch (error) {
         if (!(await this.renewedStorageSecretAfter(error, renewalsBefore))) {
            throw await this.catalogUnavailableOr(error);
         }
         try {
            return await super.runSQL(sql, options);
         } catch (rerunError) {
            throw await this.catalogUnavailableOr(rerunError);
         }
      }
   }

   /**
    * Streams SQL (the raw `sqlQuery` route) with {@link runSQL}'s recovery:
    * an expired credential is renewed and the statement run again once, and a
    * lost catalog is confirmed and reported typed. Both errors arrive before
    * the first row (DuckDB binds and opens the scan before it yields), so only
    * the first pull is checked, and nothing is buffered. An error after a row
    * has been yielded is thrown as it is: running again would repeat rows the
    * caller already has.
    */
   async *runSQLStream(
      sql: string,
      options?: Parameters<DuckDBConnection["runSQLStream"]>[1],
   ): AsyncIterableIterator<QueryRecord> {
      const renewalsBefore = this.storageSecretRenewals;
      let rows = super.runSQLStream(sql, options);
      let first: IteratorResult<QueryRecord>;
      try {
         first = await rows.next();
      } catch (error) {
         if (!(await this.renewedStorageSecretAfter(error, renewalsBefore))) {
            throw await this.catalogUnavailableOr(error);
         }
         rows = super.runSQLStream(sql, options);
         try {
            first = await rows.next();
         } catch (rerunError) {
            throw await this.catalogUnavailableOr(rerunError);
         }
      }
      if (first.done) return;
      // A consumer that stops early (a cap, an abort) returns this stream; the
      // driver's must be returned too, or its statement is never released.
      // `yield*` forwards that, but not while the first row is held here.
      try {
         yield first.value;
         yield* rows;
      } finally {
         await rows.return?.(undefined);
      }
   }

   /**
    * Re-resolve the storage credential when `error` is an expired one, so
    * the caller runs its statement again; false, renewing nothing, otherwise.
    * A renewal already in flight is joined rather than started again, and
    * one that completed since the statement started (`renewalsBefore`) is
    * taken as done, so statements failing together share one renewal and each
    * runs once more. A renewal that fails (an STS outage) is logged once and
    * answers false to every statement sharing it, so each throws its own
    * expired-credential error, the storage refusal the caller is told of,
    * rather than the renewer's.
    */
   private async renewedStorageSecretAfter(
      error: unknown,
      renewalsBefore: number,
   ): Promise<boolean> {
      const renew = this.storageSecretRenewer;
      if (
         !renew ||
         !shouldRenewStorageSecret({
            error,
            hasRenewer: !!renew,
            // The renewal's own statement runs through runSQL: a failure there
            // is the renewal's to throw, not one to wait on itself for.
            alreadyRenewing: renewingStorageSecretOf.getStore() === this,
         })
      ) {
         return false;
      }
      if (
         !this.storageSecretRenewal &&
         this.storageSecretRenewals === renewalsBefore
      ) {
         logger.info(
            "Object-storage credential expired; re-resolving and retrying once",
            { connection: this.connectionName },
         );
         const renewal = renewingStorageSecretOf
            .run(this, renew)
            .then(
               () => {
                  this.storageSecretRenewals++;
                  return true;
               },
               (renewalError: unknown) => {
                  logger.warn(
                     "Object-storage credential renewal failed; answering the expired credential's refusal",
                     {
                        connection: this.connectionName,
                        error:
                           renewalError instanceof Error
                              ? renewalError.message
                              : String(renewalError),
                     },
                  );
                  return false;
               },
            )
            .finally(() => {
               if (this.storageSecretRenewal === renewal) {
                  this.storageSecretRenewal = undefined;
               }
            });
         this.storageSecretRenewal = renewal;
      }
      // None in flight: one completed since the statement started, so its
      // credential is already fresh.
      return (await this.storageSecretRenewal) ?? true;
   }

   /**
    * `error`, or a {@link DuckLakeCatalogUnavailableError} when DuckDB failed
    * to resolve a name and the instance confirms this connection's catalog is
    * gone. The error alone cannot say: a three-part path reports `Binder
    * Error: Catalog "<catalog>" does not exist`, but a two-part one
    * (`<catalog>.<table>`, a destination with no schema) reports `Catalog
    * Error: Table with name … does not exist`, as a misbinding does. So any
    * binder or catalog error is checked against the instance, and one whose
    * catalog is attached (a misbinding) is returned as it is. A confirmed loss
    * also drops the instance's cached check, so the next lookup checks again
    * and reopens the connection.
    */
   private async catalogUnavailableOr(error: unknown): Promise<unknown> {
      if (
         !/\b(Binder|Catalog) Error\b/.test(errMessage(error)) ||
         (await aliasAttachedOnInstance(this, this.connectionName)) !== false
      ) {
         return error;
      }
      forgetDuckLakeCatalogChecked(this, this.connectionName);
      return new DuckLakeCatalogUnavailableError(this.connectionName, {
         cause: error,
      });
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

   /**
    * A compile's schema reads (`DESCRIBE`) run through `runRawSQL`, not
    * {@link runSQL}, so they confirm a lost catalog here: without it a
    * catalog lost after its check was cached fails every compile with an
    * untyped binder error while the cached check keeps lookups from ever
    * reopening the connection.
    */
   async fetchTableSchema(
      tableKey: string,
      tablePath: string,
   ): Promise<TableSourceDef> {
      try {
         return await this.fetchTableSchemaIn(tableKey, tablePath);
      } catch (error) {
         throw await this.catalogUnavailableOr(error);
      }
   }

   /** @see fetchTableSchema */
   async fetchSelectSchema(
      sqlRef: Parameters<DuckDBConnection["fetchSelectSchema"]>[0],
   ): Promise<Awaited<ReturnType<DuckDBConnection["fetchSelectSchema"]>>> {
      try {
         return await super.fetchSelectSchema(sqlRef);
      } catch (error) {
         throw await this.catalogUnavailableOr(error);
      }
   }

   private async fetchTableSchemaIn(
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
 * The SQL dialect of every storage destination: DuckLake, the one declarable
 * destination type (DECLARABLE_STORAGE_DESTINATION_TYPES), is DuckDB.
 */
export const STORAGE_DESTINATION_DIALECT = "duckdb";

/**
 * A DuckLake lookup refusing only because its catalog does not exist yet: an
 * attach never creates one, so before the first build the catalog is absent.
 */
const CATALOG_NOT_CREATED_YET =
   /creating a new DuckLake is explicitly disabled|DuckLake .*does not exist/i;

/**
 * `base` for a compile that never runs SQL, such as the dry-run preview of a
 * stored source's views. Malloy learns a connection's dialect by looking the
 * connection up, and looking up a DuckLake destination attaches its catalog,
 * which fails before the first build has created it (an attach never creates
 * a catalog) or while it is unreachable. The lookup error would leave the
 * dialect unknown and every view of a stored source would preview as `live`
 * ("Cannot determine dialect"), though serving it needs only the dialect at
 * compile. So a destination whose lookup fails answers with a stand-in that
 * carries the destination dialect and refuses anything that would run SQL.
 */
export function compileOnlyDestinationConfig(base: MalloyConfig): MalloyConfig {
   const config = new MalloyConfig({ connections: {} });
   config.wrapConnections(() => ({
      lookupConnection: async (name?: string) => {
         try {
            return await base.connections.lookupConnection(name);
         } catch (error) {
            // A name no destination holds stays unknown: the preview must not
            // serve a view from a destination that does not exist.
            const message =
               error instanceof Error ? error.message : String(error);
            if (
               !name ||
               error instanceof ConnectionNotFoundError ||
               /No connection named/.test(message)
            ) {
               throw error;
            }
            // Only a catalog the first build has not created yet compiles
            // against the dialect alone. Anything else (bad credentials, an
            // unreachable catalog host, a wrong path) would fail serving too,
            // so the preview must say so rather than predict `serves`.
            if (!CATALOG_NOT_CREATED_YET.test(message)) throw error;
            const refuse = () => {
               throw new Error(
                  `Destination ${name} could not be reached, so the preview compiles against its dialect only and cannot run SQL: ${
                     error instanceof Error ? error.message : String(error)
                  }`,
               );
            };
            return {
               name,
               dialectName: STORAGE_DESTINATION_DIALECT,
               getDigest: async () => `compile-only:${name}`,
               runSQL: refuse,
               runSQLStream: refuse,
               fetchSchemaForTables: refuse,
               fetchSchemaForSQLStruct: refuse,
               estimateQueryCost: refuse,
               close: async () => {},
               canPersist: () => false,
               canStream: () => false,
               isPool: () => false,
               get supportsNesting() {
                  return true;
               },
            } as unknown as Awaited<
               ReturnType<MalloyConfig["connections"]["lookupConnection"]>
            >;
         }
      },
   }));
   return config;
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

// TLS mode for a proxied postgres connection, as pg connectionString query
// params. The tunnel ends at 127.0.0.1, so no hostname check is possible here:
// `verify-ca` checks the chain against NODE_EXTRA_CA_CERTS without the hostname,
// `no-verify` encrypts without checking, `disable` uses no TLS. `verify-full`
// needs an `ssl` object with `servername` (see resolveProxiedTls).
export function buildProxiedSslQuery(name: string, sslmode?: string): string {
   const caBundle = externalEnv("NODE_EXTRA_CA_CERTS");
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
): EnvironmentPooledPostgresConnection {
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
   return new EnvironmentPooledPostgresConnection({
      name,
      connectionString,
      ...(ssl ? { ssl } : {}),
      ...(setupSQL ? { setupSQL } : {}),
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
 * Per locked instance, the DuckLake catalogs checked attached on it. A lock
 * never lifts and nothing can ATTACH or DETACH through Publisher after it, so
 * a check holds until a read finds the catalog missing
 * ({@link forgetDuckLakeCatalogChecked}); a lookup does not query the
 * instance each time.
 */
const checkedDuckLakeCatalogs = new WeakMap<object, Set<string>>();

/** Per instance, the catalogs found missing from it, logged once. */
const missingDuckLakeCatalogs = new WeakMap<object, Set<string>>();

/** The instances whose loss was reported to `onDuckLakeReopened`, once each. */
const reportedDuckLakeInstances = new WeakSet<object>();

function duckLakeInstanceKey(connection: DuckDBConnection): object {
   return duckDBInstanceOf(connection) ?? connection;
}

function duckLakeCatalogChecked(
   connection: DuckDBConnection,
   name: string,
): boolean {
   return (
      checkedDuckLakeCatalogs.get(duckLakeInstanceKey(connection))?.has(name) ??
      false
   );
}

function markDuckLakeCatalogChecked(
   connection: DuckDBConnection,
   name: string,
): void {
   const key = duckLakeInstanceKey(connection);
   let names = checkedDuckLakeCatalogs.get(key);
   if (!names) {
      names = new Set();
      checkedDuckLakeCatalogs.set(key, names);
   }
   names.add(name);
}

function forgetDuckLakeCatalogChecked(
   connection: DuckDBConnection,
   name: string,
): void {
   checkedDuckLakeCatalogs.get(duckLakeInstanceKey(connection))?.delete(name);
}

/** Record `name` missing on `connection`'s instance; true the first time. */
function noteDuckLakeCatalogMissing(
   connection: DuckDBConnection,
   name: string,
): boolean {
   const key = duckLakeInstanceKey(connection);
   let names = missingDuckLakeCatalogs.get(key);
   if (!names) {
      names = new Set();
      missingDuckLakeCatalogs.set(key, names);
   }
   if (names.has(name)) return false;
   names.add(name);
   return true;
}

/** Managed DuckLake connections that set their search path on a locked instance. */
const managedSearchPathSet = new WeakSet<Connection>();

/** Each instance's storage-secret renewer, for connections that join it. */
const instanceSecretRenewers = new WeakMap<
   object,
   (target: DuckDBConnection) => Promise<void>
>();

/** Environment config builds so far, numbering each one's generation. */
let environmentConfigGenerations = 0;

/**
 * @param isUpdateConnectionRequest Forces a DuckLake re-ATTACH even when the
 *   catalog is already attached, so a generation replacing one after a config
 *   change rebinds to the new catalog DSN and storage secrets rather than the
 *   prior attach state. Only the DuckLake branch reads it.
 * @param options.origin "storage-destination" for the operator's own storage
 *   destinations, which the S3 credential-chain policy treats differently.
 * @param options.onDuckLakeReopened Called when a DuckLake connection was
 *   found without its catalog on its locked instance and closed so the next
 *   lookup reopens it: anything memoized against the closed connection (a
 *   compiled serve shape) must be dropped.
 */
export function buildEnvironmentMalloyConfig(
   connections: ApiConnection[] = [],
   environmentPath: string = "",
   isUpdateConnectionRequest: boolean = false,
   {
      origin = "connection",
      openProxy: openTunnel = openProxy,
      onDuckLakeReopened,
   }: {
      origin?: ConnectionOrigin;
      openProxy?: typeof openProxy;
      onDuckLakeReopened?: (name: string) => void;
   } = {},
): EnvironmentMalloyConfig {
   const assembled = assembleEnvironmentConnections(
      connections,
      environmentPath,
      origin,
   );
   // Orders this build's attachments against other builds' on a shared
   // DuckDB instance (attachDatabasesToDuckDB).
   const generation = ++environmentConfigGenerations;
   // Promises are cached so concurrent lookups share one build and never leak a
   // losing instance's pool. One cache per branch keeps the concrete subtype.
   const duckLakeCache = new Map<string, Promise<DuckLakeConnection>>();
   const snowflakeConnectionCache = new Map<
      string,
      Promise<SnowflakeConnection>
   >();
   const azureDuckDBCache = new Map<string, Promise<AzureDuckDBConnection>>();
   const proxyConnectionCache = new Map<
      string,
      Promise<EnvironmentPooledPostgresConnection>
   >();
   const postgresConnectionCache = new Map<
      string,
      Promise<EnvironmentPooledPostgresConnection>
   >();
   const proxyEndpoints = new Map<string, ProxyEndpoint>();
   const attachPromises = new WeakMap<Connection, Promise<void>>();
   // One DuckLake ATTACH per connection object, shared by concurrent lookups.
   const duckLakeAttaches = new WeakMap<Connection, Promise<void>>();
   // DuckLake connections closed because their instance lost the catalog: a
   // lookup holding one looks up again rather than return it.
   const reopenedDuckLakes = new WeakSet<Connection>();
   // Each reopened connection's close, which a lookup going on to the fresh
   // instance waits for: opened sooner, the fresh connection would join the
   // old instance while the closing connection still holds it.
   const duckLakeClosing = new WeakMap<Connection, Promise<void>>();
   // The connection each cached DuckLake promise resolves to, read
   // synchronously so a reopen drops it before any other lookup runs.
   const duckLakeCurrent = new Map<string, DuckLakeConnection>();
   const closePromises = new WeakMap<Connection, Promise<void>>();
   // An evicted tunnel has left the caches, so release finds its cleanup here.
   const evictions = new Set<Promise<void>>();

   // pg-pool rejects a second end(), so every close path shares one run.
   function closeOnce(connection: Connection): Promise<void> {
      let closePromise = closePromises.get(connection);
      if (!closePromise) {
         closePromise = connection.close();
         closePromises.set(connection, closePromise);
      }
      return closePromise;
   }

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
      const connectionName = metadata.apiConnection.name ?? "";
      if (
         !isDuckDBConnection(connection) ||
         // Declaring none still detaches what an earlier generation attached.
         (metadata.attachedDatabases.length === 0 &&
            !mayHoldAttachments(connectionName))
      ) {
         return;
      }

      // A locked instance already holds this setup: its key digests the
      // attached databases (duckdb_lockdown.ts), and no ATTACH could run on it.
      if (isConfigurationFrozen(connection)) return;

      let attachPromise = attachPromises.get(connection);
      if (!attachPromise) {
         // One ATTACH run per connection object per config generation.
         attachPromise = attachDatabasesToDuckDB(
            connection,
            metadata.attachedDatabases,
            connectionName,
            generation,
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

   /**
    * Attach a DuckLake connection's catalog, once per connection object:
    * concurrent first lookups share one attach. Two attaches of one catalog
    * racing the lock lost it: the first lookup locked the instance between
    * the second's ATTACH and its storage secret, the refused secret's cleanup
    * detached the catalog, and the locked instance could never attach it
    * again. The attach also runs exclusive of any lockdown of its instance
    * (runExclusiveOnDuckDBInstance), so a lock never lands inside it.
    */
   function attachDuckLakeOnce(
      connection: DuckLakeConnection,
      name: string,
      metadata: EnvironmentConnectionMetadata,
   ): Promise<void> {
      let attach = duckLakeAttaches.get(connection);
      if (!attach) {
         attach = (async () => {
            // A locked instance holds this exact catalog setup (its key
            // digests the DuckLake config, duckdb_lockdown.ts), and no ATTACH
            // could run on it: an edit opens a fresh instance.
            if (isConfigurationFrozen(connection)) return;
            // Also opens the instance, so the exclusive step below is keyed
            // on it rather than on this connection alone.
            const attached = await isDatabaseAttached(connection, name);
            if (attached && !isUpdateConnectionRequest) return;
            await runExclusiveOnDuckDBInstance(connection, async () => {
               if (isConfigurationFrozen(connection)) return;
               if (
                  !isUpdateConnectionRequest &&
                  (await isDatabaseAttached(connection, name))
               ) {
                  return;
               }
               await attachDuckLake(
                  connection,
                  name,
                  metadata.apiConnection.ducklakeConnection!,
                  {
                     origin,
                     managed: metadata.apiConnection.managed === true,
                  },
               );
               if (metadata.apiConnection.managed === true) {
                  await setManagedSearchPath(connection, name);
               }
            });
         })();
         // Evicted on failure, so a transient one is retried by the next
         // lookup. Every lookup that joined this attach fails with it (one
         // transient failure fails each concurrent first lookup, once); that
         // is the price of never running two attaches at once.
         attach.catch((error: unknown) => {
            if (duckLakeAttaches.get(connection) === attach) {
               duckLakeAttaches.delete(connection);
            }
            logger.warn(
               "DuckLake attach failed; the lookups sharing it fail, and the next lookup attaches again",
               {
                  code: DUCKLAKE_ATTACH_FAILED,
                  connection: name,
                  origin,
                  instance: duckDBInstanceId(connection),
                  error: redactPgSecrets(errMessage(error)),
               },
            );
         });
         duckLakeAttaches.set(connection, attach);
      }
      return attach;
   }

   /**
    * A lookup of a DuckLake connection on a locked instance: check the catalog
    * is still there (once per instance, see {@link checkedDuckLakeCatalogs}),
    * and join the instance's setup on first use.
    *
    * A locked instance cannot ATTACH, so a catalog missing from it is never
    * coming back on that instance. Once this connection is the instance's only
    * holder, it is closed and dropped from the cache, so the next lookup opens
    * a fresh instance and attaches. While another holder (a draining
    * generation's connection) keeps the instance open, a reopen would only
    * join it again, so the connection is kept and this lookup fails with
    * {@link DuckLakeCatalogUnavailableError}, which a routed query answers
    * live. A lookup that reopens looks up again onto the fresh instance
    * ({@link resolveDuckLake}), so it succeeds. The loss is logged once per
    * instance, and `onDuckLakeReopened` is called once per instance, when it
    * is reopened.
    */
   async function verifyLockedDuckLake(
      connection: DuckLakeConnection,
      name: string,
      metadata: EnvironmentConnectionMetadata,
   ): Promise<void> {
      if (!duckLakeCatalogChecked(connection, name)) {
         const attached = await aliasAttachedOnInstance(connection, name);
         if (attached === false) {
            // Throws while another holder keeps the instance; otherwise the
            // connection is reopened, and the caller looks up again.
            catalogMissing(connection, name);
            return;
         }
         // A check that failed says nothing, so it is not cached.
         if (attached === true) markDuckLakeCatalogChecked(connection, name);
      }
      if (managedSearchPathSet.has(connection)) return;
      managedSearchPathSet.add(connection);
      // A connection joining the locked instance renews the instance's
      // storage secret as the attaching one did.
      const renew = instanceSecretRenewers.get(
         duckDBInstanceOf(connection) ?? {},
      );
      if (renew) {
         connection.setStorageSecretRenewer(() => renew(connection));
      }
      // The search path is per session, and allowed after the lock: a joining
      // connection sets its own.
      if (metadata.apiConnection.managed === true) {
         await setManagedSearchPath(connection, name);
      }
   }

   /**
    * {@link verifyLockedDuckLake} found `name` missing: report it, then throw
    * while another holder keeps the instance, or reopen the connection and
    * return, so the caller looks up again ({@link resolveDuckLake}), as it
    * does for a connection a concurrent lookup already reopened.
    */
   function catalogMissing(connection: DuckLakeConnection, name: string): void {
      if (reopenedDuckLakes.has(connection)) return;
      const instanceKey = duckLakeInstanceKey(connection);
      const first = noteDuckLakeCatalogMissing(connection, name);
      const context = {
         code: DUCKLAKE_CATALOG_UNAVAILABLE,
         connection: name,
         origin,
         instance: duckDBInstanceId(connection),
      };
      if (duckDBInstanceHasOtherHolders(connection)) {
         if (first) {
            logger.error(
               "DuckLake catalog is missing from its locked DuckDB instance; the connection reopens once the instance's other holders are released",
               context,
            );
         }
         throw new DuckLakeCatalogUnavailableError(name);
      }
      if (first) {
         logger.error(
            "DuckLake catalog is missing from its locked DuckDB instance; reopening the connection",
            context,
         );
      } else {
         logger.info(
            "Reopening a DuckLake connection whose instance lost its catalog, now that it is the instance's only holder",
            context,
         );
      }
      if (duckLakeCurrent.get(name) === connection) {
         duckLakeCache.delete(name);
         duckLakeCurrent.delete(name);
      }
      duckLakeAttaches.delete(connection);
      reopenedDuckLakes.add(connection);
      // Released here, so the generation's release has nothing left for it.
      disownDuckDBConnection(malloyConfig, connection);
      const eviction = closeOnce(connection)
         .then(() => hardCloseWhenIdle(connection))
         .catch((error) =>
            logger.warn(
               "Failed to close a DuckLake connection missing its catalog",
               { connection: name, error: errMessage(error) },
            ),
         )
         .then(() => {
            evictions.delete(eviction);
         });
      evictions.add(eviction);
      duckLakeClosing.set(connection, eviction);
      if (!reportedDuckLakeInstances.has(instanceKey)) {
         reportedDuckLakeInstances.add(instanceKey);
         onDuckLakeReopened?.(name);
      }
   }

   /**
    * The DuckLake connection for `name`, attached, and checked when its
    * instance is locked. A lookup whose connection was reopened, by its own
    * check or a concurrent lookup's (its attach or check failing on the
    * closed connection, or passing just before the close), looks up once
    * more, onto the fresh instance, rather than fail or return a closed
    * connection.
    */
   async function resolveDuckLake(
      name: string,
      metadata: EnvironmentConnectionMetadata,
   ): Promise<DuckLakeConnection> {
      for (let attempt = 0; ; attempt++) {
         let connectionPromise = duckLakeCache.get(name);
         if (!connectionPromise) {
            const entry = assembled.pojo.connections[name];
            const built = buildDuckLakeConnection(metadata, entry);
            ownDuckDBConnection(malloyConfig, built);
            connectionPromise = Promise.resolve(built);
            duckLakeCache.set(name, connectionPromise);
            duckLakeCurrent.set(name, built);
         }
         const connection = await connectionPromise;
         try {
            await attachDuckLakeOnce(connection, name, metadata);
            if (isConfigurationFrozen(connection)) {
               await verifyLockedDuckLake(connection, name, metadata);
            }
         } catch (error) {
            if (
               attempt === 0 &&
               reopenedDuckLakes.has(connection) &&
               !(error instanceof DuckLakeCatalogUnavailableError)
            ) {
               await duckLakeClosing.get(connection);
               continue;
            }
            throw error;
         }
         if (!reopenedDuckLakes.has(connection)) return connection;
         if (attempt > 0) throw new DuckLakeCatalogUnavailableError(name);
         await duckLakeClosing.get(connection);
      }
   }

   malloyConfig.wrapConnections(
      (base: LookupConnection<Connection>): LookupConnection<Connection> => {
         const resolveConnection = async (
            name: string | undefined,
            metadata: EnvironmentConnectionMetadata | undefined,
         ): Promise<Connection> => {
            if (metadata?.isDuckLake) {
               return resolveDuckLake(name!, metadata);
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
                     const endpoint = await openTunnel(metadata.proxy!, {
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
                        // A dead tunnel is evicted so the next lookup dials a
                        // fresh one; the identity check skips a superseded entry.
                        endpoint.onClose((cause) => {
                           if (
                              proxyConnectionCache.get(name!) !==
                              connectionPromise
                           ) {
                              return;
                           }
                           proxyConnectionCache.delete(name!);
                           proxyEndpoints.delete(name!);
                           logger.info(
                              "SSH tunnel died; rebuilding on the next query",
                              { connection: name, error: cause?.message },
                           );
                           const eviction = Promise.allSettled([
                              closeOnce(connection).catch((error) =>
                                 logger.warn(
                                    "Failed to close evicted proxied connection",
                                    { connection: name, error },
                                 ),
                              ),
                              endpoint.close(),
                           ]).then(() => {
                              evictions.delete(eviction);
                           });
                           evictions.add(eviction);
                        });
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

            // Every Snowflake connection, password or key pair, so each reads
            // its rows the same way (snowflake_connection.ts).
            if (metadata?.apiConnection.type === "snowflake") {
               let connectionPromise = snowflakeConnectionCache.get(name!);
               if (!connectionPromise) {
                  connectionPromise = Promise.resolve(
                     buildSnowflakeConnection(
                        name!,
                        metadata.apiConnection.snowflakeConnection,
                     ),
                  );
                  snowflakeConnectionCache.set(name!, connectionPromise);
               }
               return connectionPromise;
            }

            if (metadata?.hasAzureAttachment) {
               let connectionPromise = azureDuckDBCache.get(name!);
               if (!connectionPromise) {
                  const entry = assembled.pojo.connections[name!];
                  const built = buildAzureDuckDBConnection(metadata, entry);
                  ownDuckDBConnection(malloyConfig, built);
                  connectionPromise = Promise.resolve(built);
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
            // This generation's lookup created it, so releasing the generation
            // hard-closes it.
            ownDuckDBConnection(malloyConfig, connection);
            if (metadata?.apiConnection.type === "bigquery") {
               guardBigQueryTruncation(connection);
            }
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
                  // A networked instance's setup is done: lock it before any
                  // caller or model SQL reaches it. Malloy locks the sandboxed
                  // and local tiers itself (duckdb_lockdown.ts).
                  if (!isLockedByOptions(connection)) {
                     // A `duckdb` connection with attached databases still
                     // reads its environment's local files, relative ones
                     // included; a DuckLake catalog reads only its data path.
                     const localRoot =
                        metadata &&
                        !metadata.isDuckLake &&
                        metadata.attachedDatabases.length > 0
                           ? metadata.workingDirectory
                           : undefined;
                     const relative = localRoot
                        ? relativeReadEntries(localRoot)
                        : { directories: [], files: [] };
                     await lockDownDuckDBSession(connection, {
                        ...(metadata?.apiConnection.type === "motherduck"
                           ? { motherDuck: name ?? "motherduck" }
                           : {}),
                        allow: localRoot
                           ? [localRoot, ...relative.directories]
                           : [],
                        // Its own database file, which checkpoints write.
                        paths: [
                           ...(metadata?.databasePath
                              ? [
                                   metadata.databasePath,
                                   `${metadata.databasePath}.wal`,
                                ]
                              : []),
                           ...relative.files,
                        ],
                     });
                  }
               }
               // Its configured type, and whether it is `managed`, which a
               // build's eligibility reads (a dialect does not tell DuckDB
               // from DuckLake, nor a managed catalog from another).
               recordConnectionType(connection, metadata?.apiConnection.type, {
                  managed: metadata?.apiConnection.managed === true,
               });
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
            ...snowflakeConnectionCache.values(),
            ...azureDuckDBCache.values(),
            ...proxyConnectionCache.values(),
            ...postgresConnectionCache.values(),
         ];
         const closeResults = await Promise.allSettled([
            malloyConfig.shutdown("close"),
            ...wrapperPromises.map(async (promise) => {
               const connection = await promise;
               await closeOnce(connection);
            }),
            ...evictions,
         ]);
         // Every DuckDB connection this generation created is closed now, so
         // free each native database as soon as its last query settles.
         hardCloseOwnedDuckDBConnections(malloyConfig);
         // Tunnels close only after the builds above settle: an in-flight
         // openProxy registers its endpoint inside that promise, and the tunnel
         // must stay up while the DB connections drain.
         const endpointResults = await Promise.allSettled(
            [...proxyEndpoints.values()].map((ep) => ep.close()),
         );
         duckLakeCache.clear();
         duckLakeCurrent.clear();
         snowflakeConnectionCache.clear();
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
         const attached = connectionDialectOf(attachedDb.type)?.attached;
         if (attached) {
            await attached.test(duckdbConnection, attachedDb.name);
            continue;
         }
         switch (attachedDb.type) {
            case "gcs": {
               await runInternalSQL(
                  duckdbConnection,
                  `SELECT name FROM duckdb_secrets() WHERE name LIKE '%${escapeSQL(attachedDb.name)}%' LIMIT 1`,
               );
               logger.info(
                  `Cloud storage credentials test passed: ${attachedDb.name}`,
               );
               break;
            }
            case "s3": {
               await runInternalSQL(
                  duckdbConnection,
                  `SELECT name FROM duckdb_secrets() WHERE name LIKE '%${escapeSQL(attachedDb.name)}%' LIMIT 1`,
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
                  await runInternalSQL(
                     duckdbConnection,
                     `SELECT name FROM duckdb_secrets() WHERE name LIKE '%${escapeSQL(attachedDb.name)}%' LIMIT 1`,
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
