// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * The `ducklake` connection type's registry entry: a DuckDB-dialect connection
 * over a DuckLake catalog, and the attach of that catalog to a session
 * (`destination`), with its format preflight, create-race retry and scoped
 * storage secret.
 */

import type { DuckDBConnection } from "@malloydata/db-duckdb";
import type { components } from "../../api";
import {
   type ConnectionOrigin,
   getDuckLakeRowGroupSizeBytes,
   getDuckLakeTargetFileSizeBytes,
} from "../../config";
import {
   catalogFormatRangeForEngine,
   isCatalogFormatInRange,
} from "../../ducklake_version";
import { BadRequestError, UnsupportedCatalogFormatError } from "../../errors";
import { logger } from "../../logger";
import { redactPgSecrets } from "../../pg_helpers";
import { errMessage } from "../../utils";
import { attachCloudStorage } from "../cloud_storage_secret";
import {
   allowForLockdown,
   duckDBInstanceId,
   isInstanceLocked,
   networkedDuckDBOptions,
   runInternalSQL,
   setupIdentity,
} from "../duckdb_lockdown";
import {
   applyExtensionSessionSettings,
   escapeSQL,
   installAndLoadExtension,
   runSQLRows,
} from "../duckdb_session_setup";
import {
   DUCKLAKE_ATTACH_FAILED,
   DUCKLAKE_CATALOG_UNAVAILABLE,
} from "../ducklake_catalog_unavailable";
import {
   resolveCloudStorageCredentials,
   validateS3ProviderShape,
} from "../gcs_s3_utils";
import {
   buildPgConnectionString,
   createPgAttachSecret,
   validatePostgresSslmode,
} from "../postgres_conninfo";
import {
   managedCatalogMessage,
   managedSchemas,
   normalizeReadableSchemas,
} from "../managed_catalog_scope";
import { quoteIdentifier } from "../quoting";
import {
   recordedCatalogSettings,
   recordedSchemaPaths,
   storageSecretScope,
} from "./ducklake_catalog";
import type { Connection } from "@malloydata/malloy";
import { standardizeRunSQLResult } from "../gcs_s3_utils";
import { runIntrospectionSQL, sqlLiteral } from "../introspection_sql";
import {
   groupColumnRowsIntoTables,
   sqlInFilter,
   withIntrospectionFailure,
} from "../introspection_helpers";
import type {
   ApiConnection,
   ApiSchema,
   ApiTable,
   ConnectionDialect,
   CoreConnectionEntry,
   DestinationAttachMode,
   NamedConnection,
} from "./types";

type AttachedDatabase = components["schemas"]["AttachedDatabase"];
type PostgresConnection = components["schemas"]["PostgresConnection"];

export const DUCKLAKE: ConnectionDialect = Object.freeze({
   type: "ducklake",
   attributes: Object.freeze({
      dialectName: "duckdb",
      isPool: false,
      canPersist: true,
      canStream: true,
   }),
   validateShape: validateDuckLakeShape,
   coreEntry: buildDuckLakeEntry,
   lockedDown: true,
   fileNamed: true,
   introspector: Object.freeze({
      schemas: getSchemasForDuckLake,
      tables: listTablesForDuckLake,
   }),
   destination: Object.freeze({
      attach: (
         session: DuckDBConnection,
         name: string,
         connection: ApiConnection,
         mode: DestinationAttachMode,
      ) =>
         attachDuckLakeWithMode(
            session,
            name,
            connection.ducklakeConnection!,
            mode,
         ),
      overlap: ducklakeOverlap,
      // Every row a build writes lands in the catalog's data files, never
      // inlined into its metadata database.
      buildSettings: Object.freeze([
         "SET ducklake_default_data_inlining_row_limit=0",
      ]),
   }),
});

/** A plain identifier: what metadataSchema and destinationSchema allow. */
const PLAIN_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_]*$/;

/** A lowercase schema name: what `readableSchemas` and the admin API's schema create accept. */
export const READABLE_SCHEMA_NAME = /^[a-z_][a-z0-9_]*$/;

/** Schemas the engine owns: never created, dropped, listed or read through the API. */
export const SYSTEM_SCHEMAS = [
   "information_schema",
   "pg_catalog",
   "temp",
   "system",
];

/** Names no catalog schema may take: the engine's own, and `memory`, which would make `memory.t` ambiguous with DuckDB's built-in catalog. */
export const RESERVED_SCHEMA_NAMES = ["memory", ...SYSTEM_SCHEMAS];

function validateDuckLakeShape(
   connection: ApiConnection,
   origin: ConnectionOrigin,
): void {
   // Every field the ATTACH demands (see attachDuckLakeWithMode). A
   // destination is only attached at its first BUILD, so a field missed here
   // would surface hours after the config change that caused it.
   if (!connection.ducklakeConnection) {
      throw new Error("DuckLake connection configuration is missing.");
   }
   if (!connection.ducklakeConnection.catalog?.postgresConnection) {
      throw new Error(
         `PostgreSQL connection configuration is required for DuckLake catalog: ${connection.name}`,
      );
   }
   // No bucketUrl is allowed: the catalog must then exist, and its recorded
   // data path is used (dataPathClauses).
   // Shape only, as in the duckdb branch: a throw here fails the ENTIRE
   // environment. The mutual-exclusion and enum guards are safe; the key-pair
   // requirement is NOT applied, because failing every other connection over
   // an incomplete s3Connection is worse than reporting it when used.
   // validateStorageDestinations rejects one destination at a time, so it
   // applies the full check.
   const storage = connection.ducklakeConnection.storage;
   if (storage?.s3Connection) {
      validateS3ProviderShape(storage.s3Connection, connection.name, origin);
   }
   // The attach applies the catalog's own sslmode (buildPgConnectionString) and
   // throws on a value it cannot honor, at every query or build. An enum check,
   // like the guards above. A connectionString carries its own sslmode and the
   // field is ignored beside it, so it is not checked there.
   const catalogPg = connection.ducklakeConnection.catalog.postgresConnection;
   if (catalogPg.sslmode != null && !catalogPg.connectionString) {
      validatePostgresSslmode(
         `DuckLake catalog of connection '${connection.name}'`,
         catalogPg.sslmode,
      );
   }
   // metadataSchema reaches the ATTACH as a quoted string literal AND the
   // catalog-format preflight as a quoted identifier. Rather than escape one
   // value for two grammars, restrict it to a plain identifier.
   //
   // The typeof check is load-bearing: the value comes from untyped JSON and
   // RegExp.test() coerces, so `true` or `null` would pass the pattern and
   // reach escapeSQL as a non-string, a TypeError at the first attach.
   if (connection.ducklakeConnection.catalog.metadataSchema !== undefined) {
      const schema = connection.ducklakeConnection.catalog.metadataSchema;
      if (typeof schema !== "string" || !PLAIN_IDENTIFIER.test(schema)) {
         throw new Error(
            `DuckLake catalog metadataSchema must be a plain identifier ` +
               `([A-Za-z_][A-Za-z0-9_]*), got '${schema}' for connection: ` +
               `${connection.name}`,
         );
      }
   }
   // destinationSchema becomes part of every stored table's path, quoted as an
   // identifier by quoteTablePath; a plain identifier keeps that one grammar.
   const destinationSchema = connection.ducklakeConnection.destinationSchema;
   if (destinationSchema !== undefined && destinationSchema !== null) {
      if (
         typeof destinationSchema !== "string" ||
         !PLAIN_IDENTIFIER.test(destinationSchema)
      ) {
         throw new Error(
            `DuckLake destinationSchema must be a plain identifier ` +
               `([A-Za-z_][A-Za-z0-9_]*), got '${destinationSchema}' for connection: ` +
               `${connection.name}`,
         );
      }
   }
   // Each name becomes a prefix of the view's storage secret scope, so anything
   // but a plain lowercase identifier would widen or break it.
   const readable = connection.ducklakeConnection.readableSchemas;
   if (readable !== undefined && readable !== null) {
      if (origin === "storage-destination") {
         throw new Error(
            `DuckLake readableSchemas applies to a managed connection view, not a ` +
               `storage destination: ${connection.name}`,
         );
      }
      if (
         connection.managed !== true ||
         connection.ducklakeConnection.readOnly === false
      ) {
         throw new Error(
            `DuckLake readableSchemas requires a managed, read-only connection: ` +
               `${connection.name}`,
         );
      }
      if (
         !Array.isArray(readable) ||
         readable.some(
            (name) =>
               typeof name !== "string" || !READABLE_SCHEMA_NAME.test(name),
         )
      ) {
         throw new Error(
            `DuckLake readableSchemas must be a list of lowercase schema names ` +
               `([a-z_][a-z0-9_]*) for connection: ${connection.name}`,
         );
      }
      const reserved = readable.find((name) =>
         RESERVED_SCHEMA_NAMES.includes(name),
      );
      if (reserved !== undefined) {
         throw new Error(
            `DuckLake readableSchemas names '${reserved}', a reserved schema name, ` +
               `for connection: ${connection.name}`,
         );
      }
   }
}

function buildDuckLakeEntry(connection: NamedConnection): CoreConnectionEntry {
   // Shape is validated up front by validateConnectionShape.
   return {
      // The catalog is Postgres and the data remote, so the primary holds
      // nothing: in memory, so a changed generation never waits on a file.
      is: "duckdb",
      databasePath: ":memory:",
      // Attached, then locked to its data path (duckdb_lockdown.ts). The key
      // digests the catalog config, so an edit opens a fresh instance rather
      // than re-attaching on a locked one. The name keeps two lakes with
      // identical config off one locked instance.
      ...networkedDuckDBOptions(
         "ducklake",
         setupIdentity({
            name: connection.name,
            ducklakeConnection: connection.ducklakeConnection
               ? {
                    ...connection.ducklakeConnection,
                    readableSchemas: normalizeReadableSchemas(
                       connection.ducklakeConnection.readableSchemas,
                    ),
                 }
               : null,
            managed: connection.managed === true,
         }),
      ),
   };
}

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
   options: DestinationAttachMode,
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

   const storage = await storageAttachment(
      connection,
      dbName,
      ducklakeConfig,
      options.origin,
   );

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
   await secureCatalogStorage(connection, dbName, storage, {
      dataPath,
      metadataSchema,
      readableSchemas: normalizeReadableSchemas(ducklakeConfig.readableSchemas),
      options,
   });
   return true;
}

/**
 * The storage attachment a catalog's data files are read through, with its
 * credential resolved (and refused by policy) before anything attaches; for
 * S3, the instance's global S3 fallback is switched off first. Undefined for
 * a catalog on local storage.
 */
async function storageAttachment(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
   origin: ConnectionOrigin | undefined,
): Promise<AttachedDatabase | undefined> {
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
      resolveCloudStorageCredentials(storage, origin ?? "connection");
   }
   return storage;
}

/**
 * After the ATTACH: allow what the catalog's lock may read, and create its
 * storage secret scoped to the catalog's prefixes, detaching the catalog again
 * when the secret fails on an unlocked instance.
 */
async function secureCatalogStorage(
   connection: DuckDBConnection,
   dbName: string,
   storage: AttachedDatabase | undefined,
   input: {
      dataPath: string | undefined;
      metadataSchema: string | undefined;
      readableSchemas: string[] | undefined;
      options: DestinationAttachMode;
   },
): Promise<void> {
   const { dataPath, metadataSchema, readableSchemas, options } = input;
   // Only a connection view is narrowed: an operator's destination serves
   // every schema, whatever flags its entry carries.
   const managedView =
      options.managed === true && options.origin !== "storage-destination";
   if (!storage) {
      // No storage secret (a local data path): what its lock allows is the
      // whole data path, or, for a managed read-only view, only the files of
      // `main` and its readable schemas, as a secret would be scoped. A managed
      // view whose main path cannot be read is allowed no data file at all.
      const roots =
         managedView && options.readOnly
            ? await attachedDuckLakeScope(connection, dbName, {
                 configuredDataPath: dataPath,
                 metadataSchema,
                 readableSchemas,
                 readOnly: true,
                 managed: true,
              }).catch(() => undefined)
            : [
                 dataPath ||
                    (await recordedCatalogSettings(
                       connection,
                       dbName,
                       metadataSchema,
                    )
                       .then((settings) => settings.data_path)
                       .catch(() => undefined)),
              ];
      for (const root of roots ?? []) {
         if (root) allowForLockdown(connection, root);
      }
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
            readableSchemas,
            readOnly: options.readOnly,
            managed: managedView,
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
         for (const prefix of scope) allowForLockdown(connection, prefix);
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
}

/**
 * The scope of an attached catalog's storage secret ({@link storageSecretScope}),
 * reading from the catalog's metadata whatever the config does not state: the
 * recorded data path when the config has none, and the paths of `main` and the
 * readable schemas for a managed read-only view. Runs on the attached catalog,
 * so nothing here is a guess the preflight could fail softly on.
 */
async function attachedDuckLakeScope(
   connection: DuckDBConnection,
   dbName: string,
   input: {
      configuredDataPath?: string;
      metadataSchema?: string;
      readableSchemas?: string[];
      readOnly: boolean;
      managed: boolean;
   },
): Promise<string[] | undefined> {
   const recordedDataPath = input.configuredDataPath
      ? undefined
      : (
           await recordedCatalogSettings(
              connection,
              dbName,
              input.metadataSchema,
           )
        ).data_path;
   const schemaPaths =
      input.managed && input.readOnly
         ? await recordedSchemaPaths(connection, dbName, input.metadataSchema, [
              ...managedSchemas(input.readableSchemas),
           ])
         : undefined;
   return storageSecretScope({
      catalog: dbName,
      configuredDataPath: input.configuredDataPath,
      recordedDataPath,
      schemaPaths,
      readableSchemas: input.readableSchemas,
      readOnly: input.readOnly,
      managed: input.managed,
   });
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
 * After a managed DuckLake attach, make the catalog's `main` the session's
 * search path: DuckDB never searches an attached catalog for an unqualified
 * table name, and `<name>.table('t')` compiles to the bare name. The alias is
 * quoted as an identifier inside the string literal the setting takes.
 */
export function managedSearchPathSql(dbName: string): string {
   const alias = `"${dbName.replace(/"/g, '""')}"`;
   return `SET search_path = '${`${alias}.main`.replace(/'/g, "''")}'`;
}

async function getSchemasForDuckLake(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   return withIntrospectionFailure(
      `schemas for DuckLake connection ${connection.name}`,
      async () => {
         // The catalog is attached with the connection name (see attachDuckLake in connection.ts)
         const catalogName = connection.name ?? "";
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '${sqlLiteral(catalogName, connection.type)}' ORDER BY schema_name`,
         );
         // A managed connection reads only its catalog's `main` and readable
         // schemas (managed_catalog_scope.ts): the others are not listed to it.
         const readable =
            connection.managed === true
               ? managedSchemas(connection.ducklakeConnection?.readableSchemas)
               : undefined;
         const rows = standardizeRunSQLResult(result).filter(
            (row) =>
               !readable ||
               readable.has(
                  (row as Record<string, unknown>).schema_name as string,
               ),
         );

         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = typedRow.schema_name as string;
            const shouldShow =
               schemaName === "main" ||
               schemaName === "public" ||
               readable?.has(schemaName) === true;
            return {
               name: schemaName,
               isHidden: !shouldShow,
               isDefault: false,
            };
         });
      },
   );
}

async function listTablesForDuckLake(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   // Prefix bare schema names with the catalog (connection) name.
   // Two-part names like "catalog.schema" are already qualified.
   if (!schemaName.includes(".")) {
      schemaName = `${connection.name}.${schemaName}`;
   }

   const catalogName = schemaName.split(".")[0];
   const actualSchemaName = schemaName.split(".")[1];
   if (
      connection.managed === true &&
      (catalogName.toLowerCase() !== (connection.name ?? "").toLowerCase() ||
         !managedSchemas(connection.ducklakeConnection?.readableSchemas).has(
            actualSchemaName?.toLowerCase() ?? "",
         ) ||
         schemaName.split(".").length !== 2)
   ) {
      throw new BadRequestError(
         managedCatalogMessage(
            "A table listing",
            connection.name ?? "",
            `the schema '${schemaName}'`,
         ),
      );
   }
   return withIntrospectionFailure(
      `tables for DuckLake schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${sqlLiteral(actualSchemaName, connection.type)}' AND table_catalog = '${sqlLiteral(catalogName, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}

/**
 * Why a DuckLake `connection` reaches DuckLake `destination`'s tables
 * (destination_overlap.ts): it names the same catalog database and metadata
 * schema, or its data path overlaps the destination's.
 */
function ducklakeOverlap(
   connection: ApiConnection,
   destination: ApiConnection,
): string | undefined {
   const c = connection.ducklakeConnection;
   const d = destination.ducklakeConnection;
   if (!c || !d) return undefined;
   const cCatalog = catalogKey(c.catalog?.postgresConnection);
   if (
      cCatalog !== undefined &&
      cCatalog === catalogKey(d.catalog?.postgresConnection) &&
      (c.catalog?.metadataSchema ?? "") === (d.catalog?.metadataSchema ?? "")
   ) {
      return "it names the same DuckLake catalog";
   }
   const cPath = dataPath(c.storage?.bucketUrl);
   const dPath = dataPath(d.storage?.bucketUrl);
   if (cPath && dPath && (cPath.startsWith(dPath) || dPath.startsWith(cPath))) {
      return "its data path overlaps the destination's";
   }
   return undefined;
}

/** The catalog database a DuckLake config names, normalized. */
function catalogKey(pg: PostgresConnection | undefined): string | undefined {
   if (!pg) return undefined;
   if (pg.connectionString?.trim()) return pg.connectionString.trim();
   if (!pg.databaseName) return undefined;
   return `${(pg.host ?? "localhost").toLowerCase()}:${pg.port ?? 5432}/${pg.databaseName}`;
}

/** A data path, normalized for a prefix comparison. */
function dataPath(url: string | undefined): string | undefined {
   const trimmed = url?.trim().replace(/\/+$/, "");
   return trimmed ? `${trimmed}/` : undefined;
}
