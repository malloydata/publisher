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
} from "@malloydata/db-postgres";
// Registers the "publisher" connection type (proxies SQL to a remote Publisher
// dataplane). No live-class branch is needed in lookupConnection — the default
// path resolves it through the registry like any other registered type.
import "@malloydata/db-publisher";
import {
   buildPoolOptions,
   SnowflakeConnection,
} from "@malloydata/db-snowflake";
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
import fs from "fs/promises";
import { components } from "../api";
import { getExtensionFetchPolicy } from "../config";
import {
   catalogFormatRangeForEngine,
   isCatalogFormatInRange,
} from "../ducklake_version";
import { UnsupportedCatalogFormatError } from "../errors";
import { logAxiosError, logger } from "../logger";
import { redactPgSecrets } from "../pg_helpers";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   safeJoinUnderRoot,
} from "../path_safety";
import {
   assembleEnvironmentConnections,
   CoreConnectionEntry,
   EnvironmentConnectionMetadata,
   normalizeSnowflakePrivateKey,
} from "./connection_config";
import { CloudStorageCredentials } from "./gcs_s3_utils";
import { openProxy, type ProxyEndpoint } from "./proxy";

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

// Extends the public API connection with the internal connection objects
// which contains passwords and connection strings.
export type InternalConnection = ApiConnection & {
   postgresConnection?: components["schemas"]["PostgresConnection"];
   bigqueryConnection?: components["schemas"]["BigqueryConnection"];
   snowflakeConnection?: components["schemas"]["SnowflakeConnection"];
   trinoConnection?: components["schemas"]["TrinoConnection"];
   databricksConnection?: components["schemas"]["DatabricksConnection"];
   mysqlConnection?: components["schemas"]["MysqlConnection"];
   duckdbConnection?: components["schemas"]["DuckdbConnection"];
   ducklakeConnection?: components["schemas"]["DucklakeConnection"];
};

// Shared utilities

// Publisher-owned DuckDB sessions whose extension PRAGMAs have already been
// pinned. The lookup funnel runs on every resolve, and the attach paths pin
// too, so this WeakSet keeps the SETs to once per connection object. The
// DuckLake tier pins with `alwaysDisableAutoinstall` inside its attach, which
// runs before the funnel ever sees the connection, so the stronger always-off
// setting is recorded first and never re-evaluated down to policy here.
const extensionSessionPinned = new WeakSet<Connection>();

/**
 * Pin the extension-management PRAGMAs on a Publisher-owned DuckDB session.
 * Publisher installs the extensions it needs explicitly (see
 * {@link installAndLoadExtension}), so DuckDB's own IMPLICIT auto-install
 * (fetching a known extension a query happens to reference) is never
 * load-bearing.
 *
 * Auto-LOAD is left ON (DuckDB's default) so any locally-present extension
 * still lazy-loads — the local-first UX every deployment relies on. Implicit
 * auto-INSTALL is turned OFF when EITHER:
 *   - `alwaysDisableAutoinstall` is set — the DuckLake tier's build/serve
 *     sessions only ever need the curated extension set, so they never
 *     implicitly fetch, regardless of the deployment policy; OR
 *   - `EXTENSION_FETCH_POLICY=local-only` — the air-gapped / pinned-image
 *     posture, where no session may reach the network.
 *
 * Under the default `on-demand` policy a generic session is left untouched
 * (DuckDB's default), so existing standalone/OSS users see no behaviour change.
 *
 * Called for EVERY Publisher-owned DuckDB session — the environment lookup
 * funnel, the per-package sandbox, and the attach entry points — so the pin has
 * no gap regardless of whether a session has attached databases. On a failed
 * SET: under `local-only` we FAIL CLOSED (throw), because the whole promise of
 * that mode is that no session can reach the network; under the tier's
 * on-demand always-off case the pin is defense-in-depth (the install path
 * already avoids implicit fetches), so we log and continue rather than fail an
 * otherwise-valid attach.
 */
export async function applyExtensionSessionSettings(
   connection: DuckDBConnection,
   {
      alwaysDisableAutoinstall = false,
   }: { alwaysDisableAutoinstall?: boolean } = {},
): Promise<void> {
   const policy = getExtensionFetchPolicy();
   const disableAutoinstall =
      alwaysDisableAutoinstall || policy === "local-only";
   if (!disableAutoinstall || extensionSessionPinned.has(connection)) {
      return;
   }
   try {
      await connection.runSQL("SET autoinstall_known_extensions=false;");
      // Keep autoload on: it is DuckDB's default, but set it explicitly next to
      // an install lockdown so a locally-present/baked extension still loads.
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

async function installAndLoadExtension(
   connection: DuckDBConnection,
   extensionName: string,
   fromCommunity = false,
): Promise<void> {
   const policy = getExtensionFetchPolicy();

   // `local-only` never fetches: skip INSTALL entirely and rely on the
   // extension already being present on disk (baked into the image). Auto-LOAD
   // stays on (see applyExtensionSessionSettings), so a present extension still
   // lazy-loads; a missing one fails the LOAD below and is reported loudly.
   if (policy === "on-demand") {
      try {
         // Plain INSTALL is local-first: it no-ops when the extension is already
         // present and only fetches when it is missing. We dropped FORCE, which
         // re-fetched from the community CDN whenever the network was up — even
         // for a baked extension — silently drifting the running binary past the
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
      logger.warn(`Failed to check existing databases:`, error);
      return false;
   }
}

function sanitizeSecretName(name: string): string {
   return `secret_${name.replace(/[^a-zA-Z0-9_]/g, "_")}`;
}

function escapeSQL(value: string): string {
   return value.replace(/'/g, "''");
}

function handleAlreadyAttachedError(error: unknown, dbName: string): void {
   if (error instanceof Error && error.message.includes("already exists")) {
      logger.info(`Database ${dbName} is already attached, skipping`);
   } else {
      throw error;
   }
}

// Database-specific attachment handlers
async function attachBigQuery(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.bigqueryConnection) {
      throw new Error(
         `BigQuery connection configuration missing for: ${attachedDb.name}`,
      );
   }

   const config = attachedDb.bigqueryConnection;
   let projectId = config.defaultProjectId;
   let serviceAccountJson: string | undefined;

   // Parse and validate service account key
   if (config.serviceAccountKeyJson) {
      const keyData = JSON.parse(config.serviceAccountKeyJson as string);

      const requiredFields = [
         "type",
         "project_id",
         "private_key",
         "client_email",
      ];
      for (const field of requiredFields) {
         if (!keyData[field]) {
            throw new Error(
               `Invalid service account key: missing "${field}" field`,
            );
         }
      }

      if (keyData.type !== "service_account") {
         throw new Error('Invalid service account key: incorrect "type" field');
      }

      projectId = keyData.project_id || config.defaultProjectId;
      serviceAccountJson = config.serviceAccountKeyJson as string;
      logger.info(`Using service account: ${keyData.client_email}`);
   }

   if (!projectId || !serviceAccountJson) {
      throw new Error(
         `BigQuery project_id and service account key required for: ${attachedDb.name}`,
      );
   }

   await installAndLoadExtension(connection, "bigquery", true);

   const secretName = sanitizeSecretName(`bigquery_${attachedDb.name}`);
   const escapedJson = escapeSQL(serviceAccountJson);

   const createSecretCommand = `
      CREATE OR REPLACE SECRET ${secretName} (
         TYPE BIGQUERY,
         SCOPE 'bq://${projectId}',
         SERVICE_ACCOUNT_JSON '${escapedJson}'
      );
   `;

   await connection.runSQL(createSecretCommand);
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

   // Validate required fields
   const requiredFields = {
      account: config.account,
      username: config.username,
      password: config.password,
   };
   for (const [field, value] of Object.entries(requiredFields)) {
      if (!value) {
         throw new Error(
            `Snowflake ${field} is required for: ${attachedDb.name}`,
         );
      }
   }

   await installAndLoadExtension(connection, "snowflake", true);

   // Verify ADBC driver
   try {
      const version = await connection.runSQL("SELECT snowflake_version();");
      logger.info(`Snowflake ADBC driver verified with version:`, version.rows);
   } catch (error) {
      throw new Error(
         `Snowflake ADBC driver verification failed: ${error instanceof Error ? error.message : String(error)}`,
      );
   }

   // Build connection parameters
   const params = {
      account: escapeSQL(config.account || ""),
      user: escapeSQL(config.username || ""),
      password: escapeSQL(config.password || ""),
      database: config.database ? escapeSQL(config.database) : undefined,
      warehouse: config.warehouse ? escapeSQL(config.warehouse) : undefined,
      schema: config.schema ? escapeSQL(config.schema) : undefined,
      role: config.role ? escapeSQL(config.role) : undefined,
   };

   // Create attach string
   const attachParts = [
      `account=${params.account}`,
      `user=${params.user}`,
      `password=${params.password}`,
   ];

   if (params.database) attachParts.push(`database=${params.database}`);
   if (params.warehouse) attachParts.push(`warehouse=${params.warehouse}`);

   const secretString = `CREATE OR REPLACE SECRET ${attachedDb.name}_secret (
      TYPE snowflake,
      ACCOUNT '${params.account}',
      USER '${params.user}',
      PASSWORD '${params.password}',
      DATABASE '${params.database}',
      WAREHOUSE '${params.warehouse}'
   );`;

   await connection.runSQL(secretString);

   const testresult = await connection.runSQL(
      `SELECT * FROM snowflake_query('SELECT 1', '${attachedDb.name}_secret');`,
   );
   logger.info(`Testing Snowflake connection:`, testresult.rows);

   const attachCommand = `ATTACH '${attachedDb.name}' AS ${attachedDb.name} (TYPE snowflake, SECRET ${attachedDb.name}_secret, READ_ONLY);`;
   await connection.runSQL(attachCommand);
   logger.info(`Successfully attached Snowflake database: ${attachedDb.name}`);
}

function buildPgConnectionString(
   pg: components["schemas"]["PostgresConnection"],
): string {
   if (pg.connectionString) {
      return pg.connectionString;
   }

   const parts: string[] = [];
   if (pg.host) parts.push(`host=${pg.host}`);
   if (pg.port) parts.push(`port=${pg.port}`);
   if (pg.databaseName) parts.push(`dbname=${pg.databaseName}`);
   if (pg.userName) parts.push(`user=${pg.userName}`);
   if (pg.password) parts.push(`password=${pg.password}`);

   const pgSSLMode = process.env.PGSSLMODE;

   if (pgSSLMode) {
      const mapping: Record<string, string> = {
         "no-verify": "disable",
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

   await installAndLoadExtension(connection, "postgres");

   const config = attachedDb.postgresConnection;
   const attachString: string = buildPgConnectionString(config);

   const attachCommand = `ATTACH '${escapeSQL(attachString)}' AS ${attachedDb.name} (TYPE postgres, READ_ONLY);`;
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
 * Runtime range-preflight for a DuckLake ATTACH. Reads the catalog's recorded
 * format version from its `ducklake_metadata` table via a plain postgres ATTACH
 * (this does NOT invoke the DuckLake extension on the catalog), and checks it
 * against the format range the running DuckDB engine supports (see
 * `ducklake_version.ts`). On a mismatch it throws an
 * {@link UnsupportedCatalogFormatError} (→ HTTP 422) naming the format found,
 * the supported range, and where to migrate — instead of letting the real
 * DuckLake ATTACH fail deep inside DuckDB as an opaque 500.
 *
 * Non-load-bearing on its own read: any failure of the preflight itself
 * (missing `ducklake_metadata` table, non-postgres catalog, query timeout,
 * connect failure, an engine with no matrix row) logs and returns so the real
 * ATTACH below stays the source of truth for unrelated errors. Only a
 * successfully-read, definitively-out-of-range format raises the clean 422.
 *
 * The temp attach uses a per-call unique name so a failed DETACH can't wedge
 * the fixed name (which would make every later preflight for this connection
 * fail its ATTACH with "already exists" and silently skip), and so two
 * concurrent first-touch lookups can't cross-DETACH each other.
 */
let ducklakePreflightSeq = 0;
async function preflightDuckLakeCatalogFormat(
   connection: DuckDBConnection,
   dbName: string,
   pgConnString: string,
): Promise<void> {
   const tempDb = `${dbName}_fmt_preflight_${++ducklakePreflightSeq}`;
   let catalogFormat: string | undefined;
   try {
      await connection.runSQL(
         `ATTACH '${escapeSQL(pgConnString)}' AS ${tempDb} (TYPE postgres, READ_ONLY);`,
      );
      const result = await connection.runSQL(
         `SELECT value FROM ${tempDb}.ducklake_metadata WHERE key = 'version' LIMIT 1;`,
      );
      const value = runSQLRows(result)[0]?.value;
      catalogFormat = typeof value === "string" ? value : undefined;
   } catch (error) {
      logger.warn(
         "DuckLake catalog-format preflight read failed; falling back to ATTACH",
         {
            dbName,
            error: redactPgSecrets(
               error instanceof Error ? error.message : String(error),
            ),
         },
      );
      return;
   } finally {
      try {
         await connection.runSQL(`DETACH ${tempDb};`);
      } catch {
         // The ATTACH may have failed, so there may be nothing to detach.
      }
   }

   if (!catalogFormat) {
      // Table present but no version row, or unreadable value: let ATTACH decide.
      return;
   }

   // Derive the supported range from the engine DuckDB actually reports. An
   // unknown engine (no matrix row) yields null → skip (the CI version-contract
   // check is what guarantees the pinned engine is always covered).
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
      return;
   }

   const range = catalogFormatRangeForEngine(engineVersion);
   if (!range) {
      logger.warn(
         "DuckLake catalog-format preflight: engine not covered by the format matrix; skipping",
         { dbName, engineVersion },
      );
      return;
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
}

async function attachDuckLake(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
): Promise<void> {
   // Tier build/serve DuckLake sessions never implicitly auto-install (Publisher
   // installs the DuckLake stack explicitly below) — regardless of the policy.
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

   if (!ducklakeConfig.storage?.bucketUrl) {
      throw new Error(`Storage bucketUrl is required for DuckLake: ${dbName}`);
   }

   // Set up cloud storage secret so DuckDB can access S3/GCS
   const hasS3 = !!ducklakeConfig.storage.s3Connection;
   const hasGCS = !!ducklakeConfig.storage.gcsConnection;

   if (hasS3) {
      await attachCloudStorage(connection, {
         name: `${dbName}_storage`,
         type: "s3",
         s3Connection: ducklakeConfig.storage.s3Connection,
      });
   } else if (hasGCS) {
      await attachCloudStorage(connection, {
         name: `${dbName}_storage`,
         type: "gcs",
         gcsConnection: ducklakeConfig.storage.gcsConnection,
      });
   }

   const pg = ducklakeConfig.catalog.postgresConnection;
   const pgConnString: string = buildPgConnectionString(pg);
   // Attach DuckLake with Postgres catalog and cloud storage data path in READ_ONLY mode
   // The client manages metadata - we only read from the catalogs
   logger.info(`pgConnString: ${redactPgSecrets(pgConnString)}`);
   const escapedPgConnString = escapeSQL(pgConnString);
   logger.info(
      `Final escaped connection string: ${redactPgSecrets(escapedPgConnString)}`,
   );
   const escapedBucketUrl = escapeSQL(ducklakeConfig.storage.bucketUrl);
   logger.info(`escapedBucketUrl: ${escapedBucketUrl}`);
   // Range-preflight the catalog's recorded format version so an unsupported
   // catalog fails as a clean, actionable 422 rather than a deep DuckDB 500.
   await preflightDuckLakeCatalogFormat(connection, dbName, pgConnString);
   const attachCommand = `ATTACH OR REPLACE 'ducklake:postgres:${escapedPgConnString}' AS ${dbName} (DATA_PATH '${escapedBucketUrl}', OVERRIDE_DATA_PATH true, READ_ONLY true);`;
   logger.info(
      `Attaching DuckLake database using command: ${redactPgSecrets(attachCommand)}`,
   );
   try {
      await connection.runSQL(attachCommand);
      logger.info(
         `Successfully attached DuckLake database in READ_ONLY mode: ${dbName}`,
      );
   } catch (error) {
      // Handle case where DuckLake database is already attached
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
}

async function attachCloudStorage(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   const isGCS = attachedDb.type === "gcs";
   const isS3 = attachedDb.type === "s3";

   if (!isGCS && !isS3) {
      throw new Error(`Invalid cloud storage type: ${attachedDb.type}`);
   }

   const storageType = attachedDb.type?.toUpperCase() || "";
   let credentials: CloudStorageCredentials;

   if (isGCS) {
      if (!attachedDb.gcsConnection) {
         throw new Error(
            `GCS connection configuration missing for: ${attachedDb.name}`,
         );
      }
      if (!attachedDb.gcsConnection.keyId || !attachedDb.gcsConnection.secret) {
         throw new Error(
            `GCS keyId and secret are required for: ${attachedDb.name}`,
         );
      }
      credentials = {
         type: "gcs",
         accessKeyId: attachedDb.gcsConnection.keyId,
         secretAccessKey: attachedDb.gcsConnection.secret,
      };
   } else {
      if (!attachedDb.s3Connection) {
         throw new Error(
            `S3 connection configuration missing for: ${attachedDb.name}`,
         );
      }
      if (
         !attachedDb.s3Connection.accessKeyId ||
         !attachedDb.s3Connection.secretAccessKey
      ) {
         throw new Error(
            `S3 accessKeyId and secretAccessKey are required for: ${attachedDb.name}`,
         );
      }
      credentials = {
         type: "s3",
         accessKeyId: attachedDb.s3Connection.accessKeyId,
         secretAccessKey: attachedDb.s3Connection.secretAccessKey,
         region: attachedDb.s3Connection.region,
         endpoint: attachedDb.s3Connection.endpoint,
         sessionToken: attachedDb.s3Connection.sessionToken,
      };
   }

   await installAndLoadExtension(connection, "httpfs");

   const secretName = sanitizeSecretName(
      `${attachedDb.type}_${attachedDb.name}`,
   );
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
      const region = credentials.region || "us-east-1";

      if (credentials.endpoint) {
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

   if (await doesSecretExistInDuckDB(connection, secretName)) {
      // Force refresh attachments using this storage
      await connection.runSQL(`DETACH ${attachedDb.name};`).catch(() => {});
   }
   await connection.runSQL(createSecretCommand);

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

   // Extensions are loaded once in attachDatabasesToDuckDB before the loop
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

      // For SAS token auth, DuckDB can read the HTTPS SAS URL directly via httpfs.
      // No Azure secret is needed — just ensure httpfs is loaded.
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

// Main attachment function
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

   // Generic (non-tier) attach session: defer to the deployment policy — off
   // under local-only, left at DuckDB's default under on-demand so existing
   // users see no behaviour change. Publisher still installs each attached
   // type's extension explicitly below.
   await applyExtensionSessionSettings(duckdbConnection);

   // Pre-load extensions needed by any attached database type, once per connection
   const hasAzure = attachedDatabases.some((db) => db.type === "azure");
   if (hasAzure) {
      await installAndLoadExtension(duckdbConnection, "azure");
      await installAndLoadExtension(duckdbConnection, "httpfs");
   }

   for (const attachedDb of attachedDatabases) {
      try {
         // Check if already attached
         if (
            await isDatabaseAttached(duckdbConnection, attachedDb.name || "")
         ) {
            logger.info(
               `Database ${attachedDb.name} is already attached, skipping`,
            );
            continue;
         }

         // Get the appropriate handler
         const handler =
            attachHandlers[attachedDb.type as keyof typeof attachHandlers];
         if (!handler) {
            throw new Error(`Unsupported database type: ${attachedDb.type}`);
         }

         // Execute attachment
         try {
            await handler(duckdbConnection, attachedDb);
         } catch (attachError) {
            handleAlreadyAttachedError(attachError, attachedDb.name || "");
         }
      } catch (error) {
         logger.error(`Failed to attach database ${attachedDb.name}:`, error);
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
      // Single file URL — replace the filename portion so we don't double-append
      if (/\.(parquet|csv|json|jsonl|ndjson)$/i.test(baseUrl)) {
         const dir = baseUrl.replace(/\/[^/]+$/, "");
         return `${dir}/${blobName}${token}`;
      }
      // Glob or directory — strip trailing glob/slash and append blobName
      const cleanBase = baseUrl.replace(/\/\*[^/]*$/, "").replace(/\/+$/, "");
      return `${cleanBase}/${blobName}${token}`;
   } else if (azureConn.authType === "service_principal" && azureConn.fileUrl) {
      const url = azureConn.fileUrl;
      // Single file URL (ends with a data file extension) — replace the
      // filename with blobName so we don't double-append
      if (/\.(parquet|csv|json|jsonl|ndjson)$/i.test(url)) {
         return url.replace(/\/[^/]+$/, `/${blobName}`);
      }
      // Glob or directory — strip glob pattern and trailing slash, append blobName
      const base = url.replace(/\*[^/]*$/, "").replace(/\/+$/, "");
      return `${base}/${blobName}`;
   }
   throw new Error(
      `Cannot build Azure file URL: missing sasUrl or fileUrl in config`,
   );
}

/**
 * Extends DuckDBConnection to resolve Azure attached-database table paths.
 * When Malloy compiles di.table('azure_schema.blob_name'), the path
 * 'azure_schema.blob_name' is passed to fetchTableSchema.  This override
 * detects that prefix, constructs the real Azure URL, and forwards it to
 * DuckDB so the azure/httpfs extension can read the file.
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
               throw new Error(`Azure file not found: ${azureUrl}`);
            }
            return result;
         }
      }

      const result = await super.fetchTableSchema(tableKey, tablePath);
      if (!result) {
         throw new Error(`Table ${tablePath} not found`);
      }
      return result;
   }
}

class DuckLakeConnection extends DuckDBConnection {
   private connectionName: string;

   constructor(options: PublisherDuckDBOptions) {
      super(options);

      // Validate that this is a DuckLake connection by checking the database path pattern
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
      // DuckLake-specific logic: prefix table path with connection name if needed
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
            throw new Error(
               `Table ${prefixedPath} not found in connection ${this.connectionName}`,
            );
         }
         return result;
      }
      // If already prefixed or has 3+ parts, use as-is;
      // For attached databases, in the future
      const result = await super.fetchTableSchema(tableKey, tablePath);
      if (!result) {
         throw new Error(
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
   // `name` is always present; only the other fields may be undefined.
   return { ...removeUndefined(rest), name };
}

function removeUndefined<T extends object>(value: T): Partial<T> {
   return Object.fromEntries(
      Object.entries(value).filter(
         ([, fieldValue]) => fieldValue !== undefined,
      ),
   ) as Partial<T>;
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
      // Match the pool sizing used on the registry path (and in main's
      // pre-MalloyConfig switch). Server-owned, not exposed via API.
      poolOptions: buildPoolOptions({ poolMin: 1, poolMax: 20 }),
   });
}

// TLS mode for a proxied postgres connection, as `pg` connectionString query
// params. @malloydata/db-postgres exposes no ssl config, but it forwards a
// connectionString straight to pg — which parses sslmode. The tunnel terminates
// at 127.0.0.1, so pg's hostname check can't apply: `verify-ca` validates the
// cert chain against the trusted CA bundle (NODE_EXTRA_CA_CERTS, e.g. the baked
// Amazon RDS roots) WITHOUT the hostname; `no-verify` encrypts without checking;
// `disable` uses no TLS. Full verify-full through a tunnel needs a servername
// override (a raw ssl object) — awaits an upstream passthrough (malloydata/malloy#2960).
export function buildProxiedSslQuery(name: string, sslmode?: string): string {
   const caBundle = process.env.NODE_EXTRA_CA_CERTS;
   // Default: encrypt (no-verify) so a force-SSL target (the common RDS case)
   // isn't rejected for plaintext. Operators opt up to verify-ca when the
   // target's CA is in the trust bundle, or down to disable for non-TLS DBs.
   const mode = sslmode ?? "no-verify";
   switch (mode) {
      case "disable":
         // Explicit: with no sslmode in the connectionString, pg falls back to
         // the PGSSLMODE env (set on the non-proxied path), which would verify
         // and fail the 127.0.0.1 hostname check. Force plaintext explicitly.
         return "?sslmode=disable";
      case "no-verify":
         return "?sslmode=no-verify";
      case "verify-ca":
         // Env-set backstop; validateConnectionShape does the readable-file check
         // at config load (so a bad bundle fails fast instead of at connect time).
         if (!caBundle) {
            throw new Error(
               `Connection proxy on '${name}' uses sslmode 'verify-ca' but no trusted CA bundle is available (NODE_EXTRA_CA_CERTS is unset). Add the CA bundle to the image or use sslmode 'no-verify'.`,
            );
         }
         return `?uselibpqcompat=true&sslmode=verify-ca&sslrootcert=${encodeURIComponent(caBundle)}`;
      default:
         throw new Error(
            `Connection proxy on '${name}' has unsupported sslmode '${mode}' (expected disable | no-verify | verify-ca).`,
         );
   }
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
   // Build a connectionString to the LOCAL tunnel endpoint carrying the TLS mode.
   // This is NOT the tenant's connectionString (rejected in validateConnectionShape
   // because it can't be tunneled) — we construct it ourselves and it targets
   // 127.0.0.1:<localport>, so there's no ambiguity about where it connects.
   const enc = encodeURIComponent;
   const auth = pg.userName
      ? `${enc(pg.userName)}${pg.password ? `:${enc(pg.password)}` : ""}@`
      : "";
   const db = pg.databaseName ? `/${enc(pg.databaseName)}` : "";
   const ssl = buildProxiedSslQuery(name, pg.sslmode);
   const connectionString = `postgresql://${auth}${endpoint.host}:${endpoint.port}${db}${ssl}`;
   return new PooledPostgresConnection({
      name,
      connectionString,
      // Pool sizing mirrors buildSnowflakePrivateKeyConnection.
      poolMin: 1,
      poolMax: 20,
   });
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
 * The fingerprint is the connection's data identity as declared by the caller
 * (secret-free, stable across credential rotation). When set, it replaces the
 * locally derived `getDigest()` so it becomes the connection's contribution to
 * content-addressed source ids everywhere a digest is read:
 *
 *  - build planning (`compilePackageBuildPlan` -> `conn.getDigest()`),
 *  - the package-load worker's connection-metadata RPC, and
 *  - Malloy's own serve-time manifest resolution (the runtime calls
 *    `conn.getDigest()` internally when a build manifest is bound).
 *
 * Overriding the method on the resolved instance — inside the one
 * `lookupConnection` wrapper every resolution passes through — is what keeps
 * build-time and serve-time ids identical; applying it per call site would
 * invite divergence and a silent live-serving fallback. The value is used
 * verbatim (never re-hashed or combined with local details) and treated as an
 * opaque token: it is not a secret, but it is never logged or parsed.
 * Without a fingerprint the connection keeps its locally derived digest.
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
 * @param isUpdateConnectionRequest Forces a re-ATTACH on the DuckLake wrapper
 *   even when its catalog database is already attached on the cached
 *   connection. Set true when this build is replacing a prior generation due
 *   to a connection-config change, so the new generation rebinds against the
 *   updated DuckLake settings (catalog DSN, storage secrets) rather than
 *   trusting whatever attach state the prior generation left behind. On a
 *   fresh environment the existing-attach check fails anyway, so the flag is a
 *   no-op there. Only the DuckLake branch consults it today.
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
   // Cache the build Promise rather than the resolved Connection so two
   // concurrent lookupConnection() calls for the same name share one build
   // and we never leak a losing-instance pool/handle. Per-branch caches
   // preserve the concrete subtype so use sites don't need narrowing casts.
   const duckLakeCache = new Map<string, Promise<DuckLakeConnection>>();
   const snowflakeJwtCache = new Map<string, Promise<SnowflakeConnection>>();
   const azureDuckDBCache = new Map<string, Promise<AzureDuckDBConnection>>();
   const proxyConnectionCache = new Map<
      string,
      Promise<PooledPostgresConnection>
   >();
   const proxyEndpoints = new Map<string, ProxyEndpoint>();
   const attachPromises = new WeakMap<Connection, Promise<void>>();

   const malloyConfig = new MalloyConfig(assembled.pojo, {
      config: contextOverlay({ rootDirectory: environmentPath }),
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
                  // validateConnectionShape already enforces explicit host/port
                  // at config load (the connectionString form can't be tunneled);
                  // this is a defense-in-depth guard for any path that reaches
                  // lookup without that validation.
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
                        // Register only after the build succeeds: a throw between
                        // openProxy and here would otherwise leave an orphaned
                        // tunnel in proxyEndpoints that a retry can't reach (the
                        // outer .catch evicts the cache but not the endpoint).
                        proxyEndpoints.set(name!, endpoint);
                        return connection;
                     } catch (err) {
                        await endpoint.close().catch(() => {});
                        throw err;
                     }
                  })();
                  // Drop a rejected build from the cache so a later lookup can
                  // retry, rather than replaying a transient SSH failure (bastion
                  // restart, network blip) until the environment is rebuilt.
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

            const connection = await base.lookupConnection(name);
            if (metadata) {
               await attachOnce(connection, metadata);
            }
            return connection;
         };

         return {
            lookupConnection: async (name?: string): Promise<Connection> => {
               const metadata = getMetadataForLookup(assembled.metadata, name);
               const connection = await resolveConnection(name, metadata);
               // Pin every environment-level DuckDB session against implicit
               // auto-install (policy-driven) at this one exit — independent of
               // the attach paths, so the guarantee holds even if a resolution
               // branch is ever added that doesn't attach. The DuckLake tier
               // already pinned always-off inside its attach; the WeakSet makes
               // this a no-op there. (The per-package `:memory:` sandbox is a
               // separate MalloyConfig and is pinned in package.ts.)
               if (isDuckDBConnection(connection)) {
                  await applyExtensionSessionSettings(connection);
               }
               // Every resolution branch funnels through this one exit so a
               // configured fingerprint is applied no matter which wrapper
               // produced the connection (see applyConnectionFingerprint).
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
         ];
         const closeResults = await Promise.allSettled([
            malloyConfig.shutdown("close"),
            ...wrapperPromises.map(async (promise) => {
               const connection = await promise;
               await connection.close();
            }),
         ]);
         // Close proxy tunnels only AFTER the build promises above have settled:
         // openProxy may still be in flight during a teardown race, and the
         // endpoint is registered inside that promise — capturing the map
         // earlier would miss it and leak the tunnel. Closing after the DB
         // connections also keeps the tunnel alive while they drain.
         const endpointResults = await Promise.allSettled(
            [...proxyEndpoints.values()].map((ep) => ep.close()),
         );
         duckLakeCache.clear();
         snowflakeJwtCache.clear();
         azureDuckDBCache.clear();
         proxyConnectionCache.clear();
         proxyEndpoints.clear();

         const failures = [...closeResults, ...endpointResults].filter(
            (result): result is PromiseRejectedResult =>
               result.status === "rejected",
         );
         // Log every failure individually before throwing — Promise.allSettled
         // already let every branch run, and a single AggregateError otherwise
         // hides per-branch detail from callers that just log the error.
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

// TODO: Re-write these tests to validate based on credentials provided in the connection config
async function testDuckDBConnection(
   duckdbConnection: DuckDBConnection,
   connectionConfig: InternalConnection,
): Promise<void> {
   // Test base DuckDB connection with a simple query
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

   // Test each attached database if configured
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
         // Test the attached database by querying its tables/schemas
         // Different database types require different test queries
         switch (attachedDb.type) {
            case "postgres": {
               // Test postgres attachment by listing schemas
               await duckdbConnection.runSQL(
                  `SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '${attachedDb.name}' LIMIT 1`,
               );
               logger.info(
                  `Attached Postgres database test passed: ${attachedDb.name}`,
               );
               break;
            }
            case "bigquery": {
               // Test BigQuery attachment by listing datasets
               // BigQuery attached databases show as catalogs
               await duckdbConnection.runSQL(
                  `SELECT database_name FROM duckdb_databases() WHERE database_name = '${attachedDb.name}'`,
               );
               logger.info(
                  `Attached BigQuery database test passed: ${attachedDb.name}`,
               );
               break;
            }
            case "snowflake": {
               // Test Snowflake attachment by verifying database is attached
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
         logger.error(errorMessage);
         failedAttachments.push(errorMessage);
      }
   }

   if (failedAttachments.length > 0) {
      throw new Error(
         `DuckDB connection test failed for attached databases:\n${failedAttachments.join("\n")}`,
      );
   }
}

export async function testConnectionConfig(
   connectionConfig: ApiConnection,
): Promise<ApiConnectionStatus> {
   let environmentConfig: EnvironmentMalloyConfig | null = null;
   try {
      // Validate that connection name is provided
      if (!connectionConfig.name) {
         throw new Error("Connection name is required");
      }

      environmentConfig = buildEnvironmentMalloyConfig([connectionConfig]);
      const connection =
         await environmentConfig.malloyConfig.connections.lookupConnection(
            connectionConfig.name,
         );

      // Handle DuckDB connections specially since they have attached databases
      if (connectionConfig.type === "duckdb") {
         await testDuckDBConnection(
            connection as DuckDBConnection,
            connectionConfig as InternalConnection,
         );
      } else if (connectionConfig.type === "ducklake") {
         // DuckLake uses DuckDB internally — verify the database is attached
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
         // Test other connection types using their test() method
         await (
            connection as
               | PostgresConnection
               | BigQueryConnection
               | SnowflakeConnection
               | TrinoConnection
               | MySQLConnection
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
         logger.error(error);
      }

      return {
         status: "failed",
         errorMessage: (error as Error).message,
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

      if (connectionConfig.type === "ducklake" && connectionConfig.name) {
         await deleteDuckLakeConnectionFile(
            connectionConfig.name,
            process.cwd(),
         );
      }
   }
}
