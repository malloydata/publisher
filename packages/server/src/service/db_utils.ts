// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { ClientSecretCredential } from "@azure/identity";
import { ContainerClient } from "@azure/storage-blob";
import { BigQuery } from "@google-cloud/bigquery";
import { Impersonated } from "google-auth-library";
import { Connection, TableSourceDef } from "@malloydata/malloy";
import { components } from "../api";
import { BadRequestError, InvalidArgumentError } from "../errors";
import { logger } from "../logger";
import {
   CloudStorageCredentials,
   gcsConnectionToCredentials,
   getCloudTablesWithColumns,
   getFileType,
   isDataFile,
   listCloudDirectorySchemas,
   listDataFilesInDirectory,
   parseCloudUri,
   s3ConnectionToCredentials,
   standardizeRunSQLResult,
} from "./gcs_s3_utils";
import { getImpersonatedAuthClient } from "./gcp_impersonation";
import { assertGoogleCredentialsIsNotADirectory } from "./google_credentials";
import { ApiConnection } from "./model";
import { runIntrospectionSQL, sqlLiteral } from "./introspection_sql";

export { sqlLiteral };

type ApiSchema = components["schemas"]["Schema"];
type ApiTable = components["schemas"]["Table"];
type ApiAzureConnection = components["schemas"]["AzureConnection"];

/**
 * Build a SQL `AND column IN (...)` fragment for optional table-name filtering.
 * Returns an empty string when `values` is undefined or empty.
 */
export function sqlInFilter(
   columnName: string,
   values?: string[],
   connectionType?: string,
): string {
   if (!values || values.length === 0) return "";
   const escaped = values.map((v) => `'${sqlLiteral(v, connectionType)}'`);
   return `AND ${columnName} IN (${escaped.join(", ")})`;
}

/**
 * A catalog or database name safe to splice into an identifier position
 * (`FROM <catalog>.INFORMATION_SCHEMA.COLUMNS`). Quoting rules differ per
 * dialect, so this REJECTS rather than escapes.
 *
 * The value that matters is the caller's `schemaName` split on its first dot,
 * which `search_database_schema` takes straight from a model-controlled MCP
 * argument. Left raw, `(SELECT c1 AS TABLE_NAME, ... FROM customers) x -- .public`
 * comments out the rest of the query and returns ROW VALUES from a tool whose
 * invariant is that it never returns one. Catalog names from config or SHOW
 * CATALOGS are validated too, for consistency; in the SHOW CATALOGS loop a
 * rejected name is warned and skipped. This shuts out hyphenated Trino catalogs
 * (`hive-prod`); quoting identifiers per dialect would be the real fix.
 *
 * NO HYPHEN: `a--b.public` splices as `FROM a--b.information_schema...`, which
 * runs as `FROM a` with the WHERE commented away. Snowflake, Trino and Unity
 * Catalog need quoting for a hyphenated name anyway, and BigQuery (whose names
 * do carry hyphens) introspects through its client API, never reaching this.
 */
const SAFE_SQL_IDENTIFIER = /^[A-Za-z_][A-Za-z0-9_$]*$/;

export function assertSafeSqlIdentifier(value: string, what: string): string {
   if (!SAFE_SQL_IDENTIFIER.test(value)) {
      throw new InvalidArgumentError(
         `Invalid ${what} "${value}": expected a plain identifier (letters, digits, underscore or dollar, not starting with a digit).`,
      );
   }
   return value;
}

/**
 * Group INFORMATION_SCHEMA.COLUMNS rows into ApiTable objects.
 * Handles both upper-case (Snowflake) and lower-case (Postgres/DuckDB) column names.
 */
function groupColumnRowsIntoTables(
   rows: unknown[],
   buildResource: (tableName: string) => string,
): ApiTable[] {
   const tableMap = new Map<string, { name: string; type: string }[]>();
   for (const row of rows) {
      const r = row as Record<string, unknown>;
      const tableName = String(r.TABLE_NAME ?? r.table_name ?? "");
      const columnName = String(r.COLUMN_NAME ?? r.column_name ?? "");
      const dataType = String(r.DATA_TYPE ?? r.data_type ?? "").toLowerCase();
      if (!tableName) continue;
      if (!tableMap.has(tableName)) tableMap.set(tableName, []);
      tableMap.get(tableName)!.push({ name: columnName, type: dataType });
   }
   const tables: ApiTable[] = [];
   for (const [tableName, columns] of tableMap) {
      tables.push({ resource: buildResource(tableName), columns });
   }
   return tables;
}

/**
 * Whether a BadRequestError escapes an introspector untouched, and whether it
 * is logged first. Rewrapped as a plain Error it would classify as an internal
 * fault ("try again later"), and an agent would retry an invalid argument
 * forever. Introspectors that never raise one leave it "wrap".
 */
type BadRequestPassThrough = "wrap" | "before-log" | "after-log";

/**
 * Run one introspection call, logging and rewrapping a failure with the
 * connection named. `label` completes both "Error getting <label>" (the log)
 * and "Failed to get <label>: <reason>" (the thrown error).
 */
async function withIntrospectionFailure<T>(
   label: string,
   fn: () => Promise<T>,
   badRequest: BadRequestPassThrough = "wrap",
): Promise<T> {
   try {
      return await fn();
   } catch (error) {
      if (badRequest === "before-log" && error instanceof BadRequestError) {
         throw error;
      }
      logger.error(`Error getting ${label}`, { error });
      if (badRequest === "after-log" && error instanceof BadRequestError) {
         throw error;
      }
      throw new Error(`Failed to get ${label}: ${(error as Error).message}`, {
         cause: error,
      });
   }
}

async function createBigQueryClient(
   connection: ApiConnection,
): Promise<BigQuery> {
   if (!connection.bigqueryConnection) {
      throw new Error("BigQuery connection is required");
   }

   const config: {
      projectId: string;
      credentials?: object;
      keyFilename?: string;
      authClient?: Impersonated;
   } = {
      projectId: connection.bigqueryConnection.defaultProjectId || "",
   };

   // Discovery must run as the same identity as query execution: falling
   // through to ambient ADC would read schemas as the publisher's own broad
   // credential, the identity impersonation exists to avoid.
   if (connection.bigqueryConnection.impersonateServiceAccount) {
      config.authClient = await getImpersonatedAuthClient(
         connection.bigqueryConnection.impersonateServiceAccount,
      );
      if (!config.projectId) {
         throw new Error(
            "BigQuery project ID is required. Set defaultProjectId on the connection when using impersonateServiceAccount.",
         );
      }
   } else if (connection.bigqueryConnection.serviceAccountKeyJson) {
      let credentials: Record<string, unknown>;
      try {
         credentials = JSON.parse(
            connection.bigqueryConnection.serviceAccountKeyJson,
         );
      } catch (parseError) {
         throw new Error(
            `Failed to parse BigQuery service account key JSON: ${(parseError as Error).message}`,
         );
      }
      config.credentials = credentials;

      if (!config.projectId && credentials.project_id) {
         config.projectId = credentials.project_id as string;
      }

      if (!config.projectId) {
         throw new Error(
            "BigQuery project ID is required. Either set the defaultProjectId in the connection configuration or the project_id in the service account key JSON.",
         );
      }
   } else if (
      Object.keys(connection.bigqueryConnection).length === 0 &&
      process.env.GOOGLE_APPLICATION_CREDENTIALS
   ) {
      // Note: The BigQuery client will infer the project ID from the ADC file.
      assertGoogleCredentialsIsNotADirectory();
      config.keyFilename = process.env.GOOGLE_APPLICATION_CREDENTIALS || "";
   } else {
      throw new Error(
         "BigQuery connection is required, either set the bigqueryConnection in the connection configuration or set the GOOGLE_APPLICATION_CREDENTIALS environment variable.",
      );
   }

   return new BigQuery(config);
}

function getCloudCredentialsFromAttachedDatabases(
   attachedDatabases: components["schemas"]["AttachedDatabase"][],
   storageType: "gcs" | "s3",
): CloudStorageCredentials | null {
   for (const attachedDb of attachedDatabases) {
      if (
         attachedDb.type === "gcs" &&
         storageType === "gcs" &&
         attachedDb.gcsConnection
      ) {
         return gcsConnectionToCredentials(attachedDb.gcsConnection);
      }
      if (
         attachedDb.type === "s3" &&
         storageType === "s3" &&
         attachedDb.s3Connection
      ) {
         return s3ConnectionToCredentials(attachedDb.s3Connection);
      }
   }
   return null;
}

async function getSchemasForBigQuery(
   connection: ApiConnection,
): Promise<ApiSchema[]> {
   if (!connection.bigqueryConnection) {
      throw new Error("BigQuery connection is required");
   }
   return withIntrospectionFailure(
      `schemas for BigQuery connection ${connection.name}`,
      async () => {
         const bigquery = await createBigQueryClient(connection);
         const [datasets] = await bigquery.getDatasets();

         return await Promise.all(
            datasets.map(async (dataset) => {
               const [metadata] = await dataset.getMetadata();
               return {
                  name: dataset.id,
                  isHidden: false,
                  isDefault: false,
                  description: (metadata as { description?: string })
                     ?.description,
               };
            }),
         );
      },
   );
}

async function getSchemasForPostgres(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.postgresConnection) {
      throw new Error("Postgres connection is required");
   }
   return withIntrospectionFailure(
      `schemas for Postgres connection ${connection.name}`,
      async () => {
         // Wrap in row_to_json because the Malloy Postgres driver's runSQL
         // de-JSONs each row via row.row (matching Malloy-generated queries).
         const result = await runIntrospectionSQL(
            malloyConnection,
            "SELECT row_to_json(t) as row FROM (SELECT schema_name FROM information_schema.schemata ORDER BY schema_name) t",
         );
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = String(
               typedRow.schema_name ?? typedRow.SCHEMA_NAME ?? "",
            );
            return {
               name: schemaName,
               isHidden: [
                  "information_schema",
                  "pg_catalog",
                  "pg_toast",
               ].includes(schemaName),
               isDefault: schemaName === "public",
            };
         });
      },
   );
}

/**
 * The SQL Server schemas owning at least one table or view, as a modeler
 * addresses one (`connection.table('schema.table')`). Read from
 * `information_schema.tables`: `schemata` also lists fixed-role schemas
 * (`db_owner`, ...) that never hold a table, and `sys.tables` would miss
 * view-only schemas, which the column listing includes.
 */
async function getSchemasForSQLServer(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.sqlserverConnection) {
      throw new Error("SQL Server connection is required");
   }
   return withIntrospectionFailure(
      `schemas for SQL Server connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            "SELECT DISTINCT table_schema AS schema_name FROM information_schema.tables ORDER BY table_schema",
         );
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = String(
               typedRow.schema_name ?? typedRow.SCHEMA_NAME ?? "",
            );
            return {
               name: schemaName,
               isHidden: false,
               isDefault: schemaName === "dbo",
            };
         });
      },
   );
}

async function getSchemasForMySQL(
   connection: ApiConnection,
): Promise<ApiSchema[]> {
   if (!connection.mysqlConnection) {
      throw new Error("Mysql connection is required");
   }
   return [
      {
         name: connection.mysqlConnection.database || "mysql",
         isHidden: false,
         isDefault: true,
      },
   ];
}

/**
 * Snowflake-managed databases (the metadata share and the sample-data share),
 * hidden from an account-wide schema listing: neither is user data, and
 * together they contribute most schemas on an empty account.
 */
const SNOWFLAKE_SYSTEM_DATABASES = new Set([
   "SNOWFLAKE",
   "SNOWFLAKE_SAMPLE_DATA",
]);

/** One schema row, normalized across the two sources that can produce it. */
interface SnowflakeSchemaRow {
   catalogName: string;
   schemaName: string;
   owner: string;
}

/**
 * Schemas within one configured database, via that database's
 * INFORMATION_SCHEMA. Column names come back upper-case.
 */
async function listSnowflakeSchemasInDatabase(
   connection: ApiConnection,
   malloyConnection: Connection,
   database: string,
   // `null` as well as `undefined`: the API contract declares the optional
   // Snowflake fields nullable, so an omitted schema arrives as either.
   schema: string | null | undefined,
): Promise<SnowflakeSchemaRow[]> {
   const filters = [
      `CATALOG_NAME = '${sqlLiteral(database, connection.type)}'`,
   ];
   if (schema) {
      filters.push(`SCHEMA_NAME = '${sqlLiteral(schema, connection.type)}'`);
   }
   // Identifier position, so validated rather than escaped. The name is
   // operator config, so this is consistency with the catalog handling rather
   // than a trust boundary.
   assertSafeSqlIdentifier(database, "database name");
   const result = await runIntrospectionSQL(
      malloyConnection,
      `SELECT CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER FROM ${database}.INFORMATION_SCHEMA.SCHEMATA WHERE ${filters.join(
         " AND ",
      )} ORDER BY SCHEMA_NAME`,
   );
   return standardizeRunSQLResult(result).map((row: unknown) => {
      const r = row as Record<string, unknown>;
      return {
         catalogName: String(r.CATALOG_NAME ?? r.catalog_name ?? ""),
         schemaName: String(r.SCHEMA_NAME ?? r.schema_name ?? ""),
         owner: String(r.SCHEMA_OWNER ?? r.schema_owner ?? ""),
      };
   });
}

/**
 * Snowflake's maximum `LIMIT` for a SHOW command. Stated explicitly because
 * without it SHOW SCHEMAS ERRORS above ten thousand rows and returns nothing;
 * with it, a large account gets a partial list, and the caller warns on hitting
 * the limit because a partial list looks complete.
 *
 * {@link runIntrospectionSQL}'s cap cannot substitute: the Snowflake driver
 * applies `rowLimit` by slicing rows it already fetched, so it never reaches the
 * server. 10,000 is Snowflake's ceiling (`LIMIT 10001` is rejected); an account
 * above it needs SHOW per database, the `FROM '<name>'` cursor, or
 * SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA.
 */
const SNOWFLAKE_SHOW_ROW_LIMIT = 10_000;

/**
 * Every role-visible schema in the account, for a connection with no configured
 * database. SHOW needs no current database.
 *
 * Its output columns are LOWER-case, unlike INFORMATION_SCHEMA's; read with
 * upper-case keys every owner is blank, and the caller's `isHidden` rule would
 * hide every row. SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA is avoided on purpose: it
 * needs elevated grants and lags by up to ~2 hours.
 */
async function listSnowflakeSchemasInAccount(
   malloyConnection: Connection,
): Promise<SnowflakeSchemaRow[]> {
   const result = await runIntrospectionSQL(
      malloyConnection,
      `SHOW SCHEMAS IN ACCOUNT LIMIT ${SNOWFLAKE_SHOW_ROW_LIMIT}`,
   );
   const returnedRows = standardizeRunSQLResult(result);
   if (returnedRows.length >= SNOWFLAKE_SHOW_ROW_LIMIT) {
      logger.warn(
         "Snowflake account-wide schema listing hit the SHOW row limit; the schema list is incomplete and some tables will not be discoverable",
         {
            rowLimit: SNOWFLAKE_SHOW_ROW_LIMIT,
            returnedRows: returnedRows.length,
         },
      );
   }
   const parsed = returnedRows.map((row: unknown) => {
      const r = row as Record<string, unknown>;
      return {
         catalogName: String(r.database_name ?? r.DATABASE_NAME ?? ""),
         schemaName: String(r.name ?? r.NAME ?? ""),
         owner: String(r.owner ?? r.OWNER ?? ""),
      };
   });
   // A row missing either half cannot form a DATABASE.SCHEMA name that
   // listTablesForSnowflake can parse.
   const usable = parsed.filter((r) => r.catalogName && r.schemaName);
   if (usable.length < parsed.length) {
      // Warn, so unrecognized SHOW columns (a rename, or a role that cannot see
      // them) are not mistaken for an account with no schemas.
      logger.warn(
         "Dropped Snowflake schema rows missing a database or schema name; the schema list is incomplete",
         { dropped: parsed.length - usable.length, returned: parsed.length },
      );
   }
   return usable.sort(
      (a, b) =>
         a.catalogName.localeCompare(b.catalogName) ||
         a.schemaName.localeCompare(b.schemaName),
   );
}

async function getSchemasForSnowflake(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const snowflake = connection.snowflakeConnection;
   if (!snowflake) {
      throw new Error("Snowflake connection is required");
   }
   // "before-log": a rejected database name is the config's to fix and must
   // stay a BadRequestError (see BadRequestPassThrough).
   return withIntrospectionFailure(
      `schemas for Snowflake connection ${connection.name}`,
      async () => {
         const database = snowflake.database;
         const schema = snowflake.schema;

         // INFORMATION_SCHEMA.SCHEMATA is per-database: without a current
         // database the unqualified form fails outright, and no qualified form
         // spans databases. So with no configured database, use SHOW SCHEMAS IN
         // ACCOUNT. Both branches emit `DATABASE.SCHEMA`, which
         // listTablesForSnowflake parses back apart.
         const rows = database
            ? await listSnowflakeSchemasInDatabase(
                 connection,
                 malloyConnection,
                 database,
                 schema,
              )
            : await listSnowflakeSchemasInAccount(malloyConnection);

         return rows.map(({ catalogName, schemaName, owner }) => ({
            name: `${catalogName}.${schemaName}`,
            isHidden:
               owner === "SNOWFLAKE" ||
               // A blank owner means system-owned only within a configured
               // database. Account-wide, SHOW reports a blank owner for every
               // schema of an IMPORTED DATABASE (a Marketplace or partner share
               // the operator subscribed to), which must stay visible. The other
               // account-wide blank-owner schemas are caught by the
               // INFORMATION_SCHEMA and system-database rules below.
               (Boolean(database) && owner === "") ||
               schemaName === "INFORMATION_SCHEMA" ||
               // Snowflake's own databases are noise account-wide; a connection
               // pointed at one by name still lists it.
               (!database && SNOWFLAKE_SYSTEM_DATABASES.has(catalogName)),
            // Only within a configured database: account-wide, a same-named
            // schema in every database would be a default.
            isDefault:
               Boolean(database) && Boolean(schema) && schemaName === schema,
         }));
      },
      "before-log",
   );
}

async function getSchemasForTrino(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const trino = connection.trinoConnection;
   if (!trino) {
      throw new Error("Trino connection is required");
   }
   return withIntrospectionFailure(
      `schemas for Trino connection ${connection.name}`,
      async () => {
         const configuredSchema = trino.schema;
         let allRows: { catalog: string; schema: string }[] = [];

         if (trino.catalog) {
            const catalog = trino.catalog;
            const result = await runIntrospectionSQL(
               malloyConnection,
               `SELECT schema_name FROM ${assertSafeSqlIdentifier(
                  catalog,
                  "catalog name",
               )}.information_schema.schemata ORDER BY schema_name`,
            );
            const rows = standardizeRunSQLResult(result);
            allRows = rows.map((row: unknown) => {
               const r = row as Record<string, unknown>;
               return {
                  catalog,
                  schema: String(r.schema_name ?? r.Schema ?? ""),
               };
            });
         } else {
            const catalogsResult = await runIntrospectionSQL(
               malloyConnection,
               `SHOW CATALOGS`,
            );
            const catalogNames = standardizeRunSQLResult(catalogsResult).map(
               (row: unknown) => {
                  const r = row as Record<string, unknown>;
                  return String(r.Catalog ?? r.catalog ?? "");
               },
            );

            for (const catalog of catalogNames) {
               try {
                  const result = await runIntrospectionSQL(
                     malloyConnection,
                     `SELECT schema_name FROM ${assertSafeSqlIdentifier(
                        catalog,
                        "catalog name",
                     )}.information_schema.schemata ORDER BY schema_name`,
                  );
                  const rows = standardizeRunSQLResult(result);
                  for (const row of rows) {
                     const r = row as Record<string, unknown>;
                     allRows.push({
                        catalog,
                        schema: String(r.schema_name ?? r.Schema ?? ""),
                     });
                  }
               } catch (catalogError) {
                  logger.warn(
                     `Failed to list schemas for Trino catalog ${catalog}`,
                     { error: catalogError },
                  );
               }
            }
         }

         return allRows.map(({ catalog, schema }) => {
            const name = trino?.catalog ? schema : `${catalog}.${schema}`;
            return {
               name,
               isHidden: ["information_schema", "performance_schema"].includes(
                  schema,
               ),
               isDefault: configuredSchema
                  ? schema === configuredSchema
                  : false,
            };
         });
      },
      "before-log",
   );
}

async function getSchemasForDatabricks(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const databricks = connection.databricksConnection;
   if (!databricks) {
      throw new Error("Databricks connection is required");
   }
   return withIntrospectionFailure(
      `schemas for Databricks connection ${connection.name}`,
      async () => {
         const configuredSchema = databricks.defaultSchema;
         let allRows: { catalog: string; schema: string }[] = [];

         if (databricks.defaultCatalog) {
            const catalog = databricks.defaultCatalog;
            const result = await runIntrospectionSQL(
               malloyConnection,
               `SELECT schema_name FROM ${assertSafeSqlIdentifier(
                  catalog,
                  "catalog name",
               )}.information_schema.schemata ORDER BY schema_name`,
            );
            const rows = standardizeRunSQLResult(result);
            allRows = rows.map((row: unknown) => {
               const r = row as Record<string, unknown>;
               return {
                  catalog,
                  schema: String(r.schema_name ?? r.Schema ?? ""),
               };
            });
         } else {
            const catalogsResult = await runIntrospectionSQL(
               malloyConnection,
               `SHOW CATALOGS`,
            );
            const catalogNames = standardizeRunSQLResult(catalogsResult).map(
               (row: unknown) => {
                  const r = row as Record<string, unknown>;
                  return String(r.catalog ?? r.Catalog ?? r.catalog_name ?? "");
               },
            );

            for (const catalog of catalogNames) {
               try {
                  const result = await runIntrospectionSQL(
                     malloyConnection,
                     `SELECT schema_name FROM ${assertSafeSqlIdentifier(
                        catalog,
                        "catalog name",
                     )}.information_schema.schemata ORDER BY schema_name`,
                  );
                  const rows = standardizeRunSQLResult(result);
                  for (const row of rows) {
                     const r = row as Record<string, unknown>;
                     allRows.push({
                        catalog,
                        schema: String(r.schema_name ?? r.Schema ?? ""),
                     });
                  }
               } catch (catalogError) {
                  logger.warn(
                     `Failed to list schemas for Databricks catalog ${catalog}`,
                     { error: catalogError },
                  );
               }
            }
         }
         logger.info("allRows for Schemas for Databricks", { allRows });
         return allRows.map(({ catalog, schema }) => {
            const name = databricks?.defaultCatalog
               ? schema
               : `${catalog}.${schema}`;
            return {
               name,
               isHidden: ["information_schema"].includes(schema),
               isDefault: configuredSchema
                  ? schema === configuredSchema
                  : false,
            };
         });
      },
      "before-log",
   );
}

async function getSchemasForDuckDB(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const duckdb = connection.duckdbConnection;
   if (!duckdb) {
      throw new Error("DuckDB connection is required");
   }
   return withIntrospectionFailure(
      `schemas for DuckDB connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            "SELECT DISTINCT schema_name,catalog_name FROM information_schema.schemata ORDER BY catalog_name,schema_name",
         );

         const rows = standardizeRunSQLResult(result);

         const schemas: ApiSchema[] = rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = String(typedRow.schema_name ?? "");
            const catalogName = String(typedRow.catalog_name ?? "");

            return {
               name: `${catalogName}.${schemaName}`,
               isHidden:
                  [
                     "information_schema",
                     "performance_schema",
                     "pg_catalog",
                     "pg_toast",
                     "",
                  ].includes(schemaName) ||
                  ["md_information_schema", "system"].includes(catalogName),
               isDefault: catalogName === "main",
            };
         });

         const attachedDatabases = duckdb.attachedDatabases || [];

         const cloudDatabases = attachedDatabases.filter(
            (attachedDb) =>
               (attachedDb.type === "gcs" || attachedDb.type === "s3") &&
               (attachedDb.gcsConnection || attachedDb.s3Connection),
         );

         const cloudDbPromises = cloudDatabases.map(async (attachedDb) => {
            const dbType = attachedDb.type as "gcs" | "s3";
            const credentials =
               dbType === "gcs"
                  ? gcsConnectionToCredentials(attachedDb.gcsConnection!)
                  : s3ConnectionToCredentials(attachedDb.s3Connection!);

            try {
               return await listCloudDirectorySchemas(credentials);
            } catch (cloudError) {
               logger.warn(
                  `Failed to list ${dbType.toUpperCase()} directory schemas for ${attachedDb.name}`,
                  { error: cloudError },
               );
               return [];
            }
         });

         const cloudSchemaArrays = await Promise.all(cloudDbPromises);
         for (const cloudSchemas of cloudSchemaArrays) {
            schemas.push(...cloudSchemas);
         }

         const azureDatabases = attachedDatabases.filter(
            (attachedDb) =>
               attachedDb.type === "azure" && attachedDb.azureConnection,
         );
         for (const attachedDb of azureDatabases) {
            if (attachedDb.name) {
               schemas.push({
                  name: attachedDb.name,
                  isHidden: false,
                  isDefault: false,
               });
            }
         }

         return schemas;
      },
   );
}

async function getSchemasForMotherDuck(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   const motherduck = connection.motherduckConnection;
   if (!motherduck) {
      throw new Error("MotherDuck connection is required");
   }
   return withIntrospectionFailure(
      `schemas for MotherDuck connection ${connection.name}`,
      async () => {
         const database = motherduck.database;
         const whereClause = database
            ? `WHERE catalog_name = '${sqlLiteral(database, connection.type)}'`
            : "";
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT DISTINCT schema_name FROM information_schema.schemata ${whereClause} ORDER BY schema_name`,
         );
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = String(
               typedRow.schema_name ?? typedRow.SCHEMA_NAME ?? "",
            );
            return {
               name: schemaName,
               isHidden: [
                  "information_schema",
                  "performance_schema",
                  "",
               ].includes(schemaName),
               isDefault: schemaName === "main",
            };
         });
      },
   );
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
         const rows = standardizeRunSQLResult(result);

         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = typedRow.schema_name as string;
            const shouldShow = schemaName === "main" || schemaName === "public";
            return {
               name: schemaName,
               isHidden: !shouldShow,
               isDefault: false,
            };
         });
      },
   );
}

/** Abort introspection requests that hang instead of holding the slot open. */
const PUBLISHER_INTROSPECTION_TIMEOUT_MS = 60_000;

/**
 * Strip userinfo (`user:pass@`) from a URL before it reaches a log or error
 * message. `connectionUri` comes from config and may embed credentials; the
 * bearer token travels only in the header.
 */
function redactUrlCredentials(url: string): string {
   try {
      const parsed = new URL(url);
      parsed.username = "";
      parsed.password = "";
      return parsed.toString();
   } catch {
      // Unparseable: nothing to redact.
      return url;
   }
}

/**
 * Publisher proxy connections introspect against the remote dataplane, which
 * serves the same connection API, so discovery is a GET passthrough returning
 * the same Schema/Table shapes verbatim.
 *
 * `connectionUri` must point at the connection resource itself
 * (`.../connections/<name>`). This path appends `pathSuffix` to it verbatim,
 * while db-publisher's `PublisherConnection` rebuilds it through its generated
 * client, so the remote must serve both forms for the same connection.
 */
async function fetchFromPublisherDataplane<T>(
   connection: ApiConnection,
   pathSuffix: string,
): Promise<T> {
   const publisher = connection.publisherConnection;
   // Type-narrowing guard only; the user-facing validation is
   // validateConnectionShape, which every connection passes first.
   if (!publisher?.connectionUri) {
      throw new Error(
         `Publisher connection "${connection.name}" is missing connectionUri`,
      );
   }
   const url = `${publisher.connectionUri.replace(/\/+$/, "")}${pathSuffix}`;
   const headers: Record<string, string> = {};
   if (publisher.accessToken) {
      headers["Authorization"] = `Bearer ${publisher.accessToken}`;
   }

   let response: Response;
   try {
      response = await fetch(url, {
         headers,
         signal: AbortSignal.timeout(PUBLISHER_INTROSPECTION_TIMEOUT_MS),
      });
   } catch (error) {
      // Network-level failure (no HTTP status). Never echo the token-bearing
      // header.
      const reason =
         (error as Error)?.name === "TimeoutError"
            ? `timed out after ${PUBLISHER_INTROSPECTION_TIMEOUT_MS}ms`
            : (error as Error).message;
      throw new Error(
         `Publisher dataplane request to ${redactUrlCredentials(url)} failed ` +
            `for connection "${connection.name}": ${reason}`,
         // Keeps fetch's socket code reachable, so a dataplane that refused
         // the connection answers 502 CONNECTION_FAILED rather than 500.
         { cause: error },
      );
   }

   if (!response.ok) {
      const body = await response.text().catch(() => "");
      throw new Error(
         `Publisher dataplane request to ${redactUrlCredentials(url)} failed ` +
            `(${response.status}): ${body.slice(0, 200)}`,
      );
   }
   return (await response.json()) as T;
}

async function getSchemasForPublisher(
   connection: ApiConnection,
): Promise<ApiSchema[]> {
   return fetchFromPublisherDataplane<ApiSchema[]>(connection, "/schemas");
}

async function listTablesForPublisher(
   connection: ApiConnection,
   schemaName: string,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const tables = await fetchFromPublisherDataplane<ApiTable[]>(
      connection,
      `/schemas/${encodeURIComponent(schemaName)}/tables`,
   );
   if (!tableNames) {
      return tables;
   }
   // `tableNames` holds bare table names, but the remote qualifies `resource`
   // per its dialect, so match the last segment, the schema-stripped form, and
   // the full resource.
   const allowed = new Set(tableNames);
   const prefix = `${schemaName}.`;
   return tables.filter((table) => {
      const resource = table.resource ?? "";
      const schemaStripped = resource.startsWith(prefix)
         ? resource.slice(prefix.length)
         : resource;
      const bareName = resource.slice(resource.lastIndexOf(".") + 1);
      return (
         allowed.has(bareName) ||
         allowed.has(schemaStripped) ||
         allowed.has(resource)
      );
   });
}

type ConnectionType = NonNullable<ApiConnection["type"]>;

/** How one connection type lists its schemas and the tables in one schema. */
interface Introspector {
   schemas(
      connection: ApiConnection,
      malloyConnection: Connection,
   ): Promise<ApiSchema[]>;
   tables(
      connection: ApiConnection,
      schemaName: string,
      malloyConnection: Connection,
      tableNames?: string[],
   ): Promise<ApiTable[]>;
}

const INTROSPECTOR_BY_TYPE: Record<ConnectionType, Introspector> = {
   bigquery: {
      schemas: getSchemasForBigQuery,
      tables: listTablesForBigQuery,
   },
   postgres: {
      schemas: getSchemasForPostgres,
      tables: listTablesForPostgres,
   },
   mysql: { schemas: getSchemasForMySQL, tables: listTablesForMySQL },
   sqlserver: {
      schemas: getSchemasForSQLServer,
      tables: listTablesForSQLServer,
   },
   snowflake: {
      schemas: getSchemasForSnowflake,
      tables: listTablesForSnowflake,
   },
   trino: { schemas: getSchemasForTrino, tables: listTablesForTrino },
   databricks: {
      schemas: getSchemasForDatabricks,
      tables: listTablesForDatabricks,
   },
   duckdb: { schemas: getSchemasForDuckDB, tables: listTablesForDuckDB },
   motherduck: {
      schemas: getSchemasForMotherDuck,
      tables: listTablesForMotherDuck,
   },
   ducklake: {
      schemas: getSchemasForDuckLake,
      tables: listTablesForDuckLake,
   },
   publisher: {
      schemas: getSchemasForPublisher,
      tables: (connection, schemaName, _malloyConnection, tableNames) =>
         listTablesForPublisher(connection, schemaName, tableNames),
   },
};

// A Map, so a caller-supplied type such as "constructor" finds nothing.
const INTROSPECTORS = new Map(Object.entries(INTROSPECTOR_BY_TYPE));

function introspectorFor(connection: ApiConnection): Introspector {
   const introspector = INTROSPECTORS.get(connection.type ?? "");
   if (!introspector) {
      throw new Error(`Unsupported connection type: ${connection.type}`);
   }
   return introspector;
}

export async function getSchemasForConnection(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   return introspectorFor(connection).schemas(connection, malloyConnection);
}

/**
 * Lists blobs in an Azure container matching a glob-like prefix/extension filter.
 * Parses an HTTPS SAS URL like:
 *   https://account.blob.core.windows.net/container/path/*.parquet?sasToken
 * Returns individual file URLs with the SAS token appended.
 */
async function listAzureBlobs(
   fileUrl: string,
   azureConnection?: ApiAzureConnection,
): Promise<{ url: string; blobName: string }[]> {
   // Split URL and SAS token carefully to avoid encoding issues with signatures
   const queryStart = fileUrl.indexOf("?");
   const baseUrl = queryStart >= 0 ? fileUrl.substring(0, queryStart) : fileUrl;
   const sasToken = queryStart >= 0 ? fileUrl.substring(queryStart) : "";

   let accountUrl: string;
   let container: string;
   let blobPath: string;

   if (baseUrl.startsWith("abfss://")) {
      // abfss://container/path or abfss://account.dfs.core.windows.net/container/path
      const withoutScheme = baseUrl.substring("abfss://".length);
      const parts = withoutScheme.split("/").filter(Boolean);
      if (parts[0].includes(".")) {
         // Fully qualified: abfss://account.dfs.core.windows.net/container/path
         const accountName = parts[0].split(".")[0];
         accountUrl = `https://${accountName}.blob.core.windows.net`;
         container = parts[1];
         blobPath = parts.slice(2).join("/");
      } else {
         // Short form: abfss://container/path — need accountName from config
         if (!azureConnection?.accountName) {
            throw new Error(
               "accountName is required to list blobs with abfss:// URLs",
            );
         }
         accountUrl = `https://${azureConnection.accountName}.blob.core.windows.net`;
         container = parts[0];
         blobPath = parts.slice(1).join("/");
      }
   } else {
      // https://account.blob.core.windows.net/container/path
      const url = new URL(baseUrl);
      const pathParts = url.pathname.split("/").filter(Boolean);
      container = pathParts[0];
      blobPath = pathParts.slice(1).join("/");
      accountUrl = `${url.protocol}//${url.host}`;
   }

   // Three supported glob patterns:
   //   path/file.ext       → single file (handled upstream by isAzureSingleFileUrl)
   //   path/*.ext          → files directly in path/ with that extension (no subdirs)
   //   path/**             → all valid data files in path/ and nested dirs (recursive)
   let prefix: string;
   let extensionFilter = ""; // for *.ext pattern
   let recursive = true; // for ** pattern

   if (blobPath.endsWith("**")) {
      prefix = blobPath.slice(0, -2);
      recursive = true;
   } else if (blobPath.includes("*")) {
      const starIndex = blobPath.indexOf("*");
      prefix = blobPath.substring(0, starIndex);
      extensionFilter = blobPath.substring(starIndex + 1); // e.g. ".parquet"
      recursive = false;
   } else {
      // No glob — use blobPath as prefix (container-level listing)
      prefix = blobPath;
      recursive = true;
   }

   let containerClient: ContainerClient;
   if (
      azureConnection?.authType === "service_principal" &&
      azureConnection.tenantId &&
      azureConnection.clientId &&
      azureConnection.clientSecret
   ) {
      const credential = new ClientSecretCredential(
         azureConnection.tenantId,
         azureConnection.clientId,
         azureConnection.clientSecret,
      );
      containerClient = new ContainerClient(
         `${accountUrl}/${container}`,
         credential,
      );
   } else {
      // SAS token auth — append token to container URL
      const containerUrl = `${accountUrl}/${container}${sasToken}`;
      containerClient = new ContainerClient(containerUrl);
   }

   const matchingFiles: { url: string; blobName: string }[] = [];
   for await (const blob of containerClient.listBlobsFlat({
      prefix: prefix || undefined,
   })) {
      if (extensionFilter && !blob.name.endsWith(extensionFilter)) continue;
      // For *.ext (non-recursive): only allow files directly in prefix dir
      if (!recursive) {
         const nameAfterPrefix = blob.name.substring(prefix.length);
         if (nameAfterPrefix.includes("/")) continue;
      }
      if (!isDataFile(blob.name)) continue;
      // For SPN: use abfss:// URLs that DuckDB's azure extension can read
      // For SAS: use https:// URLs with token appended
      let url: string;
      if (azureConnection?.authType === "service_principal") {
         const account =
            azureConnection.accountName ||
            accountUrl.split("//")[1]?.split(".")[0];
         url = `abfss://${account}.dfs.core.windows.net/${container}/${blob.name}`;
      } else {
         url = `${accountUrl}/${container}/${blob.name}${sasToken}`;
      }
      matchingFiles.push({ url, blobName: blob.name });
   }

   logger.info(
      `Listed ${matchingFiles.length} matching blobs in Azure container ${container} with prefix "${prefix}"`,
   );
   return matchingFiles;
}

/** These builders always run against DuckDB, whatever connection reached them. */
const DUCKDB_DIALECT = "duckdb";

/**
 * DESCRIBE a single remote data file.
 *
 * `fileUri` is caller-controlled: `listTablesForDuckDB` routes any `schemaName`
 * beginning with `https://`, `abfss://` or `az://` here straight from an MCP
 * tool argument, with no warehouse credentials needed, so it is escaped like any
 * literal (DuckDB's ANSI quote-doubling).
 */
async function describeRemoteFile(
   malloyConnection: Connection,
   fileUri: string,
): Promise<ApiTable> {
   const pathWithoutQuery = fileUri.split("?")[0];
   const fileType = getFileType(pathWithoutQuery);

   let describeQuery: string;
   switch (fileType) {
      case "csv":
         describeQuery = `DESCRIBE SELECT * FROM read_csv('${sqlLiteral(fileUri, DUCKDB_DIALECT)}', auto_detect=true) LIMIT 1`;
         break;
      case "parquet":
         describeQuery = `DESCRIBE SELECT * FROM read_parquet('${sqlLiteral(fileUri, DUCKDB_DIALECT)}') LIMIT 1`;
         break;
      case "json":
         describeQuery = `DESCRIBE SELECT * FROM read_json('${sqlLiteral(fileUri, DUCKDB_DIALECT)}', auto_detect=true) LIMIT 1`;
         break;
      case "jsonl":
         describeQuery = `DESCRIBE SELECT * FROM read_json('${sqlLiteral(fileUri, DUCKDB_DIALECT)}', format='newline_delimited', auto_detect=true) LIMIT 1`;
         break;
      default:
         logger.warn(`Unsupported file type for file: ${fileUri}`);
         return { resource: fileUri, columns: [] };
   }

   const result = await runIntrospectionSQL(malloyConnection, describeQuery);
   const rows = standardizeRunSQLResult(result);
   const columns = rows.map((row: unknown) => {
      const typedRow = row as Record<string, unknown>;
      return {
         name: (typedRow.column_name || typedRow.name) as string,
         type: (typedRow.column_type || typedRow.type) as string,
      };
   });

   const fileName = pathWithoutQuery.split("/").pop() || fileUri;
   return { resource: fileName, columns };
}

function isAzureSingleFileUrl(fileUri: string): boolean {
   const pathWithoutQuery = fileUri.split("?")[0];
   // Has a glob — not a single file
   if (pathWithoutQuery.includes("*")) return false;
   // Ends with / — directory listing
   if (pathWithoutQuery.endsWith("/")) return false;
   // Check if the last path segment has a data file extension
   const lastSegment = pathWithoutQuery.split("/").pop() || "";
   return isDataFile(lastSegment);
}

async function describeAzureFile(
   malloyConnection: Connection,
   fileUri: string,
   azureConnection?: ApiAzureConnection,
): Promise<ApiTable[]> {
   try {
      if (isAzureSingleFileUrl(fileUri)) {
         // Single file — describe directly via DuckDB
         return [await describeRemoteFile(malloyConnection, fileUri)];
      }

      // Glob pattern or container/directory URL — list blobs via Azure SDK
      const blobs = await listAzureBlobs(fileUri, azureConnection);
      if (blobs.length === 0) {
         return [{ resource: fileUri, columns: [] }];
      }

      const results = await Promise.all(
         blobs.map(async ({ url, blobName }) => {
            try {
               const table = await describeRemoteFile(malloyConnection, url);
               return { ...table, resource: blobName };
            } catch (error) {
               logger.warn(`Failed to describe Azure blob: ${url}`, { error });
               return { resource: blobName, columns: [] } as ApiTable;
            }
         }),
      );
      return results;
   } catch (error) {
      logger.error(`Failed to describe Azure file: ${fileUri}`, { error });
      throw new Error(
         `Failed to describe Azure file: ${error instanceof Error ? error.message : String(error)}`,
      );
   }
}

export async function listTablesForSchema(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   return introspectorFor(connection).tables(
      connection,
      schemaName,
      malloyConnection,
      tableNames,
   );
}

/**
 * BigQuery: list tables via API client, then fetch each table's schema
 * individually since BigQuery's INFORMATION_SCHEMA is region-scoped.
 */
async function listTablesForBigQuery(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   return withIntrospectionFailure(
      `tables for BigQuery schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const bigquery = await createBigQueryClient(connection);
         // bigquery.dataset() takes a BARE dataset id, so a project-qualified
         // schema ("project.dataset") must be split, or it resolves to a missing
         // dataset in the default project and the orphan sweep cannot see its
         // tables. Split on the LAST dot so "domain.com:project" ids survive.
         const lastDot = schemaName.lastIndexOf(".");
         const dataset =
            lastDot === -1
               ? bigquery.dataset(schemaName)
               : bigquery.dataset(schemaName.slice(lastDot + 1), {
                    projectId: schemaName.slice(0, lastDot),
                 });
         const [tables] = await dataset.getTables();

         let names = tables
            .map((table) => table.id)
            .filter((id): id is string => id !== undefined);
         if (tableNames) {
            const allowed = new Set(tableNames);
            names = names.filter((id) => allowed.has(id));
         }

         const results = await Promise.all(
            names.map(async (tableName) => {
               const tablePath = `${schemaName}.${tableName}`;
               try {
                  const source = await (
                     malloyConnection as Connection & {
                        fetchTableSchema: (
                           tableKey: string,
                           tablePath: string,
                        ) => Promise<TableSourceDef | undefined>;
                     }
                  ).fetchTableSchema(tableName, tablePath);
                  const columns =
                     source?.fields?.map((field) => ({
                        name: field.name,
                        type: field.type,
                     })) || [];
                  return { resource: tablePath, columns };
               } catch (error) {
                  logger.warn(`Failed to get schema for table ${tableName}`, {
                     error: extractErrorDataFromError(error),
                     schemaName,
                     tableName,
                  });
                  return { resource: tablePath, columns: [] };
               }
            }),
         );
         return results;
      },
   );
}

async function listTablesForMySQL(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.mysqlConnection) {
      throw new Error("Mysql connection is required");
   }
   return withIntrospectionFailure(
      `tables for MySQL schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_schema = '${sqlLiteral(schemaName, connection.type)}' ${sqlInFilter("TABLE_NAME", tableNames, connection.type)} ORDER BY TABLE_NAME, ORDINAL_POSITION`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}

/**
 * A SQL Server Unicode string literal, `N'…'`. Here rather than in
 * {@link sqlLiteral}, which serves dialects with no `N'…'` syntax.
 */
function sqlserverNLiteral(value: string): string {
   return `N'${sqlLiteral(value, "sqlserver")}'`;
}

/** {@link sqlInFilter} for SQL Server, with each value a Unicode literal. */
function sqlserverInFilter(columnName: string, values?: string[]): string {
   if (!values || values.length === 0) return "";
   return `AND ${columnName} IN (${values.map(sqlserverNLiteral).join(", ")})`;
}

async function listTablesForSQLServer(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.sqlserverConnection) {
      throw new Error("SQL Server connection is required");
   }
   return withIntrospectionFailure(
      `tables for SQL Server schema ${schemaName} in connection ${connection.name}`,
      async () => {
         // Rows come back as plain objects, so no row_to_json as for Postgres.
         // Every literal is N'…': a plain '…' is converted to the database code
         // page first, so under a Latin1 collation `Продажи` becomes question
         // marks and matches nothing against the nvarchar catalog columns.
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = ${sqlserverNLiteral(schemaName)} ${sqlserverInFilter("table_name", tableNames)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}

async function listTablesForPostgres(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.postgresConnection) {
      throw new Error("Postgres connection is required");
   }
   return withIntrospectionFailure(
      `tables for Postgres schema ${schemaName} in connection ${connection.name}`,
      async () => {
         // Wrap in row_to_json because the Malloy Postgres driver's runSQL
         // de-JSONs each row via row.row (matching Malloy-generated queries).
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT row_to_json(t) as row FROM (SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${sqlLiteral(schemaName, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position) t`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
      },
   );
}

async function listTablesForSnowflake(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const snowflake = connection.snowflakeConnection;
   if (!snowflake) {
      throw new Error("Snowflake connection is required");
   }
   return withIntrospectionFailure(
      `tables for Snowflake schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const parts = schemaName.split(".");
         let databaseName: string;
         let schemaOnly: string;

         if (parts.length >= 2) {
            databaseName = parts[0];
            schemaOnly = parts[1];
         } else {
            databaseName = snowflake.database ?? "";
            schemaOnly = parts[0];
         }

         if (!databaseName) {
            throw new Error(
               `Cannot resolve database for schema "${schemaName}": provide DATABASE.SCHEMA or configure a database on the connection`,
            );
         }

         // Identifier position below, so this is validated, not escaped.
         assertSafeSqlIdentifier(databaseName, "database name");
         const qualifiedSchema = `${databaseName}.${schemaOnly}`;
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ${databaseName}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${sqlLiteral(schemaOnly, connection.type)}' ${sqlInFilter("TABLE_NAME", tableNames, connection.type)} ORDER BY TABLE_NAME, ORDINAL_POSITION`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(
            rows,
            (t) => `${qualifiedSchema}.${t}`,
         );
      },
      "after-log",
   );
}

async function listTablesForTrino(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const trino = connection.trinoConnection;
   if (!trino) {
      throw new Error("Trino connection is required");
   }
   return withIntrospectionFailure(
      `tables for Trino schema ${schemaName} in connection ${connection.name}`,
      async () => {
         let catalogPrefix: string;
         let schemaOnly: string;
         let resourcePrefix: string;

         if (trino.catalog) {
            catalogPrefix = `${trino.catalog}.`;
            schemaOnly = schemaName;
            resourcePrefix = `${trino.catalog}.${schemaName}`;
         } else {
            const dotIdx = schemaName.indexOf(".");
            if (dotIdx > 0) {
               catalogPrefix = `${assertSafeSqlIdentifier(
                  schemaName.substring(0, dotIdx),
                  "catalog name",
               )}.`;
               schemaOnly = schemaName.substring(dotIdx + 1);
            } else {
               catalogPrefix = "";
               schemaOnly = schemaName;
            }
            resourcePrefix = schemaName;
         }

         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM ${catalogPrefix}information_schema.columns WHERE table_schema = '${sqlLiteral(schemaOnly, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(
            rows,
            (t) => `${resourcePrefix}.${t}`,
         );
      },
      "after-log",
   );
}

async function listTablesForDatabricks(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   const databricks = connection.databricksConnection;
   if (!databricks) {
      throw new Error("Databricks connection is required");
   }
   return withIntrospectionFailure(
      `tables for Databricks schema ${schemaName} in connection ${connection.name}`,
      async () => {
         let catalogPrefix: string;
         let schemaOnly: string;
         let resourcePrefix: string;

         if (databricks.defaultCatalog) {
            catalogPrefix = `${databricks.defaultCatalog}.`;
            schemaOnly = schemaName;
            resourcePrefix = `${databricks.defaultCatalog}.${schemaName}`;
         } else {
            const dotIdx = schemaName.indexOf(".");
            if (dotIdx > 0) {
               catalogPrefix = `${assertSafeSqlIdentifier(
                  schemaName.substring(0, dotIdx),
                  "catalog name",
               )}.`;
               schemaOnly = schemaName.substring(dotIdx + 1);
            } else {
               catalogPrefix = "";
               schemaOnly = schemaName;
            }
            resourcePrefix = schemaName;
         }

         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM ${catalogPrefix}information_schema.columns WHERE table_schema = '${sqlLiteral(schemaOnly, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(
            rows,
            (t) => `${resourcePrefix}.${t}`,
         );
      },
      "after-log",
   );
}

async function listTablesForDuckDB(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.duckdbConnection) {
      throw new Error("DuckDB connection is required");
   }

   const attachedDbs = connection.duckdbConnection.attachedDatabases || [];

   // Azure attached database matched by name
   const azureDb = attachedDbs.find(
      (db) =>
         db.type === "azure" && db.name === schemaName && db.azureConnection,
   );
   if (azureDb) {
      const azureConn = azureDb.azureConnection!;
      const fileUrl =
         azureConn.authType === "sas_token"
            ? azureConn.sasUrl
            : azureConn.fileUrl;
      if (fileUrl) {
         return describeAzureFile(malloyConnection, fileUrl, azureConn);
      }
   }

   // Azure ADLS file path (abfss://, https://, az://)
   if (
      schemaName.startsWith("abfss://") ||
      schemaName.startsWith("https://") ||
      schemaName.startsWith("az://")
   ) {
      return describeAzureFile(malloyConnection, schemaName);
   }

   // Cloud storage (GCS/S3)
   const parsedUri = parseCloudUri(schemaName);
   if (parsedUri) {
      const {
         type: cloudType,
         bucket: bucketName,
         path: directoryPath,
      } = parsedUri;
      const credentials = getCloudCredentialsFromAttachedDatabases(
         attachedDbs,
         cloudType,
      );
      if (!credentials) {
         throw new Error(
            `${cloudType.toUpperCase()} credentials not found in attached databases`,
         );
      }
      const fileKeys = await listDataFilesInDirectory(
         credentials,
         bucketName,
         directoryPath,
      );
      return getCloudTablesWithColumns(
         malloyConnection,
         credentials,
         bucketName,
         fileKeys,
      );
   }

   // Regular DuckDB schema — query information_schema.columns
   const dotIdx = schemaName.indexOf(".");
   if (dotIdx < 0) {
      // InvalidArgumentError: a plain Error would classify as an internal fault
      // ("try again later"), which an agent retries forever.
      throw new InvalidArgumentError(
         `DuckDB schema name must be qualified as "catalog.schema", got "${schemaName}". List this connection's schemas and use one of those names verbatim.`,
      );
   }
   const catalogName = schemaName.substring(0, dotIdx);
   const actualSchemaName = schemaName.substring(dotIdx + 1);

   return withIntrospectionFailure(
      `tables for DuckDB schema ${schemaName} in connection ${connection.name}`,
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

async function listTablesForMotherDuck(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.motherduckConnection) {
      throw new Error("MotherDuck connection is required");
   }
   return withIntrospectionFailure(
      `tables for MotherDuck schema ${schemaName} in connection ${connection.name}`,
      async () => {
         const result = await runIntrospectionSQL(
            malloyConnection,
            `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${sqlLiteral(schemaName, connection.type)}' ${sqlInFilter("table_name", tableNames, connection.type)} ORDER BY table_name, ordinal_position`,
         );
         const rows = standardizeRunSQLResult(result);
         return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
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

export function extractErrorDataFromError(error: unknown): {
   error: string;
   stack?: string;
   task?: unknown;
} {
   const errorMessage = error instanceof Error ? error.message : String(error);
   const errorData: { error: string; stack?: string; task?: unknown } = {
      error: errorMessage,
   };
   if (error instanceof Error && logger.level === "debug") {
      errorData.stack = error.stack;
   }
   if (error && typeof error === "object" && "task" in error) {
      errorData.task = (error as { task?: unknown }).task;
   }
   return errorData;
}
