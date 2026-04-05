import { ClientSecretCredential } from "@azure/identity";
import { ContainerClient } from "@azure/storage-blob";
import { BigQuery } from "@google-cloud/bigquery";
import { Connection, TableSourceDef } from "@malloydata/malloy";
import { components } from "../api";
import { logger } from "../logger";
import {
   CloudStorageCredentials,
   gcsConnectionToCredentials,
   getCloudTablesWithColumns,
   listCloudDirectorySchemas,
   listDataFilesInDirectory,
   parseCloudUri,
   s3ConnectionToCredentials,
} from "./gcs_s3_utils";
import { ApiConnection } from "./model";

type ApiSchema = components["schemas"]["Schema"];
type ApiTable = components["schemas"]["Table"];
type ApiAzureConnection = components["schemas"]["AzureConnection"];

/**
 * Build a SQL `AND column IN (...)` fragment for optional table-name filtering.
 * Returns an empty string when `values` is undefined or empty.
 */
export function sqlInFilter(columnName: string, values?: string[]): string {
   if (!values || values.length === 0) return "";
   const escaped = values.map((v) => `'${v.replace(/'/g, "''")}'`);
   return `AND ${columnName} IN (${escaped.join(", ")})`;
}

/**
 * Group INFORMATION_SCHEMA.COLUMNS rows into ApiTable objects.
 * Handles both upper-case (Snowflake) and lower-case (Postgres/DuckDB) column names.
 */
function groupColumnRowsIntoTables(
   rows: unknown[],
   buildResource: (tableName: string) => string,
   normalizeType?: (rawType: string) => string,
): ApiTable[] {
   const tableMap = new Map<string, { name: string; type: string }[]>();
   for (const row of rows) {
      const r = row as Record<string, unknown>;
      const tableName = String(r.TABLE_NAME ?? r.table_name ?? "");
      const columnName = String(r.COLUMN_NAME ?? r.column_name ?? "");
      const rawType = String(r.DATA_TYPE ?? r.data_type ?? "").toLowerCase();
      const dataType = normalizeType ? normalizeType(rawType) : rawType;
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

function createBigQueryClient(connection: ApiConnection): BigQuery {
   if (!connection.bigqueryConnection) {
      throw new Error("BigQuery connection is required");
   }

   const config: {
      projectId: string;
      credentials?: object;
      keyFilename?: string;
   } = {
      projectId: connection.bigqueryConnection.defaultProjectId || "",
   };

   // Add service account key if provided
   if (connection.bigqueryConnection.serviceAccountKeyJson) {
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
      config.keyFilename = process.env.GOOGLE_APPLICATION_CREDENTIALS || "";
   } else {
      throw new Error(
         "BigQuery connection is required, either set the bigqueryConnection in the connection configuration or set the GOOGLE_APPLICATION_CREDENTIALS environment variable.",
      );
   }

   return new BigQuery(config);
}

function standardizeRunSQLResult(result: unknown): unknown[] {
   // Handle different result formats from malloyConnection.runSQL
   return Array.isArray(result)
      ? result
      : (result as { rows?: unknown[] }).rows || [];
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
   try {
      const bigquery = createBigQueryClient(connection);
      const [datasets] = await bigquery.getDatasets();

      return await Promise.all(
         datasets.map(async (dataset) => {
            const [metadata] = await dataset.getMetadata();
            return {
               name: dataset.id,
               isHidden: false,
               isDefault: false,
               description: (metadata as { description?: string })?.description,
            };
         }),
      );
   } catch (error) {
      logger.error(
         `Error getting schemas for BigQuery connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get schemas for BigQuery connection ${connection.name}: ${(error as Error).message}`,
      );
   }
}

async function getSchemasForPostgres(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.postgresConnection) {
      throw new Error("Postgres connection is required");
   }
   try {
      // Wrap in row_to_json because the Malloy Postgres driver's runSQL
      // de-JSONs each row via row.row (matching Malloy-generated queries).
      const result = await malloyConnection.runSQL(
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
            isHidden: ["information_schema", "pg_catalog", "pg_toast"].includes(
               schemaName,
            ),
            isDefault: schemaName === "public",
         };
      });
   } catch (error) {
      logger.error(
         `Error getting schemas for Postgres connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get schemas for Postgres connection ${connection.name}: ${(error as Error).message}`,
      );
   }
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

async function getSchemasForSnowflake(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.snowflakeConnection) {
      throw new Error("Snowflake connection is required");
   }
   try {
      const database = connection.snowflakeConnection.database;
      const schema = connection.snowflakeConnection.schema;

      const filters: string[] = [];
      if (database) {
         filters.push(`CATALOG_NAME = '${database}'`);
      }
      if (schema) {
         filters.push(`SCHEMA_NAME = '${schema}'`);
      }
      const whereClause =
         filters.length > 0 ? `WHERE ${filters.join(" AND ")}` : "";

      const result = await malloyConnection.runSQL(
         `SELECT CATALOG_NAME, SCHEMA_NAME, SCHEMA_OWNER FROM ${database ? `${database}.` : ""}INFORMATION_SCHEMA.SCHEMATA ${whereClause} ORDER BY SCHEMA_NAME`,
      );
      const rows = standardizeRunSQLResult(result);
      return rows.map((row: unknown) => {
         const typedRow = row as Record<string, unknown>;
         const catalogName = String(
            typedRow.CATALOG_NAME ?? typedRow.catalog_name ?? "",
         );
         const schemaName = String(
            typedRow.SCHEMA_NAME ?? typedRow.schema_name ?? "",
         );
         const owner = String(
            typedRow.SCHEMA_OWNER ?? typedRow.schema_owner ?? "",
         );
         return {
            name: `${catalogName}.${schemaName}`,
            isHidden:
               ["SNOWFLAKE", ""].includes(owner) ||
               schemaName === "INFORMATION_SCHEMA",
            isDefault: schema ? schemaName === schema : false,
         };
      });
   } catch (error) {
      logger.error(
         `Error getting schemas for Snowflake connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get schemas for Snowflake connection ${connection.name}: ${(error as Error).message}`,
      );
   }
}

async function getSchemasForTrino(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.trinoConnection) {
      throw new Error("Trino connection is required");
   }
   try {
      const configuredSchema = connection.trinoConnection.schema;
      let allRows: { catalog: string; schema: string }[] = [];

      if (connection.trinoConnection.catalog) {
         const catalog = connection.trinoConnection.catalog;
         const result = await malloyConnection.runSQL(
            `SELECT schema_name FROM ${catalog}.information_schema.schemata ORDER BY schema_name`,
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
         const catalogsResult = await malloyConnection.runSQL(`SHOW CATALOGS`);
         const catalogNames = standardizeRunSQLResult(catalogsResult).map(
            (row: unknown) => {
               const r = row as Record<string, unknown>;
               return String(r.Catalog ?? r.catalog ?? "");
            },
         );

         for (const catalog of catalogNames) {
            try {
               const result = await malloyConnection.runSQL(
                  `SELECT schema_name FROM ${catalog}.information_schema.schemata ORDER BY schema_name`,
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
         const name = connection.trinoConnection?.catalog
            ? schema
            : `${catalog}.${schema}`;
         return {
            name,
            isHidden: ["information_schema", "performance_schema"].includes(
               schema,
            ),
            isDefault: configuredSchema ? schema === configuredSchema : false,
         };
      });
   } catch (error) {
      logger.error(
         `Error getting schemas for Trino connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get schemas for Trino connection ${connection.name}: ${(error as Error).message}`,
      );
   }
}

async function getSchemasForDuckDB(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.duckdbConnection) {
      throw new Error("DuckDB connection is required");
   }
   try {
      const result = await malloyConnection.runSQL(
         "SELECT DISTINCT schema_name,catalog_name FROM information_schema.schemata ORDER BY catalog_name,schema_name",
         { rowLimit: 1000 },
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

      const attachedDatabases =
         connection.duckdbConnection.attachedDatabases || [];

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
   } catch (error) {
      logger.error(
         `Error getting schemas for DuckDB connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get schemas for DuckDB connection ${connection.name}: ${(error as Error).message}`,
      );
   }
}

async function getSchemasForMotherDuck(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (!connection.motherduckConnection) {
      throw new Error("MotherDuck connection is required");
   }
   try {
      const database = connection.motherduckConnection.database;
      const whereClause = database ? `WHERE catalog_name = '${database}'` : "";
      const result = await malloyConnection.runSQL(
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
            isHidden: ["information_schema", "performance_schema", ""].includes(
               schemaName,
            ),
            isDefault: schemaName === "main",
         };
      });
   } catch (error) {
      logger.error(
         `Error getting schemas for MotherDuck connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get schemas for MotherDuck connection ${connection.name}: ${(error as Error).message}`,
      );
   }
}

async function getSchemasForDuckLake(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   try {
      // The catalog is attached with the connection name (see attachDuckLake in connection.ts)
      const catalogName = connection.name;
      const result = await malloyConnection.runSQL(
         `SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '${catalogName}' ORDER BY schema_name`,
         { rowLimit: 1000 },
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
   } catch (error) {
      logger.error(
         `Error getting schemas for DuckLake connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get schemas for DuckLake connection ${connection.name}: ${(error as Error).message}`,
      );
   }
}

export async function getSchemasForConnection(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   switch (connection.type) {
      case "bigquery":
         return getSchemasForBigQuery(connection);
      case "postgres":
         return getSchemasForPostgres(connection, malloyConnection);
      case "mysql":
         return getSchemasForMySQL(connection);
      case "snowflake":
         return getSchemasForSnowflake(connection, malloyConnection);
      case "trino":
         return getSchemasForTrino(connection, malloyConnection);
      case "duckdb":
         return getSchemasForDuckDB(connection, malloyConnection);
      case "motherduck":
         return getSchemasForMotherDuck(connection, malloyConnection);
      case "ducklake":
         return getSchemasForDuckLake(connection, malloyConnection);
      default:
         throw new Error(`Unsupported connection type: ${connection.type}`);
   }
}

function getFileType(key: string): string {
   const lowerKey = key.toLowerCase();
   if (lowerKey.endsWith(".csv")) return "csv";
   if (lowerKey.endsWith(".parquet")) return "parquet";
   if (lowerKey.endsWith(".json")) return "json";
   if (lowerKey.endsWith(".jsonl") || lowerKey.endsWith(".ndjson"))
      return "jsonl";
   return "unknown";
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

   // Parse the URL to extract account, container, and blob path
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
      // Recursive listing: everything under this prefix
      prefix = blobPath.slice(0, -2);
      recursive = true;
   } else if (blobPath.includes("*")) {
      // Single-level glob: path/*.ext — files directly in that dir only
      const starIndex = blobPath.indexOf("*");
      prefix = blobPath.substring(0, starIndex);
      extensionFilter = blobPath.substring(starIndex + 1); // e.g. ".parquet"
      recursive = false;
   } else {
      // No glob — use blobPath as prefix (container-level listing)
      prefix = blobPath;
      recursive = true;
   }

   // Create ContainerClient with appropriate authentication
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

function isDataFile(key: string): boolean {
   const lowerKey = key.toLowerCase();
   return (
      lowerKey.endsWith(".csv") ||
      lowerKey.endsWith(".parquet") ||
      lowerKey.endsWith(".json") ||
      lowerKey.endsWith(".jsonl") ||
      lowerKey.endsWith(".ndjson")
   );
}

async function describeRemoteFile(
   malloyConnection: Connection,
   fileUri: string,
): Promise<ApiTable> {
   const pathWithoutQuery = fileUri.split("?")[0];
   const fileType = getFileType(pathWithoutQuery);

   let describeQuery: string;
   switch (fileType) {
      case "csv":
         describeQuery = `DESCRIBE SELECT * FROM read_csv('${fileUri}', auto_detect=true) LIMIT 1`;
         break;
      case "parquet":
         describeQuery = `DESCRIBE SELECT * FROM read_parquet('${fileUri}') LIMIT 1`;
         break;
      case "json":
         describeQuery = `DESCRIBE SELECT * FROM read_json('${fileUri}', auto_detect=true) LIMIT 1`;
         break;
      case "jsonl":
         describeQuery = `DESCRIBE SELECT * FROM read_json('${fileUri}', format='newline_delimited', auto_detect=true) LIMIT 1`;
         break;
      default:
         logger.warn(`Unsupported file type for file: ${fileUri}`);
         return { resource: fileUri, columns: [] };
   }

   const result = await malloyConnection.runSQL(describeQuery);
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
   switch (connection.type) {
      case "bigquery":
         return listTablesForBigQuery(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         );
      case "mysql":
         return listTablesForMySQL(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         );
      case "postgres":
         return listTablesForPostgres(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         );
      case "snowflake":
         return listTablesForSnowflake(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         );
      case "trino":
         return listTablesForTrino(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         );
      case "duckdb":
         return listTablesForDuckDB(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         );
      case "motherduck":
         return listTablesForMotherDuck(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         );
      case "ducklake":
         return listTablesForDuckLake(
            connection,
            schemaName,
            malloyConnection,
            tableNames,
         );
      default:
         throw new Error(`Unsupported connection type: ${connection.type}`);
   }
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
   try {
      const bigquery = createBigQueryClient(connection);
      const dataset = bigquery.dataset(schemaName);
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
   } catch (error) {
      logger.error(
         `Error getting tables for BigQuery schema ${schemaName} in connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get tables for BigQuery schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
      );
   }
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
   try {
      const result = await malloyConnection.runSQL(
         `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_schema = '${schemaName}' ${sqlInFilter("TABLE_NAME", tableNames)} ORDER BY TABLE_NAME, ORDINAL_POSITION`,
      );
      const rows = standardizeRunSQLResult(result);
      return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
   } catch (error) {
      logger.error(
         `Error getting tables for MySQL schema ${schemaName} in connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get tables for MySQL schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
      );
   }
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
   try {
      // Wrap in row_to_json because the Malloy Postgres driver's runSQL
      // de-JSONs each row via row.row (matching Malloy-generated queries).
      const result = await malloyConnection.runSQL(
         `SELECT row_to_json(t) as row FROM (SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${schemaName}' ${sqlInFilter("table_name", tableNames)} ORDER BY table_name, ordinal_position) t`,
      );
      const rows = standardizeRunSQLResult(result);
      return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
   } catch (error) {
      logger.error(
         `Error getting tables for Postgres schema ${schemaName} in connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get tables for Postgres schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
      );
   }
}

/**
 * Map raw Snowflake SQL types to Malloy type names so that column types
 * returned by listTables match those returned by Malloy's fetchTableSchema.
 * Mirrors the snowflakeToMalloyTypes map in @malloydata/malloy's Snowflake dialect.
 */
const snowflakeTypeToMalloy: Record<string, string> = {
   // string
   varchar: "string",
   text: "string",
   string: "string",
   char: "string",
   character: "string",
   nvarchar: "string",
   nvarchar2: "string",
   "char varying": "string",
   "nchar varying": "string",
   // integer
   integer: "number",
   int: "number",
   bigint: "number",
   smallint: "number",
   tinyint: "number",
   byteint: "number",
   number: "number",
   numeric: "number",
   decimal: "number",
   dec: "number",
   // float
   float: "number",
   float4: "number",
   float8: "number",
   double: "number",
   "double precision": "number",
   real: "number",
   // boolean
   boolean: "boolean",
   // date/time
   date: "date",
   timestamp: "timestamp",
   timestampntz: "timestamp",
   timestamp_ntz: "timestamp",
   "timestamp without time zone": "timestamp",
   timestamptz: "timestamptz",
   timestamp_tz: "timestamptz",
   "timestamp with time zone": "timestamptz",
};

function normalizeSnowflakeType(rawType: string): string {
   return snowflakeTypeToMalloy[rawType] ?? rawType;
}

async function listTablesForSnowflake(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.snowflakeConnection) {
      throw new Error("Snowflake connection is required");
   }
   try {
      const parts = schemaName.split(".");
      let databaseName: string;
      let schemaOnly: string;

      if (parts.length >= 2) {
         databaseName = parts[0];
         schemaOnly = parts[1];
      } else {
         databaseName = connection.snowflakeConnection.database ?? "";
         schemaOnly = parts[0];
      }

      if (!databaseName) {
         throw new Error(
            `Cannot resolve database for schema "${schemaName}": provide DATABASE.SCHEMA or configure a database on the connection`,
         );
      }

      const qualifiedSchema = `${databaseName}.${schemaOnly}`;
      const result = await malloyConnection.runSQL(
         `SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE FROM ${databaseName}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = '${schemaOnly}' ${sqlInFilter("TABLE_NAME", tableNames)} ORDER BY TABLE_NAME, ORDINAL_POSITION`,
      );
      const rows = standardizeRunSQLResult(result);
      return groupColumnRowsIntoTables(
         rows,
         (t) => `${qualifiedSchema}.${t}`,
         normalizeSnowflakeType,
      );
   } catch (error) {
      logger.error(
         `Error getting tables for Snowflake schema ${schemaName} in connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get tables for Snowflake schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
      );
   }
}

async function listTablesForTrino(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
   tableNames?: string[],
): Promise<ApiTable[]> {
   if (!connection.trinoConnection) {
      throw new Error("Trino connection is required");
   }
   try {
      let catalogPrefix: string;
      let schemaOnly: string;
      let resourcePrefix: string;

      if (connection.trinoConnection.catalog) {
         catalogPrefix = `${connection.trinoConnection.catalog}.`;
         schemaOnly = schemaName;
         resourcePrefix = `${connection.trinoConnection.catalog}.${schemaName}`;
      } else {
         const dotIdx = schemaName.indexOf(".");
         if (dotIdx > 0) {
            catalogPrefix = `${schemaName.substring(0, dotIdx)}.`;
            schemaOnly = schemaName.substring(dotIdx + 1);
         } else {
            catalogPrefix = "";
            schemaOnly = schemaName;
         }
         resourcePrefix = schemaName;
      }

      const result = await malloyConnection.runSQL(
         `SELECT table_name, column_name, data_type FROM ${catalogPrefix}information_schema.columns WHERE table_schema = '${schemaOnly}' ${sqlInFilter("table_name", tableNames)} ORDER BY table_name, ordinal_position`,
      );
      const rows = standardizeRunSQLResult(result);
      return groupColumnRowsIntoTables(rows, (t) => `${resourcePrefix}.${t}`);
   } catch (error) {
      logger.error(
         `Error getting tables for Trino schema ${schemaName} in connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get tables for Trino schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
      );
   }
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
      throw new Error(
         `DuckDB schema name must be qualified as "catalog.schema", got "${schemaName}"`,
      );
   }
   const catalogName = schemaName.substring(0, dotIdx);
   const actualSchemaName = schemaName.substring(dotIdx + 1);

   try {
      const result = await malloyConnection.runSQL(
         `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${actualSchemaName}' AND table_catalog = '${catalogName}' ${sqlInFilter("table_name", tableNames)} ORDER BY table_name, ordinal_position`,
      );
      const rows = standardizeRunSQLResult(result);
      return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
   } catch (error) {
      logger.error(
         `Error getting tables for DuckDB schema ${schemaName} in connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get tables for DuckDB schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
      );
   }
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
   try {
      const result = await malloyConnection.runSQL(
         `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${schemaName}' ${sqlInFilter("table_name", tableNames)} ORDER BY table_name, ordinal_position`,
      );
      const rows = standardizeRunSQLResult(result);
      return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
   } catch (error) {
      logger.error(
         `Error getting tables for MotherDuck schema ${schemaName} in connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get tables for MotherDuck schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
      );
   }
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
   try {
      const result = await malloyConnection.runSQL(
         `SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '${actualSchemaName}' AND table_catalog = '${catalogName}' ${sqlInFilter("table_name", tableNames)} ORDER BY table_name, ordinal_position`,
      );
      const rows = standardizeRunSQLResult(result);
      return groupColumnRowsIntoTables(rows, (t) => `${schemaName}.${t}`);
   } catch (error) {
      logger.error(
         `Error getting tables for DuckLake schema ${schemaName} in connection ${connection.name}`,
         { error },
      );
      throw new Error(
         `Failed to get tables for DuckLake schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
      );
   }
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
