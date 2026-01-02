import { BigQuery } from "@google-cloud/bigquery";
import { Connection, TableSourceDef } from "@malloydata/malloy";
import { components } from "../api";
import { ConnectionError } from "../errors";
import { logger } from "../logger";
import {
   buildGCSUri,
   GCSCredentials,
   getFileType,
   isDataFile,
   listAllGCSFiles,
   listGCSBuckets,
} from "./gcs_utils";
import { ApiConnection } from "./model";

type ApiSchema = components["schemas"]["Schema"];
type ApiTable = components["schemas"]["Table"];
type ApiTableSource = components["schemas"]["TableSource"];

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
      try {
         const credentials = JSON.parse(
            connection.bigqueryConnection.serviceAccountKeyJson,
         );
         config.credentials = credentials;

         // Use project_id from credentials if defaultProjectId is not set
         if (!config.projectId && credentials.project_id) {
            config.projectId = credentials.project_id;
         }

         if (!config.projectId) {
            throw new Error(
               "BigQuery project ID is required. Either set the defaultProjectId in the connection configuration or the project_id in the service account key JSON.",
            );
         }
      } catch (error) {
         logger.warn(
            "Failed to parse service account key JSON, using default credentials",
            { error },
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

export async function getSchemasForConnection(
   connection: ApiConnection,
   malloyConnection: Connection,
): Promise<ApiSchema[]> {
   if (connection.type === "bigquery") {
      if (!connection.bigqueryConnection) {
         throw new Error("BigQuery connection is required");
      }
      try {
         const bigquery = createBigQueryClient(connection);
         const [datasets] = await bigquery.getDatasets();

         const schemas = await Promise.all(
            datasets.map(async (dataset) => {
               const [metadata] = await dataset.getMetadata();
               return {
                  name: dataset.id,
                  isHidden: false,
                  isDefault: false,
                  // Include description from dataset metadata if available
                  description: (metadata as { description?: string })
                     ?.description,
               };
            }),
         );
         return schemas;
      } catch (error) {
         console.error(
            `Error getting schemas for BigQuery connection ${connection.name}:`,
            error,
         );
         throw new Error(
            `Failed to get schemas for BigQuery connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "postgres") {
      if (!connection.postgresConnection) {
         throw new Error("Postgres connection is required");
      }
      try {
         // Use the connection's runSQL method to query schemas
         const result = await malloyConnection.runSQL(
            "SELECT schema_name as row FROM information_schema.schemata ORDER BY schema_name",
         );

         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const schemaName = row as string;
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
      } catch (error) {
         console.error(
            `Error getting schemas for Postgres connection ${connection.name}:`,
            error,
         );
         throw new Error(
            `Failed to get schemas for Postgres connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "mysql") {
      if (!connection.mysqlConnection) {
         throw new Error("Mysql connection is required");
      }
      try {
         // For MySQL, return the database name as the schema
         return [
            {
               name: connection.mysqlConnection.database || "mysql",
               isHidden: false,
               isDefault: true,
            },
         ];
      } catch (error) {
         console.error(
            `Error getting schemas for MySQL connection ${connection.name}:`,
            error,
         );
         throw new Error(
            `Failed to get schemas for MySQL connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "snowflake") {
      if (!connection.snowflakeConnection) {
         throw new Error("Snowflake connection is required");
      }
      try {
         // Use the connection's runSQL method to query schemas
         const result = await malloyConnection.runSQL("SHOW SCHEMAS");

         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            return {
               name: typedRow.name as string,
               isHidden: ["SNOWFLAKE", ""].includes(typedRow.owner as string),
               isDefault: typedRow.isDefault === "Y",
            };
         });
      } catch (error) {
         console.error(
            `Error getting schemas for Snowflake connection ${connection.name}:`,
            error,
         );
         throw new Error(
            `Failed to get schemas for Snowflake connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "trino") {
      if (!connection.trinoConnection) {
         throw new Error("Trino connection is required");
      }
      try {
         let result: unknown;
         // Use the connection's runSQL method to query schemas
         if (connection.trinoConnection.catalog) {
            result = await malloyConnection.runSQL(
               `SHOW SCHEMAS FROM ${connection.trinoConnection.catalog}`,
            );
         } else {
            const catalogs = await malloyConnection.runSQL(`SHOW CATALOGS`);
            console.log("catalogs", catalogs);
            let catalogNames = standardizeRunSQLResult(catalogs);
            catalogNames = catalogNames.map((catalog: unknown) => {
               const typedCatalog = catalog as Record<string, unknown>;
               return typedCatalog.Catalog as string;
            });

            const schemas: unknown[] = [];

            console.log("catalogNames", catalogNames);
            for (const catalog of catalogNames) {
               const schemasResult = await malloyConnection.runSQL(
                  `SHOW SCHEMAS FROM ${catalog}`,
               );
               const schemasResultRows = standardizeRunSQLResult(schemasResult);
               console.log("schemasResultRows", schemasResultRows);

               // Concat catalog name to schema name for each schema row
               const schemasWithCatalog = schemasResultRows.map(
                  (row: unknown) => {
                     const typedRow = row as Record<string, unknown>;
                     // For display, use the convention "catalog.schema"
                     return {
                        ...typedRow,
                        Schema: `${catalog}.${typedRow.Schema ?? typedRow.schema ?? ""}`,
                     };
                  },
               );
               schemas.push(...schemasWithCatalog);
               console.log("schemas", schemas);
            }
            result = schemas;
         }

         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            return {
               name: typedRow.Schema as string,
               isHidden: ["information_schema", "performance_schema"].includes(
                  typedRow.Schema as string,
               ),
               isDefault:
                  typedRow.Schema === connection.trinoConnection?.schema,
            };
         });
      } catch (error) {
         console.error(
            `Error getting schemas for Trino connection ${connection.name}:`,
            error,
         );
         throw new Error(
            `Failed to get schemas for Trino connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "duckdb") {
      if (!connection.duckdbConnection) {
         throw new Error("DuckDB connection is required");
      }
      try {
         // Use DuckDB's INFORMATION_SCHEMA.SCHEMATA to list schemas
         // Use DISTINCT to avoid duplicates from attached databases
         const result = await malloyConnection.runSQL(
            "SELECT DISTINCT schema_name,catalog_name FROM information_schema.schemata ORDER BY catalog_name,schema_name",
            { rowLimit: 1000 },
         );

         const rows = standardizeRunSQLResult(result);

         const schemas: ApiSchema[] = rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            const schemaName = typedRow.schema_name as string;
            const catalogName = typedRow.catalog_name as string;

            return {
               name: `${catalogName}.${schemaName}`,
               isHidden:
                  [
                     "information_schema",
                     "performance_schema",
                     "",
                     "SNOWFLAKE",
                     "information_schema",
                     "pg_catalog",
                     "pg_toast",
                  ].includes(schemaName as string) ||
                  ["md_information_schema", "system"].includes(
                     catalogName as string,
                  ),
               isDefault: catalogName === "main",
            };
         });

         // Add GCS buckets as schemas for each GCS attached database
         const attachedDatabases =
            connection.duckdbConnection.attachedDatabases || [];
         for (const attachedDb of attachedDatabases) {
            if (attachedDb.type === "gcs" && attachedDb.gcsConnection) {
               const gcsCredentials: GCSCredentials = {
                  keyId: attachedDb.gcsConnection.keyId || "",
                  secret: attachedDb.gcsConnection.secret || "",
               };

               try {
                  const buckets = await listGCSBuckets(gcsCredentials);
                  for (const bucket of buckets) {
                     schemas.push({
                        name: `gcs.${bucket.name}`,
                        isHidden: false,
                        isDefault: false,
                     });
                  }
                  logger.info(
                     `Listed ${buckets.length} GCS buckets for attached database ${attachedDb.name}`,
                  );
               } catch (gcsError) {
                  logger.warn(
                     `Failed to list GCS buckets for ${attachedDb.name}`,
                     { error: gcsError },
                  );
               }
            }
         }

         return schemas;
      } catch (error) {
         console.error(
            `Error getting schemas for DuckDB connection ${connection.name}:`,
            error,
         );
         throw new Error(
            `Failed to get schemas for DuckDB connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "motherduck") {
      if (!connection.motherduckConnection) {
         throw new Error("MotherDuck connection is required");
      }
      try {
         // Use MotherDuck's INFORMATION_SCHEMA.SCHEMATA to list schemas
         const result = await malloyConnection.runSQL(
            "SELECT DISTINCT schema_name as row FROM information_schema.schemata ORDER BY schema_name",
            { rowLimit: 1000 },
         );
         const rows = standardizeRunSQLResult(result);
         console.log(rows);
         return rows.map((row: unknown) => {
            const typedRow = row as { row: string };
            return {
               name: typedRow.row,
               isHidden: [
                  "information_schema",
                  "performance_schema",
                  "",
               ].includes(typedRow.row),
               isDefault: false,
            };
         });
      } catch (error) {
         console.error(
            `Error getting schemas for MotherDuck connection ${connection.name}:`,
            error,
         );
         throw new Error(
            `Failed to get schemas for MotherDuck connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else {
      throw new Error(`Unsupported connection type: ${connection.type}`);
   }
}

export async function getTablesForSchema(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
): Promise<ApiTable[]> {
   // First get the list of table names
   const tableNames = await listTablesForSchema(
      connection,
      schemaName,
      malloyConnection,
   );

   const catalogName = schemaName.split(".")[0];

   // Handle GCS files - use DuckDB to read schema
   if (catalogName === "gcs" && connection.type === "duckdb") {
      console.log("Getting GCS tables for schema", schemaName);
      console.log("tableNames", tableNames);
      const bucketName = schemaName.split(".")[1];
      console.log("bucketName", bucketName);
      return await getGCSTablesWithColumns(
         malloyConnection,
         bucketName,
         tableNames,
      );
   }

   // Fetch all table sources in parallel
   const tableSourcePromises = tableNames.map(async (tableName) => {
      try {
         let tablePath: string;

         if (connection.type === "trino") {
            if (connection.trinoConnection?.catalog) {
               tablePath = `${connection.trinoConnection?.catalog}.${schemaName}.${tableName}`;
            } else {
               // Catalog name is included in the schema name
               tablePath = `${schemaName}.${tableName}`;
            }
         } else {
            tablePath = `${schemaName}.${tableName}`;
         }

         logger.info(
            `Processing table: ${tableName} in schema: ${schemaName}`,
            { tablePath, connectionType: connection.type },
         );
         const tableSource = await getConnectionTableSource(
            malloyConnection,
            tableName,
            tablePath,
         );

         return {
            resource: tablePath,
            columns: tableSource.columns,
         };
      } catch (error) {
         logger.warn(`Failed to get schema for table ${tableName}`, {
            error,
            schemaName,
            tableName,
         });
         // Return table without columns if schema fetch fails
         return {
            resource: `${schemaName}.${tableName}`,
            columns: [],
         };
      }
   });

   // Wait for all table sources to be fetched
   const tableResults = await Promise.all(tableSourcePromises);

   return tableResults;
}

async function getGCSTablesWithColumns(
   malloyConnection: Connection,
   bucketName: string,
   fileKeys: string[],
): Promise<ApiTable[]> {
   const tables: ApiTable[] = [];

   for (const fileKey of fileKeys) {
      const gcsUri = buildGCSUri(bucketName, fileKey);
      const fileType = getFileType(fileKey);

      try {
         let describeQuery: string;

         switch (fileType) {
            case "csv":
               describeQuery = `DESCRIBE SELECT * FROM read_csv('${gcsUri}', auto_detect=true) LIMIT 1`;
               break;
            case "parquet":
               describeQuery = `DESCRIBE SELECT * FROM read_parquet('${gcsUri}') LIMIT 1`;
               break;
            case "json":
               describeQuery = `DESCRIBE SELECT * FROM read_json('${gcsUri}', auto_detect=true) LIMIT 1`;
               break;
            case "jsonl":
               describeQuery = `DESCRIBE SELECT * FROM read_json('${gcsUri}', format='newline_delimited', auto_detect=true) LIMIT 1`;
               break;
            default:
               logger.warn(`Unsupported file type for ${fileKey}`);
               tables.push({
                  resource: gcsUri,
                  columns: [],
               });
               continue;
         }

         const result = await malloyConnection.runSQL(describeQuery);
         console.log("result", result);
         const rows = standardizeRunSQLResult(result);
         console.log("rows", rows);
         const columns = rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            return {
               name: (typedRow.column_name || typedRow.name) as string,
               type: (typedRow.column_type || typedRow.type) as string,
            };
         });

         tables.push({
            resource: gcsUri,
            columns,
         });

         logger.info(`Got schema for GCS file: ${gcsUri}`, {
            columnCount: columns.length,
         });
      } catch (error) {
         logger.warn(`Failed to get schema for GCS file: ${gcsUri}`, {
            error,
         });
         tables.push({
            resource: gcsUri,
            columns: [],
         });
      }
   }

   return tables;
}

export async function getConnectionTableSource(
   malloyConnection: Connection,
   tableKey: string,
   tablePath: string,
): Promise<ApiTableSource> {
   try {
      logger.info(`Attempting to fetch table schema for: ${tablePath}`, {
         tableKey,
         tablePath,
      });
      const source = await (
         malloyConnection as Connection & {
            fetchTableSchema: (
               tableKey: string,
               tablePath: string,
            ) => Promise<TableSourceDef | undefined>;
         }
      ).fetchTableSchema(tableKey, tablePath);
      if (source === undefined) {
         throw new ConnectionError(
            `Table ${tablePath} not found: ${JSON.stringify(source)}`,
         );
      }

      // Validate that source has the expected structure
      if (!source) {
         throw new ConnectionError(
            `Invalid table source returned for ${tablePath}`,
         );
      } else if (typeof source !== "object") {
         throw new ConnectionError(JSON.stringify(source));
      }

      const malloyFields = (source as TableSourceDef).fields;
      if (!malloyFields || !Array.isArray(malloyFields)) {
         throw new ConnectionError(
            `Table ${tablePath} has no fields or invalid field structure`,
         );
      }

      //This is for the Trino connection. The connection will not throw an error if the table is not found.
      // Instead it will return an empty fields array. So we need to check for that.
      // But it is fine to have it for all other connections as well.
      if (malloyFields.length === 0) {
         throw new ConnectionError(`Table ${tablePath} not found`);
      }

      const fields = malloyFields.map((field) => {
         return {
            name: field.name,
            type: field.type,
         };
      });
      logger.info(`Successfully fetched schema for ${tablePath}`, {
         fieldCount: fields.length,
      });
      return {
         source: JSON.stringify(source),
         resource: tablePath,
         columns: fields,
      };
   } catch (error) {
      const errorMessage =
         error instanceof Error
            ? error.message
            : typeof error === "string"
              ? error
              : JSON.stringify(error);
      logger.error("fetchTableSchema error", {
         error,
         tableKey,
         tablePath,
      });
      throw new ConnectionError(errorMessage);
   }
}

export async function listTablesForSchema(
   connection: ApiConnection,
   schemaName: string,
   malloyConnection: Connection,
): Promise<string[]> {
   if (connection.type === "bigquery") {
      try {
         // Use BigQuery client directly for efficient table listing
         // This is much faster than querying all regions
         const bigquery = createBigQueryClient(connection);
         const dataset = bigquery.dataset(schemaName);
         const [tables] = await dataset.getTables();

         // Return table names, filtering out any undefined values
         return tables
            .map((table) => table.id)
            .filter((id): id is string => id !== undefined);
      } catch (error) {
         logger.error(
            `Error getting tables for BigQuery schema ${schemaName} in connection ${connection.name}`,
            { error },
         );
         throw new Error(
            `Failed to get tables for BigQuery schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "mysql") {
      if (!connection.mysqlConnection) {
         throw new Error("Mysql connection is required");
      }
      try {
         const result = await malloyConnection.runSQL(
            `SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = '${schemaName}' AND table_type = 'BASE TABLE'`,
         );
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            return typedRow.TABLE_NAME as string;
         });
      } catch (error) {
         logger.error(
            `Error getting tables for MySQL schema ${schemaName} in connection ${connection.name}`,
            { error },
         );
         throw new Error(
            `Failed to get tables for MySQL schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "postgres") {
      if (!connection.postgresConnection) {
         throw new Error("Postgres connection is required");
      }
      try {
         const result = await malloyConnection.runSQL(
            `SELECT table_name as row FROM information_schema.tables WHERE table_schema = '${schemaName}' ORDER BY table_name`,
         );
         const rows = standardizeRunSQLResult(result);
         return rows as string[];
      } catch (error) {
         logger.error(
            `Error getting tables for Postgres schema ${schemaName} in connection ${connection.name}`,
            { error },
         );
         throw new Error(
            `Failed to get tables for Postgres schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "snowflake") {
      if (!connection.snowflakeConnection) {
         throw new Error("Snowflake connection is required");
      }
      try {
         const result = await malloyConnection.runSQL(
            `SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '${schemaName}' AND TABLE_TYPE = 'BASE TABLE'`,
         );
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            return typedRow.TABLE_NAME as string;
         });
      } catch (error) {
         logger.error(
            `Error getting tables for Snowflake schema ${schemaName} in connection ${connection.name}`,
            { error },
         );
         throw new Error(
            `Failed to get tables for Snowflake schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "trino") {
      if (!connection.trinoConnection) {
         throw new Error("Trino connection is required");
      }
      try {
         let result: unknown;

         if (connection.trinoConnection?.catalog) {
            result = await malloyConnection.runSQL(
               `SHOW TABLES FROM ${connection.trinoConnection.catalog}.${schemaName}`,
            );
         } else {
            // Catalog name is included in the schema name
            result = await malloyConnection.runSQL(
               `SHOW TABLES FROM ${schemaName}`,
            );
         }
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            return typedRow.Table as string;
         });
      } catch (error) {
         logger.error(
            `Error getting tables for Trino schema ${schemaName} in connection ${connection.name}`,
            { error },
         );
         throw new Error(
            `Failed to get tables for Trino schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "duckdb") {
      if (!connection.duckdbConnection) {
         throw new Error("DuckDB connection is required");
      }

      const catalogName = schemaName.split(".")[0];
      const actualSchemaName = schemaName.split(".")[1];

      // Handle GCS bucket schemas
      if (catalogName === "gcs") {
         const bucketName = actualSchemaName;
         const attachedDatabases =
            connection.duckdbConnection.attachedDatabases || [];

         // Find GCS credentials from attached databases
         let gcsCredentials: GCSCredentials | null = null;
         for (const attachedDb of attachedDatabases) {
            if (attachedDb.type === "gcs" && attachedDb.gcsConnection) {
               gcsCredentials = {
                  keyId: attachedDb.gcsConnection.keyId || "",
                  secret: attachedDb.gcsConnection.secret || "",
               };
               break;
            }
         }

         if (!gcsCredentials) {
            throw new Error("GCS credentials not found in attached databases");
         }

         try {
            // Recursively list all files including those in subdirectories
            const objects = await listAllGCSFiles(gcsCredentials, bucketName);
            // Return only data files (CSV, Parquet, JSON, etc.)
            return objects
               .filter((obj) => isDataFile(obj.key))
               .map((obj) => obj.key);
         } catch (error) {
            logger.error(`Error listing GCS objects in bucket ${bucketName}`, {
               error,
            });
            throw new Error(
               `Failed to list files in GCS bucket ${bucketName}: ${(error as Error).message}`,
            );
         }
      }

      // Regular DuckDB table listing
      try {
         const result = await malloyConnection.runSQL(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = '${actualSchemaName}' and table_catalog = '${catalogName}' ORDER BY table_name`,
            { rowLimit: 1000 },
         );

         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as Record<string, unknown>;
            return typedRow.table_name as string;
         });
      } catch (error) {
         logger.error(
            `Error getting tables for DuckDB schema ${schemaName} in connection ${connection.name}`,
            { error },
         );
         throw new Error(
            `Failed to get tables for DuckDB schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else if (connection.type === "motherduck") {
      if (!connection.motherduckConnection) {
         throw new Error("MotherDuck connection is required");
      }
      try {
         const result = await malloyConnection.runSQL(
            `SELECT table_name as row FROM information_schema.tables WHERE table_schema = '${schemaName}' ORDER BY table_name`,
            { rowLimit: 1000 },
         );
         const rows = standardizeRunSQLResult(result);
         return rows.map((row: unknown) => {
            const typedRow = row as { row: string };
            return typedRow.row;
         });
      } catch (error) {
         logger.error(
            `Error getting tables for MotherDuck schema ${schemaName} in connection ${connection.name}`,
            { error },
         );
         throw new Error(
            `Failed to get tables for MotherDuck schema ${schemaName} in connection ${connection.name}: ${(error as Error).message}`,
         );
      }
   } else {
      throw new Error(`Unsupported connection type: ${connection.type}`);
   }
}
