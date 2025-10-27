import { BigQuery } from "@google-cloud/bigquery";
import { Connection, TableSourceDef } from "@malloydata/malloy";
import { components } from "../api";
import { ConnectionError } from "../errors";
import { logger } from "../logger";
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

         return datasets.map((dataset) => ({
            name: dataset.id,
            isHidden: false,
            isDefault: false,
         }));
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
         // Use the connection's runSQL method to query schemas
         const result = await malloyConnection.runSQL(
            `SHOW SCHEMAS FROM ${connection.trinoConnection.catalog}`,
         );

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

         return rows.map((row: unknown) => {
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
      } catch (error) {
         console.error(
            `Error getting schemas for DuckDB connection ${connection.name}:`,
            error,
         );
         throw new Error(
            `Failed to get schemas for DuckDB connection ${connection.name}: ${(error as Error).message}`,
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

   // Fetch all table sources in parallel
   const tableSourcePromises = tableNames.map(async (tableName) => {
      try {
         const tablePath = `${schemaName}.${tableName}`;

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
         throw new ConnectionError(`Table ${tablePath} not found`);
      }

      // Validate that source has the expected structure
      if (!source || typeof source !== "object") {
         throw new ConnectionError(
            `Invalid table source returned for ${tablePath}`,
         );
      }

      const malloyFields = (source as TableSourceDef).fields;
      if (!malloyFields || !Array.isArray(malloyFields)) {
         throw new ConnectionError(
            `Table ${tablePath} has no fields or invalid field structure`,
         );
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
      logger.error("fetchTableSchema error", { error, tableKey, tablePath });
      throw new ConnectionError((error as Error).message);
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
         const result = await malloyConnection.runSQL(
            `SHOW TABLES FROM ${connection.trinoConnection.catalog}.${schemaName}`,
         );
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
      try {
         const catalogName = schemaName.split(".")[0];
         schemaName = schemaName.split(".")[1];
         const result = await malloyConnection.runSQL(
            `SELECT table_name FROM information_schema.tables WHERE table_schema = '${schemaName}' and table_catalog = '${catalogName}' ORDER BY table_name`,
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
   } else {
      // TODO(jjs) - implement
      return [];
   }
}
