import { BigQuery } from "@google-cloud/bigquery";
import fs from "fs";
import os from "os";
import path from "path";
import { Pool } from "pg";
import * as snowflake from "snowflake-sdk";
import { v4 as uuidv4 } from "uuid";
import { components } from "../api";
import { logger } from "../logger";
import {
   ApiConnection,
   // @ts-expect-error TODO: Fix missing MysqlConnection type in API
   MysqlConnection,
   PostgresConnection,
   SnowflakeConnection,
   TrinoConnection,
} from "./model";
import { ProjectStore } from "./project_store";

import { TableSourceDef } from "@malloydata/malloy";
import { BasicAuth, Trino } from "trino-client";
import { ConnectionError } from "../errors";

type ApiSchema = components["schemas"]["Schema"];
type ApiTable = components["schemas"]["Table"];
type ApiTableSource = components["schemas"]["TableSource"];

async function getPostgresConnection(
   apiPostgresConnection: PostgresConnection,
): Promise<Pool> {
   return new Pool({
      user: apiPostgresConnection.userName,
      host: apiPostgresConnection.host,
      database: apiPostgresConnection.databaseName,
      password: apiPostgresConnection.password,
      port: apiPostgresConnection.port,
      connectionString: apiPostgresConnection.connectionString,
      max: 10,
      idleTimeoutMillis: 30000,
   });
}

async function getMysqlConnection(apiMysqlConnection: MysqlConnection) {
   // Dynamically import mysql2/promise to avoid import issues if not needed
   const mysql = await import("mysql2/promise");
   return mysql.createPool({
      host: apiMysqlConnection.host,
      port: apiMysqlConnection.port,
      user: apiMysqlConnection.user,
      password: apiMysqlConnection.password,
      database: apiMysqlConnection.database,
      waitForConnections: true,
      connectionLimit: 10,
      queueLimit: 0,
   });
}

function getBigqueryConnection(apiConnection: ApiConnection): BigQuery {
   if (!apiConnection.bigqueryConnection?.serviceAccountKeyJson) {
      // Use default credentials
      return new BigQuery();
   } else {
      const tmpKeyPath = getTempServiceKeyPath(apiConnection);
      if (!tmpKeyPath) {
         throw new Error(
            `Failed to create temporary service key file for connection: ${apiConnection.name}`,
         );
      }
      return new BigQuery({ keyFilename: tmpKeyPath });
   }
}

async function getSnowflakeConnection(
   apiSnowflakeConnection: SnowflakeConnection,
): Promise<snowflake.Connection> {
   if (!apiSnowflakeConnection.account) {
      throw new Error("Snowflake account is required");
   }
   return new Promise((resolve, reject) => {
      const connection = snowflake.createConnection({
         account: apiSnowflakeConnection.account as string,
         username: apiSnowflakeConnection.username,
         password: apiSnowflakeConnection.password,
         database: apiSnowflakeConnection.database,
         warehouse: apiSnowflakeConnection.warehouse || undefined,
         role: apiSnowflakeConnection.role || undefined,
      });
      connection.connect((err, conn) => {
         if (err) {
            reject(err);
         } else {
            resolve(conn);
         }
      });
   });
}

function getTrinoClient(trinoConn: TrinoConnection) {
   let auth: BasicAuth;
   if (trinoConn.server?.startsWith("https://")) {
      // HTTPS allows password authentication
      auth = new BasicAuth(trinoConn?.user || "", trinoConn?.password || "");
   } else {
      // HTTP only allows username, no password for security
      auth = new BasicAuth(trinoConn?.user || "");
   }

   return Trino.create({
      server: trinoConn.server,
      catalog: trinoConn.catalog,
      schema: trinoConn.schema,
      auth,
   });
}

export async function getSchemasForConnection(
   connection: ApiConnection,
): Promise<ApiSchema[]> {
   if (connection.type === "bigquery") {
      if (!connection.bigqueryConnection) {
         throw new Error("BigQuery connection is required");
      }
      try {
         const bigquery = getBigqueryConnection(connection);
         const projectId = connection.bigqueryConnection.defaultProjectId;
         const options = projectId ? { projectId } : {};
         const [datasets] = await bigquery.getDatasets(options);
         const schemas = await Promise.all(
            datasets
               .filter((dataset) => dataset.id)
               .map(async (dataset) => {
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
      const pool = await getPostgresConnection(connection.postgresConnection);
      const res = await pool.query(
         "SELECT schema_name FROM information_schema.schemata",
      );
      return res.rows.map((row) => {
         return {
            name: row.schema_name,
            isHidden: ["information_schema", "pg_catalog"].includes(
               row.schema_name,
            ),
            isDefault: row.schema_name === "public",
         };
      });
   } else if (connection.type === "mysql") {
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
   } else if (connection.type === "snowflake") {
      if (!connection.snowflakeConnection) {
         throw new Error("Snowflake connection is required");
      }
      const snowflakeConn = await getSnowflakeConnection(
         connection.snowflakeConnection,
      );
      try {
         return await getSnowflakeSchemas(snowflakeConn);
      } finally {
         snowflakeConn.destroy((error) => {
            if (error) {
               logger.error(`Error closing SnowflakeConnection: ${error}`);
            }
         });
      }
   } else if (connection.type === "trino") {
      if (!connection.trinoConnection) {
         throw new Error("Trino connection is required");
      }
      const client = getTrinoClient(connection.trinoConnection);
      const result = await client.query(
         `SHOW SCHEMAS FROM ${connection.trinoConnection.catalog}`,
      );
      const rows: string[] = [];
      let next = await result.next();
      while (!next.done) {
         if (next.value.data) {
            rows.push(...next.value.data.map((r: string[]) => r[0]));
         }
         next = await result.next();
      }
      return rows.map((r) => ({
         name: r,
         isHidden: false,
         isDefault: r === connection.trinoConnection?.schema,
      }));
   } else {
      throw new Error(`Unsupported connection type: ${connection.type}`);
   }
}

export async function getTablesForSchema(
   connection: ApiConnection,
   schemaName: string,
   projectStore: ProjectStore,
   projectName: string,
   connectionName: string,
): Promise<ApiTable[]> {
   // First get the list of table names
   const tableNames = await listTablesForSchema(connection, schemaName);

   // Fetch all table sources in parallel
   const tableSourcePromises = tableNames.map(async (tableName) => {
      try {
         const tablePath = `${schemaName}.${tableName}`;
         const tableSource = await getConnectionTableSource(
            projectStore,
            projectName,
            connectionName,
            tableName,
            tablePath,
         );

         return {
            resource: tablePath,
            columns: tableSource.columns,
         };
      } catch (error) {
         logger.warn(`Failed to get schema for table ${tableName}`, { error });
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
   projectStore: ProjectStore,
   projectName: string,
   connectionName: string,
   tableKey: string,
   tablePath: string,
): Promise<ApiTableSource> {
   const project = await projectStore.getProject(projectName, false);
   const connection = project.getMalloyConnection(connectionName);
   try {
      const source = await connection.fetchTableSchema(tableKey, tablePath);
      if (source === undefined) {
         throw new ConnectionError(`Table ${tablePath} not found`);
      }
      const malloyFields = (source as TableSourceDef).fields;
      const fields = malloyFields.map((field) => {
         return {
            name: field.name,
            type: field.type,
         };
      });
      return {
         source: JSON.stringify(source),
         resource: tablePath,
         columns: fields,
      };
   } catch (error) {
      logger.error("error", { error });
      throw new ConnectionError((error as Error).message);
   }
}

export async function listTablesForSchema(
   connection: ApiConnection,
   schemaName: string,
): Promise<string[]> {
   if (connection.type === "bigquery") {
      try {
         const bigquery = getBigqueryConnection(connection);
         const options = connection.bigqueryConnection?.defaultProjectId
            ? {
                 projectId: connection.bigqueryConnection?.defaultProjectId,
              }
            : {};
         const dataset = bigquery.dataset(schemaName, options);
         const [exists] = await dataset.exists();
         if (!exists) {
            throw new Error(
               `Dataset ${schemaName} does not exist in connection ${connection.name}`,
            );
         }

         const [tables] = await dataset.getTables();
         return tables.map((table) => table.id).filter((id) => id) as string[];
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
      const pool = await getMysqlConnection(connection.mysqlConnection);
      const [rows] = await pool.query(
         "SELECT TABLE_NAME FROM information_schema.tables WHERE table_schema = ? AND table_type = 'BASE TABLE'",
         [schemaName],
      );
      return (rows as { TABLE_NAME: string }[]).map((row) => row.TABLE_NAME);
   } else if (connection.type === "postgres") {
      if (!connection.postgresConnection) {
         throw new Error("Postgres connection is required");
      }
      const pool = await getPostgresConnection(connection.postgresConnection);
      const res = await pool.query(
         "SELECT table_name FROM information_schema.tables WHERE table_schema = $1",
         [schemaName],
      );
      return res.rows.map((row) => row.table_name);
   } else if (connection.type === "snowflake") {
      if (!connection.snowflakeConnection) {
         throw new Error("Snowflake connection is required");
      }
      const snowflakeConn = await getSnowflakeConnection(
         connection.snowflakeConnection,
      );
      try {
         return await getSnowflakeTables(
            snowflakeConn,
            connection.snowflakeConnection,
            schemaName,
         );
      } finally {
         snowflakeConn.destroy((error) => {
            if (error) {
               logger.error(`Error closing SnowflakeConnection`, { error });
            }
         });
      }
   } else if (connection.type === "trino") {
      if (!connection.trinoConnection) {
         throw new Error("Trino connection is required");
      }
      const client = getTrinoClient(connection.trinoConnection);
      const result = await client.query(
         `SHOW TABLES FROM ${connection.trinoConnection.catalog}.${schemaName}`,
      );

      const rows: string[] = [];
      let next = await result.next();
      while (!next.done) {
         if (next.value.data) {
            rows.push(...next.value.data.map((r: string[]) => r[0]));
         }
         next = await result.next();
      }
      return rows;
   } else {
      // TODO(jjs) - implement
      return [];
   }
}

function getTempServiceKeyPath(connection: ApiConnection): string {
   // If the connection is bigquery and the service account key is provided as
   // JSON, we need to write it to a temporary file for the Malloy compiling to work
   if (!connection.bigqueryConnection) {
      throw new Error(
         `BigQuery connection ${connection.name} is missing bigqueryConnection ${JSON.stringify(connection)}`,
      );
   }
   const keyJson = connection.bigqueryConnection.serviceAccountKeyJson;
   if (!keyJson) {
      throw new Error(
         `BigQuery connection ${connection.name} is missing service account key JSON`,
      );
   }
   const tmpFilepath = path.join(
      os.tmpdir(),
      `ms2_connection_${connection.name}_${uuidv4()}_key.json`,
   );
   fs.writeFileSync(tmpFilepath, keyJson, "utf8");
   return tmpFilepath;
}

async function getSnowflakeTables(
   connection: snowflake.Connection,
   connInfo: SnowflakeConnection,
   schemaName: string,
): Promise<string[]> {
   return new Promise((resolve, reject) => {
      connection.execute({
         sqlText: `USE DATABASE ${connInfo?.database} `,
         complete: (err) => {
            if (err) {
               logger.error(`Error setting database ${connInfo.database}:`, {
                  error: err,
               });
               reject([]);
               return;
            }

            const query = `
          SELECT TABLE_NAME, TABLE_SCHEMA
          FROM INFORMATION_SCHEMA.TABLES
          WHERE TABLE_SCHEMA=? AND TABLE_TYPE = 'BASE TABLE';
        `;

            connection.execute({
               sqlText: query,
               binds: [schemaName],
               complete: (err, _, rows) => {
                  if (err) {
                     logger.error(
                        `Error fetching tables from ${connInfo.database}:`,
                        { error: err },
                     );
                     reject([]);
                  } else {
                     resolve(rows?.map((row) => `${row.TABLE_NAME}`) || []);
                  }
               },
            });
         },
      });
   });
}

async function getSnowflakeSchemas(
   connection: snowflake.Connection,
): Promise<ApiSchema[]> {
   return new Promise((resolve, reject) => {
      connection.execute({
         sqlText: "SHOW SCHEMAS",
         complete: (err, _stmt, rows) => {
            if (err) {
               reject(err);
            } else {
               resolve(
                  rows?.map((row) => {
                     logger.info("row", { row });
                     return {
                        name: row.name,
                        isDefault: row.isDefault === "Y",
                        isHidden: ["SNOWFLAKE", ""].includes(row.owner),
                     };
                  }) || [],
               );
            }
         },
      });
   });
}
