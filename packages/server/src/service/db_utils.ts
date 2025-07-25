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
   MysqlConnection,
   PostgresConnection,
   SnowflakeConnection,
} from "./model";

type ApiSchemaName = components["schemas"]["SchemaName"];

async function getPostgresConnection(
   apiPostgresConnection: PostgresConnection,
): Promise<Pool> {
   return new Pool({
      user: apiPostgresConnection.userName,
      host: apiPostgresConnection.host,
      database: apiPostgresConnection.databaseName,
      password: apiPostgresConnection.password,
      port: apiPostgresConnection.port,
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
         account: apiSnowflakeConnection.account,
         username: apiSnowflakeConnection.username,
         password: apiSnowflakeConnection.password,
         database: apiSnowflakeConnection.database,
         warehouse: apiSnowflakeConnection.warehouse || undefined,
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

export async function getSchemasForConnection(
   connection: ApiConnection,
): Promise<ApiSchemaName[]> {
   if (connection.type === "bigquery") {
      if (!connection.bigqueryConnection) {
         throw new Error("BigQuery connection is required");
      }
      try {
         const bigquery = getBigqueryConnection(connection);
         const projectId = connection.bigqueryConnection.defaultProjectId;
         const options = projectId ? { projectId } : {};
         const [datasets] = await bigquery.getDatasets(options);
         return datasets
            .filter((dataset) => dataset.id)
            .map((dataset) => {
               return {
                  name: dataset.id,
                  isHidden: false,
                  isDefault: false,
               };
            });
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
   } else {
      throw new Error(`Unsupported connection type: ${connection.type}`);
   }
}

export async function getTablesForSchema(
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
): Promise<ApiSchemaName[]> {
   return new Promise((resolve, reject) => {
      connection.execute({
         sqlText: "SHOW SCHEMAS",
         complete: (err, stmt, rows) => {
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
