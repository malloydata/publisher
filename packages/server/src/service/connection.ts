import { BigQueryConnection } from "@malloydata/db-bigquery";
import { MySQLConnection } from "@malloydata/db-mysql";
import { PostgresConnection } from "@malloydata/db-postgres";
import { SnowflakeConnection } from "@malloydata/db-snowflake";
import { TrinoConnection } from "@malloydata/db-trino";
import { Connection } from "@malloydata/malloy";
import { BaseConnection } from "@malloydata/malloy/connection";
import { AxiosError } from "axios";
import fs from "fs/promises";
import path from "path";
import { v4 as uuidv4 } from "uuid";
import { components } from "../api";
import {
   convertConnectionsToApiConnections,
   getConnectionsFromPublisherConfig,
} from "../config";
import { TEMP_DIR_PATH } from "../constants";
import { BadRequestError } from "../errors";
import { logAxiosError, logger } from "../logger";

type ApiConnection = components["schemas"]["Connection"];
type ApiConnectionAttributes = components["schemas"]["ConnectionAttributes"];
type ApiConnectionStatus = components["schemas"]["ConnectionStatus"];

// Extends the public API connection with the internal connection objects
// which contains passwords and connection strings.
export type InternalConnection = ApiConnection & {
   postgresConnection?: components["schemas"]["PostgresConnection"];
   bigqueryConnection?: components["schemas"]["BigqueryConnection"];
   snowflakeConnection?: components["schemas"]["SnowflakeConnection"];
   trinoConnection?: components["schemas"]["TrinoConnection"];
   mysqlConnection?: components["schemas"]["MysqlConnection"];
};

export async function readConnectionConfig(
   _basePath: string,
   projectName?: string,
   serverRootPath?: string,
): Promise<ApiConnection[]> {
   // If no project name is provided, return empty array for backward compatibility
   if (!projectName || !serverRootPath) {
      return new Array<ApiConnection>();
   }

   // Get connections from publisher config
   const connections = getConnectionsFromPublisherConfig(
      serverRootPath,
      projectName,
   );
   return convertConnectionsToApiConnections(connections);
}

function validateAndBuildTrinoConfig(
   trinoConfig: components["schemas"]["TrinoConnection"],
) {
   if (!trinoConfig.server?.includes(trinoConfig.port?.toString() || "")) {
      trinoConfig.server = `${trinoConfig.server}:${trinoConfig.port}`;
   }

   if (trinoConfig.server?.startsWith("http://")) {
      return {
         server: trinoConfig.server,
         port: trinoConfig.port,
         catalog: trinoConfig.catalog,
         schema: trinoConfig.schema,
         user: trinoConfig.user,
      };
   } else if (
      trinoConfig.server?.startsWith("https://") &&
      trinoConfig.password
   ) {
      return {
         server: trinoConfig.server,
         port: trinoConfig.port,
         catalog: trinoConfig.catalog,
         schema: trinoConfig.schema,
         user: trinoConfig.user,
         password: trinoConfig.password,
      };
   } else {
      throw new Error(
         `Invalid Trino connection: expected "http://server:port" (no password) or "https://server:port" (with username and password).`,
      );
   }
}

export async function createConnections(
   basePath: string,
   defaultConnections: ApiConnection[] = [],
   projectName?: string,
   serverRootPath?: string,
): Promise<{
   malloyConnections: Map<string, BaseConnection>;
   apiConnections: InternalConnection[];
}> {
   const connectionMap = new Map<string, BaseConnection>();
   const connectionConfig = await readConnectionConfig(
      basePath,
      projectName,
      serverRootPath,
   );

   const allConnections = [...defaultConnections, ...connectionConfig];

   const processedConnections = new Set<string>();
   const apiConnections: InternalConnection[] = [];

   for (const connection of allConnections) {
      if (connection.name && processedConnections.has(connection.name)) {
         continue;
      }

      logger.info(`Adding connection ${connection.name}`, {
         connection,
      });

      if (!connection.name) {
         throw "Invalid connection configuration.  No name.";
      }

      processedConnections.add(connection.name);

      switch (connection.type) {
         case "postgres": {
            const configReader = async () => {
               if (!connection.postgresConnection) {
                  throw "Invalid connection configuration.  No postgres connection.";
               }
               return {
                  host: connection.postgresConnection.host,
                  port: connection.postgresConnection.port,
                  username: connection.postgresConnection.userName,
                  password: connection.postgresConnection.password,
                  databaseName: connection.postgresConnection.databaseName,
                  connectionString:
                     connection.postgresConnection.connectionString,
               };
            };
            const postgresConnection = new PostgresConnection(
               connection.name,
               () => ({}),
               configReader,
            );
            connectionMap.set(connection.name, postgresConnection);
            connection.attributes = getConnectionAttributes(postgresConnection);
            break;
         }

         case "mysql": {
            if (!connection.mysqlConnection) {
               throw "Invalid connection configuration.  No mysql connection.";
            }
            const config = {
               host: connection.mysqlConnection.host,
               port: connection.mysqlConnection.port,
               user: connection.mysqlConnection.user,
               password: connection.mysqlConnection.password,
               database: connection.mysqlConnection.database,
            };
            const mysqlConnection = new MySQLConnection(
               connection.name,
               config,
            );
            connectionMap.set(connection.name, mysqlConnection);
            connection.attributes = getConnectionAttributes(mysqlConnection);
            break;
         }

         case "bigquery": {
            if (!connection.bigqueryConnection) {
               throw "Invalid connection configuration.  No bigquery connection.";
            }

            // If a service account key file is provided, we persist it to disk
            // and pass the path to the BigQueryConnection.
            let serviceAccountKeyPath = undefined;
            if (connection.bigqueryConnection.serviceAccountKeyJson) {
               serviceAccountKeyPath = path.join(
                  TEMP_DIR_PATH,
                  `${connection.name}-${uuidv4()}-service-account-key.json`,
               );
               await fs.writeFile(
                  serviceAccountKeyPath,
                  connection.bigqueryConnection.serviceAccountKeyJson as string,
               );
            }

            const bigqueryConnectionOptions = {
               projectId: connection.bigqueryConnection.defaultProjectId,
               serviceAccountKeyPath: serviceAccountKeyPath,
               location: connection.bigqueryConnection.location,
               maximumBytesBilled:
                  connection.bigqueryConnection.maximumBytesBilled,
               timeoutMs:
                  connection.bigqueryConnection.queryTimeoutMilliseconds,
               billingProjectId: connection.bigqueryConnection.billingProjectId,
            };
            const bigqueryConnection = new BigQueryConnection(
               connection.name,
               () => ({}),
               bigqueryConnectionOptions,
            );
            connectionMap.set(connection.name, bigqueryConnection);
            connection.attributes = getConnectionAttributes(bigqueryConnection);
            break;
         }

         case "snowflake": {
            if (!connection.snowflakeConnection) {
               throw new Error(
                  "Snowflake connection configuration is missing.",
               );
            }
            if (!connection.snowflakeConnection.account) {
               throw new Error("Snowflake account is required.");
            }

            if (!connection.snowflakeConnection.username) {
               throw new Error("Snowflake username is required.");
            }

            if (!connection.snowflakeConnection.password) {
               throw new Error("Snowflake password is required.");
            }

            if (!connection.snowflakeConnection.warehouse) {
               throw new Error("Snowflake warehouse is required.");
            }

            const snowflakeConnectionOptions = {
               connOptions: {
                  account: connection.snowflakeConnection.account,
                  username: connection.snowflakeConnection.username,
                  password: connection.snowflakeConnection.password,
                  warehouse: connection.snowflakeConnection.warehouse,
                  database: connection.snowflakeConnection.database,
                  schema: connection.snowflakeConnection.schema,
                  timeout:
                     connection.snowflakeConnection.responseTimeoutMilliseconds,
               },
            };
            const snowflakeConnection = new SnowflakeConnection(
               connection.name,
               snowflakeConnectionOptions,
            );
            connectionMap.set(connection.name, snowflakeConnection);
            connection.attributes =
               getConnectionAttributes(snowflakeConnection);
            break;
         }

         case "trino": {
            if (!connection.trinoConnection) {
               throw new Error("Trino connection configuration is missing.");
            }

            const trinoConnectionOptions = validateAndBuildTrinoConfig(
               connection.trinoConnection,
            );
            const trinoConnection = new TrinoConnection(
               connection.name,
               {},
               trinoConnectionOptions,
            );
            connectionMap.set(connection.name, trinoConnection);
            connection.attributes = getConnectionAttributes(trinoConnection);
            break;
         }

         default: {
            throw new Error(`Unsupported connection type: ${connection.type}`);
         }
      }

      // Add the connection to apiConnections (this will be sanitized when returned)
      apiConnections.push(connection);
   }

   return {
      malloyConnections: connectionMap,
      apiConnections: apiConnections,
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

export async function testConnectionConfig(
   connectionConfig: ApiConnection,
): Promise<ApiConnectionStatus> {
   let testResult: { status: "ok" | "failed"; errorMessage?: string };

   switch (connectionConfig.type) {
      case "postgres": {
         if (!connectionConfig.postgresConnection) {
            throw new Error(
               "Invalid connection configuration. No postgres connection.",
            );
         }

         const postgresConfig = connectionConfig.postgresConnection;
         if (
            !postgresConfig.connectionString &&
            (!postgresConfig.host ||
               !postgresConfig.port ||
               !postgresConfig.userName ||
               !postgresConfig.databaseName)
         ) {
            throw new Error(
               "PostgreSQL connection requires: either all of host, port, userName, and databaseName, or connectionString",
            );
         }

         const configReader = async () => {
            return {
               host: postgresConfig.host,
               port: postgresConfig.port,
               username: postgresConfig.userName,
               password: postgresConfig.password,
               databaseName: postgresConfig.databaseName,
               connectionString: postgresConfig.connectionString,
            };
         };

         const postgresConnection = new PostgresConnection(
            "testConnection",
            () => ({}),
            configReader,
         );

         try {
            await postgresConnection.test();
            testResult = { status: "ok" };
         } catch (error) {
            if (error instanceof AxiosError) {
               logAxiosError(error);
            } else {
               logger.error(error);
            }
            testResult = {
               status: "failed",
               errorMessage: (error as Error).message,
            };
         }
         break;
      }

      case "snowflake": {
         if (!connectionConfig.snowflakeConnection) {
            throw new Error(
               "Invalid connection configuration. No snowflake connection.",
            );
         }

         const snowflakeConfig = connectionConfig.snowflakeConnection;
         if (
            !snowflakeConfig.account ||
            !snowflakeConfig.username ||
            !snowflakeConfig.password ||
            !snowflakeConfig.warehouse
         ) {
            throw new Error(
               "Snowflake connection requires: account, username, password, warehouse, database, and schema",
            );
         }

         const snowflakeConnectionOptions = {
            connOptions: {
               account: snowflakeConfig.account,
               username: snowflakeConfig.username,
               password: snowflakeConfig.password,
               warehouse: snowflakeConfig.warehouse,
               database: snowflakeConfig.database,
               schema: snowflakeConfig.schema,
               timeout: snowflakeConfig.responseTimeoutMilliseconds,
            },
         };
         const snowflakeConnection = new SnowflakeConnection(
            "testConnection",
            snowflakeConnectionOptions,
         );

         try {
            await snowflakeConnection.test();
            testResult = { status: "ok" };
         } catch (error) {
            if (error instanceof AxiosError) {
               logAxiosError(error);
            } else {
               logger.error(error);
            }
            testResult = {
               status: "failed",
               errorMessage: (error as Error).message,
            };
         }
         break;
      }

      case "bigquery": {
         if (!connectionConfig.bigqueryConnection) {
            throw new Error(
               "Invalid connection configuration. No bigquery connection.",
            );
         }

         const bigqueryConfig = connectionConfig.bigqueryConnection;
         let serviceAccountKeyPath = undefined;
         try {
            if (bigqueryConfig.serviceAccountKeyJson) {
               serviceAccountKeyPath = path.join(
                  TEMP_DIR_PATH,
                  `test-${uuidv4()}-service-account-key.json`,
               );
               await fs.writeFile(
                  serviceAccountKeyPath,
                  bigqueryConfig.serviceAccountKeyJson as string,
               );
            }

            const bigqueryConnectionOptions = {
               projectId: connectionConfig.bigqueryConnection.defaultProjectId,
               serviceAccountKeyPath: serviceAccountKeyPath,
               location: connectionConfig.bigqueryConnection.location,
               maximumBytesBilled:
                  connectionConfig.bigqueryConnection.maximumBytesBilled,
               timeoutMs:
                  connectionConfig.bigqueryConnection.queryTimeoutMilliseconds,
               billingProjectId:
                  connectionConfig.bigqueryConnection.billingProjectId,
            };
            const bigqueryConnection = new BigQueryConnection(
               "testConnection",
               () => ({}),
               bigqueryConnectionOptions,
            );

            await bigqueryConnection.test();
            testResult = { status: "ok" };
         } catch (error) {
            if (error instanceof AxiosError) {
               logAxiosError(error);
            } else {
               logger.error(error);
            }
            testResult = {
               status: "failed",
               errorMessage: (error as Error).message,
            };
         } finally {
            try {
               if (serviceAccountKeyPath) {
                  await fs.unlink(serviceAccountKeyPath);
               }
            } catch (cleanupError) {
               logger.warn(
                  `Failed to cleanup temporary file ${serviceAccountKeyPath}:`,
                  cleanupError,
               );
            }
         }
         break;
      }

      case "trino": {
         if (!connectionConfig.trinoConnection) {
            throw new Error("Trino connection configuration is missing.");
         }

         const trinoConfig = connectionConfig.trinoConnection;
         if (
            !trinoConfig.server ||
            !trinoConfig.port ||
            !trinoConfig.catalog ||
            !trinoConfig.schema ||
            !trinoConfig.user
         ) {
            throw new Error(
               "Trino connection requires server, port, catalog, schema, and user",
            );
         }

         const trinoConnectionOptions =
            validateAndBuildTrinoConfig(trinoConfig);
         const trinoConnection = new TrinoConnection(
            "testConnection",
            {},
            trinoConnectionOptions,
         );

         try {
            await trinoConnection.test();
            testResult = { status: "ok" };
         } catch (error) {
            if (error instanceof AxiosError) {
               logAxiosError(error);
            } else {
               logger.error(error);
            }
            testResult = {
               status: "failed",
               errorMessage: (error as Error).message,
            };
         }
         break;
      }

      case "mysql": {
         if (!connectionConfig.mysqlConnection) {
            throw new Error("MySQL connection configuration is missing.");
         }

         if (
            !connectionConfig.mysqlConnection.host ||
            !connectionConfig.mysqlConnection.port ||
            !connectionConfig.mysqlConnection.user ||
            !connectionConfig.mysqlConnection.password ||
            !connectionConfig.mysqlConnection.database
         ) {
            throw new Error(
               "MySQL connection requires: host, port, user, password, and database",
            );
         }

         const mysqlConfig = connectionConfig.mysqlConnection;
         const mysqlConnectionOptions = {
            host: mysqlConfig.host,
            port: mysqlConfig.port,
            user: mysqlConfig.user,
            password: mysqlConfig.password,
            database: mysqlConfig.database,
         };
         const mysqlConnection = new MySQLConnection(
            "testConnection",
            mysqlConnectionOptions,
         );
         try {
            await mysqlConnection.test();
            testResult = { status: "ok" };
         } catch (error) {
            if (error instanceof AxiosError) {
               logAxiosError(error);
            } else {
               logger.error(error);
            }
            testResult = {
               status: "failed",
               errorMessage: (error as Error).message,
            };
         }
         break;
      }

      default:
         throw new BadRequestError(
            `Unsupported connection type: ${connectionConfig.type}`,
         );
   }

   if (testResult.status === "failed") {
      return {
         status: "failed",
         errorMessage: testResult.errorMessage || "Connection test failed",
      };
   }

   return {
      status: "ok",
      errorMessage: "",
   };
}
