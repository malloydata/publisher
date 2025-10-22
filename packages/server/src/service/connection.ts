import { BigQueryConnection } from "@malloydata/db-bigquery";
import { DuckDBConnection } from "@malloydata/db-duckdb";
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
import { TEMP_DIR_PATH } from "../constants";
import { logAxiosError, logger } from "../logger";

type AttachedDatabase = components["schemas"]["AttachedDatabase"];
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
   duckdbConnection?: components["schemas"]["DuckdbConnection"];
};

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

async function attachDatabasesToDuckDB(
   duckdbConnection: DuckDBConnection,
   attachedDatabases: AttachedDatabase[],
): Promise<void> {
   for (const attachedDb of attachedDatabases) {
      try {
         // Check if database is already attached
         try {
            const checkQuery = `SHOW DATABASES`;
            const existingDatabases = await duckdbConnection.runSQL(checkQuery);
            const rows = Array.isArray(existingDatabases)
               ? existingDatabases
               : existingDatabases.rows || [];

            logger.debug(`Existing databases:`, rows);

            // Check if the database name exists in any column (handle different column names)
            const isAlreadyAttached = rows.some(
               (row: Record<string, unknown>) => {
                  return Object.values(row).some(
                     (value: unknown) =>
                        typeof value === "string" && value === attachedDb.name,
                  );
               },
            );

            if (isAlreadyAttached) {
               logger.info(
                  `Database ${attachedDb.name} is already attached, skipping`,
               );
               continue;
            }
         } catch (error) {
            logger.warn(
               `Failed to check existing databases, proceeding with attachment:`,
               error,
            );
         }

         switch (attachedDb.type) {
            case "bigquery": {
               if (!attachedDb.bigqueryConnection) {
                  throw new Error(
                     `BigQuery connection configuration is missing for attached database: ${attachedDb.name}`,
                  );
               }

               // Install and load the bigquery extension
               await duckdbConnection.runSQL(
                  "INSTALL bigquery FROM community;",
               );
               await duckdbConnection.runSQL("LOAD bigquery;");

               // Build the ATTACH command for BigQuery
               const bigqueryConfig = attachedDb.bigqueryConnection;
               const attachParams = new URLSearchParams();

               if (!bigqueryConfig.defaultProjectId) {
                  throw new Error(
                     `BigQuery defaultProjectId is required for attached database: ${attachedDb.name}`,
                  );
               }
               attachParams.set("project", bigqueryConfig.defaultProjectId);

               // Handle service account key if provided
               if (bigqueryConfig.serviceAccountKeyJson) {
                  const serviceAccountKeyPath = path.join(
                     TEMP_DIR_PATH,
                     `duckdb-${attachedDb.name}-${uuidv4()}-service-account-key.json`,
                  );
                  await fs.writeFile(
                     serviceAccountKeyPath,
                     bigqueryConfig.serviceAccountKeyJson as string,
                  );
                  attachParams.set(
                     "service_account_key",
                     serviceAccountKeyPath,
                  );
               }

               const attachCommand = `ATTACH '${attachParams.toString()}' AS ${attachedDb.name} (TYPE bigquery, READ_ONLY);`;
               try {
                  await duckdbConnection.runSQL(attachCommand);
                  logger.info(
                     `Successfully attached BigQuery database: ${attachedDb.name}`,
                  );
               } catch (attachError: unknown) {
                  if (
                     attachError instanceof Error &&
                     attachError.message &&
                     attachError.message.includes("already exists")
                  ) {
                     logger.info(
                        `BigQuery database ${attachedDb.name} is already attached, skipping`,
                     );
                  } else {
                     throw attachError;
                  }
               }
               break;
            }

            case "snowflake": {
               if (!attachedDb.snowflakeConnection) {
                  throw new Error(
                     `Snowflake connection configuration is missing for attached database: ${attachedDb.name}`,
                  );
               }

               // Install and load the snowflake extension
               await duckdbConnection.runSQL(
                  "INSTALL snowflake FROM community;",
               );
               await duckdbConnection.runSQL("LOAD snowflake;");

               // Build the ATTACH command for Snowflake
               const snowflakeConfig = attachedDb.snowflakeConnection;
               const attachParams = new URLSearchParams();

               if (snowflakeConfig.account) {
                  attachParams.set("account", snowflakeConfig.account);
               }
               if (snowflakeConfig.username) {
                  attachParams.set("username", snowflakeConfig.username);
               }
               if (snowflakeConfig.password) {
                  attachParams.set("password", snowflakeConfig.password);
               }
               if (snowflakeConfig.database) {
                  attachParams.set("database", snowflakeConfig.database);
               }
               if (snowflakeConfig.warehouse) {
                  attachParams.set("warehouse", snowflakeConfig.warehouse);
               }
               if (snowflakeConfig.role) {
                  attachParams.set("role", snowflakeConfig.role);
               }

               const attachCommand = `ATTACH '${attachParams.toString()}' AS ${attachedDb.name} (TYPE snowflake, READ_ONLY);`;
               try {
                  await duckdbConnection.runSQL(attachCommand);
                  logger.info(
                     `Successfully attached Snowflake database: ${attachedDb.name}`,
                  );
               } catch (attachError: unknown) {
                  if (
                     attachError instanceof Error &&
                     attachError.message &&
                     attachError.message.includes("already exists")
                  ) {
                     logger.info(
                        `Snowflake database ${attachedDb.name} is already attached, skipping`,
                     );
                  } else {
                     throw attachError;
                  }
               }
               break;
            }

            case "postgres": {
               if (!attachedDb.postgresConnection) {
                  throw new Error(
                     `PostgreSQL connection configuration is missing for attached database: ${attachedDb.name}`,
                  );
               }

               // Install and load the postgres extension
               await duckdbConnection.runSQL(
                  "INSTALL postgres FROM community;",
               );
               await duckdbConnection.runSQL("LOAD postgres;");

               // Build the ATTACH command for PostgreSQL
               const postgresConfig = attachedDb.postgresConnection;
               let attachString: string;

               // Use connection string if provided, otherwise build from individual parameters
               if (postgresConfig.connectionString) {
                  attachString = postgresConfig.connectionString;
               } else {
                  // Build connection string from individual parameters
                  const params = new URLSearchParams();

                  if (postgresConfig.host) {
                     params.set("host", postgresConfig.host);
                  }
                  if (postgresConfig.port) {
                     params.set("port", postgresConfig.port.toString());
                  }
                  if (postgresConfig.databaseName) {
                     params.set("dbname", postgresConfig.databaseName);
                  }
                  if (postgresConfig.userName) {
                     params.set("user", postgresConfig.userName);
                  }
                  if (postgresConfig.password) {
                     params.set("password", postgresConfig.password);
                  }

                  attachString = params.toString();
               }

               const attachCommand = `ATTACH '${attachString}' AS ${attachedDb.name} (TYPE postgres, READ_ONLY);`;
               try {
                  await duckdbConnection.runSQL(attachCommand);
                  logger.info(
                     `Successfully attached PostgreSQL database: ${attachedDb.name}`,
                  );
               } catch (attachError: unknown) {
                  if (
                     attachError instanceof Error &&
                     attachError.message &&
                     attachError.message.includes("already exists")
                  ) {
                     logger.info(
                        `PostgreSQL database ${attachedDb.name} is already attached, skipping`,
                     );
                  } else {
                     throw attachError;
                  }
               }
               break;
            }

            default:
               throw new Error(
                  `Unsupported attached database type: ${attachedDb.type}`,
               );
         }
      } catch (error) {
         logger.error(`Failed to attach database ${attachedDb.name}:`, error);
         throw new Error(
            `Failed to attach database ${attachedDb.name}: ${(error as Error).message}`,
         );
      }
   }
}

export async function createProjectConnections(
   connections: ApiConnection[] = [],
): Promise<{
   malloyConnections: Map<string, BaseConnection>;
   apiConnections: InternalConnection[];
}> {
   const connectionMap = new Map<string, BaseConnection>();
   const processedConnections = new Set<string>();
   const apiConnections: InternalConnection[] = [];

   for (const connection of connections) {
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
                  role: connection.snowflakeConnection.role,
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

         case "duckdb": {
            // DuckDB connections are created at the package level in package.ts
            // to ensure the workingDirectory is set correctly for each connection
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

/**
 * DuckDB connections need to be instantiated at the package level to ensure
 * the workingDirectory is set correctly. This allows DuckDB to properly resolve
 * relative paths for database files and attached databases within the project context.
 */
export async function createPackageDuckDBConnections(
   connections: ApiConnection[] = [],
   packagePath: string,
): Promise<{
   malloyConnections: Map<string, BaseConnection>;
   apiConnections: InternalConnection[];
}> {
   const connectionMap = new Map<string, BaseConnection>();

   const processedConnections = new Set<string>();
   const apiConnections: InternalConnection[] = [];

   for (const connection of connections) {
      // Only process DuckDB connections
      if (connection.type !== "duckdb") {
         continue;
      }

      if (connection.name && processedConnections.has(connection.name)) {
         throw new Error(
            `CreatePackageDuckDBConnections only supports one DuckDB connection per name, got ${connection.name}`,
         );
      }

      if (!connection.name) {
         throw "Invalid connection configuration.  No name.";
      }

      logger.info(`Adding DuckDB connection ${connection.name}`, {
         connection,
      });

      processedConnections.add(connection.name);

      if (!connection.duckdbConnection) {
         throw new Error("DuckDB connection configuration is missing.");
      }

      // Create DuckDB connection with project basePath as working directory
      // This ensures relative paths in the project are resolved correctly
      const duckdbConnection = new DuckDBConnection(
         connection.name,
         ":memory:",
         packagePath,
      );

      // Attach databases if configured
      if (
         connection.duckdbConnection.attachedDatabases &&
         Array.isArray(connection.duckdbConnection.attachedDatabases) &&
         connection.duckdbConnection.attachedDatabases.length > 0
      ) {
         await attachDatabasesToDuckDB(
            duckdbConnection,
            connection.duckdbConnection.attachedDatabases,
         );
      }

      connectionMap.set(connection.name, duckdbConnection);
      connection.attributes = getConnectionAttributes(duckdbConnection);

      // Add the connection to apiConnections (this will be sanitized when returned)
      apiConnections.push(connection);
   }

   // Create default "duckdb" connection if it doesn't exist
   if (!connectionMap.has("duckdb")) {
      const defaultDuckDBConnection = new DuckDBConnection(
         "duckdb",
         ":memory:",
         packagePath,
      );
      connectionMap.set("duckdb", defaultDuckDBConnection);

      // Create API connection for the default DuckDB connection
      const defaultApiConnection: ApiConnection = {
         name: "duckdb",
         type: "duckdb",
         duckdbConnection: {
            attachedDatabases: [],
         },
      };
      defaultApiConnection.attributes = getConnectionAttributes(
         defaultDuckDBConnection,
      );
      apiConnections.push(defaultApiConnection);
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
   try {
      // Validate that connection name is provided
      if (!connectionConfig.name) {
         throw new Error("Connection name is required");
      }

      // Use createProjectConnections to create the connection, then test it
      // TODO: Test duckdb connections?
      const { malloyConnections } = await createProjectConnections(
         [connectionConfig], // Pass the single connection config
      );

      // Get the created connection
      const connection = malloyConnections.get(connectionConfig.name);
      if (!connection) {
         throw new Error(
            `Failed to create connection: ${connectionConfig.name}`,
         );
      }

      // Test the connection - cast to union type of connection classes that have test method
      await (
         connection as
            | PostgresConnection
            | BigQueryConnection
            | SnowflakeConnection
            | TrinoConnection
            | MySQLConnection
            | DuckDBConnection
      ).test();

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
   }
}
