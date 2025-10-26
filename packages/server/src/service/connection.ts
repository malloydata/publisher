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

// Shared utilities
async function installAndLoadExtension(
   connection: DuckDBConnection,
   extensionName: string,
   fromCommunity = false,
): Promise<void> {
   try {
      const installCommand = fromCommunity
         ? `FORCE INSTALL '${extensionName}' FROM community;`
         : `INSTALL ${extensionName};`;
      await connection.runSQL(installCommand);
      logger.info(`${extensionName} extension installed`);
   } catch (error) {
      logger.info(
         `${extensionName} extension already installed or install skipped`,
         { error },
      );
   }

   await connection.runSQL(`LOAD ${extensionName};`);
   logger.info(`${extensionName} extension loaded`);
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

      logger.debug(`Existing databases:`, rows);

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
      `SELECT * FROM snowflake_scan('SELECT 1', '${attachedDb.name}_secret');`,
   );
   logger.info(`Testing Snowflake connection:`, testresult.rows);

   const attachCommand = `ATTACH '${attachedDb.name}' AS ${attachedDb.name} (TYPE snowflake, SECRET ${attachedDb.name}_secret, READ_ONLY);`;
   await connection.runSQL(attachCommand);
   logger.info(`Successfully attached Snowflake database: ${attachedDb.name}`);
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
   let attachString: string;

   if (config.connectionString) {
      attachString = config.connectionString;
   } else {
      const parts: string[] = [];
      if (config.host) parts.push(`host=${config.host}`);
      if (config.port) parts.push(`port=${config.port}`);
      if (config.databaseName) parts.push(`dbname=${config.databaseName}`);
      if (config.userName) parts.push(`user=${config.userName}`);
      if (config.password) parts.push(`password=${config.password}`);
      if (process.env.PGSSLMODE === "no-verify") parts.push(`sslmode=disable`);
      attachString = parts.join(" ");
   }

   const attachCommand = `ATTACH '${attachString}' AS ${attachedDb.name} (TYPE postgres, READ_ONLY);`;
   await connection.runSQL(attachCommand);
   logger.info(`Successfully attached PostgreSQL database: ${attachedDb.name}`);
}

async function attachMotherDuck(
   connection: DuckDBConnection,
   attachedDb: AttachedDatabase,
): Promise<void> {
   if (!attachedDb.motherDuckConnection) {
      throw new Error(
         `MotherDuck connection configuration missing for: ${attachedDb.name}`,
      );
   }

   const config = attachedDb.motherDuckConnection;

   if (!config.database) {
      throw new Error(
         `MotherDuck database name is required for: ${attachedDb.name}`,
      );
   }

   await installAndLoadExtension(connection, "motherduck");

   // Set token if provided
   if (config.accessToken) {
      const escapedToken = escapeSQL(config.accessToken);
      await connection.runSQL(`SET motherduck_token = '${escapedToken}';`);
   }

   const connectionString = `md:${config.database}`;
   logger.info(
      `Connecting to MotherDuck database: ${config.database} as ${attachedDb.name}`,
   );

   const attachCommand = `ATTACH '${connectionString}' AS ${attachedDb.name} (TYPE motherduck, READ_ONLY);`;
   await connection.runSQL(attachCommand);
   logger.info(`Successfully attached MotherDuck database: ${attachedDb.name}`);
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
      motherduck: attachMotherDuck,
   };

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

export async function createProjectConnections(
   connections: ApiConnection[] = [],
   projectPath: string = "",
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
            if (!connection.duckdbConnection) {
               throw new Error("DuckDB connection configuration is missing.");
            }

            // Create DuckDB connection with project basePath as working directory
            // This ensures relative paths in the project are resolved correctly
            const duckdbConnection = new DuckDBConnection(
               connection.name,
               ":memory:",
               projectPath,
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
      if (connectionConfig.type === "duckdb") {
         return {
            status: "ok",
            errorMessage: "",
         };
      }

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
