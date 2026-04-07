import { BigQueryConnection } from "@malloydata/db-bigquery";
import { DuckDBConnection } from "@malloydata/db-duckdb";
import { MySQLConnection } from "@malloydata/db-mysql";
import { PostgresConnection } from "@malloydata/db-postgres";
import { SnowflakeConnection } from "@malloydata/db-snowflake";
import { TrinoConnection } from "@malloydata/db-trino";
import { Connection, TableSourceDef } from "@malloydata/malloy";
import { BaseConnection } from "@malloydata/malloy/connection";
import { AxiosError } from "axios";
import fs from "fs/promises";
import path from "path";
import { v4 as uuidv4 } from "uuid";
import { components } from "../api";
import { TEMP_DIR_PATH } from "../constants";
import { logAxiosError, logger } from "../logger";
import { CloudStorageCredentials } from "./gcs_s3_utils";

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
   ducklakeConnection?: components["schemas"]["DucklakeConnection"];
};

function validateAndBuildTrinoConfig(
   trinoConfig: components["schemas"]["TrinoConnection"],
) {
   if (!trinoConfig.server?.includes(trinoConfig.port?.toString() || "")) {
      trinoConfig.server = `${trinoConfig.server}:${trinoConfig.port}`;
   }

   // Build base config
   const baseConfig: {
      server: string;
      port?: number;
      catalog?: string;
      schema?: string;
      user?: string;
      password?: string;
      extraConfig?: Record<string, unknown>;
   } = {
      server: trinoConfig.server,
      port: trinoConfig.port,
      catalog: trinoConfig.catalog,
      schema: trinoConfig.schema,
      user: trinoConfig.user,
   };

   if (trinoConfig.peakaKey) {
      baseConfig.extraConfig = {
         extraCredential: {
            peakaKey: trinoConfig.peakaKey,
         },
      };
      delete baseConfig.password;
      delete baseConfig.catalog;
      delete baseConfig.schema;
   } else if (
      trinoConfig.server?.startsWith("https://") &&
      trinoConfig.password
   ) {
      // Only add password if no peakaKey and HTTPS connection
      baseConfig.password = trinoConfig.password;
   }

   if (trinoConfig.server?.startsWith("http://")) {
      delete baseConfig.password;
      return baseConfig;
   } else if (trinoConfig.server?.startsWith("https://")) {
      return baseConfig;
   } else {
      throw new Error(
         `Invalid Trino connection: expected "http://server:port" or "https://server:port".`,
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

function normalizePrivateKey(privateKey: string): string {
   let privateKeyContent = privateKey.trim();

   if (!privateKeyContent.includes("\n")) {
      // Try encrypted key first, then unencrypted
      const keyPatterns = [
         {
            beginRegex: /-----BEGIN\s+ENCRYPTED\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+ENCRYPTED\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN ENCRYPTED PRIVATE KEY-----",
            endMarker: "-----END ENCRYPTED PRIVATE KEY-----",
         },
         {
            beginRegex: /-----BEGIN\s+PRIVATE\s+KEY-----/i,
            endRegex: /-----END\s+PRIVATE\s+KEY-----/i,
            beginMarker: "-----BEGIN PRIVATE KEY-----",
            endMarker: "-----END PRIVATE KEY-----",
         },
      ];

      for (const pattern of keyPatterns) {
         const beginMatch = privateKeyContent.match(pattern.beginRegex);
         const endMatch = privateKeyContent.match(pattern.endRegex);

         if (beginMatch && endMatch) {
            const beginPos = beginMatch.index! + beginMatch[0].length;
            const endPos = endMatch.index!;
            const keyData = privateKeyContent
               .substring(beginPos, endPos)
               .replace(/\s+/g, "");

            const lines: string[] = [];
            for (let i = 0; i < keyData.length; i += 64) {
               lines.push(keyData.slice(i, i + 64));
            }
            privateKeyContent = `${pattern.beginMarker}\n${lines.join("\n")}\n${pattern.endMarker}\n`;
            break;
         }
      }
   } else {
      if (!privateKeyContent.endsWith("\n")) {
         privateKeyContent += "\n";
      }
   }

   return privateKeyContent;
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

async function attachDuckLake(
   connection: DuckDBConnection,
   dbName: string,
   ducklakeConfig: components["schemas"]["DucklakeConnection"],
): Promise<void> {
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
   logger.info(`pgConnString: ${pgConnString}`);
   const escapedPgConnString = escapeSQL(pgConnString);
   logger.info(`Final escaped connection string: ${escapedPgConnString}`);
   const escapedBucketUrl = escapeSQL(ducklakeConfig.storage.bucketUrl);
   logger.info(`escapedBucketUrl: ${escapedBucketUrl}`);
   const attachCommand = `ATTACH OR REPLACE 'ducklake:postgres:${escapedPgConnString}' AS ${dbName} (DATA_PATH '${escapedBucketUrl}', OVERRIDE_DATA_PATH true, READ_ONLY true);`;
   logger.info(`Attaching DuckLake database using command: ${attachCommand}`);
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
      connectionName: string,
      databasePath: string,
      workingDirectory: string,
      azureDatabases: AttachedDatabase[],
   ) {
      super(connectionName, databasePath, workingDirectory);
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

   constructor(
      connectionName: string,
      databasePath: string,
      workingDirectory: string,
   ) {
      super(connectionName, databasePath, workingDirectory);

      // Validate that this is a DuckLake connection by checking the database path pattern
      if (!databasePath.endsWith("_ducklake.duckdb")) {
         throw new Error(
            `DuckLakeConnection should only be used for DuckLake connections. ` +
               `Expected database path ending with '_ducklake.duckdb', got: ${databasePath}`,
         );
      }

      this.connectionName = connectionName;
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
   projectPath: string,
): Promise<void> {
   const ducklakePath = path.join(
      projectPath,
      `${connectionName}_ducklake.duckdb`,
   );
   try {
      await fs.access(ducklakePath);
      await fs.rm(ducklakePath);
      logger.info(
         `Removed DuckLake connection file ${connectionName}_ducklake.duckdb from ${projectPath}`,
      );
   } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
         logger.debug(
            `DuckLake connection file ${connectionName}_ducklake.duckdb does not exist, skipping deletion`,
         );
      } else {
         logger.error(
            `Failed to remove DuckLake connection file ${connectionName}_ducklake.duckdb from ${projectPath}`,
            { error },
         );
      }
   }
}

export async function createProjectConnections(
   connections: ApiConnection[] = [],
   projectPath: string = "",
   isUpdateConnectionRequest: boolean = false,
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

            if (
               !connection.snowflakeConnection.password &&
               !connection.snowflakeConnection.privateKey
            ) {
               throw new Error(
                  "Snowflake password or private key or private key path is required.",
               );
            }

            if (!connection.snowflakeConnection.warehouse) {
               throw new Error("Snowflake warehouse is required.");
            }

            let privateKeyPath = undefined;

            if (connection.snowflakeConnection.privateKey) {
               privateKeyPath = path.join(
                  TEMP_DIR_PATH,
                  `${connection.name}-${uuidv4()}-private-key.pem`,
               );
               const normalizedKey = normalizePrivateKey(
                  connection.snowflakeConnection.privateKey as string,
               );
               await fs.writeFile(privateKeyPath, normalizedKey);
            }

            const snowflakeConnectionOptions = {
               connOptions: {
                  account: connection.snowflakeConnection.account,
                  username: connection.snowflakeConnection.username,
                  warehouse: connection.snowflakeConnection.warehouse,
                  database: connection.snowflakeConnection.database,
                  schema: connection.snowflakeConnection.schema,
                  role: connection.snowflakeConnection.role,
                  ...(connection.snowflakeConnection.privateKey
                     ? {
                          privateKeyPath: privateKeyPath,
                          authenticator: "SNOWFLAKE_JWT",
                          privateKeyPass:
                             connection.snowflakeConnection.privateKeyPass ||
                             undefined,
                       }
                     : {
                          password:
                             connection.snowflakeConnection.password ||
                             undefined,
                       }),
                  timeout:
                     connection.snowflakeConnection.responseTimeoutMilliseconds,
               },
               // min must stay low: generic-pool eagerly opens `min` Snowflake sessions at startup.
               // min: 12 = twelve concurrent logins + session setup (ALTER SESSION x4 each) → stalls and flakiness.
               poolOptions: {
                  min: 1,
                  max: process.env.SNOWFLAKE_MAX_CONNECTION_POOL
                     ? parseInt(process.env.SNOWFLAKE_MAX_CONNECTION_POOL)
                     : 10,
                  testOnBorrow: false,
                  testOnReturn: false,
                  testWhileIdle: true,
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

            if (
               connection.duckdbConnection.attachedDatabases?.some(
                  (database) => database.name === connection.name,
               )
            ) {
               throw new Error(
                  `DuckDB attached databases names cannot conflict with connection name ${connection.name}`,
               );
            }

            if (connection.name === "duckdb") {
               throw new Error("DuckDB connection name cannot be 'duckdb'");
            }

            if (connection.duckdbConnection?.attachedDatabases?.length == 0) {
               throw new Error(
                  "DuckDB connection must have at least one attached database",
               );
            }

            // Create DuckDB connection with project basePath as working directory
            // This ensures relative paths in the project are resolved correctly
            // Use unique memory database path to prevent sharing across connections
            const attachedDatabases =
               connection.duckdbConnection.attachedDatabases ?? [];
            const hasAzureAttached = attachedDatabases.some(
               (db) => db.type === "azure",
            );
            const duckdbConnection = hasAzureAttached
               ? new AzureDuckDBConnection(
                    connection.name,
                    path.join(projectPath, `${connection.name}.duckdb`),
                    projectPath,
                    attachedDatabases,
                 )
               : new DuckDBConnection(
                    connection.name,
                    path.join(projectPath, `${connection.name}.duckdb`),
                    projectPath,
                 );

            // Attach databases if configured
            if (attachedDatabases.length > 0) {
               await attachDatabasesToDuckDB(
                  duckdbConnection,
                  attachedDatabases,
               );
            }

            connectionMap.set(connection.name, duckdbConnection);
            connection.attributes = getConnectionAttributes(duckdbConnection);
            break;
         }

         case "motherduck": {
            if (!connection.motherduckConnection) {
               throw new Error(
                  "MotherDuck connection configuration is missing.",
               );
            }

            if (!connection.motherduckConnection.accessToken) {
               throw new Error("MotherDuck access token is required.");
            }

            let databasePath = `md:`;
            // Build the MotherDuck database path
            if (connection.motherduckConnection.database) {
               databasePath = `md:${connection.motherduckConnection.database}?attach_mode=single`;
            }

            // Create MotherDuck connection using DuckDBConnectionOptions interface
            const motherduckConnection = new DuckDBConnection({
               name: connection.name,
               databasePath: databasePath,
               motherDuckToken: connection.motherduckConnection.accessToken,
               workingDirectory: projectPath,
            });

            connectionMap.set(connection.name, motherduckConnection);
            connection.attributes =
               getConnectionAttributes(motherduckConnection);
            break;
         }

         case "ducklake": {
            if (!connection.ducklakeConnection) {
               throw new Error("DuckLake connection configuration is missing.");
            }

            // Creating one Connection per DuckLake connection to avoid conflicts with other connections and better isolation.
            const ducklakeDuckdbConnection = new DuckLakeConnection(
               connection.name,
               path.join(projectPath, `${connection.name}_ducklake.duckdb`),
               projectPath,
            );

            // Only attach DuckLake if it's not already attached or is it an update connection request
            if (
               isUpdateConnectionRequest ||
               !(await isDatabaseAttached(
                  ducklakeDuckdbConnection,
                  connection.name,
               ))
            ) {
               await attachDuckLake(
                  ducklakeDuckdbConnection,
                  connection.name,
                  connection.ducklakeConnection,
               );
            }

            connectionMap.set(connection.name, ducklakeDuckdbConnection);
            connection.attributes = getConnectionAttributes(
               ducklakeDuckdbConnection,
            );
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
   let malloyConnections: Map<string, BaseConnection> | null = null;
   try {
      // Validate that connection name is provided
      if (!connectionConfig.name) {
         throw new Error("Connection name is required");
      }

      // Use createProjectConnections to create the connection, then test it
      const result = await createProjectConnections(
         [connectionConfig], // Pass the single connection config
      );
      malloyConnections = result.malloyConnections;

      // Get the created connection
      const connection = malloyConnections.get(connectionConfig.name);
      if (!connection) {
         throw new Error(
            `Failed to create connection: ${connectionConfig.name}`,
         );
      }

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
      // Cleanup: close all connections and remove ducklake files created during testing
      if (malloyConnections) {
         for (const [connName, conn] of malloyConnections) {
            try {
               // Close the connection
               if (
                  conn &&
                  typeof (conn as DuckDBConnection).close === "function"
               ) {
                  await (conn as DuckDBConnection).close();
               }
            } catch (closeError) {
               logger.warn(
                  `Error closing connection ${connName} during test cleanup`,
                  { error: closeError },
               );
            } finally {
               // Remove ducklake files created during testing (only for ducklake connections)
               if (connectionConfig.type === "ducklake") {
                  await deleteDuckLakeConnectionFile(connName, process.cwd());
               }
            }
         }
      }
   }
}
