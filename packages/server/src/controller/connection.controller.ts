import { Connection, RunSQLOptions, TableSourceDef } from "@malloydata/malloy";
import { PersistSQLResults } from "@malloydata/malloy/connection";
import { components } from "../api";
import { BadRequestError, ConnectionError } from "../errors";
import { logger } from "../logger";
import { testConnectionConfig } from "../service/connection";
import { validateDuckdbApiSurface } from "../service/connection_config";
import { ConnectionService } from "../service/connection_service";
import {
   getSchemasForConnection,
   listTablesForSchema,
} from "../service/db_utils";
import type { Environment } from "../service/environment";
import { EnvironmentStore } from "../service/environment_store";

type ApiConnection = components["schemas"]["Connection"];
type ApiConnectionStatus = components["schemas"]["ConnectionStatus"];
type ApiSqlSource = components["schemas"]["SqlSource"];
type ApiTable = components["schemas"]["Table"];
type ApiQueryData = components["schemas"]["QueryData"];
type ApiTemporaryTable = components["schemas"]["TemporaryTable"];
type ApiSchema = components["schemas"]["Schema"];
const AZURE_SUPPORTED_SCHEMES = ["https://", "http://", "abfss://", "az://"];
const AZURE_DATA_EXTENSIONS = [
   ".parquet",
   ".csv",
   ".json",
   ".jsonl",
   ".ndjson",
];

/**
 * Validates an Azure URL against the three supported patterns:
 *   1. Single file:         path/file.parquet
 *   2. Directory glob:      path/*.ext   (direct children only, no sub-dirs)
 *   3. Recursive:           path/**      (all data files recursively)
 */
function validateAzureUrl(url: string, fieldName: string): void {
   if (!AZURE_SUPPORTED_SCHEMES.some((s) => url.startsWith(s))) {
      throw new BadRequestError(
         `Azure ${fieldName} must use one of: ${AZURE_SUPPORTED_SCHEMES.join(", ")}`,
      );
   }

   const pathWithoutQuery = url.split("?")[0];
   const stars = (pathWithoutQuery.match(/\*/g) || []).length;

   if (stars === 0) {
      // Single file — must end with a data extension
      const lower = pathWithoutQuery.toLowerCase();
      if (!AZURE_DATA_EXTENSIONS.some((ext) => lower.endsWith(ext))) {
         throw new BadRequestError(
            `Azure ${fieldName}: a single-file URL must end with a data file extension (${AZURE_DATA_EXTENSIONS.join(", ")})`,
         );
      }
   } else if (pathWithoutQuery.endsWith("**")) {
      // Recursive — valid, no further checks needed
   } else {
      // Must be exactly path/*.ext — one star, in the last path segment only
      const lastSegment = pathWithoutQuery.split("/").pop() || "";
      if (stars !== 1 || !lastSegment.startsWith("*")) {
         throw new BadRequestError(
            `Azure ${fieldName}: only three URL patterns are supported:\n` +
               `  • Single file:      path/file.parquet\n` +
               `  • Directory glob:   path/*.ext  (direct children only)\n` +
               `  • Recursive:        path/**     (all data files in subtree)\n` +
               `Multi-level globs such as "sub_dir/*/*.parquet" are not supported.`,
         );
      }
   }
}

function validateAzureAttachedDatabases(connectionConfig: ApiConnection): void {
   if (connectionConfig.type !== "duckdb") return;
   const attachedDbs =
      connectionConfig.duckdbConnection?.attachedDatabases || [];
   for (const db of attachedDbs) {
      if (db.type !== "azure" || !db.azureConnection) continue;
      const { authType, sasUrl, fileUrl } = db.azureConnection;
      if (authType === "sas_token" && sasUrl) {
         validateAzureUrl(sasUrl, `"${db.name}" sasUrl`);
      } else if (authType === "service_principal" && fileUrl) {
         validateAzureUrl(fileUrl, `"${db.name}" fileUrl`);
      }
   }
}

function validateAdminAuthoredConnection(
   connectionName: string,
   connectionConfig: ApiConnection,
): void {
   if (connectionName === "duckdb" || connectionConfig.name === "duckdb") {
      throw new BadRequestError(
         "DuckDB connection name cannot be 'duckdb'; it is reserved for Publisher package sandboxes.",
      );
   }

   try {
      validateDuckdbApiSurface(connectionConfig);
   } catch (error) {
      throw new BadRequestError((error as Error).message);
   }
}

export class ConnectionController {
   private environmentStore: EnvironmentStore;
   private connectionService: ConnectionService;
   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
      this.connectionService = new ConnectionService(environmentStore);
   }

   /**
    * Gets the appropriate Malloy connection for a given connection name.
    * For DuckDB connections, retrieves from package level; for others, from environment level.
    */
   private getApiConnectionForLookup(
      environment: Environment,
      connectionName: string,
   ): ApiConnection {
      if (connectionName === "duckdb") {
         return {
            name: "duckdb",
            type: "duckdb",
            duckdbConnection: { attachedDatabases: [] },
         };
      }
      return environment.getApiConnection(connectionName);
   }

   private async getMalloyConnection(
      environmentName: string,
      connectionName: string,
      packageName?: string,
   ): Promise<Connection> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );

      // "duckdb" is the per-package sandbox; its rootDirectory is the
      // package's directory. There is no environment-level "duckdb" — the name is
      // reserved at config time. So the lookup is intrinsically per-package
      // and the caller must say which package to use.
      if (connectionName === "duckdb") {
         const packages = await environment.listPackages();
         if (packages.length === 0) {
            // Fall through to environment; this will surface the standard
            // "connection not found" rather than silently inventing one.
            return await environment.getMalloyConnection(connectionName);
         }
         if (packageName) {
            const known = packages.some((p) => p.name === packageName);
            if (!known) {
               throw new BadRequestError(
                  `Package "${packageName}" not found in environment "${environmentName}"`,
               );
            }
            const pkg = await environment.getPackage(packageName);
            return await pkg.getMalloyConnection(connectionName);
         }
         if (packages.length === 1) {
            const onlyPackage = packages[0].name;
            if (!onlyPackage) {
               throw new ConnectionError("Package name is undefined");
            }
            const pkg = await environment.getPackage(onlyPackage);
            return await pkg.getMalloyConnection(connectionName);
         }
         throw new BadRequestError(
            `Ambiguous "duckdb" connection lookup: environment "${environmentName}" has multiple packages. ` +
               `Use /environments/${environmentName}/packages/{packageName}/connections/duckdb/... to disambiguate.`,
         );
      } else {
         return await environment.getMalloyConnection(connectionName);
      }
   }

   /**
    * Fetches a table's schema via the Malloy connection's fetchTableSchema,
    * returning an ApiTable with columns and the raw source JSON.
    */
   private async fetchTable(
      malloyConnection: Connection,
      tableKey: string,
      tablePath: string,
   ): Promise<ApiTable> {
      try {
         const source = await (
            malloyConnection as Connection & {
               fetchTableSchema: (
                  tableKey: string,
                  tablePath: string,
               ) => Promise<TableSourceDef | string | undefined>;
            }
         ).fetchTableSchema(tableKey, tablePath);
         if (!source) {
            throw new ConnectionError(`Table ${tablePath} not found`);
         }
         // BigQueryConnection returns `error.message` as a string on failure instead of throwing.
         if (typeof source === "string") {
            throw new ConnectionError(source);
         }

         return {
            source: JSON.stringify(source),
            resource: tablePath,
            columns: (source.fields || []).map((f) => ({
               name: f.name,
               type: f.type,
            })),
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

   public async getConnection(
      environmentName: string,
      connectionName: string,
   ): Promise<ApiConnection> {
      if (!environmentName || !connectionName) {
         throw new BadRequestError("Connection payload is required");
      }
      // Prefer the in-memory API connection (which was materialized by the
      // environment on load and carries `attributes`). The DB row stores the
      // raw config and FK columns, which aren't the ApiConnection shape.
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      return environment.getApiConnection(connectionName);
   }

   public async listConnections(
      environmentName: string,
   ): Promise<ApiConnection[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      return environment.listApiConnections();
   }

   // Lists schemas (namespaces) available in a connection.
   // For "duckdb", the per-package sandbox, packageName disambiguates which
   // package's DuckDB to browse in a multi-package environment.
   public async listSchemas(
      environmentName: string,
      connectionName: string,
      packageName?: string,
   ): Promise<ApiSchema[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const connection = this.getApiConnectionForLookup(
         environment,
         connectionName,
      );
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );

      return getSchemasForConnection(connection, malloyConnection);
   }

   // Lists tables available in a schema. For postgres the schema is usually "public".
   // packageName disambiguates per-package "duckdb" lookups (see listSchemas).
   public async listTables(
      environmentName: string,
      connectionName: string,
      schemaName: string,
      tableNames?: string[],
      packageName?: string,
   ): Promise<ApiTable[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const connection = this.getApiConnectionForLookup(
         environment,
         connectionName,
      );
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );

      return listTablesForSchema(
         connection,
         schemaName,
         malloyConnection,
         tableNames,
      );
   }

   public async getConnectionSqlSource(
      environmentName: string,
      connectionName: string,
      sqlStatement: string,
      packageName?: string,
   ): Promise<ApiSqlSource> {
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );
      try {
         const schema = await (
            malloyConnection as Connection & {
               fetchSelectSchema: (params: {
                  connection: string;
                  selectStr: string;
               }) => Promise<unknown>;
            }
         ).fetchSelectSchema({
            connection: connectionName,
            selectStr: sqlStatement,
         });

         // BigQueryConnection returns `error.message` as a string on failure instead of throwing.
         if (typeof schema === "string") {
            throw new ConnectionError(schema);
         }

         return {
            source: JSON.stringify(schema),
         };
      } catch (error) {
         throw new ConnectionError((error as Error).message);
      }
   }

   public async getTable(
      environmentName: string,
      connectionName: string,
      schemaName: string,
      tablePath: string,
      packageName?: string,
   ): Promise<ApiTable> {
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );
      // Use getApiConnection to get the unwrapped ApiConnection config, consistent with listSchemas and listTables.
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const connection = this.getApiConnectionForLookup(
         environment,
         connectionName,
      );

      // TODO: Move this database connection logic to the db_utils.ts file -- and
      // ultimately into a connection-specific class.
      if (connection.type === "ducklake") {
         if (tablePath.split(".").length === 1) {
            // tablePath is just the table name, construct full path
            tablePath = `${connectionName}.${schemaName}.${tablePath}`;
         } else if (
            tablePath.split(".").length === 2 &&
            !tablePath.startsWith(connectionName)
         ) {
            // tablePath is schemaName.tableName but missing connection prefix
            tablePath = `${connectionName}.${tablePath}`;
         }
         // If tablePath already has 3+ parts or starts with connection name, use as-is
      }

      // Check if this is an Azure attached database
      if (connection.type === "duckdb") {
         const attachedDbs =
            connection.duckdbConnection?.attachedDatabases || [];
         const azureDb = attachedDbs.find(
            (db) =>
               db.type === "azure" &&
               db.name === schemaName &&
               db.azureConnection,
         );
         if (azureDb && azureDb.azureConnection) {
            // Reconstruct the full SAS URL for the specific file
            const azureConn = azureDb.azureConnection;
            const baseUrl =
               azureConn.authType === "sas_token"
                  ? azureConn.sasUrl
                  : azureConn.fileUrl;
            if (baseUrl) {
               // Extract the file name from tablePath (e.g., "a.aircraft.parquet" -> "aircraft.parquet")
               const fileName = tablePath.includes(".")
                  ? tablePath.split(".").slice(1).join(".")
                  : tablePath;
               // Replace the file portion in the base URL with the specific file name
               const urlParts = baseUrl.split("?");
               const basePath = urlParts[0];
               const queryString = urlParts[1] ? `?${urlParts[1]}` : "";
               // Replace the last path segment (or glob) with the actual file name
               const dirPath = basePath.substring(
                  0,
                  basePath.lastIndexOf("/") + 1,
               );
               const fullFileUrl = `${dirPath}${fileName}${queryString}`;

               const table = await this.fetchTable(
                  malloyConnection,
                  fileName,
                  fullFileUrl,
               );
               return { ...table, resource: tablePath };
            }
         }
      }

      const tableKey = tablePath.split(".").pop();
      if (!tableKey) {
         throw new Error(`Invalid tablePath: ${tablePath}`);
      }

      return this.fetchTable(malloyConnection, tableKey, tablePath);
   }

   public async getConnectionQueryData(
      environmentName: string,
      connectionName: string,
      sqlStatement: string,
      options: string,
      packageName?: string,
   ): Promise<ApiQueryData> {
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );

      let runSQLOptions: RunSQLOptions = {};
      if (options) {
         runSQLOptions = JSON.parse(options) as RunSQLOptions;
      }
      if (runSQLOptions.abortSignal) {
         // Add support for abortSignal in the future
         logger.info("Clearing unsupported abortSignal");
         runSQLOptions.abortSignal = undefined;
      }

      try {
         return {
            data: JSON.stringify(
               await malloyConnection.runSQL(sqlStatement, runSQLOptions),
            ),
         };
      } catch (error) {
         throw new ConnectionError((error as Error).message);
      }
   }

   public async getConnectionTemporaryTable(
      environmentName: string,
      connectionName: string,
      sqlStatement: string,
      packageName?: string,
   ): Promise<ApiTemporaryTable> {
      const malloyConnection = await this.getMalloyConnection(
         environmentName,
         connectionName,
         packageName,
      );

      try {
         return {
            table: JSON.stringify(
               await (
                  malloyConnection as PersistSQLResults
               ).manifestTemporaryTable(sqlStatement),
            ),
         };
      } catch (error) {
         throw new ConnectionError((error as Error).message);
      }
   }

   public async testConnectionConfiguration(
      input: ApiConnection & { config?: ApiConnection },
   ): Promise<ApiConnectionStatus> {
      // Some clients wrap the payload as { config: <connection> }; unwrap.
      const connectionConfig: ApiConnection =
         input.config && typeof input.config === "object"
            ? input.config
            : input;

      if (
         !connectionConfig ||
         typeof connectionConfig !== "object" ||
         Object.keys(connectionConfig).length === 0
      ) {
         throw new BadRequestError(
            "Connection configuration is required and cannot be empty",
         );
      }

      if (!connectionConfig.type || typeof connectionConfig.type !== "string") {
         throw new BadRequestError(
            "Connection type is required and must be a string",
         );
      }

      try {
         return await testConnectionConfig(connectionConfig);
      } catch (error) {
         return {
            status: "failed",
            errorMessage: `Connection test failed: ${(error as Error).message}`,
         };
      }
   }

   public async addConnection(
      environmentName: string,
      connectionName: string,
      connectionConfig: ApiConnection,
   ): Promise<{ message: string }> {
      if (!connectionConfig || typeof connectionConfig !== "object") {
         throw new BadRequestError("Connection configuration is required");
      }

      if (!connectionConfig) {
         throw new BadRequestError("Connection name is required");
      }

      if (!connectionConfig.type) {
         throw new BadRequestError("Connection type is required");
      }

      validateAzureAttachedDatabases(connectionConfig);
      validateAdminAuthoredConnection(connectionName, connectionConfig);

      logger.info(
         `Creating connection "${connectionName}" in environment "${environmentName}"`,
      );

      await this.connectionService.addConnection(
         environmentName,
         connectionName,
         connectionConfig,
      );

      return {
         message: `Connection "${connectionName}" created successfully`,
      };
   }

   public async updateConnection(
      environmentName: string,
      connectionName: string,
      connection: Partial<ApiConnection>,
   ): Promise<{ message: string }> {
      if (!connection || typeof connection !== "object") {
         throw new BadRequestError("Connection payload is required");
      }

      validateAzureAttachedDatabases(connection);
      validateAdminAuthoredConnection(connectionName, connection);

      logger.info(
         `Updating connection "${connectionName}" in environment "${environmentName}"`,
      );

      await this.connectionService.updateConnection(
         environmentName,
         connectionName,
         connection,
      );

      return {
         message: `Connection "${connectionName}" updated successfully`,
      };
   }

   public async deleteConnection(
      environmentName: string,
      connectionName: string,
   ): Promise<{ message: string }> {
      logger.info(
         `Deleting connection "${connectionName}" from environment "${environmentName}"`,
      );

      await this.connectionService.deleteConnection(
         environmentName,
         connectionName,
      );

      return {
         message: `Connection "${connectionName}" deleted successfully`,
      };
   }
}
