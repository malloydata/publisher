import { Connection, RunSQLOptions } from "@malloydata/malloy";
import { PersistSQLResults } from "@malloydata/malloy/connection";
import { components } from "../api";
import { BadRequestError, ConnectionError } from "../errors";
import { logger } from "../logger";
import { testConnectionConfig } from "../service/connection";
import { ConnectionService } from "../service/connection_service";
import {
   getSchemasForConnection,
   listTablesForSchema,
} from "../service/db_utils";
import { ProjectStore } from "../service/project_store";

const SCHEMA_CACHE_TTL_MS = process.env.SCHEMA_CACHE_TTL_MS
   ? parseInt(process.env.SCHEMA_CACHE_TTL_MS)
   : 0;

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

export class ConnectionController {
   private projectStore: ProjectStore;
   private connectionService: ConnectionService;
   constructor(projectStore: ProjectStore) {
      this.projectStore = projectStore;
      this.connectionService = new ConnectionService(projectStore);
   }

   /**
    * Gets the appropriate Malloy connection for a given connection name.
    * For DuckDB connections, retrieves from package level; for others, from project level.
    */
   private async getMalloyConnection(
      projectName: string,
      connectionName: string,
   ): Promise<Connection> {
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getApiConnection(connectionName);

      // For DuckDB connections, get the connection from a package
      if (connection.name === "duckdb" && connection.type === "duckdb") {
         const packages = await project.listPackages();
         if (packages.length === 0) {
            return project.getMalloyConnection(connectionName);
         }
         // For now, use the first package's DuckDB connection
         const packageName = packages[0].name;
         if (!packageName) {
            throw new ConnectionError("Package name is undefined");
         }
         const pkg = await project.getPackage(packageName);
         return pkg.getMalloyConnection(connectionName);
      } else {
         return project.getMalloyConnection(connectionName);
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
         const refreshTimestamp = Date.now() - SCHEMA_CACHE_TTL_MS;
         const { schemas, errors } =
            await malloyConnection.fetchSchemaForTables(
               { [tableKey]: tablePath },
               { refreshTimestamp },
            );

         if (errors[tableKey]) {
            throw new ConnectionError(errors[tableKey]);
         }

         const source = schemas[tableKey];
         if (!source) {
            throw new ConnectionError(`Table ${tablePath} not found`);
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
         if (error instanceof ConnectionError) {
            throw error;
         }
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
      projectName: string,
      connectionName: string,
   ): Promise<ApiConnection> {
      if (!projectName || !connectionName) {
         throw new BadRequestError("Connection payload is required");
      }
      const { dbConnection } = await this.connectionService.getConnection(
         projectName,
         connectionName,
      );
      return dbConnection;
   }

   public async listConnections(projectName: string): Promise<ApiConnection[]> {
      const project = await this.projectStore.getProject(projectName, false);
      return project.listApiConnections();
   }

   // Lists schemas (namespaces) available in a connection
   public async listSchemas(
      projectName: string,
      connectionName: string,
   ): Promise<ApiSchema[]> {
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getApiConnection(connectionName);
      const malloyConnection = await this.getMalloyConnection(
         projectName,
         connectionName,
      );

      return getSchemasForConnection(connection, malloyConnection);
   }

   // Lists tables available in a schema. For postgres the schema is usually "public"
   public async listTables(
      projectName: string,
      connectionName: string,
      schemaName: string,
      tableNames?: string[],
   ): Promise<ApiTable[]> {
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getApiConnection(connectionName);
      const malloyConnection = await this.getMalloyConnection(
         projectName,
         connectionName,
      );

      return listTablesForSchema(
         connection,
         schemaName,
         malloyConnection,
         tableNames,
      );
   }

   public async getConnectionSqlSource(
      projectName: string,
      connectionName: string,
      sqlStatement: string,
   ): Promise<ApiSqlSource> {
      const malloyConnection = await this.getMalloyConnection(
         projectName,
         connectionName,
      );

      try {
         const refreshTimestamp = Date.now() - SCHEMA_CACHE_TTL_MS;
         const result = await malloyConnection.fetchSchemaForSQLStruct(
            { connection: connectionName, selectStr: sqlStatement },
            { refreshTimestamp },
         );

         if ("error" in result && result.error) {
            throw new ConnectionError(result.error);
         }

         return {
            source: JSON.stringify(result.structDef),
         };
      } catch (error) {
         if (error instanceof ConnectionError) {
            throw error;
         }
         throw new ConnectionError((error as Error).message);
      }
   }

   public async getTable(
      projectName: string,
      connectionName: string,
      schemaName: string,
      tablePath: string,
   ): Promise<ApiTable> {
      const malloyConnection = await this.getMalloyConnection(
         projectName,
         connectionName,
      );
      // Use getApiConnection to get the unwrapped ApiConnection config, consistent with listSchemas and listTables.
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getApiConnection(connectionName);

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
      projectName: string,
      connectionName: string,
      sqlStatement: string,
      options: string,
   ): Promise<ApiQueryData> {
      const malloyConnection = await this.getMalloyConnection(
         projectName,
         connectionName,
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
      projectName: string,
      connectionName: string,
      sqlStatement: string,
   ): Promise<ApiTemporaryTable> {
      const malloyConnection = await this.getMalloyConnection(
         projectName,
         connectionName,
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
      connectionConfig: ApiConnection,
   ): Promise<ApiConnectionStatus> {
      if (
         connectionConfig &&
         "config" in connectionConfig &&
         typeof (connectionConfig as Record<string, unknown>).config ===
            "object"
      ) {
         connectionConfig = (connectionConfig as Record<string, unknown>)
            .config as ApiConnection;
      }

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
      projectName: string,
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

      logger.info(
         `Creating connection "${connectionName}" in project "${projectName}"`,
      );

      await this.connectionService.addConnection(
         projectName,
         connectionName,
         connectionConfig,
      );

      return {
         message: `Connection "${connectionName}" created successfully`,
      };
   }

   public async updateConnection(
      projectName: string,
      connectionName: string,
      connection: Partial<ApiConnection>,
   ): Promise<{ message: string }> {
      if (!connection || typeof connection !== "object") {
         throw new BadRequestError("Connection payload is required");
      }

      validateAzureAttachedDatabases(connection as ApiConnection);

      logger.info(
         `Updating connection "${connectionName}" in project "${projectName}"`,
      );

      await this.connectionService.updateConnection(
         projectName,
         connectionName,
         connection,
      );

      return {
         message: `Connection "${connectionName}" updated successfully`,
      };
   }

   public async deleteConnection(
      projectName: string,
      connectionName: string,
   ): Promise<{ message: string }> {
      logger.info(
         `Deleting connection "${connectionName}" from project "${projectName}"`,
      );

      await this.connectionService.deleteConnection(
         projectName,
         connectionName,
      );

      return {
         message: `Connection "${connectionName}" deleted successfully`,
      };
   }
}
