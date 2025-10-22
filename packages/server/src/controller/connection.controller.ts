import { Connection, RunSQLOptions } from "@malloydata/malloy";
import { PersistSQLResults } from "@malloydata/malloy/connection";
import { components } from "../api";
import { BadRequestError, ConnectionError } from "../errors";
import { logger } from "../logger";
import { testConnectionConfig } from "../service/connection";
import {
   getConnectionTableSource,
   getSchemasForConnection,
   getTablesForSchema,
} from "../service/db_utils";
import { ProjectStore } from "../service/project_store";
type ApiConnection = components["schemas"]["Connection"];
type ApiConnectionStatus = components["schemas"]["ConnectionStatus"];
type ApiSqlSource = components["schemas"]["SqlSource"];
type ApiTableSource = components["schemas"]["TableSource"];
type ApiTable = components["schemas"]["Table"];
type ApiQueryData = components["schemas"]["QueryData"];
type ApiTemporaryTable = components["schemas"]["TemporaryTable"];
type ApiSchema = components["schemas"]["Schema"];
export class ConnectionController {
   private projectStore: ProjectStore;

   constructor(projectStore: ProjectStore) {
      this.projectStore = projectStore;
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
      if (connection.type === "duckdb") {
         const packages = await project.listPackages();
         if (packages.length === 0) {
            throw new ConnectionError(
               "No packages found for DuckDB connection",
            );
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

   public async getConnection(
      projectName: string,
      connectionName: string,
   ): Promise<ApiConnection> {
      const project = await this.projectStore.getProject(projectName, false);
      return project.getApiConnection(connectionName);
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
   ): Promise<ApiTable[]> {
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getApiConnection(connectionName);
      const malloyConnection = await this.getMalloyConnection(
         projectName,
         connectionName,
      );

      return getTablesForSchema(connection, schemaName, malloyConnection);
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
         return {
            source: JSON.stringify(
               await (
                  malloyConnection as Connection & {
                     fetchSelectSchema: (params: {
                        connection: string;
                        selectStr: string;
                     }) => Promise<unknown>;
                  }
               ).fetchSelectSchema({
                  connection: connectionName,
                  selectStr: sqlStatement,
               }),
            ),
         };
      } catch (error) {
         throw new ConnectionError((error as Error).message);
      }
   }

   public async getConnectionTableSource(
      projectName: string,
      connectionName: string,
      tableKey: string,
      tablePath: string,
   ): Promise<ApiTableSource> {
      const malloyConnection = await this.getMalloyConnection(
         projectName,
         connectionName,
      );

      return getConnectionTableSource(malloyConnection, tableKey, tablePath);
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
}
