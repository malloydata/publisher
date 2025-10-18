import { RunSQLOptions } from "@malloydata/malloy";
import { Connection, PersistSQLResults } from "@malloydata/malloy/connection";
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
      return getSchemasForConnection(connection);
   }

   // Lists tables available in a schema. For postgres the schema is usually "public"
   public async listTables(
      projectName: string,
      connectionName: string,
      schemaName: string,
   ): Promise<ApiTable[]> {
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getApiConnection(connectionName);
      return getTablesForSchema(
         connection,
         schemaName,
         this.projectStore,
         projectName,
         connectionName,
      );
   }

   public async getConnectionSqlSource(
      projectName: string,
      connectionName: string,
      sqlStatement: string,
   ): Promise<ApiSqlSource> {
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getMalloyConnection(connectionName);
      try {
         return {
            source: JSON.stringify(
               await connection.fetchSelectSchema({
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
      return getConnectionTableSource(
         this.projectStore,
         projectName,
         connectionName,
         tableKey,
         tablePath,
      );
   }

   public async getConnectionQueryData(
      projectName: string,
      connectionName: string,
      sqlStatement: string,
      options: string,
   ): Promise<ApiQueryData> {
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getMalloyConnection(connectionName);
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
               await connection.runSQL(sqlStatement, runSQLOptions),
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
      const project = await this.projectStore.getProject(projectName, false);
      const connection = project.getMalloyConnection(
         connectionName,
      ) as Connection;

      try {
         return {
            table: JSON.stringify(
               await (connection as PersistSQLResults).manifestTemporaryTable(
                  sqlStatement,
               ),
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
