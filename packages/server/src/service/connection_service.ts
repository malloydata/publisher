import { components } from "../api";
import { ConnectionNotFoundError, FrozenConfigError } from "../errors";
import { logger } from "../logger";
import { buildEnvironmentMalloyConfig } from "./connection";

type ApiConnection = components["schemas"]["Connection"];
type ReleaseCallback = () => Promise<void>;
type ConnectionUpdateEnvironment = {
   runConnectionUpdateExclusive?: <T>(fn: () => Promise<T>) => Promise<T>;
   updateConnections?: (
      nextMalloyConfig: ReturnType<typeof buildEnvironmentMalloyConfig>,
      apiConnections?: ApiConnection[],
      afterPreviousRelease?: ReleaseCallback,
   ) => void;
   deleteConnection?: (connectionName: string) => Promise<void>;
   deleteDuckDBConnection?: (connectionName: string) => Promise<void>;
   deleteDuckLakeConnection?: (connectionName: string) => Promise<void>;
};

async function runEnvironmentConnectionUpdate<T>(
   environment: ConnectionUpdateEnvironment,
   fn: () => Promise<T>,
): Promise<T> {
   if (environment.runConnectionUpdateExclusive) {
      return environment.runConnectionUpdateExclusive(fn);
   }
   return fn();
}

function updateEnvironmentConnections(
   environment: ConnectionUpdateEnvironment,
   nextMalloyConfig: ReturnType<typeof buildEnvironmentMalloyConfig>,
   afterPreviousRelease?: ReleaseCallback,
): void {
   environment.updateConnections?.(
      nextMalloyConfig,
      nextMalloyConfig.apiConnections,
      afterPreviousRelease,
   );
}

function buildDeletedConnectionCleanup(
   environment: ConnectionUpdateEnvironment,
   deletedConnection: ApiConnection,
   connectionName: string,
): ReleaseCallback | undefined {
   if (
      deletedConnection.type === "duckdb" &&
      typeof environment.deleteDuckDBConnection === "function"
   ) {
      return () => environment.deleteDuckDBConnection!(connectionName);
   }

   if (
      deletedConnection.type === "ducklake" &&
      typeof environment.deleteDuckLakeConnection === "function"
   ) {
      return () => environment.deleteDuckLakeConnection!(connectionName);
   }

   return undefined;
}

export class ConnectionService {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async getConnection(environmentName: string, connectionName: string) {
      await this.environmentStore.finishedInitialization;

      const repository = this.environmentStore.storageManager.getRepository();
      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);

      if (!dbEnvironment) {
         throw new Error(
            `Environment "${environmentName}" not found in database`,
         );
      }

      const dbConnection = await repository.getConnectionByName(
         dbEnvironment.id,
         connectionName,
      );

      if (!dbConnection) {
         throw new ConnectionNotFoundError(
            `Connection "${connectionName}" not found in environment "${environmentName}"`,
         );
      }

      return { dbEnvironment, dbConnection, repository };
   }

   public async addConnection(
      environmentName: string,
      connectionName: string,
      connection: ApiConnection,
   ): Promise<void> {
      await this.environmentStore.finishedInitialization;

      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      logger.info(
         `Adding connection "${connectionName}" to environment "${environmentName}"`,
      );

      // Get database environment record and repository
      const repository = this.environmentStore.storageManager.getRepository();
      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);

      if (!dbEnvironment) {
         throw new Error(
            `Environment "${environmentName}" not found in database`,
         );
      }

      // Check if connection already exists in database
      const existingDbConn = await repository.getConnectionByName(
         dbEnvironment.id,
         connectionName!,
      );

      if (existingDbConn) {
         throw new Error(
            `Connection "${connectionName}" already exists in environment "${environmentName}".`,
         );
      }

      // Update in-memory connections
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      await runEnvironmentConnectionUpdate(environment, async () => {
         const existingConnections = environment.listApiConnections();
         const nextMalloyConfig = buildEnvironmentMalloyConfig(
            [...existingConnections, connection],
            environment.metadata.location || "",
         );

         await this.environmentStore.addConnection(
            connection,
            dbEnvironment.id,
            repository,
         );

         updateEnvironmentConnections(environment, nextMalloyConfig);
      });

      logger.info(
         `Successfully added connection "${connection.name}" to environment "${environmentName}"`,
      );
   }

   public async updateConnection(
      environmentName: string,
      connectionName: string,
      connection: Partial<ApiConnection>,
   ): Promise<void> {
      await this.environmentStore.finishedInitialization;

      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      logger.info(
         `Updating connection "${connectionName}" in environment "${environmentName}"`,
      );

      const { dbEnvironment, dbConnection, repository } =
         await this.getConnection(environmentName, connectionName);

      // Update in-memory connections
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      await runEnvironmentConnectionUpdate(environment, async () => {
         const existingConnections = environment.listApiConnections();

         const updatedConnection = {
            ...dbConnection.config,
            ...connection,
            name: connectionName,
         };

         const updatedConnections = existingConnections.map((conn) =>
            conn.name === connectionName ? updatedConnection : conn,
         );

         // Pass isUpdateConnectionRequest=true so the DuckLake wrapper
         // re-attaches against the updated catalog/storage settings instead
         // of trusting the prior generation's persisted attach state.
         const nextMalloyConfig = buildEnvironmentMalloyConfig(
            updatedConnections,
            environment.metadata.location || "",
            true,
         );

         await this.environmentStore.updateConnection(
            updatedConnection,
            dbEnvironment.id,
            repository,
         );

         updateEnvironmentConnections(environment, nextMalloyConfig);
      });

      logger.info(
         `Successfully updated connection "${connectionName}" in environment "${environmentName}"`,
      );
   }

   public async deleteConnection(
      environmentName: string,
      connectionName: string,
   ): Promise<void> {
      await this.environmentStore.finishedInitialization;

      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      logger.info(
         `Deleting connection "${connectionName}" from environment "${environmentName}"`,
      );

      const { dbConnection, repository } = await this.getConnection(
         environmentName,
         connectionName,
      );

      // Update in-memory connections
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      await runEnvironmentConnectionUpdate(environment, async () => {
         if (typeof environment.listApiConnections !== "function") {
            if (typeof environment.deleteConnection === "function") {
               await environment.deleteConnection(connectionName);
            }
            await repository.deleteConnection(dbConnection.id);
            return;
         }

         const deletedConnection =
            "getApiConnection" in environment &&
            typeof environment.getApiConnection === "function"
               ? environment.getApiConnection(connectionName)
               : dbConnection.config;
         const updatedConnections = environment
            .listApiConnections()
            .filter((connection) => connection.name !== connectionName);
         const nextMalloyConfig = buildEnvironmentMalloyConfig(
            updatedConnections,
            environment.metadata.location || "",
         );
         const deleteConnectionFilesAfterRelease =
            buildDeletedConnectionCleanup(
               environment,
               deletedConnection,
               connectionName,
            );

         await repository.deleteConnection(dbConnection.id);

         updateEnvironmentConnections(
            environment,
            nextMalloyConfig,
            deleteConnectionFilesAfterRelease,
         );
      });

      logger.info(
         `Successfully deleted connection "${connectionName}" from environment "${environmentName}"`,
      );
   }
}
