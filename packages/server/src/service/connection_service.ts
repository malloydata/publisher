import { components } from "../api";
import { ConnectionNotFoundError, FrozenConfigError } from "../errors";
import { logger } from "../logger";
import { createEnvironmentConnections } from "./connection";
import { EnvironmentStore } from "./environment_store";

type ApiConnection = components["schemas"]["Connection"];

export class ConnectionService {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
   }

   public async getConnection(environmentName: string, connectionName: string) {
      await this.environmentStore.finishedInitialization;

      const repository = this.environmentStore.storageManager.getRepository();
      const dbEnvironment = await repository.getEnvironmentByName(environmentName);

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
      const dbEnvironment = await repository.getEnvironmentByName(environmentName);

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
      const existingConnections = environment.listApiConnections();

      const { malloyConnections, apiConnections } =
         await createEnvironmentConnections(
            [...existingConnections, connection],
            environment.metadata.location || "",
         );

      environment.updateConnections(malloyConnections, apiConnections);

      await this.environmentStore.addConnection(
         connection,
         dbEnvironment.id,
         repository,
      );

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

      const { dbEnvironment, dbConnection, repository } = await this.getConnection(
         environmentName,
         connectionName,
      );

      // Update in-memory connections
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const existingConnections = environment.listApiConnections();

      const updatedConnection = {
         ...dbConnection.config,
         ...connection,
         name: connectionName,
      };

      const updatedConnections = existingConnections.map((conn) =>
         conn.name === connectionName ? updatedConnection : conn,
      );

      const { malloyConnections, apiConnections } =
         await createEnvironmentConnections(
            updatedConnections,
            environment.metadata.location || "",
         );

      environment.updateConnections(malloyConnections, apiConnections);

      await this.environmentStore.updateConnection(
         updatedConnection,
         dbEnvironment.id,
         repository,
      );

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
      await environment.deleteConnection(connectionName);

      // Delete from database
      await repository.deleteConnection(dbConnection.id);

      logger.info(
         `Successfully deleted connection "${connectionName}" from environment "${environmentName}"`,
      );
   }
}
