import { components } from "../api";
import { ConnectionNotFoundError, FrozenConfigError } from "../errors";
import { logger } from "../logger";
import { createProjectConnections } from "./connection";
import { ProjectStore } from "./project_store";

type ApiConnection = components["schemas"]["Connection"];

export class ConnectionService {
   private projectStore: ProjectStore;

   constructor(projectStore: ProjectStore) {
      this.projectStore = projectStore;
   }

   public async getConnection(projectName: string, connectionName: string) {
      await this.projectStore.finishedInitialization;

      const repository = this.projectStore.storageManager.getRepository();
      const dbProject = await repository.getProjectByName(projectName);

      if (!dbProject) {
         throw new Error(`Project "${projectName}" not found in database`);
      }

      const dbConnection = await repository.getConnectionByName(
         dbProject.id,
         connectionName,
      );

      if (!dbConnection) {
         throw new ConnectionNotFoundError(
            `Connection "${connectionName}" not found in project "${projectName}"`,
         );
      }

      return { dbProject, dbConnection, repository };
   }

   public async addConnection(
      projectName: string,
      connectionName: string,
      connection: ApiConnection,
   ): Promise<void> {
      await this.projectStore.finishedInitialization;

      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      logger.info(
         `Adding connection "${connectionName}" to project "${projectName}"`,
      );

      // Get database project and repository
      const repository = this.projectStore.storageManager.getRepository();
      const dbProject = await repository.getProjectByName(projectName);

      if (!dbProject) {
         throw new Error(`Project "${projectName}" not found in database`);
      }

      // Check if connection already exists in database
      const existingDbConn = await repository.getConnectionByName(
         dbProject.id,
         connectionName!,
      );

      if (existingDbConn) {
         throw new Error(
            `Connection "${connectionName}" already exists in project "${projectName}".`,
         );
      }

      // Update in-memory connections
      const project = await this.projectStore.getProject(projectName, false);
      const existingConnections = project.listApiConnections();

      const { malloyConnections, apiConnections } =
         await createProjectConnections(
            [...existingConnections, connection],
            project.metadata.location || "",
         );

      project.updateConnections(malloyConnections, apiConnections);

      await this.projectStore.addConnection(
         connection,
         dbProject.id,
         repository,
      );

      logger.info(
         `Successfully added connection "${connection.name}" to project "${projectName}"`,
      );
   }

   public async updateConnection(
      projectName: string,
      connectionName: string,
      connection: Partial<ApiConnection>,
   ): Promise<void> {
      await this.projectStore.finishedInitialization;

      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      logger.info(
         `Updating connection "${connectionName}" in project "${projectName}"`,
      );

      const { dbProject, dbConnection, repository } = await this.getConnection(
         projectName,
         connectionName,
      );

      // Update in-memory connections
      const project = await this.projectStore.getProject(projectName, false);
      const existingConnections = project.listApiConnections();

      const updatedConnection = {
         ...dbConnection.config,
         ...connection,
         name: connectionName,
      };

      const updatedConnections = existingConnections.map((conn) =>
         conn.name === connectionName ? updatedConnection : conn,
      );

      const { malloyConnections, apiConnections } =
         await createProjectConnections(
            updatedConnections,
            project.metadata.location || "",
         );

      project.updateConnections(malloyConnections, apiConnections);

      await this.projectStore.updateConnection(
         updatedConnection,
         dbProject.id,
         repository,
      );

      logger.info(
         `Successfully updated connection "${connectionName}" in project "${projectName}"`,
      );
   }

   public async deleteConnection(
      projectName: string,
      connectionName: string,
   ): Promise<void> {
      await this.projectStore.finishedInitialization;

      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      logger.info(
         `Deleting connection "${connectionName}" from project "${projectName}"`,
      );

      const { dbConnection, repository } = await this.getConnection(
         projectName,
         connectionName,
      );

      // Update in-memory connections
      const project = await this.projectStore.getProject(projectName, false);
      project.deleteConnection(connectionName);

      // Delete from database
      await repository.deleteConnection(dbConnection.id);

      logger.info(
         `Successfully deleted connection "${connectionName}" from project "${projectName}"`,
      );
   }
}
