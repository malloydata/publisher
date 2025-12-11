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

   public async addConnectionToProject(
      projectName: string,
      connection: ApiConnection,
   ): Promise<void> {
      await this.projectStore.finishedInitialization;

      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      if (!connection.name) {
         throw new Error("Connection name is required");
      }

      const project = await this.projectStore.getProject(projectName, false);

      const existingConnections = project.listApiConnections();
      const connectionExists = existingConnections.some(
         (conn) => conn.name === connection.name,
      );

      if (connectionExists) {
         throw new Error(
            `Connection "${connection.name}" already exists in project "${projectName}". Use updateConnection to modify it.`,
         );
      }

      logger.info(
         `Adding connection "${connection.name}" to project "${projectName}"`,
      );

      const { malloyConnections, apiConnections } =
         await createProjectConnections(
            [...existingConnections, connection],
            project.metadata.location || "",
         );

      project.updateConnections(malloyConnections, apiConnections);

      const repository = this.projectStore.storageManager.getRepository();
      const dbProject = await repository.getProjectByName(projectName);

      if (!dbProject) {
         throw new Error(`Project "${projectName}" not found in database`);
      }

      const existingDbConnections = await repository.listConnections(
         dbProject.id,
      );

      await this.projectStore.addConnection(
         connection,
         dbProject.id,
         existingDbConnections,
         repository,
      );

      logger.info(
         `Successfully added connection "${connection.name}" to project "${projectName}"`,
      );
   }

   public async updateConnectionInProject(
      projectName: string,
      connectionName: string,
      connectionUpdate: Partial<ApiConnection>,
   ): Promise<void> {
      await this.projectStore.finishedInitialization;

      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      const project = await this.projectStore.getProject(projectName, false);

      const existingConnections = project.listApiConnections();
      const connectionIndex = existingConnections.findIndex(
         (conn) => conn.name === connectionName,
      );

      if (connectionIndex === -1) {
         throw new ConnectionNotFoundError(
            `Connection "${connectionName}" not found in project "${projectName}"`,
         );
      }

      logger.info(
         `Updating connection "${connectionName}" in project "${projectName}"`,
      );

      const updatedConnection = {
         ...existingConnections[connectionIndex],
         ...connectionUpdate,
         name: connectionName,
      };

      const updatedConnections = [...existingConnections];
      updatedConnections[connectionIndex] = updatedConnection;

      const { malloyConnections, apiConnections } =
         await createProjectConnections(
            updatedConnections,
            project.metadata.location || "",
         );

      project.updateConnections(malloyConnections, apiConnections);

      const repository = this.projectStore.storageManager.getRepository();
      const dbProject = await repository.getProjectByName(projectName);

      if (!dbProject) {
         throw new Error(`Project "${projectName}" not found in database`);
      }

      const existingDbConnections = await repository.listConnections(
         dbProject.id,
      );

      await this.projectStore.addConnection(
         updatedConnection,
         dbProject.id,
         existingDbConnections,
         repository,
      );

      logger.info(
         `Successfully updated connection "${connectionName}" in project "${projectName}"`,
      );
   }

   public async deleteConnectionFromProject(
      projectName: string,
      connectionName: string,
   ): Promise<void> {
      await this.projectStore.finishedInitialization;

      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }

      const project = await this.projectStore.getProject(projectName, false);

      const existingConnections = project.listApiConnections();
      const connectionExists = existingConnections.some(
         (conn) => conn.name === connectionName,
      );

      if (!connectionExists) {
         throw new ConnectionNotFoundError(
            `Connection "${connectionName}" not found in project "${projectName}"`,
         );
      }

      logger.info(
         `Deleting connection "${connectionName}" from project "${projectName}"`,
      );

      const updatedConnections = existingConnections.filter(
         (conn) => conn.name !== connectionName,
      );

      const { malloyConnections, apiConnections } =
         await createProjectConnections(
            updatedConnections,
            project.metadata.location || "",
         );

      project.updateConnections(malloyConnections, apiConnections);

      const repository = this.projectStore.storageManager.getRepository();
      const dbProject = await repository.getProjectByName(projectName);

      if (!dbProject) {
         throw new Error(`Project "${projectName}" not found in database`);
      }

      const existingDbConn = await repository.getConnectionByName(
         dbProject.id,
         connectionName,
      );

      if (!existingDbConn) {
         throw new ConnectionNotFoundError(
            `Connection "${connectionName}" not found in database`,
         );
      }

      await repository.deleteConnection(existingDbConn.id);

      logger.info(
         `Successfully deleted connection "${connectionName}" from project "${projectName}"`,
      );
   }
}
