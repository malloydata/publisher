import { components } from "../api";
import { ConnectionNotFoundError, FrozenConfigError } from "../errors";
import { logger } from "../logger";
import { buildProjectMalloyConfig } from "./connection";
import { ProjectStore } from "./project_store";

type ApiConnection = components["schemas"]["Connection"];
type ReleaseCallback = () => Promise<void>;
type ConnectionUpdateProject = {
   runConnectionUpdateExclusive?: <T>(fn: () => Promise<T>) => Promise<T>;
   updateConnections?: (
      nextMalloyConfig: ReturnType<typeof buildProjectMalloyConfig>,
      apiConnections?: ApiConnection[],
      afterPreviousRelease?: ReleaseCallback,
   ) => void;
   deleteConnection?: (connectionName: string) => Promise<void>;
   deleteDuckDBConnection?: (connectionName: string) => Promise<void>;
   deleteDuckLakeConnection?: (connectionName: string) => Promise<void>;
};

async function runProjectConnectionUpdate<T>(
   project: ConnectionUpdateProject,
   fn: () => Promise<T>,
): Promise<T> {
   if (project.runConnectionUpdateExclusive) {
      return project.runConnectionUpdateExclusive(fn);
   }
   return fn();
}

function updateProjectConnections(
   project: ConnectionUpdateProject,
   nextMalloyConfig: ReturnType<typeof buildProjectMalloyConfig>,
   afterPreviousRelease?: ReleaseCallback,
): void {
   project.updateConnections?.(
      nextMalloyConfig,
      nextMalloyConfig.apiConnections,
      afterPreviousRelease,
   );
}

function buildDeletedConnectionCleanup(
   project: ConnectionUpdateProject,
   deletedConnection: ApiConnection,
   connectionName: string,
): ReleaseCallback | undefined {
   if (
      deletedConnection.type === "duckdb" &&
      typeof project.deleteDuckDBConnection === "function"
   ) {
      return () => project.deleteDuckDBConnection!(connectionName);
   }

   if (
      deletedConnection.type === "ducklake" &&
      typeof project.deleteDuckLakeConnection === "function"
   ) {
      return () => project.deleteDuckLakeConnection!(connectionName);
   }

   return undefined;
}

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
      await runProjectConnectionUpdate(project, async () => {
         const existingConnections = project.listApiConnections();
         const nextMalloyConfig = buildProjectMalloyConfig(
            [...existingConnections, connection],
            project.metadata.location || "",
         );

         await this.projectStore.addConnection(
            connection,
            dbProject.id,
            repository,
         );

         updateProjectConnections(project, nextMalloyConfig);
      });

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
      await runProjectConnectionUpdate(project, async () => {
         const existingConnections = project.listApiConnections();

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
         const nextMalloyConfig = buildProjectMalloyConfig(
            updatedConnections,
            project.metadata.location || "",
            true,
         );

         await this.projectStore.updateConnection(
            updatedConnection,
            dbProject.id,
            repository,
         );

         updateProjectConnections(project, nextMalloyConfig);
      });

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
      await runProjectConnectionUpdate(project, async () => {
         if (typeof project.listApiConnections !== "function") {
            if (typeof project.deleteConnection === "function") {
               await project.deleteConnection(connectionName);
            }
            await repository.deleteConnection(dbConnection.id);
            return;
         }

         const deletedConnection =
            "getApiConnection" in project &&
            typeof project.getApiConnection === "function"
               ? project.getApiConnection(connectionName)
               : dbConnection.config;
         const updatedConnections = project
            .listApiConnections()
            .filter((connection) => connection.name !== connectionName);
         const nextMalloyConfig = buildProjectMalloyConfig(
            updatedConnections,
            project.metadata.location || "",
         );
         const deleteConnectionFilesAfterRelease =
            buildDeletedConnectionCleanup(
               project,
               deletedConnection,
               connectionName,
            );

         await repository.deleteConnection(dbConnection.id);

         updateProjectConnections(
            project,
            nextMalloyConfig,
            deleteConnectionFilesAfterRelease,
         );
      });

      logger.info(
         `Successfully deleted connection "${connectionName}" from project "${projectName}"`,
      );
   }
}
