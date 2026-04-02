import { ProjectNotFoundError } from "../errors";
import { ManifestService } from "../service/manifest_service";
import { ProjectStore } from "../service/project_store";

export class ManifestController {
   constructor(
      private projectStore: ProjectStore,
      private manifestService: ManifestService,
   ) {}

   async getManifest(projectName: string, packageName: string) {
      const projectId = await this.resolveProjectId(projectName);
      return this.manifestService.getManifest(projectId, packageName);
   }

   async loadManifest(projectName: string, packageName: string) {
      const projectId = await this.resolveProjectId(projectName);
      return this.manifestService.loadManifest(
         projectId,
         packageName,
         projectName,
      );
   }

   private async resolveProjectId(projectName: string): Promise<string> {
      const repository = this.projectStore.storageManager.getRepository();
      const dbProject = await repository.getProjectByName(projectName);
      if (!dbProject) {
         throw new ProjectNotFoundError(`Project '${projectName}' not found`);
      }
      return dbProject.id;
   }
}
