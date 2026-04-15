import { ManifestService } from "../service/manifest_service";
import { ProjectStore } from "../service/project_store";
import { resolveProjectId } from "../service/resolve_project";

export class ManifestController {
   constructor(
      private projectStore: ProjectStore,
      private manifestService: ManifestService,
   ) {}

   async getManifest(projectName: string, packageName: string) {
      const repository = this.projectStore.storageManager.getRepository();
      const projectId = await resolveProjectId(repository, projectName);
      // Verify the package exists so we return 404 instead of an empty manifest.
      const project = await this.projectStore.getProject(projectName, false);
      await project.getPackage(packageName, false);
      return this.manifestService.getManifest(projectId, packageName);
   }

   async loadManifest(projectName: string, packageName: string) {
      const repository = this.projectStore.storageManager.getRepository();
      const projectId = await resolveProjectId(repository, projectName);
      return this.manifestService.loadManifest(
         projectId,
         packageName,
         projectName,
      );
   }
}
