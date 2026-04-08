import * as path from "path";
import { components } from "../api";
import { PUBLISHER_DATA_DIR } from "../constants";
import { BadRequestError, FrozenConfigError } from "../errors";
import { logger } from "../logger";
import { ManifestService } from "../service/manifest_service";
import { ProjectStore } from "../service/project_store";

type ApiPackage = components["schemas"]["Package"];

export class PackageController {
   private projectStore: ProjectStore;
   private manifestService: ManifestService;

   constructor(projectStore: ProjectStore, manifestService: ManifestService) {
      this.projectStore = projectStore;
      this.manifestService = manifestService;
   }

   public async listPackages(projectName: string): Promise<ApiPackage[]> {
      const project = await this.projectStore.getProject(projectName, false);
      return project.listPackages();
   }

   public async getPackage(
      projectName: string,
      packageName: string,
      reload: boolean,
   ): Promise<ApiPackage> {
      const project = await this.projectStore.getProject(projectName, false);
      const _package = await project.getPackage(packageName, reload);
      const packageLocation = _package.getPackageMetadata().location;
      if (reload && packageLocation) {
         await this.downloadPackage(projectName, packageName, packageLocation);
      }
      return _package.getPackageMetadata();
   }

   async addPackage(projectName: string, body: ApiPackage) {
      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      if (!body.name) {
         throw new BadRequestError("Package name is required");
      }
      const project = await this.projectStore.getProject(projectName, false);
      if (body.location) {
         await this.downloadPackage(projectName, body.name, body.location);
      }
      const result = await project.addPackage(body.name);
      await this.projectStore.addPackageToDatabase(projectName, body.name);

      await this.tryLoadExistingManifest(projectName, body.name);

      return result;
   }

   /**
    * If there are already manifest entries for this package (e.g. from a
    * previous materialization run), reload all models with the manifest so
    * persist references resolve to the materialized tables immediately.
    */
   private async tryLoadExistingManifest(
      projectName: string,
      packageName: string,
   ): Promise<void> {
      try {
         const repository = this.projectStore.storageManager.getRepository();
         const dbProject = await repository.getProjectByName(projectName);
         if (!dbProject) return;

         const manifest = await this.manifestService.getManifest(
            dbProject.id,
            packageName,
         );
         if (Object.keys(manifest.entries).length === 0) return;

         await this.manifestService.loadManifest(
            dbProject.id,
            packageName,
            projectName,
         );
         logger.info("Auto-loaded existing manifest for added package", {
            projectName,
            packageName,
            entryCount: Object.keys(manifest.entries).length,
         });
      } catch (error) {
         logger.warn("Failed to auto-load manifest for package", {
            projectName,
            packageName,
            error,
         });
      }
   }

   public async deletePackage(projectName: string, packageName: string) {
      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const project = await this.projectStore.getProject(projectName, false);
      const result = await project.deletePackage(packageName);
      await this.projectStore.deletePackageFromDatabase(
         projectName,
         packageName,
      );

      return result;
   }

   public async updatePackage(
      projectName: string,
      packageName: string,
      body: ApiPackage,
   ) {
      if (this.projectStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const project = await this.projectStore.getProject(projectName, false);
      if (body.location) {
         await this.downloadPackage(projectName, packageName, body.location);
      }
      const result = await project.updatePackage(packageName, body);
      await this.projectStore.addPackageToDatabase(projectName, packageName);

      return result;
   }

   private async downloadPackage(
      projectName: string,
      packageName: string,
      packageLocation: string,
   ) {
      const absoluteTargetPath = path.join(
         this.projectStore.serverRootPath,
         PUBLISHER_DATA_DIR,
         projectName,
         packageName,
      );
      const isCompressedFile = packageLocation.endsWith(".zip");
      if (
         packageLocation.startsWith("https://") ||
         packageLocation.startsWith("git@")
      ) {
         await this.projectStore.downloadGitHubDirectory(
            packageLocation,
            absoluteTargetPath,
         );
      } else if (packageLocation.startsWith("gs://")) {
         await this.projectStore.downloadGcsDirectory(
            packageLocation,
            projectName,
            absoluteTargetPath,
            isCompressedFile,
         );
      } else if (packageLocation.startsWith("s3://")) {
         await this.projectStore.downloadS3Directory(
            packageLocation,
            projectName,
            absoluteTargetPath,
            isCompressedFile,
         );
      }

      if (packageLocation.startsWith("/")) {
         // Absolute paths from the publisher.config could be placed outside of /etc/publisher,
         // so we need to mount them on the right place.
         await this.projectStore.mountLocalDirectory(
            packageLocation,
            absoluteTargetPath,
            projectName,
            packageName,
         );
      }
   }
}
