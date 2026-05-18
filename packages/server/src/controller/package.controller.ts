import * as path from "path";
import { components } from "../api";
import { PUBLISHER_DATA_DIR } from "../constants";
import { BadRequestError, FrozenConfigError } from "../errors";
import { logger } from "../logger";
import { EnvironmentStore } from "../service/environment_store";
import { ManifestService } from "../service/manifest_service";

type ApiPackage = components["schemas"]["Package"];

export class PackageController {
   private environmentStore: EnvironmentStore;
   private manifestService: ManifestService;

   constructor(
      environmentStore: EnvironmentStore,
      manifestService: ManifestService,
   ) {
      this.environmentStore = environmentStore;
      this.manifestService = manifestService;
   }

   public async listPackages(environmentName: string): Promise<ApiPackage[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      return environment.listPackages();
   }

   public async getPackage(
      environmentName: string,
      packageName: string,
      reload: boolean,
   ): Promise<ApiPackage> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // On reload, re-download fresh bits *before* Package.create reads from
      // disk, and run the download inside the per-package mutex so concurrent
      // reloads can't race on the same target path. Look up the location from
      // the currently-loaded package (without triggering a load): if the
      // package isn't loaded yet, the normal non-reload path will populate it.
      let downloadFn: (() => Promise<void>) | undefined;
      if (reload) {
         const cached = await environment
            .getPackage(packageName, false)
            .catch(() => undefined);
         const packageLocation = cached?.getPackageMetadata().location;
         if (packageLocation) {
            downloadFn = () =>
               this.downloadPackage(
                  environmentName,
                  packageName,
                  packageLocation,
               );
         }
      }
      const _package = await environment.getPackage(
         packageName,
         reload,
         downloadFn,
      );
      return _package.getPackageMetadata();
   }

   async addPackage(
      environmentName: string,
      body: ApiPackage,
      options?: { autoLoadManifest?: boolean },
   ) {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      if (!body.name) {
         throw new BadRequestError("Package name is required");
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const packageName = body.name;
      const location = body.location;
      const downloadFn = location
         ? () => this.downloadPackage(environmentName, packageName, location)
         : undefined;
      const result = await environment.addPackage(packageName, downloadFn);
      await this.environmentStore.addPackageToDatabase(
         environmentName,
         packageName,
      );

      if (options?.autoLoadManifest === true) {
         await this.tryLoadExistingManifest(environmentName, packageName);
      }

      return result;
   }

   /**
    * If there are already manifest entries for this package (e.g. from a
    * previous materialization run), reload all models with the manifest so
    * persist references resolve to the materialized tables immediately.
    */
   private async tryLoadExistingManifest(
      environmentName: string,
      packageName: string,
   ): Promise<void> {
      try {
         const repository =
            this.environmentStore.storageManager.getRepository();
         const dbEnvironment =
            await repository.getEnvironmentByName(environmentName);
         if (!dbEnvironment) return;

         const manifest = await this.manifestService.getManifest(
            dbEnvironment.id,
            packageName,
         );
         if (Object.keys(manifest.entries).length === 0) return;

         await this.manifestService.reloadManifest(
            dbEnvironment.id,
            packageName,
            environmentName,
         );
         logger.info("Auto-loaded existing manifest for added package", {
            environmentName,
            packageName,
            entryCount: Object.keys(manifest.entries).length,
         });
      } catch (error) {
         logger.warn("Failed to auto-load manifest for package", {
            environmentName,
            packageName,
            error,
         });
      }
   }

   public async deletePackage(environmentName: string, packageName: string) {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const result = await environment.deletePackage(packageName);
      await this.environmentStore.deletePackageFromDatabase(
         environmentName,
         packageName,
      );

      return result;
   }

   public async updatePackage(
      environmentName: string,
      packageName: string,
      body: ApiPackage,
   ) {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const location = body.location;
      const downloadFn = location
         ? () => this.downloadPackage(environmentName, packageName, location)
         : undefined;
      const result = await environment.updatePackage(
         packageName,
         body,
         downloadFn,
      );
      await this.environmentStore.addPackageToDatabase(
         environmentName,
         packageName,
      );

      return result;
   }

   private async downloadPackage(
      environmentName: string,
      packageName: string,
      packageLocation: string,
   ) {
      const absoluteTargetPath = path.join(
         this.environmentStore.serverRootPath,
         PUBLISHER_DATA_DIR,
         environmentName,
         packageName,
      );
      const isCompressedFile = packageLocation.endsWith(".zip");
      if (
         packageLocation.startsWith("https://") ||
         packageLocation.startsWith("git@")
      ) {
         await this.environmentStore.downloadGitHubDirectory(
            packageLocation,
            absoluteTargetPath,
         );
      } else if (packageLocation.startsWith("gs://")) {
         await this.environmentStore.downloadGcsDirectory(
            packageLocation,
            environmentName,
            absoluteTargetPath,
            isCompressedFile,
         );
      } else if (packageLocation.startsWith("s3://")) {
         await this.environmentStore.downloadS3Directory(
            packageLocation,
            environmentName,
            absoluteTargetPath,
            isCompressedFile,
         );
      }

      if (packageLocation.startsWith("/")) {
         // Absolute paths from the publisher.config could be placed outside of /etc/publisher,
         // so we need to mount them on the right place.
         await this.environmentStore.mountLocalDirectory(
            packageLocation,
            absoluteTargetPath,
            environmentName,
            packageName,
         );
      }
   }
}
