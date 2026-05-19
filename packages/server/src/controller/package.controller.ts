import { randomUUID } from "crypto";
import * as fs from "fs";
import * as path from "path";
import { components } from "../api";
import { PUBLISHER_DATA_DIR } from "../constants";
import { BadRequestError, FrozenConfigError } from "../errors";
import { logger } from "../logger";
import { Environment } from "../service/environment";
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

      const cached = await environment.getPackage(packageName, false);
      const packageLocation = cached.getPackageMetadata().location;

      if (!reload) {
         return cached.getPackageMetadata();
      }

      // Reload path: stage the download OUTSIDE the dir lock so concurrent
      // reloads can run in parallel, then swap + reload under the lock.
      const stagingPath = packageLocation
         ? await this.downloadAndStagePackage(
              environment,
              packageName,
              packageLocation,
           )
         : undefined;
      try {
         return await environment.withPackageDirectoryLock(
            packageName,
            async () => {
               if (stagingPath) {
                  await this.swapStagedPackage(
                     stagingPath,
                     this.packageTargetPath(environmentName, packageName),
                  );
               }
               const reloaded = await environment.getPackage(
                  packageName,
                  true,
                  {
                     dirLockHeld: true,
                  },
               );
               return reloaded.getPackageMetadata();
            },
         );
      } catch (error) {
         if (stagingPath) await this.cleanupStagedPackage(stagingPath);
         throw error;
      }
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

      const stagingPath = body.location
         ? await this.downloadAndStagePackage(
              environment,
              packageName,
              body.location,
           )
         : undefined;
      let result;
      try {
         result = await environment.withPackageDirectoryLock(
            packageName,
            async () => {
               if (stagingPath) {
                  await this.swapStagedPackage(
                     stagingPath,
                     this.packageTargetPath(environmentName, packageName),
                  );
               }
               return environment.addPackage(packageName);
            },
         );
      } catch (error) {
         if (stagingPath) await this.cleanupStagedPackage(stagingPath);
         throw error;
      }
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
      const result = await environment.withPackageDirectoryLock(
         packageName,
         () => environment.deletePackage(packageName),
      );
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

      const stagingPath = body.location
         ? await this.downloadAndStagePackage(
              environment,
              packageName,
              body.location,
           )
         : undefined;
      let result;
      try {
         result = await environment.withPackageDirectoryLock(
            packageName,
            async () => {
               if (stagingPath) {
                  await this.swapStagedPackage(
                     stagingPath,
                     this.packageTargetPath(environmentName, packageName),
                  );
               }
               return environment.updatePackage(packageName, body);
            },
         );
      } catch (error) {
         if (stagingPath) await this.cleanupStagedPackage(stagingPath);
         throw error;
      }
      await this.environmentStore.addPackageToDatabase(
         environmentName,
         packageName,
      );

      return result;
   }

   private packageTargetPath(
      environmentName: string,
      packageName: string,
   ): string {
      return path.join(
         this.environmentStore.serverRootPath,
         PUBLISHER_DATA_DIR,
         environmentName,
         packageName,
      );
   }

   private async downloadAndStagePackage(
      environment: Environment,
      packageName: string,
      packageLocation: string,
   ): Promise<string> {
      const environmentName = environment.getEnvironmentName();
      const finalPath = this.packageTargetPath(environmentName, packageName);
      const stagingPath = `${finalPath}.staging-${randomUUID()}`;
      const isCompressedFile = packageLocation.endsWith(".zip");

      if (
         packageLocation.startsWith("https://") ||
         packageLocation.startsWith("git@")
      ) {
         await this.environmentStore.downloadGitHubDirectory(
            packageLocation,
            stagingPath,
         );
      } else if (packageLocation.startsWith("gs://")) {
         await this.environmentStore.downloadGcsDirectory(
            packageLocation,
            environmentName,
            stagingPath,
            isCompressedFile,
         );
      } else if (packageLocation.startsWith("s3://")) {
         await this.environmentStore.downloadS3Directory(
            packageLocation,
            environmentName,
            stagingPath,
            isCompressedFile,
         );
      } else if (packageLocation.startsWith("/")) {
         // Absolute paths from the publisher.config could be placed outside of /etc/publisher,
         // so we need to mount them on the right place.
         await this.environmentStore.mountLocalDirectory(
            packageLocation,
            stagingPath,
            environmentName,
            packageName,
         );
      }

      return stagingPath;
   }

   private async swapStagedPackage(
      stagingPath: string,
      finalPath: string,
   ): Promise<void> {
      await fs.promises.rm(finalPath, { recursive: true, force: true });
      await fs.promises.rename(stagingPath, finalPath);
   }

   private async cleanupStagedPackage(stagingPath: string): Promise<void> {
      try {
         await fs.promises.rm(stagingPath, { recursive: true, force: true });
      } catch (err) {
         logger.warn("Failed to clean up staging directory", {
            stagingPath,
            error: err,
         });
      }
   }
}
