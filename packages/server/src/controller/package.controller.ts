import { components } from "../api";
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

      if (reload) {
         // Resolve the package's source location from the currently-cached
         // metadata WITHOUT triggering a stale-state reload. If a `location`
         // is set, route the reload through `installPackage` so that
         // download-then-load happens atomically; otherwise fall back to an
         // in-place reload of the existing on-disk content.
         let location: string | undefined;
         try {
            const cached = await environment.getPackage(packageName, false);
            location = cached.getPackageMetadata().location;
         } catch {
            // Not previously loaded — nothing to reinstall from.
         }
         if (location) {
            const reinstalled = await environment.installPackage(
               packageName,
               (stagingPath) =>
                  this.downloadInto(
                     environmentName,
                     packageName,
                     location,
                     stagingPath,
                  ),
            );
            return reinstalled.getPackageMetadata();
         }
         const _package = await environment.getPackage(packageName, true);
         return _package.getPackageMetadata();
      }

      const _package = await environment.getPackage(packageName, false);
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
      const packageName = body.name;
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      let result;
      if (body.location) {
         const bodyLocation = body.location;
         result = await environment.installPackage(packageName, (stagingPath) =>
            this.downloadInto(
               environmentName,
               packageName,
               bodyLocation,
               stagingPath,
            ),
         );
      } else {
         result = await environment.addPackage(packageName);
      }

      // `addPackage` is typed `Package | undefined`; a missing result here is a
      // should-never-happen internal fault. Fail loudly rather than letting
      // optional chaining silently skip the explores validation below.
      if (!result) {
         throw new Error(`Failed to create package ${packageName}`);
      }

      // Strict at publish: the author is in the loop here, so reject a bad
      // explores with an actionable error instead of silently serving a
      // hidden surface. (At startup/reload we fail safe and only warn — see
      // Package.loadViaWorker.) Roll back the just-installed package so a
      // rejected publish doesn't leave a half-committed, un-persisted package.
      const invalidMsg = result.formatInvalidExplores();
      if (invalidMsg) {
         await environment.deletePackage(packageName).catch(() => {
            /* best-effort rollback; the package is not persisted below */
         });
         throw new BadRequestError(invalidMsg);
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
      if (body.location) {
         // Re-install: stream the new content into a staging dir (no lock)
         // and atomically swap it in (under the lock).
         const bodyLocation = body.location;
         await environment.installPackage(packageName, (stagingPath) =>
            this.downloadInto(
               environmentName,
               packageName,
               bodyLocation,
               stagingPath,
            ),
         );
      }
      // Apply metadata changes (publisher.json) under the same per-package
      // mutex via `Environment.updatePackage`.
      const result = await environment.updatePackage(packageName, body);
      await this.environmentStore.addPackageToDatabase(
         environmentName,
         packageName,
      );

      return result;
   }

   /**
    * Run the right downloader for the given location into `targetPath`.
    * This used to point at the canonical package directory, but the
    * install pipeline now passes a sibling staging dir so the long-running
    * download doesn't hold the per-package mutex.
    */
   private async downloadInto(
      environmentName: string,
      packageName: string,
      packageLocation: string,
      targetPath: string,
   ) {
      const isCompressedFile = packageLocation.endsWith(".zip");
      if (
         packageLocation.startsWith("https://") ||
         packageLocation.startsWith("git@")
      ) {
         await this.environmentStore.downloadGitHubDirectory(
            packageLocation,
            targetPath,
         );
      } else if (packageLocation.startsWith("gs://")) {
         await this.environmentStore.downloadGcsDirectory(
            packageLocation,
            environmentName,
            targetPath,
            isCompressedFile,
         );
      } else if (packageLocation.startsWith("s3://")) {
         await this.environmentStore.downloadS3Directory(
            packageLocation,
            environmentName,
            targetPath,
            isCompressedFile,
         );
      }

      if (packageLocation.startsWith("/")) {
         // Absolute paths from the publisher.config could be placed outside of /etc/publisher,
         // so we need to mount them on the right place.
         await this.environmentStore.mountLocalDirectory(
            packageLocation,
            targetPath,
            environmentName,
            packageName,
         );
      }
   }
}
