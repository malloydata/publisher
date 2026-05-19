import * as path from "path";
import { components } from "../api";
import { PUBLISHER_DATA_DIR } from "../constants";
import {
   BadRequestError,
   FrozenConfigError,
   ServiceUnavailableError,
} from "../errors";
import { logger } from "../logger";
import { EnvironmentStore } from "../service/environment_store";
import { ManifestService } from "../service/manifest_service";
import { PackageMemoryGovernor } from "../service/package_memory_governor";

type ApiPackage = components["schemas"]["Package"];

export class PackageController {
   private environmentStore: EnvironmentStore;
   private manifestService: ManifestService;
   private memoryGovernor: PackageMemoryGovernor | null;

   constructor(
      environmentStore: EnvironmentStore,
      manifestService: ManifestService,
      memoryGovernor: PackageMemoryGovernor | null = null,
   ) {
      this.environmentStore = environmentStore;
      this.manifestService = manifestService;
      this.memoryGovernor = memoryGovernor;
   }

   /**
    * Throw HTTP 503 when the {@link PackageMemoryGovernor} reports
    * that RSS has crossed the configured high-water mark. Only
    * consulted on paths that would load (or reload) a package into
    * memory; reads against already-loaded packages remain fully
    * serviceable so existing dashboards keep working under pressure.
    */
   private assertNotBackpressured(action: string): void {
      if (this.memoryGovernor?.isBackpressured()) {
         throw new ServiceUnavailableError(
            `Publisher is under memory pressure and cannot ${action} right now. Retry after the server's memory usage drops below the configured low-water mark.`,
         );
      }
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
      // Read-only fetches against the in-memory cache are safe under
      // pressure; only reloads (which redownload + re-create the
      // package) are gated.
      if (reload) {
         this.assertNotBackpressured("reload a package");
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const _package = await environment.getPackage(packageName, reload);
      const packageLocation = _package.getPackageMetadata().location;
      if (reload && packageLocation) {
         await this.downloadPackage(
            environmentName,
            packageName,
            packageLocation,
         );
      }
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
      this.assertNotBackpressured("add a new package");
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      if (body.location) {
         await this.downloadPackage(environmentName, body.name, body.location);
      }
      const result = await environment.addPackage(body.name);
      await this.environmentStore.addPackageToDatabase(
         environmentName,
         body.name,
      );

      if (options?.autoLoadManifest === true) {
         await this.tryLoadExistingManifest(environmentName, body.name);
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
      // A location-bearing update re-downloads and reloads the
      // package, which is a fresh memory allocation. Metadata-only
      // updates remain permitted under pressure.
      if (body.location) {
         this.assertNotBackpressured("update a package");
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      if (body.location) {
         await this.downloadPackage(
            environmentName,
            packageName,
            body.location,
         );
      }
      const result = await environment.updatePackage(packageName, body);
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
