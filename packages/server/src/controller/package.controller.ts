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
import {
   buildVersionedPackagePath,
   PackageSweeper,
} from "../service/package_sweeper";

type ApiPackage = components["schemas"]["Package"];

/**
 * Strategy: every API write that produces new package contents goes into a
 * fresh, UUID-scoped versioned directory under
 * `<env>/<pkg>.versions/<uuid>/`. The DB row's `manifest_path` is then
 * atomically swung from the old directory to the new one via
 * `repository.swapPackageDirectory`. The old directory (if any) is handed
 * to `PackageSweeper` for deferred deletion so in-flight readers that hold
 * a `Package` cached against the old path keep working until the
 * quiescence window elapses. There are no per-package mutexes — concurrent
 * writers download into different staging dirs and rely on the database as
 * the single ordering point.
 */
export class PackageController {
   private environmentStore: EnvironmentStore;
   private manifestService: ManifestService;
   private sweeper: PackageSweeper;

   constructor(
      environmentStore: EnvironmentStore,
      manifestService: ManifestService,
      sweeper: PackageSweeper,
   ) {
      this.environmentStore = environmentStore;
      this.manifestService = manifestService;
      this.sweeper = sweeper;
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

      if (!reload) {
         const _package = await environment.getPackage(packageName);
         return _package.getPackageMetadata();
      }

      // `?reload=true` is the API contract for "re-download from `location` and
      // start serving the new contents immediately." Re-uses the same
      // versioned-dir + DB-CAS write path the controller's add/update paths
      // take so the race profile is identical and the in-memory cache picks
      // up the new version on the very next read.
      const cached = await environment.getPackage(packageName);
      const packageLocation = cached.getPackageMetadata().location;
      if (!packageLocation) {
         return cached.getPackageMetadata();
      }
      await this.swapInVersionedDirectory(environment, packageName, {
         location: packageLocation,
         description: cached.getPackageMetadata().description,
      });
      const reloaded = await environment.getPackage(packageName);
      return reloaded.getPackageMetadata();
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

      if (body.location) {
         await this.swapInVersionedDirectory(environment, packageName, body);
      } else {
         // Mount-style add with no `location`: ensure a row exists so future
         // resolves go through the DB. The directory is whatever already
         // exists at `<env>/<pkg>` (legacy fallback).
         await this.ensureDatabaseRowForExistingDirectory(
            environment,
            packageName,
            body,
         );
      }

      const result = (await environment.getPackage(packageName)).getPackageMetadata();

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
      const oldDirectoryPath = await environment.deletePackage(packageName);
      await this.environmentStore.deletePackageFromDatabase(
         environmentName,
         packageName,
      );
      if (oldDirectoryPath) {
         this.sweeper.scheduleSweep(environmentName, oldDirectoryPath);
      }
      return undefined;
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
         await this.swapInVersionedDirectory(environment, packageName, body);
      } else if (body.description !== undefined) {
         // Description-only update: write straight through to the DB row,
         // leaving the on-disk version untouched.
         await this.persistMetadataOnly(environment, packageName, body);
      }

      // Refresh the in-memory `Package` so its metadata reflects the latest
      // description. The directory swap (if any) already invalidated the
      // path-keyed cache on its own.
      return environment.updatePackage(packageName, body);
   }

   /**
    * Download into a fresh `<env>/<pkg>.versions/<uuid>/` directory, atomically
    * swing the package row to point at it, and queue the previous directory
    * for sweeper-driven cleanup. Two concurrent calls for the same package
    * download into different UUID dirs and serialize through the DB CAS;
    * neither blocks on the other and the sweeper backstops any orphan that
    * results from the race.
    */
   private async swapInVersionedDirectory(
      environment: Environment,
      packageName: string,
      body: ApiPackage,
   ): Promise<void> {
      if (!body.location) return;

      const environmentName = environment.getEnvironmentName();
      const versionId = randomUUID();
      const newDirectoryPath = buildVersionedPackagePath(
         environment.getEnvironmentPath(),
         packageName,
         versionId,
      );

      try {
         await fs.promises.mkdir(path.dirname(newDirectoryPath), {
            recursive: true,
         });
         await this.downloadIntoTarget(
            environmentName,
            packageName,
            body.location,
            newDirectoryPath,
         );
      } catch (error) {
         await this.bestEffortRm(newDirectoryPath);
         throw error;
      }

      const repository = this.environmentStore.storageManager.getRepository();
      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);
      if (!dbEnvironment) {
         await this.bestEffortRm(newDirectoryPath);
         throw new Error(
            `Environment "${environmentName}" not found in database`,
         );
      }

      const swapResult = await repository.swapPackageDirectory({
         environmentId: dbEnvironment.id,
         name: packageName,
         newDirectoryPath,
         description: body.description,
         metadata: { location: body.location },
      });

      if (swapResult.oldDirectoryPath) {
         this.sweeper.scheduleSweep(
            environmentName,
            swapResult.oldDirectoryPath,
         );
      }
   }

   private async ensureDatabaseRowForExistingDirectory(
      environment: Environment,
      packageName: string,
      body: ApiPackage,
   ): Promise<void> {
      const environmentName = environment.getEnvironmentName();
      const repository = this.environmentStore.storageManager.getRepository();
      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);
      if (!dbEnvironment) {
         throw new Error(
            `Environment "${environmentName}" not found in database`,
         );
      }
      const legacyPath = path.join(
         environment.getEnvironmentPath(),
         packageName,
      );
      await repository.swapPackageDirectory({
         environmentId: dbEnvironment.id,
         name: packageName,
         newDirectoryPath: legacyPath,
         description: body.description,
         metadata: body.location ? { location: body.location } : undefined,
      });
   }

   private async persistMetadataOnly(
      environment: Environment,
      packageName: string,
      body: ApiPackage,
   ): Promise<void> {
      const environmentName = environment.getEnvironmentName();
      const repository = this.environmentStore.storageManager.getRepository();
      const dbEnvironment =
         await repository.getEnvironmentByName(environmentName);
      if (!dbEnvironment) {
         throw new Error(
            `Environment "${environmentName}" not found in database`,
         );
      }
      const existing = await repository.getPackageByName(
         dbEnvironment.id,
         packageName,
      );
      if (!existing) {
         await this.ensureDatabaseRowForExistingDirectory(
            environment,
            packageName,
            body,
         );
         return;
      }
      await repository.updatePackage(existing.id, {
         description: body.description,
      });
   }

   private async downloadIntoTarget(
      environmentName: string,
      packageName: string,
      packageLocation: string,
      targetPath: string,
   ): Promise<void> {
      const isCompressedFile = packageLocation.endsWith(".zip");
      if (
         packageLocation.startsWith("https://") ||
         packageLocation.startsWith("git@")
      ) {
         await this.environmentStore.downloadGitHubDirectory(
            packageLocation,
            targetPath,
         );
         return;
      }
      if (packageLocation.startsWith("gs://")) {
         await this.environmentStore.downloadGcsDirectory(
            packageLocation,
            environmentName,
            targetPath,
            isCompressedFile,
         );
         return;
      }
      if (packageLocation.startsWith("s3://")) {
         await this.environmentStore.downloadS3Directory(
            packageLocation,
            environmentName,
            targetPath,
            isCompressedFile,
         );
         return;
      }
      if (packageLocation.startsWith("/")) {
         // Absolute paths from publisher.config could live outside the
         // publisher data dir; mount (cp -r) into the versioned target.
         await this.environmentStore.mountLocalDirectory(
            packageLocation,
            targetPath,
            environmentName,
            packageName,
         );
         return;
      }
      throw new BadRequestError(
         `Unsupported package location scheme: ${packageLocation}`,
      );
   }

   private async bestEffortRm(targetPath: string): Promise<void> {
      try {
         await fs.promises.rm(targetPath, { recursive: true, force: true });
      } catch (error) {
         logger.warn("Failed to clean up partial download", {
            targetPath,
            error,
         });
      }
   }

   /**
    * Helper used in tests / migration paths to spell out the legacy data
    * dir for a package. Kept here for symmetry with `buildVersionedPackagePath`.
    */
   public legacyPackagePath(
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
}
