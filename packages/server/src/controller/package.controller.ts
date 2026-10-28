// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   recordPackageReload,
   type ReloadTrigger,
} from "../package_state_metrics";
import * as path from "path";
import { components } from "../api";
import { normalizeModelPath } from "../constants";
import {
   BadRequestError,
   FrozenConfigError,
   internalErrorToHttpError,
   PackageAdmissionRefusedError,
} from "../errors";
import { logger } from "../logger";
import { getPackageEmbeddingStatus } from "../mcp/tools/get_context_tool";
import { refreshPackageConnectionsEpoch } from "../package_load/package_load_pool";
import { EnvironmentStore } from "../service/environment_store";
import { treeFingerprint } from "../package_load/package_files";
import type { Package } from "../service/package";

type ApiPackage = components["schemas"]["Package"];
type ApiPackageLoadTimings = components["schemas"]["PackageLoadTimings"];

/**
 * What a reload did: `unchanged` when `ifChanged` found the tree's content sha
 * equal to the served one and nothing was loaded (so there are no timings).
 */
export interface PackageReloadResult {
   metadata: ApiPackage;
   mode: PackageReloadMode;
   unchanged?: true;
   timings?: ApiPackageLoadTimings;
}

/** The load's timings with a `totalMs` placeholder, which {@link PackageController.reloadPackage} fills in. */
const withTotal = (
   timings: Omit<ApiPackageLoadTimings, "totalMs"> | undefined,
): ApiPackageLoadTimings | undefined => timings && { ...timings, totalMs: 0 };

export interface PackageReloadOptions {
   /** Skip the reload when the tree on disk hashes to what is served. */
   ifChanged?: boolean;
   /** Clear the schema and compile caches first, to pick up a warehouse-side change. */
   refreshSchema?: boolean;
}

/**
 * Which path a reload took. `in-place` recompiles the tree already on disk and
 * leaves it alone; `reinstalled` re-fetches from the package's install location,
 * which overwrites on-disk edits.
 */
export type PackageReloadMode = "in-place" | "reinstalled";

/**
 * Every strict-at-publish check joined into one 400 message, or undefined when
 * the package is publishable: explores, persistence policy, incremental
 * refresh, pre-aggregation, and persist-target collisions (only under
 * `PERSIST_COLLISION_ENFORCE`, so a latent collision does not block a routine
 * re-publish). At startup/reload these only warn (fail-safe), except the
 * incremental-refresh and pre-aggregation gates, which fail the load too.
 */
function formatPublishRejections(
   pkg: {
      formatInvalidExplores(exploresOverride?: string[]): string;
      formatInvalidPersistencePolicy(): string;
      formatInvalidIncrementalPolicy(): string;
      formatInvalidPreaggregatePolicy(): string;
      formatPersistenceCollisionRejections(): string;
   },
   exploresOverride?: string[],
): string | undefined {
   const message = [
      pkg.formatInvalidExplores(exploresOverride),
      pkg.formatInvalidPersistencePolicy(),
      pkg.formatInvalidIncrementalPolicy(),
      pkg.formatInvalidPreaggregatePolicy(),
      pkg.formatPersistenceCollisionRejections(),
   ]
      .filter(Boolean)
      .join("\n");
   return message || undefined;
}

export class PackageController {
   private environmentStore: EnvironmentStore;

   constructor(environmentStore: EnvironmentStore) {
      this.environmentStore = environmentStore;
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
      let metadata: ApiPackage;
      if (reload) {
         const reloaded = await this.reloadPackage(
            environmentName,
            packageName,
         );
         metadata = { ...reloaded.metadata, timings: reloaded.timings };
      } else {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const _package = await environment.getPackage(packageName, false);
         metadata = _package.getPackageMetadata();
         metadata.status = environment.describePackageStatus(packageName);
      }

      // Enriched on both paths, so `?reload=true` (the request that
      // invalidates the index, and starts a caller's polling) carries it too.
      const embeddingIndex = await this.embeddingIndexStatus(
         environmentName,
         packageName,
      );
      return embeddingIndex ? { ...metadata, embeddingIndex } : metadata;
   }

   /**
    * The package's semantic-index state (`lexical` with no embedding
    * provider), or undefined when unreadable. Never fails the request: a
    * reporting field must not turn a working GET into a 500.
    */
   private async embeddingIndexStatus(
      environmentName: string,
      packageName: string,
   ): Promise<ApiPackage["embeddingIndex"] | undefined> {
      try {
         return await getPackageEmbeddingStatus(
            this.environmentStore,
            environmentName,
            packageName,
         );
      } catch (error) {
         logger.debug("Could not read the package's embedding index state", {
            environmentName,
            packageName,
            error: error instanceof Error ? error.message : String(error),
         });
         return undefined;
      }
   }

   /**
    * Reload a package and report which path ran: a reinstall overwrites
    * on-disk edits and an in-place reload does not, so a caller surfacing the
    * reload must be able to say which happened.
    */
   public async reloadPackage(
      environmentName: string,
      packageName: string,
      trigger: ReloadTrigger = "rest",
      options: PackageReloadOptions = {},
   ): Promise<PackageReloadResult> {
      const startedAt = performance.now();
      try {
         const reloaded = await this.reloadPackageFrom(
            environmentName,
            packageName,
            options,
         );
         // The whole request, so the lock wait, a reinstall's download and the
         // manifest bind are in it, not only the worker load.
         if (reloaded.timings) {
            reloaded.timings.totalMs = performance.now() - startedAt;
         }
         // An unchanged answer loaded nothing, so it is not a reload.
         if (!reloaded.unchanged) recordPackageReload(trigger, "success");
         return reloaded;
      } catch (error) {
         recordPackageReload(trigger, "failure");
         throw error;
      }
   }

   private async reloadPackageFrom(
      environmentName: string,
      packageName: string,
      options: PackageReloadOptions,
   ): Promise<PackageReloadResult> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );

      // Read the location from cached metadata without triggering a reload.
      // With a location, reinstall so download-then-load is atomic;
      // otherwise reload the on-disk tree in place.
      let resident: ApiPackage | undefined;
      let cached: Package | undefined;
      try {
         cached = await environment.getPackage(packageName, false);
         resident = cached.getPackageMetadata();
      } catch {
         // Not previously loaded, so there is nothing to reinstall from.
      }
      const location = resident?.location;
      // A refresh asks for a warehouse-side change the tree cannot show, and a
      // failed reload left a stale model that matching files would hide.
      if (
         cached &&
         options.ifChanged &&
         !options.refreshSchema &&
         !location &&
         !environment.getStaleCompileErrors().has(packageName)
      ) {
         const current = await treeFingerprint(cached.getPackagePath());
         if (current === cached.getServedFingerprint()) {
            return {
               metadata: cached.getPackageMetadata(),
               mode: "in-place",
               unchanged: true,
            };
         }
      }
      if (options.refreshSchema) {
         refreshPackageConnectionsEpoch(environmentName, packageName);
      }
      if (resident && location) {
         // Re-apply only what the orchestrator set (location and manifest),
         // else the package serves live until the next drift check. All else
         // is the new tree's to declare, and a reload stays fail-safe rather
         // than enforcing publish-time checks.
         const reinstalled = await environment.installPackage(
            packageName,
            (stagingPath) =>
               this.downloadInto(
                  environmentName,
                  packageName,
                  location,
                  stagingPath,
               ),
            undefined,
            {
               update: {
                  location,
                  ...(resident.manifestLocation
                     ? { manifestLocation: resident.manifestLocation }
                     : {}),
               },
            },
         );
         return {
            metadata: reinstalled.getPackageMetadata(),
            mode: "reinstalled",
            timings: withTotal(reinstalled.getLoadTimings()),
         };
      }
      const _package = await environment.getPackage(packageName, true);
      return {
         metadata: _package.getPackageMetadata(),
         mode: "in-place",
         timings: withTotal(_package.getLoadTimings()),
      };
   }

   async addPackage(environmentName: string, body: ApiPackage) {
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
      // Strict at publish, since the author is in the loop (startup/reload
      // only warns). A rejected publish never destroys user content:
      //   - location: validation runs inside installPackage's swap window, and
      //     a failure wipes only the fresh download.
      //   - no-location: the directory pre-existed, so validate afterwards and
      //     `unloadPackage` (keep the files) rather than delete it.
      let result;
      try {
         if (body.location) {
            const bodyLocation = body.location;
            result = await environment.installPackage(
               packageName,
               (stagingPath) =>
                  this.downloadInto(
                     environmentName,
                     packageName,
                     bodyLocation,
                     stagingPath,
                  ),
               (pkg) => formatPublishRejections(pkg),
               // Record the location and bind the manifest under the install's
               // own lock; the downloaded publisher.json carries neither.
               // Without the location a later PATCH to it could not be told
               // from new content; without the manifest the package would
               // serve live until the drift check reloaded it.
               {
                  update: {
                     location: bodyLocation,
                     // A null or empty value has nothing to revert and would
                     // only recompile a second time.
                     ...(body.manifestLocation
                        ? { manifestLocation: body.manifestLocation }
                        : {}),
                  },
               },
            );
         } else {
            result = await environment.addPackage(packageName);
         }
      } catch (error) {
         // A 5xx is an operator's problem the caller may never show one, so
         // record it for /status with the same message the response carries.
         // A 4xx is the package's own content and is not recorded. Nor is an
         // admission refusal: it answers this request, and recorded it would
         // read as a load failure until this server next loaded the package.
         const answered = internalErrorToHttpError(error as Error, {
            log: false,
         });
         if (
            answered.status >= 500 &&
            !(error instanceof PackageAdmissionRefusedError)
         ) {
            environment.recordPackageAddFailure(
               packageName,
               answered.json.message,
            );
         }
         throw error;
      }

      // Should never happen; fail loudly rather than skip validation below.
      if (!result) {
         throw new Error(`Failed to create package ${packageName}`);
      }

      if (!body.location) {
         const invalidMsg = formatPublishRejections(result);
         if (invalidMsg) {
            await environment.unloadPackage(packageName).catch(() => {
               /* best-effort; the package is not persisted below */
            });
            throw new BadRequestError(invalidMsg);
         }
      }

      await this.environmentStore.addPackageToDatabase(
         environmentName,
         packageName,
      );

      return result;
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
      // A `location` equal to the installed one is a metadata update (e.g. an
      // orchestrator's rebind after a materialization build), not a
      // reinstall: content does not change under one URI. To re-fetch, reload.
      //
      // Decide against the copy resident once nothing is loading: mid-install
      // the resident copy is about to be replaced, so deciding against it
      // could double-install an in-flight location or treat a failed install
      // as installed. A non-string location (null) names nothing to fetch.
      await environment.awaitPackageLoads(packageName);
      const installedFrom = environment
         .peekPackage(packageName)
         ?.getPackageMetadata().location;
      const reinstall =
         typeof body.location === "string" &&
         body.location !== "" &&
         body.location !== installedFrom;
      let result: ApiPackage;
      if (reinstall) {
         // Download to staging without the lock, then swap under it. Explores
         // are validated inside the swap window so a rejection rolls back to
         // the previous tree. The rest of the body is applied in the same lock
         // hold; a bad policy there 400s with the new tree in place.
         const bodyLocation = body.location as string;
         const installed = await environment.installPackage(
            packageName,
            (stagingPath) =>
               this.downloadInto(
                  environmentName,
                  packageName,
                  bodyLocation,
                  stagingPath,
               ),
            (pkg) =>
               formatPublishRejections(
                  pkg,
                  body.explores?.map(normalizeModelPath),
               ),
            { update: body },
         );
         result = installed.getPackageMetadata();
      } else {
         result = await environment.updatePackage(packageName, body);
      }
      await this.environmentStore.addPackageToDatabase(
         environmentName,
         packageName,
      );

      return result;
   }

   /**
    * Run the right downloader for the location into `targetPath`, a staging
    * dir, so the long download does not hold the per-package mutex.
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

      if (packageLocation.startsWith("/") || path.isAbsolute(packageLocation)) {
         // `path.isAbsolute` catches a Windows drive-letter path, which would
         // otherwise stage nothing and fail the swap with a bare ENOENT. Same
         // pairing as environment_store's isLocalPath.
         await this.environmentStore.mountLocalDirectory(
            packageLocation,
            targetPath,
            environmentName,
            packageName,
         );
      }
   }
}
