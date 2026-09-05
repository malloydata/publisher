// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as path from "path";
import { components } from "../api";
import { normalizeModelPath } from "../constants";
import { BadRequestError, FrozenConfigError } from "../errors";
import { logger } from "../logger";
import { getPackageEmbeddingStatus } from "../mcp/tools/get_context_tool";
import { EnvironmentStore } from "../service/environment_store";

type ApiPackage = components["schemas"]["Package"];

/**
 * Which path a reload took. `in-place` recompiles the tree already on disk and
 * leaves it alone; `reinstalled` re-fetches from the package's install location,
 * which overwrites on-disk edits.
 */
export type PackageReloadMode = "in-place" | "reinstalled";

/**
 * Everything that is strict-at-publish, joined into one 400 message (or
 * undefined when the package is publishable): invalid explores entries plus
 * the Malloy Persistence policy gate (scope is package-level; a
 * `materialization.schedule` is package-root-only + version-scope-only and
 * mutually exclusive with freshness; per-source `sharing`/`schedule` are
 * retired — see Package.persistencePolicyWarnings), plus the incremental-refresh
 * gate (a `refresh="incremental"` source must declare a watermark that names a
 * real, orderable, non-aggregate output column, on a supported dialect — see
 * Package.incrementalPolicyWarnings), plus the pre-aggregation gate (a
 * `#@ preaggregate` must sit on a measure that can be re-aggregated, at a grain
 * of dimensions its source declares — see Package.preaggregatePolicyWarnings),
 * plus persist-target
 * collisions ONLY when `PERSIST_COLLISION_ENFORCE` is set (otherwise those are
 * surfaced warn-only so a pre-existing latent collision doesn't block a routine
 * re-publish — see Package.formatPersistenceCollisionRejections). At
 * startup/reload these are warn-only instead (fail-safe; see
 * Package.loadViaWorker) — except the incremental-refresh and pre-aggregation
 * gates, which fail the load there too, so a package that never passes through
 * this endpoint still gets its rejection.
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
         metadata = (await this.reloadPackage(environmentName, packageName))
            .metadata;
      } else {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const _package = await environment.getPackage(packageName, false);
         metadata = _package.getPackageMetadata();
      }

      // Enriched on BOTH paths. This sat below a `reload` early return, so
      // `?reload=true` answered without the field while a plain GET carried
      // it: one resource in two shapes, decided by a query param. And it was
      // absent from precisely the request that INVALIDATES the index, which
      // is when a caller starts the `status` polling the field exists for.
      const embeddingIndex = await this.embeddingIndexStatus(
         environmentName,
         packageName,
      );
      return embeddingIndex ? { ...metadata, embeddingIndex } : metadata;
   }

   /**
    * The package's semantic-index state, or undefined when the server has no
    * embedding provider (nothing to describe) or the state could not be read.
    *
    * Never fails the request: this is a reporting field on a resource whose
    * primary job is package metadata, so a storage handle that is not ready
    * must not turn a working GET into a 500.
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
    * Reload a package and report WHICH path ran. The two are not equivalent to
    * anyone holding on-disk edits: an in-place reload recompiles the tree that
    * is already there, while a reinstall re-fetches from the package's install
    * location and overwrites it. A caller that surfaces the reload to a user
    * needs to be able to say which happened, so it is returned rather than
    * inferred. `getPackage` keeps returning bare metadata, so the REST contract
    * is unchanged.
    */
   public async reloadPackage(
      environmentName: string,
      packageName: string,
   ): Promise<{ metadata: ApiPackage; mode: PackageReloadMode }> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );

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
         // Not previously loaded, so there is nothing to reinstall from.
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
         return {
            metadata: reinstalled.getPackageMetadata(),
            mode: "reinstalled",
         };
      }
      const _package = await environment.getPackage(packageName, true);
      return { metadata: _package.getPackageMetadata(), mode: "in-place" };
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
      // Strict at publish: the author is in the loop here, so reject a bad
      // explores with an actionable 400 instead of silently serving a hidden
      // surface. (At startup/reload we fail safe and only warn — see
      // Package.loadViaWorker.) The rollback differs by path so a rejected
      // publish never destroys user content:
      //   - location: the tree was just downloaded into a fresh canonical, so
      //     validation runs inside installPackage's swap window and a failure
      //     wipes that download (the existing rollback) — nothing pre-existing
      //     to lose.
      //   - no-location: addPackage registered a *pre-existing* user directory,
      //     so we validate after the fact and `unloadPackage` (evict from
      //     memory, keep the files) rather than delete it.
      let result;
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
         );
      } else {
         result = await environment.addPackage(packageName);
      }

      // `addPackage`/`installPackage` are typed `Package | undefined`; a missing
      // result here is a should-never-happen internal fault. Fail loudly rather
      // than letting optional chaining silently skip the validation below.
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
      if (body.location) {
         // Re-install: stream the new content into a staging dir (no lock)
         // and atomically swap it in (under the lock). Validate the effective
         // explores (the body override, else the new tree's own manifest)
         // INSIDE the swap window, so a rejected update rolls back to the
         // previous tree instead of swapping the bad one in and 400-ing after.
         const bodyLocation = body.location;
         await environment.installPackage(
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
    * Callers pass a sibling staging dir (not the canonical package
    * directory) so the long-running download doesn't hold the per-package
    * mutex.
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
         // Absolute paths from the publisher.config could be placed outside of /etc/publisher,
         // so we need to mount them on the right place. `path.isAbsolute` is
         // what catches a Windows drive-letter path (`D:\pkgs\sales`), which no
         // other branch here claims either — without it the install stages
         // nothing and the swap fails with a bare ENOENT rename. Same pairing
         // as environment_store's isLocalPath.
         await this.environmentStore.mountLocalDirectory(
            packageLocation,
            targetPath,
            environmentName,
            packageName,
         );
      }
   }
}
