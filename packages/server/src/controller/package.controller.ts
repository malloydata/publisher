// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { normalizeModelPath } from "../constants";
import { getVersionPromotionMode } from "../config";
import {
   BadRequestError,
   FrozenConfigError,
   internalErrorToHttpError,
   PackageAdmissionRefusedError,
   PackageVersionError,
} from "../errors";
import { logger } from "../logger";
import { getPackageEmbeddingStatus } from "../mcp/tools/get_context_tool";
import { EnvironmentStore } from "../service/environment_store";
import type { Environment } from "../service/environment";
import type { Package } from "../service/package";
import type { VersionService } from "../service/versions/version_service";
import { versionManifestLocation } from "./manifest_location";
import { changedFields } from "./versioned_patch";

type ApiPackage = components["schemas"]["Package"];

/**
 * Which path a reload took. `in-place` recompiles the tree already on disk and
 * leaves it alone; `reinstalled` re-fetches from the package's install location,
 * which overwrites on-disk edits. `unchanged` answers for a published version,
 * which is immutable: nothing is recompiled or re-fetched.
 */
export type PackageReloadMode = "in-place" | "reinstalled" | "unchanged";

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
 * of dimensions its source declares — see Package.formatInvalidPreaggregatePolicy),
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
      versionId?: unknown,
   ): Promise<ApiPackage> {
      let metadata: ApiPackage;
      if (reload) {
         metadata = (
            await this.reloadPackage(environmentName, packageName, versionId)
         ).metadata;
      } else {
         const environment = await this.environmentStore.getEnvironment(
            environmentName,
            false,
         );
         const _package = await environment.getPackage(packageName, false, {
            versionId,
         });
         metadata = _package.getPackageMetadata();
         metadata.status = environment.describePackageStatus(packageName);
         // A published version also carries the package's current `latest`.
         if (metadata.versionId) {
            metadata = await environment.describePackage(_package);
         }
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
    * The package's semantic-index state (`lexical` when the server has no
    * embedding provider), or undefined when the state could not be read.
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
      versionId?: unknown,
   ): Promise<{ metadata: ApiPackage; mode: PackageReloadMode }> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );

      // The version named is resolved first: a package with none has no
      // version to name (404), and a malformed one is a 400. A published
      // version is immutable, so it has nothing to reload: it is answered as
      // it is.
      const versions = environment.getVersionService();
      if (versions && (await versions.resolve(packageName, versionId))) {
         const version = await environment.getPackage(packageName, false, {
            versionId,
         });
         return {
            metadata: await environment.describePackage(version),
            mode: "unchanged",
         };
      }

      // Resolve the package's source location from the currently-cached
      // metadata WITHOUT triggering a stale-state reload. If a `location`
      // is set, route the reload through `installPackage` so that
      // download-then-load happens atomically; otherwise fall back to an
      // in-place reload of the existing on-disk content.
      let resident: ApiPackage | undefined;
      try {
         const cached = await environment.getPackage(packageName, false);
         resident = cached.getPackageMetadata();
      } catch {
         // Not previously loaded, so there is nothing to reinstall from.
      }
      const location = resident?.location;
      if (resident && location) {
         // The re-fetched tree carries what its author wrote. The two things
         // an orchestrator set on the served copy, where it was installed from
         // and which manifest it is bound to, are re-applied inside the
         // install, else the package would serve live until the next drift
         // check. Nothing else is: the author's description, surface and
         // policy are the new tree's to declare, and a reload stays fail-safe
         // rather than enforcing publish-time checks after the swap.
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
      const versions = environment.getVersionService();
      // Published versions are immutable, so a package that has them is not
      // replaced by registering a directory under its name.
      if (
         !body.location &&
         versions &&
         (await versions.isVersioned(packageName))
      ) {
         throw new PackageVersionError(
            "PACKAGE_IS_VERSIONED",
            `Package ${packageName} has published versions, which are immutable; publish a new version instead of replacing it.`,
         );
      }
      // Strict at publish: the author is in the loop here, so a bad explores
      // is an actionable 400 instead of a silently hidden surface. (At
      // startup/reload we fail safe and only warn — see Package.loadViaWorker.)
      // The rollback differs by path so a rejected publish never destroys
      // user content:
      //   - location: a published version. Its tree was downloaded into a
      //     fresh folder and the checks run before it is recorded, so a
      //     failure removes only that download.
      //   - no-location: addPackage registered a *pre-existing* user directory,
      //     so we validate after the fact and `unloadPackage` (evict from
      //     memory, keep the files) rather than delete it.
      let result;
      try {
         if (body.location) {
            if (!versions) {
               throw new Error(
                  `Environment ${environmentName} has no version registry`,
               );
            }
            const location: unknown = body.location;
            if (typeof location !== "string") {
               throw new BadRequestError("`location` must be a string.");
            }
            const fields = body as Record<string, unknown>;
            if (
               fields.description !== undefined &&
               fields.description !== null &&
               typeof fields.description !== "string"
            ) {
               throw new BadRequestError("`description` must be a string.");
            }
            const manifestLocation = versionManifestLocation(
               fields.manifestLocation,
            );
            // The version is the one the tree's own publisher.json declares;
            // the request carries none.
            const published = await versions.publish(
               packageName,
               (stagingPath) =>
                  this.downloadInto(
                     environmentName,
                     packageName,
                     location,
                     stagingPath,
                  ),
               {
                  sourceLocation: location,
                  manifestLocation,
                  description: body.description ?? undefined,
                  promotion: getVersionPromotionMode(
                     this.environmentStore.serverRootPath,
                  ),
                  validate: (pkg) => formatPublishRejections(pkg),
               },
            );
            return published.loaded;
         }
         result = await environment.addPackage(packageName);
      } catch (error) {
         // A failure on the server's side (5xx: a mount the server cannot
         // write, an unreachable bucket) is also an operator's problem, and
         // the caller that saw the response may be an orchestrator that never
         // shows it to one. Record it where /status reports load failures,
         // with the same message the response carries, so /status never says
         // more than the caller was told. A rejection of the package's own
         // content (4xx) is answered with its reason and is not recorded.
         // An admission refusal under memory back-pressure is an answer to
         // this request, not a failure of the package, and the caller places
         // the package elsewhere; recorded here it would read as a load
         // failure until this server next loaded that package, which it may
         // never do. Every other 5xx, a worker-pool failure included, is
         // recorded: that one carries the cause an operator has to fix.
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

      // `addPackage` is typed `Package | undefined`; a missing
      // result here is a should-never-happen internal fault. Fail loudly rather
      // than letting optional chaining silently skip the validation below.
      if (!result) {
         throw new Error(`Failed to create package ${packageName}`);
      }

      const invalidMsg = formatPublishRejections(result);
      if (invalidMsg) {
         await environment.unloadPackage(packageName).catch(() => {
            /* best-effort; the package is not persisted below */
         });
         throw new BadRequestError(invalidMsg);
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
      // The rows are removed by the environment, under the package's lock
      // (see Environment.deletePackage): a versioned package's first, an
      // unversioned one's after it is unloaded, as before.
      let recordsRemoved = false;
      const result = await environment.deletePackage(packageName, {
         removeRecords: async () => {
            await this.environmentStore.deletePackageFromDatabase(
               environmentName,
               packageName,
            );
            recordsRemoved = true;
         },
      });
      if (!recordsRemoved) {
         await this.environmentStore.deletePackageFromDatabase(
            environmentName,
            packageName,
         );
      }

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
      const versions = environment.getVersionService();
      if (versions && (await versions.isVersioned(packageName))) {
         return this.updateVersionedPackage(
            environment,
            versions,
            packageName,
            body,
         );
      }
      // A `location` that matches the one the package was installed from is a
      // metadata update, not a reinstall. A package version's content does not
      // change under one URI, so re-downloading and recompiling it would only
      // repeat work and hold two compiled copies for the duration; the rebind
      // an orchestrator sends after a materialization build is exactly this
      // shape. A caller that wants the same location fetched again reloads the
      // package instead.
      //
      // The decision is made against the copy that is resident once nothing
      // is loading. During an install the resident copy, if any, is the one
      // about to be replaced; deciding against it would start a second install
      // for the location already in flight, or treat a location whose install
      // has just failed as installed. Waiting first means a PATCH for the same
      // location lands on the installed copy, and a PATCH for a location whose
      // install failed installs it, as it did before. A location that is not a
      // string (a client that serializes unset fields as null) names nothing
      // to fetch.
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
         // Re-install: stream the new content into a staging dir (no lock)
         // and atomically swap it in (under the lock). Validate the effective
         // explores (the body override, else the new tree's own manifest)
         // INSIDE the swap window, so a rejected update rolls back to the
         // previous tree instead of swapping the bad one in and 400-ing after.
         // The rest of the body is applied after the swap commits but inside
         // the same lock hold, so nothing can run between the two; a policy
         // the body gets wrong is answered 400 with the new tree in place.
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
         // Apply metadata changes (publisher.json) under the per-package
         // mutex via `Environment.updatePackage`.
         result = await environment.updatePackage(packageName, body);
      }
      await this.environmentStore.addPackageToDatabase(
         environmentName,
         packageName,
      );

      return result;
   }

   /**
    * PATCH on a package that has published versions. Deprecated: a version's
    * content is immutable, so the only changes kept are the ones that are not
    * content, for callers that rebind through this route: `manifestLocation`
    * (a URI is bound to the package's `latest` version, as
    * `PUT .../versions/{latest}/manifest` would bind it; a null leaves the
    * binding as it is) and the package's `description`.
    *
    * A body that echoes the package back is accepted, because clients send
    * whole objects: read-only fields and unset ones are ignored, and every
    * other field may carry the value latest has now (see changedFields). Only
    * a value that would change latest's content is refused, with 409
    * PACKAGE_IS_VERSIONED.
    */
   private async updateVersionedPackage(
      environment: Environment,
      versions: VersionService<Package>,
      packageName: string,
      body: ApiPackage,
   ): Promise<ApiPackage> {
      const fields = body as Record<string, unknown>;
      if (
         fields.description !== undefined &&
         fields.description !== null &&
         typeof fields.description !== "string"
      ) {
         throw new BadRequestError("`description` must be a string.");
      }
      const manifestLocation = versionManifestLocation(fields.manifestLocation);
      const latest = await versions.latestOf(packageName);
      const refuse = (why: string) =>
         new PackageVersionError(
            "PACKAGE_IS_VERSIONED",
            `Package ${packageName} has published versions, which are immutable: ${why} Publish a new version to change its content, and rebind a version's manifest with PUT .../versions/{version}/manifest.`,
         );
      if (latest === null) {
         throw refuse("it has no latest version to apply this to.");
      }
      const version = await versions.getVersion(packageName, latest);
      const current = (await environment.describePackage(
         await environment.getPackage(packageName, false, {
            versionId: latest,
         }),
      )) as Record<string, unknown>;
      const changed = changedFields(
         fields,
         current,
         packageName,
         version.sourceLocation,
      );
      if (changed.length > 0) {
         throw refuse(`this request changes ${changed.join(", ")}.`);
      }

      // A description is the package's own once a request sets one; one that
      // only echoes what the package reads as now is not set, so a package
      // with none goes on reading as its latest version's.
      if (
         typeof fields.description === "string" &&
         fields.description !== "" &&
         fields.description !== current.description
      ) {
         await versions.setPackageDescription(packageName, fields.description);
      }
      // Only a manifest URI rebinds. A null or empty one is unset, as every
      // other field a client sends back unset is, and as it is on a publish:
      // a generated client serializes a field it was never given as null,
      // which would otherwise turn latest back to serving live. Clearing a
      // binding is PUT .../versions/{version}/manifest's.
      const loaded = manifestLocation
         ? await versions.setManifest(packageName, latest, manifestLocation)
         : await environment.getPackage(packageName, false, {
              versionId: latest,
           });
      return environment.describePackage(loaded);
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
      await this.environmentStore.downloadPackageInto(
         environmentName,
         packageName,
         packageLocation,
         targetPath,
      );
   }
}
