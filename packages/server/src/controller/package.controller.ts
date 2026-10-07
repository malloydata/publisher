// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { components } from "../api";
import { getPackageVersioningMode, getVersionPromotionMode } from "../config";
import { API_PREFIX, normalizeModelPath } from "../constants";
import {
   BadRequestError,
   FrozenConfigError,
   internalErrorToHttpError,
   PackageAdmissionRefusedError,
   PackageNotFoundError,
   PackageVersionError,
} from "../errors";
import { logger } from "../logger";
import { getPackageEmbeddingStatus } from "../mcp/tools/get_context_tool";
import { EnvironmentStore } from "../service/environment_store";
import type { PackageVersion } from "../storage/DatabaseInterface";

type ApiPackage = components["schemas"]["Package"];
type ApiPackageVersion = components["schemas"]["PackageVersion"];

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
   private versionBuildCheck:
      | ((
           environmentName: string,
           packageName: string,
           versionId: string,
        ) => Promise<boolean>)
      | undefined;

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
      versionId?: string,
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
      versionId?: string,
   ): Promise<{ metadata: ApiPackage; mode: PackageReloadMode }> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );

      // A published version is immutable: there is nothing on disk newer than
      // what it serves, and re-fetching it could only produce the same tree.
      // So a reload of one reports the version as it is.
      if (environment.isVersionedPackage(packageName)) {
         const _package = await environment.getPackage(packageName, false, {
            versionId,
         });
         return { metadata: _package.getPackageMetadata(), mode: "in-place" };
      }
      if (versionId) {
         // Refused the same way a read naming a version of this package is.
         environment.resolveSlot(packageName, versionId);
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
               this.environmentStore.downloadPackageInto(
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
      // With package versioning on, a publish from a location is a published
      // version: immutable, numbered by the package's own publisher.json, and
      // recorded in the version registry. Without it, or without a location,
      // the package is the single unversioned slot it always was.
      // A location of null or "" is no location: such a publish is the
      // unversioned add, and writes its row below like any other.
      const versioned =
         Boolean(body.location) &&
         getPackageVersioningMode(this.environmentStore.serverRootPath) ===
            "on";
      let result;
      try {
         if (body.location && versioned) {
            const bodyLocation = body.location;
            result = await environment.publishPackageVersion(
               packageName,
               (stagingPath) =>
                  this.environmentStore.downloadPackageInto(
                     environmentName,
                     packageName,
                     bodyLocation,
                     stagingPath,
                  ),
               {
                  sourceLocation: bodyLocation,
                  promotion: getVersionPromotionMode(
                     this.environmentStore.serverRootPath,
                  ),
                  validate: (pkg) => formatPublishRejections(pkg),
                  manifestLocation: body.manifestLocation,
               },
            );
         } else if (body.location) {
            const bodyLocation = body.location;
            result = await environment.installPackage(
               packageName,
               (stagingPath) =>
                  this.environmentStore.downloadPackageInto(
                     environmentName,
                     packageName,
                     bodyLocation,
                     stagingPath,
                  ),
               (pkg) => formatPublishRejections(pkg),
               // The install records where it fetched from, and a publish that
               // names a manifest binds it, both under the install's own lock.
               // The downloaded tree's publisher.json carries neither: the
               // location is the caller's, and the orchestrator computes the
               // manifest, not the author. Without the location a later PATCH
               // naming the same location could not be told from new content;
               // without the manifest the package came up serving live and was
               // fully reloaded moments later by the drift check.
               {
                  update: {
                     location: bodyLocation,
                     // Only a manifest to bind. A fresh install serves live
                     // already, so a null or empty value has nothing to
                     // revert and would only recompile the package a second
                     // time.
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

      // A versioned publish already wrote the package's registry row.
      if (!versioned) {
         await this.environmentStore.addPackageToDatabase(
            environmentName,
            packageName,
         );
      }

      return result;
   }

   /**
    * The package's published versions, highest first, as the versions
    * resource returns them. An unversioned package has none; a package this
    * environment does not have at all is 404.
    */
   public async listPackageVersions(
      environmentName: string,
      packageName: string,
   ): Promise<ApiPackageVersion[]> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const { latest, versions } = environment.listPackageVersions(packageName);
      if (
         versions.length === 0 &&
         environment.getPackageStatus(packageName) === undefined
      ) {
         throw new PackageNotFoundError(`Package ${packageName} not found`);
      }
      return versions.map((v) =>
         toApiPackageVersion(environmentName, v, latest),
      );
   }

   /** One published version, archived or not. */
   public async getPackageVersion(
      environmentName: string,
      packageName: string,
      versionId: string,
   ): Promise<ApiPackageVersion> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const { latest, versions } = environment.listPackageVersions(packageName);
      const version = versions.find((v) => v.version === versionId);
      if (!version) {
         throw new PackageVersionError(
            "VERSION_NOT_FOUND",
            `Package ${packageName} has no version ${versionId}.`,
         );
      }
      return toApiPackageVersion(environmentName, version, latest);
   }

   /**
    * Point the package's `latest` at a published, unarchived version. This
    * is how an orchestrator running `versionPromotion: "explicit"` promotes,
    * and how anyone rolls back.
    */
   public async setLatestVersion(
      environmentName: string,
      packageName: string,
      body: unknown,
   ): Promise<ApiPackageVersion> {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const versionId = (body as { versionId?: unknown } | undefined)
         ?.versionId;
      if (typeof versionId !== "string" || versionId === "") {
         throw new BadRequestError(
            "versionId must be a string naming one of the package's published versions.",
         );
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const version = await environment.setLatestVersion(
         packageName,
         versionId,
      );
      return toApiPackageVersion(environmentName, version, versionId);
   }

   /**
    * Bind one version to a build manifest, or back to live with null. Answers
    * the version as a package resource, which is where the binding's outcome
    * (`manifestBindingStatus`, `boundManifestUri`) is reported.
    */
   public async setVersionManifest(
      environmentName: string,
      packageName: string,
      versionId: string,
      body: unknown,
   ): Promise<ApiPackage> {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const fields = (body ?? {}) as { manifestLocation?: unknown };
      if (
         !("manifestLocation" in fields) ||
         (fields.manifestLocation !== null &&
            (typeof fields.manifestLocation !== "string" ||
               fields.manifestLocation === ""))
      ) {
         throw new BadRequestError(
            "manifestLocation is required: a manifest URI, or null to serve the version live.",
         );
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const pkg = await environment.setVersionManifest(
         packageName,
         versionId,
         fields.manifestLocation as string | null,
      );
      return pkg.getPackageMetadata();
   }

   /**
    * Archive or unarchive one version. Refused while a materialization of
    * that version is running, since archiving reclaims the tables it is
    * writing.
    */
   public async updatePackageVersion(
      environmentName: string,
      packageName: string,
      versionId: string,
      body: unknown,
   ): Promise<ApiPackageVersion> {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const archiveStatus = (body as { archiveStatus?: unknown } | undefined)
         ?.archiveStatus;
      if (archiveStatus !== "archive" && archiveStatus !== "unarchive") {
         throw new BadRequestError(
            'archiveStatus must be "archive" or "unarchive".',
         );
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const check = this.versionBuildCheck;
      const version = await environment.setVersionArchiveStatus(
         packageName,
         versionId,
         archiveStatus,
         {
            // Asked under the package lock, after the latest check, so the
            // answer is never stale by the time the archive commits.
            isBuilding: check
               ? () => check(environmentName, packageName, versionId)
               : undefined,
         },
      );
      return toApiPackageVersion(
         environmentName,
         version,
         environment.listPackageVersions(packageName).latest,
      );
   }

   /**
    * Say how to tell whether a version has a materialization running. Set by
    * the server once the materialization service exists; without it an
    * archive is never refused for a running build.
    */
   public setVersionBuildCheck(
      check:
         | ((
              environmentName: string,
              packageName: string,
              versionId: string,
           ) => Promise<boolean>)
         | null,
   ): void {
      this.versionBuildCheck = check ?? undefined;
   }

   public async deletePackage(environmentName: string, packageName: string) {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      // The rows go under the package lock, with the files: see
      // Environment.deletePackage.
      const result = await environment.deletePackage(packageName, {
         forget: () =>
            this.environmentStore.deletePackageFromDatabase(
               environmentName,
               packageName,
            ),
      });

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
               this.environmentStore.downloadPackageInto(
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
}

/** A registry version as the versions resource returns it. */
function toApiPackageVersion(
   environmentName: string,
   version: PackageVersion,
   latest: string | null,
): ApiPackageVersion {
   return {
      resource: `${API_PREFIX}/environments/${environmentName}/packages/${version.packageName}/versions/${encodeURIComponent(version.version)}`,
      packageName: version.packageName,
      id: version.version,
      latest: version.version === latest,
      archiveStatus: version.archiveStatus,
      archivedAt: version.archivedAt ? version.archivedAt.toISOString() : null,
      ...(version.description !== null
         ? { description: version.description }
         : {}),
      ...(version.sourceLocation !== null
         ? { location: version.sourceLocation }
         : {}),
      contentHash: version.contentHash,
      manifestLocation: version.manifestLocation,
      gitCommitSha: version.gitCommitSha,
      gitRef: version.gitRef,
      createdAt: version.createdAt.toISOString(),
      updatedAt: version.updatedAt.toISOString(),
   };
}
