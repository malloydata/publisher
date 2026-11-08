// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   recordPackageReload,
   type ReloadTrigger,
} from "../package_state_metrics";
import { components } from "../api";
import { normalizeModelPath } from "../constants";
import {
   getVersionPromotionMode,
   isPackageInlineFieldsEnabled,
   isVersioningEnabled,
} from "../config";
import { recordDeprecatedUsage } from "../deprecated_usage_metrics";
import type { HeaderSetter } from "../filter_deprecation";
import {
   BadRequestError,
   FrozenConfigError,
   internalErrorToHttpError,
   DryRunCapacityError,
   PackageAdmissionRefusedError,
   PackageVersionError,
   BuildPlanUnavailableError,
   ConflictError,
} from "../errors";
import { logger } from "../logger";
import type { RetrievalService } from "../retrieval/service";
import type { PackageAdmin } from "../service/environment_ports";
import type { Environment } from "../service/environment";
import type { Package } from "../service/package";
import { formatPublishRejections } from "../service/publish_checks";
import {
   type PackageDryRunOutcome,
   recordPackageDryRun,
} from "../package_load_metrics";
import type { VersionService } from "../service/versions/version_service";
import { versionManifestLocation } from "./manifest_location";
import { LOST_ON_A_RELOAD, changedFields } from "./versioned_patch";
import { loadedPackage } from "./loaded_package";
import {
   parseEnvironmentName,
   parsePackageName,
   VersionRef,
} from "../service/identity";

type ApiPackage = components["schemas"]["Package"];
type ApiModelEntities = components["schemas"]["ModelEntities"];
type ApiBuildPlan = components["schemas"]["BuildPlan"];
type ApiPackageServingState = components["schemas"]["PackageServingState"];
type ApiClassificationSummary = components["schemas"]["ClassificationSummary"];

/**
 * A reload's result: the package response, the serving state it moved to
 * (its new `servedRevision` and `sourceContentSha` among them), and which
 * path ran.
 */
export type ReloadedPackage = {
   metadata: ApiPackage;
   operational: ReturnType<Package["getOperationalFields"]>;
   mode: PackageReloadMode;
};

/**
 * Which path a reload took. `in-place` recompiles the tree already on disk and
 * leaves it alone; `reinstalled` re-fetches from the package's install location,
 * which overwrites on-disk edits. `unchanged` answers for a published version,
 * which is immutable: nothing is recompiled or re-fetched.
 */
export type PackageReloadMode = "in-place" | "reinstalled" | "unchanged";

/**
 * Say, on a publish from a location with package versioning off, that the
 * package is installed in place as before versions, and that versions will be
 * required. PackageController says it once per package (see
 * warnedVersioningOff), so a package republished routinely does not repeat it.
 */
export function warnVersioningOff(packageName: string): void {
   logger.warn(
      `Package versioning is off, so package ${packageName} is installed in place, replacing the previous publish, as before versions. Versions will be required in a future release: set PUBLISHER_PACKAGE_VERSIONING=on and give publisher.json a semantic "version" (such as "1.0.0") to publish immutable versions.`,
      { packageName },
   );
}

/**
 * Say, on a `PATCH …/packages/{pkg}` to a package with published versions,
 * that the route is deprecated and what replaces it. It keeps working so that
 * callers which predate versions do not break. PackageController says it once
 * per such package (see warnedPackagePatch), so a caller that rebinds
 * routinely does not repeat it; a package with no versions has no replacement
 * route, so it is not warned. Every request is still counted in
 * `publisher_deprecated_usage_total`.
 */
export function warnPackagePatchDeprecated(
   environmentName: string,
   packageName: string,
): void {
   logger.warn(
      `PATCH .../packages/${packageName} is deprecated and will be removed. To rebind a published version to a build manifest, use PUT .../packages/${packageName}/versions/{version}/manifest; to change a package's content, publish it again, as a new version when versioning is on.`,
      { environmentName, packageName },
   );
}

/**
 * Mark a `PATCH …/packages/{pkg}` response deprecated (RFC 8594), linking the
 * version routes that replace it, as the `#(filter)` fields are marked.
 */
export function setPackagePatchDeprecationHeaders(res: HeaderSetter): void {
   res.setHeader("Deprecation", "true");
   res.setHeader(
      "Link",
      '<https://github.com/malloydata/publisher/blob/main/docs/package-versions.md>; rel="deprecation"; type="text/markdown"',
   );
}

export class PackageController {
   private environmentStore: PackageAdmin;
   private retrieval: RetrievalService;
   /** Packages (`env/pkg`) already warned that versioning is off, this run. */
   private warnedVersioningOff = new Set<string>();
   /** Packages with versions (`env/pkg`) already warned that their PATCH is deprecated, this run. */
   private warnedPackagePatch = new Set<string>();

   /**
    * `retrieval` is the composition root's one instance; a spec that builds a
    * controller alone gets a store-backed one.
    */
   constructor(environmentStore: PackageAdmin, retrieval: RetrievalService) {
      this.environmentStore = environmentStore;
      this.retrieval = retrieval;
   }

   public async listPackages(environmentName: string): Promise<ApiPackage[]> {
      const environment = await this.environmentStore.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      return environment.listPackages();
   }

   public async getPackage(
      environmentName: string,
      packageName: string,
      reload: boolean,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiPackage> {
      let metadata: ApiPackage;
      if (reload) {
         const reloaded = await this.reloadPackage(
            environmentName,
            packageName,
            "rest",
            versionId,
         );
         // The reload's receipt rides its own response, whatever
         // PUBLISHER_PACKAGE_INLINE_FIELDS says, so a caller needs no second
         // read that another reload could race.
         metadata = {
            ...reloaded.metadata,
            servedRevision: reloaded.operational.servedRevision,
            sourceContentSha: reloaded.operational.sourceContentSha,
         };
      } else {
         const environment = await this.environmentStore.getEnvironment(
            parseEnvironmentName(environmentName),
            false,
         );
         const _package = await environment.getPackage(
            parsePackageName(packageName),
            false,
            {
               versionId,
            },
         );
         metadata = _package.getPackageMetadata();
         // Serving state (`GET …/status`), on a package response only while
         // PUBLISHER_PACKAGE_INLINE_FIELDS is on.
         if (isPackageInlineFieldsEnabled()) {
            metadata.status = environment.describePackageStatus(packageName);
         }
         // A published version also carries the package's current `latest`.
         if (metadata.versionId) {
            metadata = await environment.describePackage(_package);
         }
      }

      // `GET …/embedding-index` and `GET …/classification`, on a package
      // response only while PUBLISHER_PACKAGE_INLINE_FIELDS is on, on both
      // paths, so `?reload=true` (the request that invalidates the index)
      // carries them too. Classification only on this single-package read: a
      // listing and the `/status` poll never walk fields.
      const inline = isPackageInlineFieldsEnabled();
      const embeddingIndex = inline
         ? await this.embeddingIndexStatus(
              environmentName,
              packageName,
              versionId,
           )
         : undefined;
      const classification = inline
         ? await this.inlineClassification(
              environmentName,
              packageName,
              versionId,
           )
         : undefined;
      return {
         ...metadata,
         ...(embeddingIndex ? { embeddingIndex } : {}),
         ...(classification ? { classification } : {}),
      };
   }

   /**
    * The deprecated inline `classification`: absent when nothing published
    * is classified or the package cannot be read, never a failed request.
    */
   private async inlineClassification(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiClassificationSummary | undefined> {
      try {
         const pkg = await this.loadedPackage(
            environmentName,
            packageName,
            versionId,
         );
         return pkg.getClassificationSummary();
      } catch (error) {
         logger.debug("Could not read the package's classification summary", {
            environmentName,
            packageName,
            error: error instanceof Error ? error.message : String(error),
         });
         return undefined;
      }
   }

   private environment(environmentName: string) {
      return this.environmentStore.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
   }

   /** The loaded package `versionId` names, for a sub-resource read. */
   private loadedPackage(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ) {
      return loadedPackage(
         this.environmentStore,
         environmentName,
         packageName,
         versionId,
      );
   }

   /**
    * `GET …/status`: how this server holds the package (or the named version)
    * right now. Never loads it.
    */
   public async getPackageServingState(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiPackageServingState> {
      const environment = await this.environment(environmentName);
      return environment.describeServingState(packageName, versionId);
   }

   /**
    * `GET …/entities`: the package's surface, as `GET …/models/{path}` shows
    * it, or with `includeHiddenFilesAndSources` every file and source, for a
    * host that indexes them.
    */
   public async getPackageEntities(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
      includeHiddenFilesAndSources = false,
   ): Promise<ApiModelEntities[]> {
      const pkg = await this.loadedPackage(
         environmentName,
         packageName,
         versionId,
      );
      return includeHiddenFilesAndSources
         ? [...(pkg.getEntities() ?? [])]
         : pkg.getSurfaceEntities();
   }

   /**
    * `GET …/build-plan`: the persist build plan of the load; an empty plan
    * when the package declares no persist source, and 503 when the plan could
    * not be derived at load.
    */
   public async getPackageBuildPlan(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiBuildPlan> {
      const pkg = await this.loadedPackage(
         environmentName,
         packageName,
         versionId,
      );
      if (pkg.isBuildPlanUnavailable()) {
         throw new BuildPlanUnavailableError(
            `The build plan of package ${packageName} could not be derived at load. Reload the package to derive it again; retrying the read does not.`,
         );
      }
      return pkg.getBuildPlan() ?? { graphs: [], sources: {} };
   }

   /**
    * `GET …/classification`: each published entry point's `#(carries)` and
    * field count by class; `sources` is empty when nothing published is
    * classified.
    */
   public async getPackageClassification(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiClassificationSummary> {
      const pkg = await this.loadedPackage(
         environmentName,
         packageName,
         versionId,
      );
      return pkg.getClassificationSummary() ?? { sources: [] };
   }

   /** `GET …/embedding-index`: the state of the package's semantic index. */
   public async getPackageEmbeddingIndex(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<NonNullable<ApiPackage["embeddingIndex"]>> {
      // The version named, so an unknown one is a 404 and a pinned read
      // describes that version's index, not latest's.
      const pkg = await this.loadedPackage(
         environmentName,
         packageName,
         versionId,
      );
      return this.retrieval.status(environmentName, packageName, pkg);
   }

   /**
    * The package's semantic-index state (`lexical` with no embedding
    * provider), or undefined when unreadable. Never fails the request: a
    * reporting field must not turn a working GET into a 500.
    */
   private async embeddingIndexStatus(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ApiPackage["embeddingIndex"] | undefined> {
      try {
         // The version named, as `GET …/embedding-index` reads it.
         const pkg = await this.loadedPackage(
            environmentName,
            packageName,
            versionId,
         );
         return await this.retrieval.status(environmentName, packageName, pkg);
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
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ReloadedPackage> {
      try {
         const reloaded = await this.reloadPackageFrom(
            environmentName,
            packageName,
            versionId,
         );
         recordPackageReload(trigger, "success");
         return reloaded;
      } catch (error) {
         recordPackageReload(trigger, "failure");
         throw error;
      }
   }

   private async reloadPackageFrom(
      environmentName: string,
      packageName: string,
      versionId: VersionRef = VersionRef.LATEST,
   ): Promise<ReloadedPackage> {
      const environment = await this.environmentStore.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );

      // The version named is resolved first: a package with none has no
      // version to name (404), and a malformed one is a 400. A published
      // version is immutable, so it has nothing to reload: it is answered as
      // it is.
      const versions = environment.getVersionService();
      if (
         versions &&
         (await versions.resolve(packageName, versionId.requested()))
      ) {
         const version = await environment.getPackage(
            parsePackageName(packageName),
            false,
            {
               versionId,
            },
         );
         return {
            metadata: await environment.describePackage(version),
            operational: version.getOperationalFields(),
            mode: "unchanged",
         };
      }

      // Read the location from cached metadata without triggering a reload.
      // With a location, reinstall so download-then-load is atomic;
      // otherwise reload the on-disk tree in place.
      let resident: ApiPackage | undefined;
      try {
         const cached = await environment.getPackage(
            parsePackageName(packageName),
            false,
         );
         resident = cached.getPackageMetadata();
      } catch {
         // Not previously loaded, so there is nothing to reinstall from.
      }
      const location = resident?.location;
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
            operational: reinstalled.getOperationalFields(),
            mode: "reinstalled",
         };
      }
      const _package = await environment.getPackage(
         parsePackageName(packageName),
         true,
      );
      return {
         metadata: _package.getPackageMetadata(),
         operational: _package.getOperationalFields(),
         mode: "in-place",
      };
   }

   /**
    * `POST …/packages?dryRun=true`: what a publish of the tree at `location`
    * would answer, with nothing installed. A 200 is the package as it would
    * load, with the `location` it came from and, with versioning on, the
    * `versionId` it would publish as. It carries no `latestVersion` or
    * `status`: those describe the package being served, which a dry run does
    * not change. Refused on a frozen server like a publish, since it still
    * downloads and loads the caller's content.
    */
   async dryRunPackage(
      environmentName: string,
      body: ApiPackage,
   ): Promise<ApiPackage> {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      if (!body.name) {
         throw new BadRequestError("Package name is required");
      }
      const bodyLocation: unknown = body.location;
      if (typeof bodyLocation !== "string" || bodyLocation.length === 0) {
         throw new BadRequestError(
            "A dry run needs `location`: the tree it loads.",
         );
      }
      const packageName = body.name;
      const environment = await this.environmentStore.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      const versioned = Boolean(
         environment.getVersionService() && isVersioningEnabled(),
      );
      // The request's own fields, refused as a versioned publish refuses them.
      if (versioned) versionedPublishFields(body);
      const started = performance.now();
      let outcome: PackageDryRunOutcome = "loaded";
      try {
         return await environment.dryRunInstall(
            packageName,
            (stagingPath) =>
               this.downloadInto(
                  environmentName,
                  packageName,
                  bodyLocation,
                  stagingPath,
               ),
            {
               validate: (loaded) => formatPublishRejections(loaded),
               // A versioned publish refuses a tree with no valid version.
               readVersion: versioned,
               describe: (pkg, versionId) => ({
                  ...pkg.getPackageMetadata(),
                  location: bodyLocation,
                  ...(versionId !== undefined && { versionId }),
               }),
            },
         );
      } catch (error) {
         outcome = dryRunOutcome(error);
         throw error;
      } finally {
         recordPackageDryRun(outcome, performance.now() - started);
         logger.info("Package dry run", {
            environmentName,
            packageName,
            outcome,
            durationMs: Math.round(performance.now() - started),
         });
      }
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
         parseEnvironmentName(environmentName),
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
      // Strict at publish: the author is in the loop here, so reject a bad
      // explores with an actionable 400 instead of silently serving a hidden
      // surface. (At startup/reload we fail safe and only warn — see
      // Package.loadViaWorker.) The rollback differs by path so a rejected
      // publish never destroys user content:
      //   - location, versioning on: a published version. Its tree was
      //     downloaded into a fresh folder and the checks run before it is
      //     recorded, so a failure removes only that download.
      //   - location, versioning off: the tree was just downloaded into a
      //     fresh canonical, so validation runs inside installPackage's swap
      //     window and a failure wipes that download (the existing rollback)
      //     — nothing pre-existing to lose.
      //   - no-location: addPackage registered a *pre-existing* user directory,
      //     so we validate after the fact and `unloadPackage` (evict from
      //     memory, keep the files) rather than delete it.
      let result;
      try {
         if (body.location) {
            const bodyLocation: unknown = body.location;
            if (typeof bodyLocation !== "string") {
               throw new BadRequestError("`location` must be a string.");
            }
            const download = (stagingPath: string) =>
               this.downloadInto(
                  environmentName,
                  packageName,
                  bodyLocation,
                  stagingPath,
               );
            // Versioning on: the tree is published as the immutable version
            // its publisher.json declares, and one that declares none is
            // refused (400).
            if (versions && isVersioningEnabled()) {
               return await this.publishVersion(
                  versions,
                  packageName,
                  download,
                  bodyLocation,
                  body,
               );
            }
            // Versioning off, the transition default: installed in
            // place, as every publish from a location was before versions,
            // whatever its publisher.json declares. Versions already published
            // are not replaced.
            if (versions && (await versions.isVersioned(packageName))) {
               throw new PackageVersionError(
                  "PACKAGE_IS_VERSIONED",
                  `Package ${packageName} has published versions, which are immutable, and package versioning is off, so this publish would replace them. Set PUBLISHER_PACKAGE_VERSIONING=on to publish a new version.`,
               );
            }
            const warnKey = `${environmentName}/${packageName}`;
            if (!this.warnedVersioningOff.has(warnKey)) {
               this.warnedVersioningOff.add(warnKey);
               warnVersioningOff(packageName);
            }
            result = await environment.installPackage(
               packageName,
               download,
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

   /**
    * Publish the downloaded tree as the version its publisher.json declares:
    * the request carries no version. The request's own fields are checked
    * before anything is downloaded.
    */
   private async publishVersion(
      versions: VersionService<Package>,
      packageName: string,
      download: (stagingPath: string) => Promise<void>,
      location: string,
      body: ApiPackage,
   ): Promise<Package> {
      const { manifestLocation } = versionedPublishFields(body);
      const published = await versions.publish(packageName, download, {
         sourceLocation: location,
         manifestLocation,
         description: body.description ?? undefined,
         promotion: getVersionPromotionMode(),
         validate: (pkg) => formatPublishRejections(pkg),
      });
      return published.loaded;
   }

   public async deletePackage(environmentName: string, packageName: string) {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const environment = await this.environmentStore.getEnvironment(
         parseEnvironmentName(environmentName),
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
      // Deprecated, and kept working for callers that predate versions: count
      // every request, whatever it answers.
      recordDeprecatedUsage("package_patch");
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      const environment = await this.environmentStore.getEnvironment(
         parseEnvironmentName(environmentName),
         false,
      );
      const versions = environment.getVersionService();
      if (versions && (await versions.isVersioned(packageName))) {
         // Warned once per package, and only for a package with published
         // versions: those have a replacement to name, and recording only a
         // package that exists keeps a caller from growing the set with names.
         const warnKey = `${environmentName}/${packageName}`;
         if (!this.warnedPackagePatch.has(warnKey)) {
            this.warnedPackagePatch.add(warnKey);
            warnPackagePatchDeprecated(environmentName, packageName);
         }
         return this.updateVersionedPackage(
            environment,
            versions,
            packageName,
            body,
         );
      }
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
      if (installedFrom) {
         this.refuseChangesAReloadDiscards(
            environment,
            packageName,
            installedFrom,
            body,
         );
      }
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
    * Refuse a PATCH of a package installed from a `location` that changes
    * anything a reload would discard. A reload fetches the package again from
    * that location and replaces its files, `publisher.json` included, keeping
    * only the location and the manifest binding (see reloadPackageFrom). A
    * body that only echoes the package back is accepted, since clients send
    * whole objects. Interim, until package storage keeps server-side changes
    * apart from the fetched copy (#1560).
    */
   private refuseChangesAReloadDiscards(
      environment: Environment,
      packageName: string,
      installedFrom: string,
      body: ApiPackage,
   ): void {
      const resident = environment.peekPackage(packageName);
      if (!resident) return;
      const fields = body as Record<string, unknown>;
      const written = Object.fromEntries(
         LOST_ON_A_RELOAD.filter((key) => key in fields).map((key) => [
            key,
            fields[key],
         ]),
      );
      const changed = changedFields(
         written,
         resident.getPackageMetadata() as Record<string, unknown>,
         packageName,
         installedFrom,
         new Set(),
      );
      if (changed.length > 0) {
         throw new ConflictError(
            `Package ${packageName} was installed from ${installedFrom}, and a reload fetches it again from there, which would discard this request's change to ${changed.join(", ")}. Make the change in the package's source and reload it, or publish it as a version.`,
         );
      }
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
         await environment.getPackage(parsePackageName(packageName), false, {
            versionId: VersionRef.parse(latest),
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
         : await environment.getPackage(parsePackageName(packageName), false, {
              versionId: VersionRef.parse(latest),
           });
      return environment.describePackage(loaded);
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
      await this.environmentStore.downloadPackageInto(
         environmentName,
         packageName,
         packageLocation,
         targetPath,
      );
   }
}

/**
 * How a failed dry run ended. Classified without logging: the route logs the
 * error once when it answers.
 */
function dryRunOutcome(error: unknown): PackageDryRunOutcome {
   if (error instanceof DryRunCapacityError) {
      return "at_capacity";
   }
   if (error instanceof PackageAdmissionRefusedError) {
      return "backpressure";
   }
   return internalErrorToHttpError(error as Error, { log: false }).status < 500
      ? "refused"
      : "failed";
}

/**
 * The request fields a versioned publish checks before it downloads anything:
 * a `description` that is not a string, or a `manifestLocation` that names no
 * manifest, is a 400.
 */
function versionedPublishFields(body: ApiPackage): {
   manifestLocation: string | null | undefined;
} {
   const fields = body as Record<string, unknown>;
   if (
      fields.description !== undefined &&
      fields.description !== null &&
      typeof fields.description !== "string"
   ) {
      throw new BadRequestError("`description` must be a string.");
   }
   return {
      manifestLocation: versionManifestLocation(fields.manifestLocation),
   };
}
