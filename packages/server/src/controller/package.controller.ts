// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   recordPackageReload,
   type ReloadTrigger,
} from "../package_state_metrics";
import { components } from "../api";
import {
   getVersionPromotionMode,
   isPackageInlineFieldsEnabled,
} from "../config";
import {
   BadRequestError,
   FrozenConfigError,
   internalErrorToHttpError,
   DryRunCapacityError,
   PackageAdmissionRefusedError,
   BuildPlanUnavailableError,
} from "../errors";
import { logger } from "../logger";
import { getPackageEmbeddingStatus } from "../mcp/tools/get_context_tool";
import { EnvironmentStore } from "../service/environment_store";
import type { Package } from "../service/package";
import { formatPublishRejections } from "../service/publish_checks";
import {
   type PackageDryRunOutcome,
   recordPackageDryRun,
} from "../package_load_metrics";
import type { VersionService } from "../service/versions/version_service";
import { versionManifestLocation } from "./manifest_location";

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
 * Which path a reload took. `in-place` recompiles the tree already on disk (a
 * package a watched environment mounts from its source). `unchanged` answers
 * for a published version, which is immutable: nothing is recompiled.
 */
export type PackageReloadMode = "in-place" | "unchanged";

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
            environmentName,
            false,
         );
         const _package = await environment.getPackage(packageName, false, {
            versionId,
         });
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
      versionId?: unknown,
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
      return this.environmentStore.getEnvironment(environmentName, false);
   }

   /** The loaded package `versionId` names, for a sub-resource read. */
   private async loadedPackage(
      environmentName: string,
      packageName: string,
      versionId?: unknown,
   ) {
      const environment = await this.environment(environmentName);
      return environment.getPackage(packageName, false, { versionId });
   }

   /**
    * `GET …/status`: how this server holds the package (or the named version)
    * right now. Never loads it.
    */
   public async getPackageServingState(
      environmentName: string,
      packageName: string,
      versionId?: unknown,
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
      versionId?: unknown,
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
      versionId?: unknown,
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
      versionId?: unknown,
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
      versionId?: unknown,
   ): Promise<NonNullable<ApiPackage["embeddingIndex"]>> {
      // The version named, so an unknown one is a 404 and a pinned read
      // describes that version's index, not latest's.
      const pkg = await this.loadedPackage(
         environmentName,
         packageName,
         versionId,
      );
      return getPackageEmbeddingStatus(
         this.environmentStore,
         environmentName,
         packageName,
         pkg,
      );
   }

   /**
    * The package's semantic-index state (`lexical` with no embedding
    * provider), or undefined when unreadable. Never fails the request: a
    * reporting field must not turn a working GET into a 500.
    */
   private async embeddingIndexStatus(
      environmentName: string,
      packageName: string,
      versionId?: unknown,
   ): Promise<ApiPackage["embeddingIndex"] | undefined> {
      try {
         // The version named, as `GET …/embedding-index` reads it.
         const pkg = await this.loadedPackage(
            environmentName,
            packageName,
            versionId,
         );
         return await getPackageEmbeddingStatus(
            this.environmentStore,
            environmentName,
            packageName,
            pkg,
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
    * Reload a package and report which path ran: a published version is
    * answered unchanged, and only a package mounted in place recompiles.
    */
   public async reloadPackage(
      environmentName: string,
      packageName: string,
      trigger: ReloadTrigger = "rest",
      versionId?: unknown,
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
      versionId?: unknown,
   ): Promise<ReloadedPackage> {
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
            operational: version.getOperationalFields(),
            mode: "unchanged",
         };
      }

      const _package = await environment.getPackage(packageName, true);
      return {
         metadata: _package.getPackageMetadata(),
         operational: _package.getOperationalFields(),
         mode: "in-place",
      };
   }

   /**
    * `POST …/packages?dryRun=true`: what a publish of the tree at `location`
    * would answer, with nothing installed. A 200 is the package as it would
    * load, with the `location` it came from and the `versionId` it would
    * publish as. It carries no `latestVersion` or
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
         environmentName,
         false,
      );
      // The request's own fields, refused as a publish refuses them.
      versionedPublishFields(body);
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
               describe: (pkg, versionId) => ({
                  ...pkg.getPackageMetadata(),
                  location: bodyLocation,
                  versionId,
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

   /**
    * `POST …/packages`: publish the tree at `location` as the immutable version
    * its publisher.json declares. A request without a `location` is refused:
    * every package is served as a published version (a watched environment's
    * packages are its config's, mounted in place).
    */
   async addPackage(environmentName: string, body: ApiPackage) {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
      if (!body.name) {
         throw new BadRequestError("Package name is required");
      }
      const packageName = body.name;
      const bodyLocation: unknown = body.location;
      if (
         bodyLocation === undefined ||
         bodyLocation === null ||
         bodyLocation === ""
      ) {
         throw new BadRequestError(
            "A publish needs `location`: the tree to publish as the version its publisher.json declares.",
         );
      }
      if (typeof bodyLocation !== "string") {
         throw new BadRequestError("`location` must be a string.");
      }
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const versions = environment.getVersionService();
      try {
         if (!versions) {
            throw new Error(
               `Package versions are not available in environment ${environmentName}.`,
            );
         }
         // The tree is downloaded into a fresh folder and checked before it is
         // recorded, so a refused publish removes only that download and the
         // versions already published keep serving.
         return await this.publishVersion(
            versions,
            packageName,
            (stagingPath: string) =>
               this.downloadInto(
                  environmentName,
                  packageName,
                  bodyLocation,
                  stagingPath,
               ),
            bodyLocation,
            body,
         );
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
