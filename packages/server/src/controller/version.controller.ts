// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   isManifestLocation,
   MANIFEST_LOCATION_FORMS,
} from "./manifest_location";
import { components } from "../api";
import { API_PREFIX } from "../constants";
import {
   BadRequestError,
   FrozenConfigError,
   PackageNotFoundError,
} from "../errors";
import type { EnvironmentStore } from "../service/environment_store";
import type { Package } from "../service/package";
import type { VersionService } from "../service/versions/version_service";
import type { Version } from "../storage/DatabaseInterface";

type ApiPackageVersion = components["schemas"]["PackageVersion"];
type ApiPackage = components["schemas"]["Package"];

/**
 * A package's published versions and their lifecycle: list, get, archive and
 * unarchive, delete, manifest binding, and the `latest` pointer. The rules
 * live in the environment's VersionService; this is their HTTP shape.
 */
export class VersionController {
   constructor(private readonly environmentStore: EnvironmentStore) {}

   async listVersions(
      environmentName: string,
      packageName: string,
   ): Promise<ApiPackageVersion[]> {
      const { versions, environment } = await this.service(environmentName);
      const listed = await versions.listVersions(packageName);
      // A package the server does not hold at all is a 404; one it holds with
      // no versions has none to list.
      if (
         listed.versions.length === 0 &&
         environment.getPackageStatus(packageName) === undefined
      ) {
         throw new PackageNotFoundError(
            `Package ${packageName} not found in environment ${environmentName}`,
         );
      }
      return listed.versions.map((v) =>
         toApiVersion(environmentName, v, listed.latest),
      );
   }

   async getVersion(
      environmentName: string,
      packageName: string,
      version: unknown,
   ): Promise<ApiPackageVersion> {
      const { versions } = await this.service(environmentName);
      const row = await versions.getVersion(packageName, version);
      return toApiVersion(
         environmentName,
         row,
         await versions.latestOf(packageName),
      );
   }

   async updateVersion(
      environmentName: string,
      packageName: string,
      version: unknown,
      body: unknown,
   ): Promise<ApiPackageVersion> {
      this.assertWritable();
      const status = (body as { archiveStatus?: unknown } | null)
         ?.archiveStatus;
      if (status !== "archive" && status !== "unarchive") {
         throw new BadRequestError(
            '`archiveStatus` must be "archive" or "unarchive".',
         );
      }
      const { versions } = await this.service(environmentName);
      const row = await versions.setArchiveStatus(packageName, version, status);
      return toApiVersion(
         environmentName,
         row,
         await versions.latestOf(packageName),
      );
   }

   /**
    * Delete a version for good. Answers the version as it was. When it was
    * the package's only version the package went with it, and so do the
    * package's cached embeddings (dropped where the package is removed, so
    * also when a publish the delete waited for removes it).
    */
   async deleteVersion(
      environmentName: string,
      packageName: string,
      version: unknown,
   ): Promise<ApiPackageVersion> {
      this.assertWritable();
      const { versions } = await this.service(environmentName);
      const deleted = await versions.deleteVersion(packageName, version);
      return toApiVersion(environmentName, deleted.version, null);
   }

   async setManifest(
      environmentName: string,
      packageName: string,
      version: unknown,
      body: unknown,
   ): Promise<ApiPackage> {
      this.assertWritable();
      const fields = (body ?? {}) as { manifestLocation?: unknown };
      const location = fields.manifestLocation;
      // A location the manifest loader reads (see isManifestLocation), or
      // null to clear the binding.
      if (
         !("manifestLocation" in fields) ||
         (location !== null &&
            (typeof location !== "string" || !isManifestLocation(location)))
      ) {
         throw new BadRequestError(
            `\`manifestLocation\` must be ${MANIFEST_LOCATION_FORMS}, or null to serve live.`,
         );
      }
      const { versions, environment } = await this.service(environmentName);
      const loaded: Package = await versions.setManifest(
         packageName,
         version,
         location as string | null,
      );
      return environment.describePackage(loaded);
   }

   async setLatest(
      environmentName: string,
      packageName: string,
      body: unknown,
   ): Promise<ApiPackageVersion> {
      this.assertWritable();
      const version = (body as { version?: unknown } | null)?.version;
      const { versions } = await this.service(environmentName);
      const row = await versions.setLatest(packageName, version);
      return toApiVersion(environmentName, row, row.versionId);
   }

   private assertWritable(): void {
      if (this.environmentStore.publisherConfigIsFrozen) {
         throw new FrozenConfigError();
      }
   }

   private async service(environmentName: string): Promise<{
      versions: VersionService<Package>;
      environment: Awaited<ReturnType<EnvironmentStore["getEnvironment"]>>;
   }> {
      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const versions = environment.getVersionService();
      if (!versions) {
         throw new Error(
            `Environment ${environmentName} has no version registry`,
         );
      }
      return { versions, environment };
   }
}

function toApiVersion(
   environmentName: string,
   v: Version,
   latest: string | null,
): ApiPackageVersion {
   return {
      resource: `${API_PREFIX}/environments/${environmentName}/packages/${v.packageName}/versions/${encodeURIComponent(v.versionId)}`,
      packageName: v.packageName,
      id: v.versionId,
      latest: v.versionId === latest,
      archiveStatus: v.archiveStatus,
      archivedAt: v.archivedAt?.toISOString() ?? null,
      promotedAt: v.promotedAt?.toISOString() ?? null,
      demotedAt: v.demotedAt?.toISOString() ?? null,
      ...(v.description !== null ? { description: v.description } : {}),
      ...(v.sourceLocation !== null ? { location: v.sourceLocation } : {}),
      contentHash: v.contentHash,
      manifestLocation: v.manifestPath,
      gitCommitSha: v.gitCommitSha,
      gitRef: v.gitRef,
      createdAt: v.createdAt.toISOString(),
      updatedAt: v.updatedAt.toISOString(),
   };
}
