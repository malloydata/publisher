// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PackageVersionError } from "../../errors";
import { ResourceRepository, Version } from "../../storage/DatabaseInterface";
import { isSemver } from "./semver";

/** The registry calls the version rules use. */
export type VersionRegistry = Pick<
   ResourceRepository,
   | "getPackageByName"
   | "listVersions"
   | "listVersionsByEnvironment"
   | "getVersion"
   | "commitPublish"
   | "setLatestVersion"
   | "setVersionArchiveStatus"
   | "setVersionManifestPath"
>;

/**
 * A `versionId` as a request carries it, normalized: absent, `""` and null
 * all mean "no version", which serves the package's `latest`. A value that is
 * not a semantic version is refused with 400, so a malformed value is told
 * apart from a version that does not exist.
 */
export function requestedVersionId(raw: unknown): string | undefined {
   if (raw === undefined || raw === null || raw === "") return undefined;
   if (typeof raw !== "string" || !isSemver(raw)) {
      throw new PackageVersionError(
         "VERSION_ID_INVALID",
         `versionId ${JSON.stringify(raw)} is not a semantic version such as "1.2.0" or "1.2.0-rc1".`,
      );
   }
   return raw;
}

/**
 * The version rules of one environment's packages, in one place: which
 * version a request means, and (in later steps) publishing, moving `latest`,
 * archiving and manifest binding. Backed by the registry, which is the
 * authority on which versions exist.
 */
export class VersionService {
   constructor(
      private readonly registry: VersionRegistry,
      private readonly environmentId: string,
   ) {}

   /**
    * The version a request is served from, decided once per request from the
    * registry, never from the disk.
    *
    *  - An unversioned package (one with no version rows) resolves to null,
    *    and is served as before, when the request names no version. Naming
    *    one is 404 VERSION_NOT_FOUND.
    *  - A named version must exist (404 VERSION_NOT_FOUND) and must not be
    *    archived (410 VERSION_ARCHIVED).
    *  - No version means the package's `latest`. A versioned package with no
    *    `latest` yet (explicit promotion, before the first move) is 404.
    */
   async resolve(
      packageName: string,
      rawVersionId?: unknown,
   ): Promise<Version | null> {
      const requested = requestedVersionId(rawVersionId);
      if (requested !== undefined) {
         const row = await this.registry.getVersion(
            this.environmentId,
            packageName,
            requested,
         );
         if (!row) {
            throw new PackageVersionError(
               "VERSION_NOT_FOUND",
               `Package ${packageName} has no version ${requested}.`,
            );
         }
         if (row.archiveStatus === "archive") {
            throw new PackageVersionError(
               "VERSION_ARCHIVED",
               `Version ${requested} of package ${packageName} is archived. Unarchive it to serve it again.`,
            );
         }
         return row;
      }

      const pkg = await this.registry.getPackageByName(
         this.environmentId,
         packageName,
      );
      const latest = pkg?.latestVersion ?? null;
      if (latest !== null) {
         const row = await this.registry.getVersion(
            this.environmentId,
            packageName,
            latest,
         );
         if (row) return row;
      }
      if (await this.isVersioned(packageName)) {
         throw new PackageVersionError(
            "VERSION_NOT_FOUND",
            `Package ${packageName} has no latest version yet; name a version, or make one latest.`,
         );
      }
      return null;
   }

   /** Whether the package has any published version. */
   async isVersioned(packageName: string): Promise<boolean> {
      return (
         (await this.registry.listVersions(this.environmentId, packageName))
            .length > 0
      );
   }
}
