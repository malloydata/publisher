// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { S3 } from "@aws-sdk/client-s3";
import { Storage } from "@google-cloud/storage";
import * as path from "path";
import { simpleGit } from "simple-git";
import { GcsPackageSource, isGCSURL } from "./gcs_source";
import { GitPackageSource, isGitHubURL } from "./git_source";
import { isLocalPath, LocalPackageSource } from "./local_source";
import { isS3URL, S3PackageSource } from "./s3_source";
import type { PackageSource } from "./types";

export {
   CloneProgressReporter,
   cloneProgressLabel,
   GIT_CLONE_OPTIONS,
   stripGitProgressNoise,
} from "./clone_progress";
export { isGCSURL } from "./gcs_source";
export { GitPackageSource, isGitHubURL } from "./git_source";
export { parseGitHubUrl } from "./github_url";
export {
   isLocalPath,
   LocalPackageSource,
   resolvePackageLocation,
} from "./local_source";
export { isS3URL } from "./s3_source";
export type { PackageFetchOptions, PackageSource } from "./types";
export { extractZipInto, unzipEnvironment } from "./zip";

/** The clients the remote sources use; each defaults to the real one, built on first use. */
export interface PackageSourceClients {
   gcsClient?: Storage;
   s3Client?: S3;
   simpleGit?: typeof simpleGit;
}

/** A configured location's source, with what the load logs and 404 name. */
export interface SelectedPackageSource {
   source: PackageSource;
   /** The location as the source reads it (a local path resolved). */
   location: string;
   /** What failed, for the log line and the 404. */
   action: string;
   subject: string;
   starting: string;
}

/**
 * The package sources, selected by location scheme. Two selections, because
 * the two callers have always differed: a configured package
 * ({@link forConfiguredLocation}) resolves a relative local path and refuses
 * a location no source claims, while a publish ({@link fetchForPublish})
 * treats any `https://` or `git@` location as a GitHub clone and only an
 * absolute path as local.
 */
export class PackageSources {
   readonly git: GitPackageSource;
   readonly gcs: GcsPackageSource;
   readonly s3: S3PackageSource;
   readonly local = new LocalPackageSource();

   constructor(
      clients: PackageSourceClients,
      /** Resolves a relative or `~/` local location to an absolute path. */
      private readonly resolveLocalPath: (location: string) => string,
   ) {
      let gcsClient = clients.gcsClient;
      let s3Client = clients.s3Client;
      this.git = new GitPackageSource(clients.simpleGit ?? simpleGit);
      this.gcs = new GcsPackageSource(() => (gcsClient ??= new Storage()));
      this.s3 = new S3PackageSource(
         () =>
            (s3Client ??= new S3({
               followRegionRedirects: true,
            })),
      );
   }

   /**
    * How to bring a configured `location` to `targetPath`, chosen by its
    * scheme: GCS, GitHub, S3, then an absolute or relative local path.
    * Undefined for any other form.
    */
   forConfiguredLocation(
      location: string,
      targetPath: string,
   ): SelectedPackageSource | undefined {
      if (isGCSURL(location)) {
         return {
            source: this.gcs,
            location,
            action: "download GCS directory",
            subject: location,
            starting: `Downloading GCS directory from "${location}" to "${targetPath}"`,
         };
      }

      if (isGitHubURL(location)) {
         return {
            source: this.git,
            location,
            action: "clone GitHub repository",
            subject: location,
            starting: `Cloning GitHub repository from "${location}" to "${targetPath}"`,
         };
      }

      if (isS3URL(location)) {
         return {
            source: this.s3,
            location,
            action: "download S3 directory",
            subject: location,
            starting: `Downloading S3 directory from "${location}" to "${targetPath}"`,
         };
      }

      if (isLocalPath(location)) {
         const packagePath: string = this.resolveLocalPath(location);
         return {
            source: this.local,
            location: packagePath,
            action: "mount local directory",
            subject: packagePath,
            starting: `Mounting local directory at "${packagePath}" to "${targetPath}"`,
         };
      }

      return undefined;
   }

   /**
    * Fetch a published package location into `targetPath`: a GitHub URL, a
    * gs:// or s3:// prefix (or .zip), or an absolute local directory or .zip.
    * Callers pass a staging folder, so a long download holds no lock.
    */
   async fetchForPublish(
      environmentName: string,
      packageName: string,
      packageLocation: string,
      targetPath: string,
   ): Promise<void> {
      const opts = { environmentName, packageName };
      if (
         packageLocation.startsWith("https://") ||
         packageLocation.startsWith("git@")
      ) {
         // No progress context: a publish's clone is labeled by repo alone.
         await this.git.fetch(packageLocation, targetPath);
      } else if (packageLocation.startsWith("gs://")) {
         await this.gcs.fetch(packageLocation, targetPath, opts);
      } else if (packageLocation.startsWith("s3://")) {
         await this.s3.fetch(packageLocation, targetPath, opts);
      }

      if (packageLocation.startsWith("/") || path.isAbsolute(packageLocation)) {
         // Absolute paths from the publisher.config could be placed outside of
         // /etc/publisher, so they are mounted in the right place.
         // `path.isAbsolute` is what catches a Windows drive-letter path
         // (`D:\pkgs\sales`), which no other branch claims either; without it
         // the install stages nothing and fails with a bare ENOENT rename.
         // Same pairing as isLocalPath.
         await this.local.fetch(packageLocation, targetPath, opts);
      }
   }
}
