// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/** What a fetch needs to know beyond the location: who asked, for messages. */
export interface PackageFetchOptions {
   /** Named in a "not found" error. */
   environmentName: string;
   /** Named in a "not found" error. */
   packageName: string;
   /**
    * Labels the clone progress line (`[env] cloning owner/repo (pkgs)`).
    * Omitted on the publish path, which labels a clone by the repo alone.
    */
   progress?: { environmentName?: string; packageNames?: string[] };
}

/**
 * One way of bringing a package location to a directory on disk: a git
 * clone, a GCS or S3 prefix (or .zip), or a local directory (or .zip).
 * `fetch` replaces whatever `targetDir` held with the package's files.
 */
export interface PackageSource {
   fetch(
      location: string,
      targetDir: string,
      opts: PackageFetchOptions,
   ): Promise<void>;
}
