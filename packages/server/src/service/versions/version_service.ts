// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import type { VersionPromotionMode } from "../../config";
import { BadRequestError, PackageVersionError } from "../../errors";
import { logger } from "../../logger";
import { assertSafePackageName } from "../../path_safety";
import {
   PromoteRule,
   ResourceRepository,
   Version,
} from "../../storage/DatabaseInterface";
import { compareSemver, isSemver } from "./semver";
import { VersionCache, VersionEvictedDuringLoadError } from "./version_cache";
import { StagedVersion, VersionStore } from "./version_store";

/** The registry calls the version rules use. */
export type VersionRegistry = Pick<
   ResourceRepository,
   | "getPackageByName"
   | "listVersions"
   | "hasVersions"
   | "listVersionsByEnvironment"
   | "getVersion"
   | "commitPublish"
   | "setLatestVersion"
   | "setVersionArchiveStatus"
   | "setVersionManifestPath"
>;

/**
 * What the version rules need from the environment that serves the versions:
 * compiling one, letting it go, and the environment's own package lock. The
 * rules decide when; the host knows how.
 */
export interface VersionHost<P> {
   /**
    * Compile the version whose files are at `versionPath`, bound to
    * `manifestPath` when one is set. Throws when it does not load.
    */
   loadVersion(
      packageName: string,
      versionPath: string,
      version: Pick<Version, "versionId" | "manifestPath" | "sourceLocation">,
   ): Promise<P>;
   /** Release a loaded version's connections; it is no longer served. */
   releaseVersion(packageName: string, versionId: string, loaded: P): void;
   /** Bind a loaded version to a build manifest, or to none (live). */
   bindVersionManifest(loaded: P, manifestPath: string | null): Promise<void>;
   /** The downloader for a location, writing into a staging folder. */
   downloaderFor(
      packageName: string,
      location: string,
   ): (stagingPath: string) => Promise<void>;
   /** Refuse new work under memory pressure (503). */
   admit(packageName: string, reason: string): void;
   /** Run `fn` holding the package's lock (the one unversioned reads take). */
   withPackageLock<T>(packageName: string, fn: () => Promise<T>): Promise<T>;
   /** Whether the package is a watch-mounted source directory. */
   isWatchMounted(packageName: string): Promise<boolean>;
   /**
    * The package's first version is committed: stop serving its unversioned
    * copy, if one is loaded. Called holding the package lock.
    */
   retireUnversioned(packageName: string): void;
   /** Make sure the package has its registry row, with this description. */
   ensurePackageRecord(
      packageName: string,
      description: string | undefined,
   ): Promise<void>;
   /** A version became served; `isLatest` says whether it is `latest`. */
   onVersionLoaded?(packageName: string, loaded: P, isLatest: boolean): void;
}

export interface PublishOptions<P> {
   /** The location the version is fetched from, recorded as given. */
   sourceLocation: string;
   /** A build manifest to bind the version to; null or absent serves live. */
   manifestLocation?: string | null;
   /** The create request's description: the PACKAGE's, when sent. */
   description?: string;
   promotion: VersionPromotionMode;
   /** The publish checks; a message refuses the version with 400. */
   validate?: (loaded: P) => string | undefined;
}

export interface PublishResult<P> {
   loaded: P;
   version: Version;
   /** False for a re-load of a version this server already had. */
   created: boolean;
}

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
 * version a request means, publishing, moving `latest`, archiving and
 * manifest binding. The registry is the authority on which versions exist,
 * the store holds their files, and the cache their compiled models.
 */
export class VersionService<P = unknown> {
   readonly cache: VersionCache<P>;
   private readonly versionLocks = new Map<string, Mutex>();

   constructor(
      private readonly registry: VersionRegistry,
      private readonly environmentId: string,
      private readonly store?: VersionStore,
      private readonly host?: VersionHost<P>,
   ) {
      this.cache = new VersionCache<P>({
         load: (packageName, versionId) =>
            this.loadFromRegistry(packageName, versionId),
         release: (packageName, versionId, loaded) =>
            this.requireHost().releaseVersion(packageName, versionId, loaded),
      });
   }

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

      const latest = await this.latestOf(packageName);
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
      return this.registry.hasVersions(this.environmentId, packageName);
   }

   /** The package's `latest` version, or null. */
   async latestOf(packageName: string): Promise<string | null> {
      const pkg = await this.registry.getPackageByName(
         this.environmentId,
         packageName,
      );
      return pkg?.latestVersion ?? null;
   }

   /**
    * The version a request names (or `latest`) as a loaded package, or null
    * for a package with no versions. Loads it on first use. A load overtaken
    * by an archive or a delete resolves again, so the caller gets what that
    * change means (410, 404) rather than a version no longer served.
    */
   async getLoaded(
      packageName: string,
      rawVersionId?: unknown,
   ): Promise<{ loaded: P; version: Version } | null> {
      for (let attempt = 0; ; attempt++) {
         const version = await this.resolve(packageName, rawVersionId);
         if (!version) return null;
         try {
            return {
               loaded: await this.cache.get(packageName, version.versionId),
               version,
            };
         } catch (err) {
            if (err instanceof VersionEvictedDuringLoadError && attempt < 2) {
               continue;
            }
            throw err;
         }
      }
   }

   /**
    * Publish a version: the version is the one the downloaded tree's
    * publisher.json declares.
    *
    *  1. No lock: download into staging, read the version, hash the tree.
    *  2. The version's lock: refuse different content under an existing
    *     version (409), treat the same content as a re-load; otherwise place
    *     the tree at `<pkg>/<dir>/`, compile it and run the publish checks.
    *  3. One registry transaction: record the version and, when the promotion
    *     rule says so, move `latest`. Any failure before it removes the
    *     placed folder, so a refused version leaves nothing behind.
    *  4. The package's own lock only for its first version, which moves its
    *     unversioned tree aside and puts it back if the publish fails.
    */
   async publish(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
      options: PublishOptions<P>,
   ): Promise<PublishResult<P>> {
      const host = this.requireHost();
      const store = this.requireStore();
      // Before any path is built from it.
      assertSafePackageName(packageName);
      if (await host.isWatchMounted(packageName)) {
         throw new BadRequestError(
            `Package ${packageName} is mounted for watch mode, which serves its source directory as it changes, so it cannot hold published versions. Publish it from another location, or start the server without watching it.`,
         );
      }
      host.admit(packageName, "publish a package version");

      const staged = await store.stage(packageName, downloader);
      try {
         if (await this.isVersioned(packageName)) {
            return await this.publishStaged(staged, options, false);
         }
         return await host.withPackageLock(packageName, () =>
            this.publishStaged(staged, options, true),
         );
      } finally {
         // Placed trees were renamed away; this only removes one that was not.
         await store.discard(staged);
      }
   }

   private async publishStaged(
      staged: StagedVersion,
      options: PublishOptions<P>,
      holdingPackageLock: boolean,
   ): Promise<PublishResult<P>> {
      const { packageName, versionId, dirName } = staged;
      return this.withVersionLock(packageName, versionId, async () => {
         const host = this.requireHost();
         const store = this.requireStore();
         const existing = await this.registry.getVersion(
            this.environmentId,
            packageName,
            versionId,
         );
         if (existing) return this.reload(existing, staged, options);

         const versions = await this.registry.listVersions(
            this.environmentId,
            packageName,
         );
         const caseTwin = versions.find(
            (v) => v.versionId.toLowerCase() === versionId.toLowerCase(),
         );
         if (caseTwin) {
            throw new PackageVersionError(
               "VERSION_CONFLICT",
               `Version ${versionId} differs from the published version ${caseTwin.versionId} only by letter case, and the two would share a folder on a case-insensitive filesystem. Publish under another version.`,
            );
         }

         // Only the package's first version moves an unversioned tree aside,
         // and only while holding the package lock that unversioned reads
         // take. Rows read under that lock are final: a concurrent first
         // publish of another version committed before this one got it.
         const first = holdingPackageLock && versions.length === 0;
         await host.ensurePackageRecord(packageName, options.description);
         const held = first ? await store.holdLegacy(packageName) : null;
         // A folder with no row is a placement whose publish never committed.
         if (await store.isPlaced(packageName, dirName)) {
            await store.remove(packageName, dirName);
         }

         let loaded: P | undefined;
         let committed: { version: Version; promoted: boolean };
         try {
            await store.place(staged);
            loaded = await host.loadVersion(
               packageName,
               store.versionPath(packageName, dirName),
               {
                  versionId,
                  manifestPath: options.manifestLocation || null,
                  sourceLocation: options.sourceLocation,
               },
            );
            const refusal = options.validate?.(loaded);
            if (refusal) throw new BadRequestError(refusal);
            committed = await this.registry.commitPublish(
               {
                  versionId,
                  environmentId: this.environmentId,
                  packageName,
                  dirName,
                  contentHash: staged.contentHash,
                  sourceLocation: options.sourceLocation,
                  manifestPath: options.manifestLocation || null,
                  description: staged.description,
               },
               promoteOnPublish(versionId, options.promotion),
            );
         } catch (err) {
            if (loaded !== undefined) {
               host.releaseVersion(packageName, versionId, loaded);
            }
            await store.remove(packageName, dirName);
            if (held) {
               await store
                  .restoreLegacy(held)
                  .catch((restoreErr) =>
                     logger.error(
                        "Could not put back an unversioned package tree after a failed first versioned publish; it is restored at the next startup",
                        { packageName, error: restoreErr },
                     ),
                  );
            }
            throw err;
         }

         this.adopt(packageName, versionId, loaded);
         if (held) {
            await store.dropLegacy(held);
         }
         if (first) host.retireUnversioned(packageName);
         host.onVersionLoaded?.(packageName, loaded, committed.promoted);
         return { loaded, version: committed.version, created: true };
      });
   }

   /**
    * Publishing a version this server already holds: placement, not a new
    * publish. The same content succeeds and writes no files; different
    * content is 409 and an archived version 410. A non-null manifest on the
    * request binds the version to it, and `latest` moves only to a version
    * strictly higher than the current one.
    */
   private async reload(
      existing: Version,
      staged: StagedVersion,
      options: PublishOptions<P>,
   ): Promise<PublishResult<P>> {
      const { packageName, versionId } = existing;
      const store = this.requireStore();
      const host = this.requireHost();
      if (existing.contentHash !== staged.contentHash) {
         throw new PackageVersionError(
            "VERSION_CONFLICT",
            `Version ${versionId} of package ${packageName} is already published with different content. Bump the "version" in publisher.json and publish again.`,
         );
      }
      if (existing.archiveStatus === "archive") {
         throw new PackageVersionError(
            "VERSION_ARCHIVED",
            `Version ${versionId} of package ${packageName} is archived. Unarchive it instead of publishing it again.`,
         );
      }
      if (!(await store.isPlaced(packageName, existing.dirName))) {
         await store.place(staged);
      }

      let version = existing;
      if (
         options.manifestLocation &&
         options.manifestLocation !== existing.manifestPath
      ) {
         version = await this.registry.setVersionManifestPath(
            this.environmentId,
            packageName,
            versionId,
            options.manifestLocation,
         );
         const resident = this.cache.peek(packageName, versionId);
         if (resident !== undefined) {
            await host.bindVersionManifest(resident, options.manifestLocation);
         }
      }

      // Loaded before `latest` can point at it: a version that cannot load
      // must never become what every request without a version reaches.
      const loaded = await this.cache.get(packageName, versionId);
      if (options.promotion === "on-publish") {
         await this.registry.setLatestVersion(
            this.environmentId,
            packageName,
            versionId,
            (current) =>
               current === null || compareSemver(versionId, current) > 0,
         );
      }
      return {
         loaded,
         version:
            (await this.registry.getVersion(
               this.environmentId,
               packageName,
               versionId,
            )) ?? version,
         created: false,
      };
   }

   /** Run `fn` holding one version's lock. */
   async withVersionLock<T>(
      packageName: string,
      versionId: string,
      fn: () => Promise<T>,
   ): Promise<T> {
      const key = `${packageName}@${versionId}`;
      let lock = this.versionLocks.get(key);
      if (!lock) {
         lock = new Mutex();
         this.versionLocks.set(key, lock);
      }
      return lock.runExclusive(fn);
   }

   /** Load a registered version for the cache, restoring a missing tree. */
   private async loadFromRegistry(
      packageName: string,
      versionId: string,
   ): Promise<P> {
      const host = this.requireHost();
      const store = this.requireStore();
      const row = await this.registry.getVersion(
         this.environmentId,
         packageName,
         versionId,
      );
      if (!row) {
         throw new PackageVersionError(
            "VERSION_NOT_FOUND",
            `Package ${packageName} has no version ${versionId}.`,
         );
      }
      host.admit(packageName, "load a package version");
      if (!(await store.isPlaced(packageName, row.dirName))) {
         const restored =
            row.sourceLocation !== null &&
            (await store.restore(
               packageName,
               row,
               host.downloaderFor(packageName, row.sourceLocation),
            ));
         if (!restored) {
            throw new Error(
               `The files of version ${versionId} of package ${packageName} are missing and could not be fetched again from where it was published.`,
            );
         }
      }
      const loaded = await host.loadVersion(
         packageName,
         store.versionPath(packageName, row.dirName),
         row,
      );
      const latest = await this.latestOf(packageName);
      host.onVersionLoaded?.(packageName, loaded, latest === versionId);
      return loaded;
   }

   /** Put a version compiled by its publish into the cache. */
   private adopt(packageName: string, versionId: string, loaded: P): void {
      this.cache.put(packageName, versionId, loaded);
   }

   private requireHost(): VersionHost<P> {
      if (!this.host) throw new Error("VersionService has no host");
      return this.host;
   }

   private requireStore(): VersionStore {
      if (!this.store) throw new Error("VersionService has no store");
      return this.store;
   }
}

/**
 * Whether a newly published version becomes `latest`: under `on-publish`,
 * unless a later version already is (a version that ties by semver
 * precedence, differing only in build metadata, is not later, so the newer
 * publish wins); under `explicit`, never.
 */
export function promoteOnPublish(
   versionId: string,
   promotion: VersionPromotionMode,
): PromoteRule {
   if (promotion === "explicit") return () => false;
   return (current) =>
      current === null || compareSemver(current, versionId) <= 0;
}
