// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Mutex } from "async-mutex";
import * as fs from "fs";
import * as path from "path";
import type { VersionPromotionMode } from "../../config";
import {
   BadRequestError,
   PackageVersionError,
   VersionFilesMissingError,
} from "../../errors";
import { logger } from "../../logger";
import { assertSafePackageName } from "../../path_safety";
import {
   PromoteRule,
   ResourceRepository,
   Version,
} from "../../storage/DatabaseInterface";
import { compareSemver, isSemver } from "./semver";
import { VersionCache, VersionEvictedDuringLoadError } from "./version_cache";
import { StagedVersion, UnversionedStage, VersionStore } from "./version_store";

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
   /**
    * Make sure the package has its registry row, with this description when
    * one is given (null clears it; undefined leaves it). Returns whether this
    * call created the row.
    */
   ensurePackageRecord(
      packageName: string,
      description: string | null | undefined,
   ): Promise<boolean>;
   /** Remove a package row this service created for a publish that failed. */
   removePackageRecord(packageName: string): Promise<void>;
   /** The build manifest a loaded version is bound to now, or null. */
   boundManifestOf?(loaded: P): string | null;
   /** A version became served; `isLatest` says whether it is `latest`. */
   onVersionLoaded?(packageName: string, loaded: P, isLatest: boolean): void;
   /** Whether a materialization of this version is running now. */
   isVersionBuilding?(packageName: string, versionId: string): boolean;
   /**
    * A version was archived and unloaded: reclaim what it alone owns (its
    * `scope: version` tables). Runs in the background; must not throw.
    */
   onVersionArchived?(packageName: string, versionId: string): void;
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
         `versionId ${echo(raw)} is not a semantic version such as "1.2.0" or "1.2.0-rc1".`,
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
   private readonly filesLocks = new Map<string, Mutex>();
   /**
    * The `latest` last read or set for each package, for the callers that
    * need it synchronously (which version answers a lookup by name). Never
    * consulted to decide a request: `latestOf` reads the registry.
    */
   private readonly latestSeen = new Map<string, string | null>();
   /**
    * When restoring a version's missing folder last failed, by package,
    * version and hash, so a listing or poll that touches it does not fetch
    * the location again on every call.
    */
   private readonly restoreFailedAt = new Map<string, number>();

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
         // After the cache has kept it: a load an evict overtook is released
         // instead, and must not report itself served.
         loaded: (packageName, versionId, loaded) => {
            void this.latestOf(packageName).then(
               (latest) => {
                  // Still the one served: an archive or a delete may have
                  // unloaded it while `latest` was read, and a version no
                  // longer served must not report itself serving.
                  if (this.cache.peek(packageName, versionId) !== loaded)
                     return;
                  this.host?.onVersionLoaded?.(
                     packageName,
                     loaded,
                     latest === versionId,
                  );
               },
               (error) =>
                  logger.warn("Could not read latest after a version loaded", {
                     packageName,
                     error,
                  }),
            );
         },
      });
   }

   /** The loaded `latest` of a package, without loading or reading anything. */
   peekLatestLoaded(packageName: string): P | undefined {
      const latest = this.latestSeen.get(packageName);
      return latest ? this.cache.peek(packageName, latest) : undefined;
   }

   /**
    * Forget everything held for a package being deleted: its loaded versions
    * (released) and what was remembered about it.
    */
   forgetPackage(packageName: string): void {
      this.cache.evictPackage(packageName);
      this.latestSeen.delete(packageName);
      for (const key of [...this.restoreFailedAt.keys()]) {
         if (key.startsWith(`${packageName}@`))
            this.restoreFailedAt.delete(key);
      }
      // A version published again after a delete may differ.
      for (const key of [...this.publishedManifests.keys()]) {
         if (key.startsWith(`${packageName}@`))
            this.publishedManifests.delete(key);
      }
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

   /**
    * The version a page's URL pins, for routes that take it leniently (static
    * files): `raw` when it names a version this package has (archived
    * included, so a pinned page of an archived version is still refused), and
    * undefined otherwise, which serves `latest`. A proxy may forward a page's
    * query string with a `versionId` of its own; that must not 404 the page.
    */
   async pinnableVersion(
      packageName: string,
      raw: unknown,
   ): Promise<string | undefined> {
      if (typeof raw !== "string" || !isSemver(raw)) return undefined;
      const row = await this.registry.getVersion(
         this.environmentId,
         packageName,
         raw,
      );
      return row ? raw : undefined;
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
      const latest = pkg?.latestVersion ?? null;
      this.latestSeen.set(packageName, latest);
      return latest;
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
      await this.refuseWatchMounted(packageName);
      host.admit(packageName, "publish a package version");
      return this.publishStagedVersion(
         await store.stage(packageName, downloader),
         options,
      );
   }

   /**
    * Download a tree for a publish and read the version its publisher.json
    * declares. A tree that declares no semantic version comes back as an
    * UnversionedStage, still staged: the caller installs it in place, as a
    * package with no versions, or discards it (discardStage).
    */
   async stageForPublish(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
   ): Promise<StagedVersion | UnversionedStage> {
      const host = this.requireHost();
      // Before any path is built from it.
      assertSafePackageName(packageName);
      host.admit(packageName, "publish a package");
      return this.requireStore().stageAny(packageName, downloader);
   }

   /** Remove a staged tree that will not be published or installed. */
   async discardStage(stage: { stagingPath: string }): Promise<void> {
      await this.requireStore().discard(stage);
   }

   /** Publish a staged version (from stageForPublish); the stage is consumed. */
   async publishStagedVersion(
      staged: StagedVersion,
      options: PublishOptions<P>,
   ): Promise<PublishResult<P>> {
      const host = this.requireHost();
      const store = this.requireStore();
      const { packageName } = staged;
      try {
         await this.refuseWatchMounted(packageName);
         if (await this.isVersioned(packageName)) {
            return await this.publishStaged(staged, options, false);
         }
         return await host.withPackageLock(packageName, () =>
            this.publishStaged(staged, options, true),
         );
      } finally {
         // Placed trees were renamed away; this only removes one that was not.
         // Never allowed to replace the publish's own error.
         await store.discard(staged).catch((error) =>
            logger.warn("Could not remove a staged package version", {
               packageName,
               error,
            }),
         );
      }
   }

   /** A watch-mounted package serves its own source directory: no versions. */
   private async refuseWatchMounted(packageName: string): Promise<void> {
      if (await this.requireHost().isWatchMounted(packageName)) {
         throw new BadRequestError(
            `Package ${packageName} is mounted for watch mode, which serves its source directory as it changes, so it cannot hold published versions. Publish it from another location, or start the server without watching it.`,
         );
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
         let createdRecord = false;
         let held: Awaited<ReturnType<VersionStore["holdLegacy"]>> = null;
         let loaded: P | undefined;
         let committed: { version: Version; promoted: boolean };
         try {
            // The package's row, which the version's commit needs; created
            // here only for a package new to this server, and removed again
            // if this publish fails. The request's description is applied
            // only once the version is committed.
            createdRecord = await host.ensurePackageRecord(
               packageName,
               undefined,
            );
            held = first ? await store.holdLegacy(packageName) : null;
            await this.withFilesLock(packageName, dirName, async () => {
               // A folder with no row is a placement whose publish never
               // committed.
               if (await store.isPlaced(packageName, dirName)) {
                  await store.remove(packageName, dirName);
               }
               if (!(await store.place(staged))) {
                  throw new Error(
                     `The folder for version ${versionId} of package ${packageName} appeared while it was being placed.`,
                  );
               }
            });
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
            if (createdRecord) {
               await host
                  .removePackageRecord(packageName)
                  .catch((error) =>
                     logger.warn(
                        "Could not remove the package row a failed publish created",
                        { packageName, error },
                     ),
                  );
            }
            throw err;
         }

         this.adopt(packageName, versionId, loaded);
         if (committed.promoted) this.latestSeen.set(packageName, versionId);
         if (held) {
            await store.dropLegacy(held);
         }
         if (first) host.retireUnversioned(packageName);
         // The package's own description is one a request set. A package
         // that was unversioned may carry one synced from its old tree's
         // publisher.json: its first version clears it, so the package reads
         // as its latest version's until a request sets one.
         if (first || options.description !== undefined) {
            await host.ensurePackageRecord(
               packageName,
               options.description ?? null,
            );
         }
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
      await this.withFilesLock(packageName, existing.dirName, async () => {
         if (!(await store.isPlaced(packageName, existing.dirName))) {
            await store.place(staged);
         }
      });

      let version = existing;
      const newManifest =
         options.manifestLocation &&
         options.manifestLocation !== existing.manifestPath
            ? options.manifestLocation
            : undefined;
      if (newManifest) {
         version = await this.registry.setVersionManifestPath(
            this.environmentId,
            packageName,
            versionId,
            newManifest,
         );
         const resident = this.cache.peek(packageName, versionId);
         if (resident !== undefined) {
            await host.bindVersionManifest(resident, newManifest);
         }
      }

      // Loaded before `latest` can point at it: a version that cannot load
      // must never become what every request without a version reaches.
      const loaded = await this.cache.get(packageName, versionId);
      // A load already running when the manifest was written binds the one
      // it read before; bring it up to the row.
      if (
         newManifest &&
         host.boundManifestOf &&
         host.boundManifestOf(loaded) !== newManifest
      ) {
         await host.bindVersionManifest(loaded, newManifest);
      }
      if (options.description !== undefined) {
         await host.ensurePackageRecord(packageName, options.description);
      }
      if (options.promotion === "on-publish") {
         const moved = await this.registry.setLatestVersion(
            this.environmentId,
            packageName,
            versionId,
            (current) =>
               current === null || compareSemver(versionId, current) > 0,
         );
         if (moved) {
            this.latestSeen.set(packageName, versionId);
            host.onVersionLoaded?.(packageName, loaded, true);
         }
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

   /**
    * A package's versions, highest first, archived ones included, with the
    * package's `latest`. A package with no versions has none to list.
    */
   async listVersions(
      packageName: string,
   ): Promise<{ latest: string | null; versions: Version[] }> {
      const versions = await this.registry.listVersions(
         this.environmentId,
         packageName,
      );
      versions.sort((a, b) => {
         const order = compareSemver(b.versionId, a.versionId);
         // Build metadata does not order versions; the later publish first.
         return order !== 0
            ? order
            : b.createdAt.getTime() - a.createdAt.getTime();
      });
      return { latest: await this.latestOf(packageName), versions };
   }

   /** Every version in the environment, each with its package's `latest`. */
   async listAllVersions(): Promise<(Version & { latest: string | null })[]> {
      const rows = await this.registry.listVersionsByEnvironment(
         this.environmentId,
      );
      const latestByPackage = new Map<string, string | null>();
      for (const name of new Set(rows.map((r) => r.packageName))) {
         latestByPackage.set(name, await this.latestOf(name));
      }
      return rows.map((r) => ({
         ...r,
         latest: latestByPackage.get(r.packageName) ?? null,
      }));
   }

   /** Set the package's own description (not any version's). */
   async setPackageDescription(
      packageName: string,
      description: string,
   ): Promise<void> {
      await this.requireHost().ensurePackageRecord(packageName, description);
   }

   /**
    * The package's own description, set by a create or PATCH request; null
    * when none was. A version's publisher.json description is that version's.
    */
   async packageDescriptionOf(packageName: string): Promise<string | null> {
      const row = await this.registry.getPackageByName(
         this.environmentId,
         packageName,
      );
      return row?.description ?? null;
   }

   /** Every version in service (not archived), of every package here. */
   async activeVersions(): Promise<Version[]> {
      return (
         await this.registry.listVersionsByEnvironment(this.environmentId)
      ).filter((v) => v.archiveStatus !== "archive");
   }

   /**
    * A version's publisher.json as it was published, read from its folder
    * without loading the version. The folder never changes, so it is read
    * once. Null when the folder is missing (it is fetched again when the
    * version next loads) or the file does not parse.
    */
   async publishedManifestOf(
      version: Pick<Version, "packageName" | "dirName">,
   ): Promise<Record<string, unknown> | null> {
      const key = `${version.packageName}@${version.dirName}`;
      const cached = this.publishedManifests.get(key);
      if (cached) return cached;
      try {
         const text = await fs.promises.readFile(
            path.join(
               this.requireStore().versionPath(
                  version.packageName,
                  version.dirName,
               ),
               "publisher.json",
            ),
            "utf8",
         );
         const parsed: unknown = JSON.parse(text);
         if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) {
            return null;
         }
         this.publishedManifests.set(key, parsed as Record<string, unknown>);
         return parsed as Record<string, unknown>;
      } catch {
         return null;
      }
   }

   private readonly publishedManifests = new Map<
      string,
      Record<string, unknown>
   >();

   /** One version, archived or not: 400 for a malformed id, 404 for none. */
   async getVersion(
      packageName: string,
      rawVersion: unknown,
   ): Promise<Version> {
      const versionId = requiredVersionId(rawVersion);
      const row = await this.registry.getVersion(
         this.environmentId,
         packageName,
         versionId,
      );
      if (!row) throw versionNotFound(packageName, versionId);
      return row;
   }

   /**
    * Archive or unarchive a version. Archiving refuses the package's
    * `latest`, its last version in service, and a version a materialization
    * is building; then unloads it and hands its own tables to reclaim. Its
    * files stay. Unarchiving puts it back in service; it loads on its next
    * read. The state it is already in changes nothing.
    */
   async setArchiveStatus(
      packageName: string,
      rawVersion: unknown,
      status: "archive" | "unarchive",
   ): Promise<Version> {
      const versionId = requiredVersionId(rawVersion);
      const host = this.requireHost();
      // Looked up before the lock, so a request naming a version that does
      // not exist leaves no lock behind.
      await this.getVersion(packageName, versionId);
      return this.withVersionLock(packageName, versionId, async () => {
         const current = await this.getVersion(packageName, versionId);
         if (current.archiveStatus === status) {
            // An archived version is never served: if anything of it is
            // loaded, it goes, whatever let it in.
            if (status === "archive") this.cache.evict(packageName, versionId);
            return current;
         }
         // A version being built is never archived: the archive would reclaim
         // the tables the run is writing. Refused whichever version it is; the
         // reason says why the archive cannot happen now.
         if (
            status === "archive" &&
            host.isVersionBuilding?.(packageName, versionId)
         ) {
            if ((await this.latestOf(packageName)) === versionId) {
               throw new PackageVersionError(
                  "VERSION_IS_LATEST",
                  `Version ${versionId} is the latest version of package ${packageName}; make another version latest before archiving it.`,
               );
            }
            throw new PackageVersionError(
               "VERSION_BUILDING",
               `A materialization of version ${versionId} of package ${packageName} is running, and archiving would reclaim the tables it is writing. Wait for it, or stop it.`,
            );
         }
         const updated = await this.registry.setVersionArchiveStatus(
            this.environmentId,
            packageName,
            versionId,
            status,
         );
         if (status === "archive") {
            this.cache.evict(packageName, versionId);
            // The archive is committed; reclaiming its tables must not turn
            // that into a failure.
            try {
               host.onVersionArchived?.(packageName, versionId);
            } catch (error) {
               logger.warn("Could not start reclaiming an archived version", {
                  packageName,
                  versionId,
                  error,
               });
            }
         }
         return updated;
      });
   }

   /**
    * Bind a version to a build manifest, or to none (null: serves live). The
    * binding is serving state, not content, so it is the one thing about a
    * published version that changes. An archived version is refused before
    * anything is written. Returns the version, loaded and bound.
    */
   async setManifest(
      packageName: string,
      rawVersion: unknown,
      manifestLocation: string | null,
   ): Promise<P> {
      const versionId = requiredVersionId(rawVersion);
      const host = this.requireHost();
      // Looked up before the lock, so a request naming a version that does
      // not exist leaves no lock behind.
      await this.getVersion(packageName, versionId);
      return this.withVersionLock(packageName, versionId, async () => {
         const current = await this.getVersion(packageName, versionId);
         if (current.archiveStatus === "archive") {
            throw versionArchived(packageName, versionId);
         }
         await this.registry.setVersionManifestPath(
            this.environmentId,
            packageName,
            versionId,
            manifestLocation,
         );
         try {
            const resident = this.cache.peek(packageName, versionId);
            // Not loaded: load it (through memory admission, like any load).
            // A load already running read the row before this write and binds
            // the previous manifest, so it is brought up to the row after.
            const loaded =
               resident ?? (await this.loadOrRefuse(packageName, versionId));
            if (
               resident !== undefined ||
               !host.boundManifestOf ||
               host.boundManifestOf(loaded) !== manifestLocation
            ) {
               await host.bindVersionManifest(loaded, manifestLocation);
            }
            return loaded;
         } catch (error) {
            // Nothing was bound: the row goes back, so it never names a
            // binding the version does not serve.
            await this.registry
               .setVersionManifestPath(
                  this.environmentId,
                  packageName,
                  versionId,
                  current.manifestPath,
               )
               .catch((restoreError) =>
                  logger.error(
                     "Could not restore a version's manifest after its bind failed",
                     { packageName, versionId, error: restoreError },
                  ),
               );
            throw error;
         }
      });
   }

   /**
    * Load a version for a lifecycle change. A load an archive or a delete
    * overtook answers what that change means (410, 404) rather than a 500.
    */
   private async loadOrRefuse(
      packageName: string,
      versionId: string,
   ): Promise<P> {
      for (let attempt = 0; ; attempt++) {
         try {
            return await this.cache.get(packageName, versionId);
         } catch (error) {
            if (!(error instanceof VersionEvictedDuringLoadError)) throw error;
            const row = await this.getVersion(packageName, versionId);
            if (row.archiveStatus === "archive") {
               throw versionArchived(packageName, versionId);
            }
            if (attempt >= 2) throw error;
         }
      }
   }

   /**
    * Make a version the package's `latest`: the one every request with no
    * version is served. It must exist and be in service, and it is loaded
    * first, so a version that cannot load never becomes `latest`.
    */
   async setLatest(packageName: string, rawVersion: unknown): Promise<Version> {
      const versionId = requiredVersionId(rawVersion);
      const target = await this.getVersion(packageName, versionId);
      if (target.archiveStatus === "archive") {
         throw versionArchived(packageName, versionId);
      }
      const loaded = await this.loadOrRefuse(packageName, versionId);
      const moved = await this.registry.setLatestVersion(
         this.environmentId,
         packageName,
         versionId,
      );
      if (moved) {
         this.latestSeen.set(packageName, versionId);
         this.requireHost().onVersionLoaded?.(packageName, loaded, true);
      }
      return this.getVersion(packageName, versionId);
   }

   /**
    * Run `fn` holding one version's lock. Keyed without letter case, so two
    * versions that differ only by case (and would share a folder on a
    * case-insensitive filesystem) never run side by side.
    */
   async withVersionLock<T>(
      packageName: string,
      versionId: string,
      fn: () => Promise<T>,
   ): Promise<T> {
      return lockFor(
         this.versionLocks,
         `${packageName}@${versionId.toLowerCase()}`,
      ).runExclusive(fn);
   }

   /**
    * Run `fn` holding the lock on one version's folder: every placement,
    * removal and restore of it, so two of them never rename into the same
    * target.
    */
   private async withFilesLock<T>(
      packageName: string,
      dirName: string,
      fn: () => Promise<T>,
   ): Promise<T> {
      return lockFor(
         this.filesLocks,
         `${packageName}@${dirName.toLowerCase()}`,
      ).runExclusive(fn);
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
      if (!row) throw versionNotFound(packageName, versionId);
      // An archived version is never loaded, whatever a caller checked before.
      if (row.archiveStatus === "archive") {
         throw versionArchived(packageName, versionId);
      }
      host.admit(packageName, "load a package version");
      await this.withFilesLock(packageName, row.dirName, () =>
         this.restoreIfMissing(row),
      );
      return host.loadVersion(
         packageName,
         store.versionPath(packageName, row.dirName),
         row,
      );
   }

   /**
    * Put a registered version's missing folder back from where it was
    * published, kept only when it hashes to the published hash. A failure is
    * remembered for a while, so reads that keep touching the version do not
    * fetch the location again each time. Called holding the folder's lock.
    */
   private async restoreIfMissing(row: Version): Promise<void> {
      const store = this.requireStore();
      const host = this.requireHost();
      const { packageName, versionId, dirName } = row;
      if (await store.isPlaced(packageName, dirName)) return;
      const failureKey = `${packageName}@${versionId}@${row.contentHash}`;
      const failedAt = this.restoreFailedAt.get(failureKey);
      const missing = new VersionFilesMissingError(
         `The files of version ${versionId} of package ${packageName} are missing and could not be fetched again from where it was published.`,
      );
      if (
         failedAt !== undefined &&
         Date.now() - failedAt < RESTORE_RETRY_AFTER_MS
      ) {
         throw missing;
      }
      const restored =
         row.sourceLocation !== null &&
         (await store.restore(
            packageName,
            row,
            host.downloaderFor(packageName, row.sourceLocation),
         ));
      if (!restored) {
         this.restoreFailedAt.set(failureKey, Date.now());
         throw missing;
      }
      this.restoreFailedAt.delete(failureKey);
      // The package may have been deleted while the files were fetched: a
      // folder no row owns is removed rather than left for nobody to find.
      if (
         !(await this.registry.getVersion(
            this.environmentId,
            packageName,
            versionId,
         ))
      ) {
         await store.remove(packageName, dirName);
         throw versionNotFound(packageName, versionId);
      }
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

/** How long a failed restore of a version's folder is not tried again. */
const RESTORE_RETRY_AFTER_MS = 60_000;

function lockFor(locks: Map<string, Mutex>, key: string): Mutex {
   let lock = locks.get(key);
   if (!lock) {
      lock = new Mutex();
      locks.set(key, lock);
   }
   return lock;
}

/** A short rendering of a refused value: never more than 64 characters. */
function echo(raw: unknown): string {
   const text = JSON.stringify(raw) ?? String(raw);
   return text.length > 64 ? `${text.slice(0, 61)}...` : text;
}

/** A version a route names in its path or body: required, and semver. */
function requiredVersionId(raw: unknown): string {
   const versionId = requestedVersionId(raw);
   if (versionId === undefined) {
      throw new PackageVersionError(
         "VERSION_ID_INVALID",
         'A version is required, such as "1.2.0".',
      );
   }
   return versionId;
}

function versionNotFound(packageName: string, versionId: string) {
   return new PackageVersionError(
      "VERSION_NOT_FOUND",
      `Package ${packageName} has no version ${versionId}.`,
   );
}

function versionArchived(packageName: string, versionId: string) {
   return new PackageVersionError(
      "VERSION_ARCHIVED",
      `Version ${versionId} of package ${packageName} is archived. Unarchive it first.`,
   );
}
