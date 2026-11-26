// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import * as crypto from "crypto";
import * as fs from "fs";
import * as path from "path";
import { PACKAGE_MANIFEST_NAME } from "../../constants";
import { PackageVersionError } from "../../errors";
import { logger } from "../../logger";
import {
   assertSafeEnvironmentPath,
   assertSafePackageName,
   safeJoinUnderRoot,
} from "../../path_safety";
import { hashPackageTree } from "./package_content_hash";
import { isSemver, versionDirName } from "./semver";
import { pathToFileURL } from "url";
import { readPackageURL } from "../../utils";

/** Downloads in progress, shared with the unversioned install path. */
const STAGING_DIR_NAME = ".staging";
/**
 * Unversioned trees moved aside by a package's first versioned publish. A tree
 * stays here only while that publish runs: it is dropped when the publish
 * commits and put back when it fails, and one left by a crash is put back at
 * startup unless the package has versions by then.
 */
const LEGACY_DIR_NAME = ".legacy";

/**
 * A version folder name: `versionDirName` of a semantic version, so the
 * version's own characters with `+` written as `_`. Checked on every name the
 * store joins into a path, including names read back from the registry.
 * Unambiguous by construction (the suffix must start with `-` or `_`), so a
 * long non-matching name fails in linear time.
 */
const DIR_NAME_RE = /^[0-9]+\.[0-9]+\.[0-9]+(?:[-_][0-9A-Za-z._-]*)?$/;

/** A downloaded tree, read and hashed, not yet placed. */
export interface StagedVersion {
   packageName: string;
   stagingPath: string;
   versionId: string;
   dirName: string;
   contentHash: string;
   /** The `description` of the version's own publisher.json. */
   description: string | null;
}

/** An unversioned tree held in `.legacy/` while a first versioned publish runs. */
export interface LegacyTree {
   packageName: string;
   heldPath: string;
}

/**
 * The files of a package's versions, under the environment directory:
 *
 *  - `<pkg>/<dir>/` holds one version, written once and never changed. `<dir>`
 *    is the version's `dir_name`; the registry row says which folders exist.
 *  - `.staging/<pkg>-<uuid>/` holds a download until it is placed.
 *  - `.legacy/<pkg>-<uuid>/` holds an unversioned tree a first versioned
 *    publish moved aside.
 *
 * It knows nothing of the registry or of loading: callers decide, from the
 * rows, what to place, restore and clean up, and hold the locks.
 */
export class VersionStore {
   private readonly environmentPath: string;

   constructor(environmentPath: string) {
      assertSafeEnvironmentPath(environmentPath);
      this.environmentPath = environmentPath;
   }

   /** Where a version's files live. */
   versionPath(packageName: string, dirName: string): string {
      assertSafePackageName(packageName);
      assertSafeDirName(dirName);
      return safeJoinUnderRoot(this.environmentPath, packageName, dirName);
   }

   /**
    * Download into a fresh staging folder, then read the version from the
    * tree's publisher.json and hash it. Takes no lock: this is the long part
    * of a publish. Refuses a missing or non-semver version with 400, and
    * removes the staging folder on any failure.
    */
   async stage(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
   ): Promise<StagedVersion> {
      assertSafePackageName(packageName);
      const stagingPath = safeJoinUnderRoot(
         this.environmentPath,
         STAGING_DIR_NAME,
         `${packageName}-${crypto.randomUUID()}`,
      );
      await fs.promises.mkdir(path.dirname(stagingPath), { recursive: true });
      try {
         await downloader(stagingPath);
         const manifest = await readManifestVersion(stagingPath);
         return {
            packageName,
            stagingPath,
            versionId: manifest.versionId,
            dirName: versionDirName(manifest.versionId),
            contentHash: await hashPackageTree(stagingPath),
            description: manifest.description,
         };
      } catch (err) {
         await removeQuietly(stagingPath);
         throw err;
      }
   }

   /** Remove a staged tree that will not be placed. */
   async discard(staged: { stagingPath: string }): Promise<void> {
      await removeQuietly(this.within(STAGING_DIR_NAME, staged.stagingPath));
   }

   /**
    * `candidate`, checked to sit inside this environment's `area` folder. The
    * staged and held paths callers hand back were made here, but the store
    * never removes or renames a path on the caller's word alone.
    */
   private within(area: string, candidate: string): string {
      const root = safeJoinUnderRoot(this.environmentPath, area);
      // Resolved first, so no `..` segment survives to escape; a package name
      // may still contain `..` inside a segment (`a..b`), which is harmless.
      const resolved = path.resolve(candidate);
      if (!resolved.startsWith(root + path.sep)) {
         throw new Error(
            `Not a path under ${area}: ${JSON.stringify(candidate)}`,
         );
      }
      return resolved;
   }

   /**
    * Move a staged tree into `<pkg>/<dir>/`. Write-once: a folder already
    * there is never replaced, and the staged tree is discarded instead.
    * Returns whether this call placed it.
    */
   async place(staged: StagedVersion): Promise<boolean> {
      const target = this.versionPath(staged.packageName, staged.dirName);
      if (await exists(target)) {
         await this.discard(staged);
         return false;
      }
      await fs.promises.mkdir(path.dirname(target), { recursive: true });
      await fs.promises.rename(
         this.within(STAGING_DIR_NAME, staged.stagingPath),
         target,
      );
      return true;
   }

   /** Whether a version's folder is on disk. */
   async isPlaced(packageName: string, dirName: string): Promise<boolean> {
      return exists(this.versionPath(packageName, dirName));
   }

   /**
    * Remove one version's folder. Only for a version with no registry row: a
    * placement whose publish failed, or an orphan.
    */
   async remove(packageName: string, dirName: string): Promise<void> {
      await removeQuietly(this.versionPath(packageName, dirName));
   }

   /** Remove every version folder of a package, with the package folder. */
   async removePackage(packageName: string): Promise<void> {
      assertSafePackageName(packageName);
      await removeQuietly(safeJoinUnderRoot(this.environmentPath, packageName));
   }

   /**
    * Put a registered version's missing folder back, from `downloader` (its
    * `source_location`). Kept only when it hashes to the published hash: a
    * location whose content has changed must not be served under the old
    * version's name. A folder already present is left alone. Returns whether
    * the folder is in place afterwards.
    */
   async restore(
      packageName: string,
      expected: { versionId: string; dirName: string; contentHash: string },
      downloader: (stagingPath: string) => Promise<void>,
   ): Promise<boolean> {
      if (await this.isPlaced(packageName, expected.dirName)) return true;
      let staged: StagedVersion;
      try {
         staged = await this.stage(packageName, downloader);
      } catch (err) {
         logger.warn("Could not fetch a missing version again", {
            packageName,
            versionId: expected.versionId,
            error: err,
         });
         return false;
      }
      if (
         staged.versionId !== expected.versionId ||
         staged.contentHash !== expected.contentHash
      ) {
         logger.warn(
            "A missing version's location no longer holds what was published; it stays missing",
            {
               packageName,
               versionId: expected.versionId,
               fetchedVersion: staged.versionId,
            },
         );
         await this.discard(staged);
         return false;
      }
      await this.place(staged);
      return true;
   }

   /**
    * Before a package's first versioned publish: move its unversioned tree,
    * if it has one, out to `.legacy/` so `<pkg>/` can hold version folders.
    * Returns the held tree, to put back or drop when the publish ends.
    *
    * A package has at most one held tree. One a failed put-back left held is
    * still the package's unversioned tree, so it is this publish's hold too,
    * and the empty folder beside it is removed rather than held. A tree
    * installed at `<pkg>/` since (its own publisher.json) is newer, so it is
    * held and the older one dropped. Version folders beside a held tree are a
    * failed publish's uncommitted placements and are removed. Anything else
    * beside a held tree is refused, moving nothing.
    */
   async holdLegacy(packageName: string): Promise<LegacyTree | null> {
      assertSafePackageName(packageName);
      const packagePath = safeJoinUnderRoot(this.environmentPath, packageName);
      // Only a missing folder means "no tree". Any other failure (a file named
      // <pkg>, EACCES, EMFILE) throws, so the publish fails before it touches
      // the folder rather than treating an unreadable tree as absent.
      const contents = await fs.promises.readdir(packagePath).catch((err) => {
         if ((err as NodeJS.ErrnoException).code === "ENOENT") return null;
         throw err;
      });
      const prior = (await this.listHeldLegacy())
         .filter((held) => held.packageName === packageName)
         .sort(heldLastFirst);
      if (prior.length > 0) {
         // Version folders beside a held tree are a failed publish's
         // placements: a first publish holds the tree only when the package
         // has no version rows, so none of them was committed. They go, as
         // startup's cleanup removes a version folder no row owns.
         if (contents !== null && contents.length > 0) {
            const leftovers = await this.versionFoldersOnly(
               packagePath,
               contents,
            );
            if (leftovers) {
               for (const dirName of leftovers) {
                  await this.remove(packageName, dirName);
               }
               contents.length = 0;
            }
         }
         if (contents === null || contents.length === 0) {
            const [kept, ...older] = prior;
            for (const held of older) await this.dropLegacy(held);
            if (contents !== null) await fs.promises.rmdir(packagePath);
            return { packageName, heldPath: kept.heldPath };
         }
         if (!contents.includes(PACKAGE_MANIFEST_NAME)) {
            throw new Error(
               `The unversioned tree of package ${packageName} is still held in ${LEGACY_DIR_NAME}/ after a failed publish, and its folder holds files of no package. Restart the server to put the tree back, then publish again.`,
            );
         }
         for (const held of prior) await this.dropLegacy(held);
      }
      if (contents === null) return null;
      if (contents.length === 0) {
         await fs.promises.rmdir(packagePath);
         return null;
      }
      const heldPath = safeJoinUnderRoot(
         this.environmentPath,
         LEGACY_DIR_NAME,
         `${packageName}-${uuidV7()}`,
      );
      await fs.promises.mkdir(path.dirname(heldPath), { recursive: true });
      await fs.promises.rename(packagePath, heldPath);
      return { packageName, heldPath };
   }

   /**
    * Put a held unversioned tree back at `<pkg>/`, replacing whatever the
    * failed publish left there. That leftover is moved aside first and only
    * removed once the held tree is back, so a failed rename never leaves the
    * package with no tree at all.
    */
   async restoreLegacy(held: LegacyTree): Promise<void> {
      assertSafePackageName(held.packageName);
      const heldPath = this.within(LEGACY_DIR_NAME, held.heldPath);
      const packagePath = safeJoinUnderRoot(
         this.environmentPath,
         held.packageName,
      );
      let leftover: string | null = null;
      if (await exists(packagePath)) {
         leftover = safeJoinUnderRoot(
            this.environmentPath,
            STAGING_DIR_NAME,
            `${held.packageName}-${crypto.randomUUID()}`,
         );
         await fs.promises.mkdir(path.dirname(leftover), { recursive: true });
         await fs.promises.rename(packagePath, leftover);
      }
      try {
         await fs.promises.rename(heldPath, packagePath);
      } catch (err) {
         // The leftover goes back unless it is an empty folder, which would
         // only be held as the package's tree by the next publish.
         if (leftover) {
            if (await isEmptyDir(leftover)) {
               await removeQuietly(leftover);
            } else {
               await fs.promises.rename(leftover, packagePath).catch(() => {});
            }
         }
         throw err;
      }
      if (leftover) await removeQuietly(leftover);
   }

   /**
    * The entries of `<pkg>/` when every one is a version folder (a directory
    * named as `versionDirName` names one), or null when any is not.
    */
   private async versionFoldersOnly(
      packagePath: string,
      entries: string[],
   ): Promise<string[] | null> {
      for (const name of entries) {
         if (!DIR_NAME_RE.test(name)) return null;
         const stat = await fs.promises
            .lstat(path.join(packagePath, name))
            .catch(() => undefined);
         if (!stat?.isDirectory()) return null;
      }
      return entries;
   }

   /** Remove a held unversioned tree once the publish that held it commits. */
   async dropLegacy(held: LegacyTree): Promise<void> {
      await removeQuietly(this.within(LEGACY_DIR_NAME, held.heldPath));
   }

   /**
    * Startup cleanup, before anything loads. `versionedPackages` maps each
    * package that has registry rows to the folder names those rows own; it
    * is the only thing that says a folder belongs to a version.
    *
    *  - A held legacy tree is dropped if its package has versions (its first
    *    versioned publish committed before the crash), or if `<pkg>/` holds a
    *    tree of its own (a publisher.json: installed since it was held), and
    *    put back otherwise. Of several held for one package, only the one held
    *    last is put back (see heldLastFirst).
    *  - In a versioned package's folder, a version folder with no row is
    *    removed: a placement whose publish never committed. Folders of
    *    packages with no version rows are never touched there, since those
    *    hold unversioned packages.
    *  - With `packagesWithRows` (every package that has a package row), a
    *    package folder none owns is removed when it holds only version
    *    folders, each with a publisher.json, and no publisher.json of its
    *    own: a new package's first publish placed it and crashed before its
    *    commit wrote the rows. A real package always has its own
    *    publisher.json, and a symlink (a watch mount) is never touched.
    */
   async cleanup(
      versionedPackages: Map<string, Set<string>>,
      packagesWithRows?: Set<string>,
   ): Promise<void> {
      const allHeld = (await this.listHeldLegacy()).sort(heldLastFirst);
      const newest = new Map<string, (typeof allHeld)[number]>();
      for (const held of allHeld) {
         if (!newest.has(held.packageName)) {
            newest.set(held.packageName, held);
         }
      }
      for (const held of allHeld) {
         try {
            if (
               versionedPackages.has(held.packageName) ||
               newest.get(held.packageName) !== held ||
               (await exists(
                  path.join(
                     safeJoinUnderRoot(this.environmentPath, held.packageName),
                     PACKAGE_MANIFEST_NAME,
                  ),
               ))
            ) {
               await this.dropLegacy(held);
            } else {
               logger.warn(
                  "Putting back an unversioned package tree a crashed first versioned publish left aside",
                  { packageName: held.packageName },
               );
               await this.restoreLegacy(held);
            }
         } catch (err) {
            logger.warn("Could not settle a held unversioned package tree", {
               packageName: held.packageName,
               error: err,
            });
         }
      }

      for (const [packageName, owned] of versionedPackages) {
         let entries: fs.Dirent[];
         try {
            assertSafePackageName(packageName);
            entries = await fs.promises.readdir(
               safeJoinUnderRoot(this.environmentPath, packageName),
               { withFileTypes: true },
            );
         } catch {
            continue;
         }
         for (const entry of entries) {
            if (!entry.isDirectory() || owned.has(entry.name)) continue;
            if (!DIR_NAME_RE.test(entry.name)) continue;
            logger.info("Removing a version folder no published version owns", {
               packageName,
               dirName: entry.name,
            });
            await this.remove(packageName, entry.name);
         }
      }

      if (packagesWithRows)
         await this.removeUnownedPlacements(packagesWithRows);
   }

   /** See cleanup: a crashed first publish's folder, with no package row. */
   private async removeUnownedPlacements(
      packagesWithRows: Set<string>,
   ): Promise<void> {
      let entries: fs.Dirent[];
      try {
         entries = await fs.promises.readdir(this.environmentPath, {
            withFileTypes: true,
         });
      } catch {
         return;
      }
      for (const entry of entries) {
         const name = entry.name;
         // A symlink is a watch mount, and a dot-folder the server's own.
         if (!entry.isDirectory() || name.startsWith(".")) continue;
         if (packagesWithRows.has(name)) continue;
         try {
            assertSafePackageName(name);
            const packagePath = safeJoinUnderRoot(this.environmentPath, name);
            const inside = await fs.promises.readdir(packagePath);
            if (inside.length === 0 || inside.includes(PACKAGE_MANIFEST_NAME)) {
               continue;
            }
            if (!(await this.versionFoldersOnly(packagePath, inside))) continue;
            let placed = true;
            for (const dirName of inside) {
               if (
                  !(await exists(
                     path.join(packagePath, dirName, PACKAGE_MANIFEST_NAME),
                  ))
               ) {
                  placed = false;
                  break;
               }
            }
            if (!placed) continue;
            logger.info(
               "Removing a package folder a first publish placed and never committed",
               { packageName: name, versionFolders: inside },
            );
            await this.removePackage(name);
         } catch (err) {
            logger.warn("Could not check a package folder no row owns", {
               packageName: name,
               error: err,
            });
         }
      }
   }

   private async listHeldLegacy(): Promise<HeldTree[]> {
      const legacyRoot = safeJoinUnderRoot(
         this.environmentPath,
         LEGACY_DIR_NAME,
      );
      let names: string[];
      try {
         names = await fs.promises.readdir(legacyRoot);
      } catch {
         return [];
      }
      const held: HeldTree[] = [];
      for (const name of names) {
         // `<pkg>-<uuid>`: a v4 UUID is 36 characters.
         const packageName = name.slice(0, -37);
         if (name.charAt(name.length - 37) !== "-" || packageName === "") {
            continue;
         }
         try {
            assertSafePackageName(packageName);
         } catch {
            continue;
         }
         const heldPath = safeJoinUnderRoot(legacyRoot, name);
         const stat = await fs.promises.lstat(heldPath).catch(() => undefined);
         if (!stat) continue;
         held.push({
            packageName,
            heldPath,
            mtimeMs: stat.mtimeMs,
            id: name.slice(-36),
         });
      }
      return held;
   }
}

/**
 * The version a tree declares, from its publisher.json: the publish takes no
 * version of its own, so this is the only place one comes from.
 */
export async function readManifestVersion(
   packagePath: string,
): Promise<{ versionId: string; description: string | null }> {
   const manifestPath = path.join(packagePath, PACKAGE_MANIFEST_NAME);
   let text: string;
   try {
      // Inside the package only: a manifest that links out, to anything or
      // nothing, reads as no manifest at all.
      text = await readPackageURL(packagePath, pathToFileURL(manifestPath));
   } catch {
      throw new PackageVersionError(
         "MANIFEST_VERSION_MISSING",
         `The package has no ${PACKAGE_MANIFEST_NAME}, so it declares no version. Add one with a "version" field (for example "1.0.0") to publish it.`,
      );
   }
   let manifest: unknown;
   try {
      manifest = JSON.parse(text);
   } catch {
      throw new PackageVersionError(
         "MANIFEST_VERSION_INVALID",
         `The package's ${PACKAGE_MANIFEST_NAME} is not valid JSON, so its version cannot be read.`,
      );
   }
   const fields =
      manifest && typeof manifest === "object"
         ? (manifest as Record<string, unknown>)
         : {};
   const version = fields.version;
   if (version === undefined || version === null || version === "") {
      throw new PackageVersionError(
         "MANIFEST_VERSION_MISSING",
         `The package's ${PACKAGE_MANIFEST_NAME} has no "version". Set one (for example "1.0.0") to publish it; bump it for every release.`,
      );
   }
   if (typeof version !== "string" || !isSemver(version)) {
      throw new PackageVersionError(
         "MANIFEST_VERSION_INVALID",
         `The "version" in the package's ${PACKAGE_MANIFEST_NAME} (${JSON.stringify(version)}) is not a semantic version such as "1.2.0" or "1.2.0-rc1".`,
      );
   }
   return {
      versionId: version,
      description:
         typeof fields.description === "string" ? fields.description : null,
   };
}

/** A held tree as listed from `.legacy/`: its UUID and its folder's mtime. */
type HeldTree = LegacyTree & { mtimeMs: number; id: string };

/**
 * A UUIDv7: 48 bits of Unix milliseconds, then random bits. Lower-case hex,
 * so names compare in the order they were made. A held tree's name carries
 * one: its folder's mtime does not change when it is renamed into place, so
 * only the name says when it was held.
 */
export function uuidV7(nowMs: number = Date.now()): string {
   const bytes = crypto.randomBytes(16);
   bytes.writeUIntBE(nowMs, 0, 6);
   bytes[6] = (bytes[6] & 0x0f) | 0x70;
   bytes[8] = (bytes[8] & 0x3f) | 0x80;
   const hex = bytes.toString("hex");
   return `${hex.slice(0, 8)}-${hex.slice(8, 12)}-${hex.slice(12, 16)}-${hex.slice(16, 20)}-${hex.slice(20)}`;
}

/**
 * Held trees, the one held last first: by name for a v7 UUID, which every
 * hold writes from this release on, and below those, by mtime as before,
 * for the v4 UUID an older release wrote.
 */
function heldLastFirst(a: HeldTree, b: HeldTree): number {
   const aV7 = a.id.charAt(14) === "7";
   const bV7 = b.id.charAt(14) === "7";
   if (aV7 !== bV7) return aV7 ? -1 : 1;
   if (aV7) return a.id < b.id ? 1 : a.id > b.id ? -1 : 0;
   return b.mtimeMs - a.mtimeMs;
}

async function isEmptyDir(target: string): Promise<boolean> {
   const entries = await fs.promises.readdir(target).catch(() => null);
   return entries !== null && entries.length === 0;
}

function assertSafeDirName(dirName: string): void {
   if (!DIR_NAME_RE.test(dirName) || dirName.indexOf("..") !== -1) {
      throw new Error(`Not a version folder name: ${JSON.stringify(dirName)}`);
   }
}

async function exists(target: string): Promise<boolean> {
   return fs.promises
      .lstat(target)
      .then(() => true)
      .catch(() => false);
}

async function removeQuietly(target: string): Promise<void> {
   await fs.promises
      .rm(target, { recursive: true, force: true })
      .catch((err) =>
         logger.warn(`Failed to remove ${target}`, { error: err }),
      );
}
