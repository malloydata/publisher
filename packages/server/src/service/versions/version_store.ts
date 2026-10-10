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

/**
 * A downloaded tree whose publisher.json declares no semantic version, kept
 * staged so it can be installed in place, as a package with no versions.
 */
export interface UnversionedStage {
   packageName: string;
   stagingPath: string;
   /** Why the tree is not a version: what a versioned publish refuses it with. */
   reason: PackageVersionError;
   /**
    * Whether the tree has a publisher.json at all. One without is no package,
    * and its install fails as it always has.
    */
   hasManifest: boolean;
}

/** Whether a stage is a tree that declares no semantic version. */
export function isUnversionedStage(
   stage: StagedVersion | UnversionedStage,
): stage is UnversionedStage {
   return "reason" in stage;
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
      const staged = await this.stageAny(packageName, downloader);
      if (isUnversionedStage(staged)) {
         await this.discard(staged);
         throw staged.reason;
      }
      return staged;
   }

   /**
    * Download into a fresh staging folder and read the version its
    * publisher.json declares. A tree that declares no semantic version (none,
    * one that is not semver, or no readable publisher.json) is kept staged
    * and returned as such, for an install in place.
    */
   async stageAny(
      packageName: string,
      downloader: (stagingPath: string) => Promise<void>,
   ): Promise<StagedVersion | UnversionedStage> {
      assertSafePackageName(packageName);
      const stagingPath = safeJoinUnderRoot(
         this.environmentPath,
         STAGING_DIR_NAME,
         `${packageName}-${crypto.randomUUID()}`,
      );
      await fs.promises.mkdir(path.dirname(stagingPath), { recursive: true });
      try {
         await downloader(stagingPath);
         let manifest: Awaited<ReturnType<typeof readManifestVersion>>;
         try {
            manifest = await readManifestVersion(stagingPath);
         } catch (err) {
            if (!(err instanceof PackageVersionError)) throw err;
            return {
               packageName,
               stagingPath,
               reason: err,
               hasManifest: await exists(
                  path.join(stagingPath, PACKAGE_MANIFEST_NAME),
               ),
            };
         }
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
    */
   async holdLegacy(packageName: string): Promise<LegacyTree | null> {
      assertSafePackageName(packageName);
      const packagePath = safeJoinUnderRoot(this.environmentPath, packageName);
      if (!(await exists(packagePath))) return null;
      const heldPath = safeJoinUnderRoot(
         this.environmentPath,
         LEGACY_DIR_NAME,
         `${packageName}-${crypto.randomUUID()}`,
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
         if (leftover) {
            await fs.promises.rename(leftover, packagePath).catch(() => {});
         }
         throw err;
      }
      if (leftover) await removeQuietly(leftover);
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
    *    versioned publish committed before the crash) and put back otherwise.
    *    Of several held for one package, only the newest is put back.
    *  - In a versioned package's folder, a version folder with no row is
    *    removed: a placement whose publish never committed. Folders of
    *    packages with no rows are never touched, since those hold unversioned
    *    packages.
    */
   async cleanup(versionedPackages: Map<string, Set<string>>): Promise<void> {
      const allHeld = await this.listHeldLegacy();
      const newest = new Map<string, (typeof allHeld)[number]>();
      for (const held of allHeld) {
         const current = newest.get(held.packageName);
         if (!current || held.mtimeMs > current.mtimeMs) {
            newest.set(held.packageName, held);
         }
      }
      for (const held of allHeld) {
         try {
            if (
               versionedPackages.has(held.packageName) ||
               newest.get(held.packageName) !== held
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
   }

   private async listHeldLegacy(): Promise<
      (LegacyTree & { mtimeMs: number })[]
   > {
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
      const held: (LegacyTree & { mtimeMs: number })[] = [];
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
         held.push({ packageName, heldPath, mtimeMs: stat.mtimeMs });
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
      text = await fs.promises.readFile(manifestPath, "utf8");
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
