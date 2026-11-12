// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

/**
 * An in-memory stand-in for the repository's package rows and version
 * registry, for specs that boot an `EnvironmentStore` over a stubbed
 * `StorageManager` (see storage_manager_stub.ts). A configured package is
 * published as a version at boot, so such a store needs somewhere to record
 * it; these specs are about boot resilience, not the registry, which
 * VersionRepository.spec.ts covers against the real database.
 *
 * It follows DuckDB's VersionRepository: one package row per environment and
 * name (a duplicate create throws a UNIQUE error, as the store expects), the
 * package's `latest` on that row and moved only by the version calls, a
 * duplicate version is VERSION_CONFLICT, and a version recorded against a
 * package with no row throws. Spread the result into a stub repository,
 * after its own package methods, so these win. Build one per test: it is the
 * database, so two stores in one test share it.
 */

import { PackageVersionError } from "../errors";
import type {
   NewVersion,
   PackageRecord,
   PromoteRule,
   Version,
   VersionArchiveStatus,
} from "../storage/DatabaseInterface";

export function inMemoryVersionRegistry(): Record<string, unknown> {
   const packages = new Map<string, PackageRecord>();
   const versions = new Map<string, Version>();
   let nextId = 0;

   const packageKey = (environmentId: string, name: string) =>
      `${environmentId}\u0000${name}`;
   const versionKey = (
      environmentId: string,
      packageName: string,
      versionId: string,
   ) => `${environmentId}\u0000${packageName}\u0000${versionId}`;
   const versionsOf = (environmentId: string, packageName: string) =>
      [...versions.values()].filter(
         (v) =>
            v.environmentId === environmentId && v.packageName === packageName,
      );
   const notFound = (packageName: string, versionId: string) =>
      new PackageVersionError(
         "VERSION_NOT_FOUND",
         `Package ${packageName} has no version ${versionId}.`,
      );
   const readLatest = (environmentId: string, packageName: string) => {
      const row = packages.get(packageKey(environmentId, packageName));
      if (!row) {
         throw new Error(
            `Package ${packageName} has no registry row; record the package before its versions.`,
         );
      }
      return row.latestVersion ?? null;
   };
   const movePointer = (
      environmentId: string,
      packageName: string,
      previous: string | null,
      next: string,
   ) => {
      const now = new Date();
      const row = packages.get(packageKey(environmentId, packageName))!;
      row.latestVersion = next;
      row.updatedAt = now;
      const promoted = versions.get(
         versionKey(environmentId, packageName, next),
      );
      if (promoted) promoted.promotedAt = now;
      if (previous !== null) {
         const demoted = versions.get(
            versionKey(environmentId, packageName, previous),
         );
         if (demoted) demoted.demotedAt = now;
      }
   };
   const findPackageById = (id: string) =>
      [...packages.entries()].find(([, row]) => row.id === id);

   return {
      getPackageByName: async (environmentId: string, name: string) => {
         const row = packages.get(packageKey(environmentId, name));
         return row ? { ...row } : null;
      },
      createPackage: async (
         data: Omit<PackageRecord, "id" | "createdAt" | "updatedAt">,
      ) => {
         const key = packageKey(data.environmentId, data.name);
         if (packages.has(key)) {
            throw new Error(
               `UNIQUE constraint failed: package ${data.name} already exists`,
            );
         }
         const now = new Date();
         const row: PackageRecord = {
            ...data,
            id: `pkg-${++nextId}`,
            latestVersion: null,
            createdAt: now,
            updatedAt: now,
         };
         packages.set(key, row);
         return { ...row };
      },
      updatePackage: async (id: string, updates: Partial<PackageRecord>) => {
         const found = findPackageById(id);
         if (!found) throw new Error(`Package ${id} not found`);
         const [, row] = found;
         // `latest` moves only through the version calls.
         const { latestVersion: _ignored, ...rest } = updates;
         Object.assign(row, rest, { updatedAt: new Date() });
         return { ...row };
      },
      deletePackage: async (id: string) => {
         const found = findPackageById(id);
         if (!found) return;
         const [key, row] = found;
         packages.delete(key);
         for (const v of versionsOf(row.environmentId, row.name)) {
            versions.delete(
               versionKey(v.environmentId, v.packageName, v.versionId),
            );
         }
      },
      deletePackageRecord: async (id: string) => {
         const found = findPackageById(id);
         if (found) packages.delete(found[0]);
      },

      listVersions: async (environmentId: string, packageName: string) =>
         versionsOf(environmentId, packageName).map((v) => ({ ...v })),
      hasVersions: async (environmentId: string, packageName: string) =>
         versionsOf(environmentId, packageName).length > 0,
      listVersionsByEnvironment: async (environmentId: string) =>
         [...versions.values()]
            .filter((v) => v.environmentId === environmentId)
            .map((v) => ({ ...v })),
      getVersion: async (
         environmentId: string,
         packageName: string,
         versionId: string,
      ) => {
         const row = versions.get(
            versionKey(environmentId, packageName, versionId),
         );
         return row ? { ...row } : null;
      },
      commitPublish: async (version: NewVersion, promote: PromoteRule) => {
         const { environmentId, packageName, versionId } = version;
         const current = readLatest(environmentId, packageName);
         const key = versionKey(environmentId, packageName, versionId);
         if (versions.has(key)) {
            throw new PackageVersionError(
               "VERSION_CONFLICT",
               `Version ${versionId} of package ${packageName} is already published.`,
            );
         }
         const promoted = current !== versionId && promote(current);
         const now = new Date();
         versions.set(key, {
            ...version,
            archiveStatus: "unarchive",
            archivedAt: null,
            promotedAt: null,
            demotedAt: null,
            gitCommitSha: null,
            gitRef: null,
            createdAt: now,
            updatedAt: now,
         });
         if (promoted) {
            movePointer(environmentId, packageName, current, versionId);
         }
         return { version: { ...versions.get(key)! }, promoted };
      },
      setLatestVersion: async (
         environmentId: string,
         packageName: string,
         versionId: string,
         onlyIf?: PromoteRule,
      ) => {
         const target = versions.get(
            versionKey(environmentId, packageName, versionId),
         );
         if (!target) throw notFound(packageName, versionId);
         if (target.archiveStatus === "archive") {
            throw new PackageVersionError(
               "VERSION_ARCHIVED",
               `Version ${versionId} of package ${packageName} is archived; unarchive it before making it latest.`,
            );
         }
         const current = readLatest(environmentId, packageName);
         if (current === versionId) return false;
         if (onlyIf && !onlyIf(current)) return false;
         movePointer(environmentId, packageName, current, versionId);
         return true;
      },
      setVersionArchiveStatus: async (
         environmentId: string,
         packageName: string,
         versionId: string,
         status: VersionArchiveStatus,
      ) => {
         const target = versions.get(
            versionKey(environmentId, packageName, versionId),
         );
         if (!target) throw notFound(packageName, versionId);
         if (target.archiveStatus === status) return { ...target };
         if (status === "archive") {
            if (readLatest(environmentId, packageName) === versionId) {
               throw new PackageVersionError(
                  "VERSION_IS_LATEST",
                  `Version ${versionId} is the latest version of package ${packageName}; make another version latest before archiving it.`,
               );
            }
            const active = versionsOf(environmentId, packageName).filter(
               (v) => v.archiveStatus === "unarchive",
            );
            if (active.length <= 1) {
               throw new PackageVersionError(
                  "VERSION_IS_LAST_ACTIVE",
                  `Version ${versionId} is the last version of package ${packageName} in service; delete the package to take it out of service.`,
               );
            }
         }
         const now = new Date();
         target.archiveStatus = status;
         target.archivedAt = status === "archive" ? now : null;
         target.updatedAt = now;
         return { ...target };
      },
      setVersionManifestPath: async (
         environmentId: string,
         packageName: string,
         versionId: string,
         manifestPath: string | null,
      ) => {
         const target = versions.get(
            versionKey(environmentId, packageName, versionId),
         );
         if (!target) throw notFound(packageName, versionId);
         target.manifestPath = manifestPath;
         target.updatedAt = new Date();
         return { ...target };
      },
      deleteVersion: async (
         environmentId: string,
         packageName: string,
         versionId: string,
      ) => {
         const key = versionKey(environmentId, packageName, versionId);
         const target = versions.get(key);
         if (!target) throw notFound(packageName, versionId);
         const all = versionsOf(environmentId, packageName);
         const wasLast = all.length <= 1;
         if (!wasLast && readLatest(environmentId, packageName) === versionId) {
            throw new PackageVersionError(
               "VERSION_IS_LATEST",
               `Version ${versionId} is the latest version of package ${packageName}; make another version latest before deleting it.`,
            );
         }
         versions.delete(key);
         return { version: { ...target }, wasLast };
      },
      deletePackageIfNoVersions: async (
         environmentId: string,
         packageName: string,
      ) => {
         if (versionsOf(environmentId, packageName).length > 0) return false;
         packages.delete(packageKey(environmentId, packageName));
         return true;
      },

      // A version's load re-reads its serve bindings from the materialization
      // records; a failed read unloads it again. Nothing here has built.
      listMaterializations: async () => [],
   };
}
