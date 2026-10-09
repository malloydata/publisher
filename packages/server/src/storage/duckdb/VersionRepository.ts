// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { PackageVersionError } from "../../errors";
import {
   NewVersion,
   PromoteRule,
   Version,
   VersionArchiveStatus,
} from "../DatabaseInterface";
import {
   DuckDBConnection,
   isUniqueViolation,
   TransactionQueries,
} from "./DuckDBConnection";

type Row = Record<string, unknown>;

const KEY = "environment_id = ? AND package_name = ? AND version_id = ?";

/**
 * The `versions` registry: one row per published version, plus the package's
 * `latest` pointer on `packages.latest_version`.
 *
 * Every write that reads state to decide what to write (publish with
 * promotion, moving `latest`, archiving) runs as ONE transaction on the
 * registry connection. The transaction holds that connection for its whole
 * length, so the read and the write cannot be split by another caller, and a
 * failure leaves nothing half-written.
 */
export class VersionRepository {
   constructor(private db: DuckDBConnection) {}

   async list(environmentId: string, packageName: string): Promise<Version[]> {
      const rows = await this.db.all<Row>(
         "SELECT * FROM versions WHERE environment_id = ? AND package_name = ? ORDER BY created_at",
         [environmentId, packageName],
      );
      return rows.map(mapRow);
   }

   async listByEnvironment(environmentId: string): Promise<Version[]> {
      const rows = await this.db.all<Row>(
         "SELECT * FROM versions WHERE environment_id = ? ORDER BY package_name, created_at",
         [environmentId],
      );
      return rows.map(mapRow);
   }

   async get(
      environmentId: string,
      packageName: string,
      versionId: string,
   ): Promise<Version | null> {
      const row = await this.db.get<Row>(
         `SELECT * FROM versions WHERE ${KEY}`,
         [environmentId, packageName, versionId],
      );
      return row ? mapRow(row) : null;
   }

   async commitPublish(
      version: NewVersion,
      promote: PromoteRule,
   ): Promise<{ version: Version; promoted: boolean }> {
      const { environmentId, packageName, versionId } = version;
      return this.db.transaction(async (tx) => {
         const current = await readLatest(tx, environmentId, packageName);
         const promoted = current !== versionId && promote(current);
         const now = new Date().toISOString();
         let row: Row | null;
         try {
            row = await tx.get<Row>(
               `INSERT INTO versions (version_id, environment_id, package_name, dir_name, content_hash, source_location, manifest_path, archive_status, archived_at, promoted_at, demoted_at, description, git_commit_sha, git_ref, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'unarchive', NULL, ?, NULL, ?, NULL, NULL, ?, ?)
                RETURNING *`,
               [
                  versionId,
                  environmentId,
                  packageName,
                  version.dirName,
                  version.contentHash,
                  version.sourceLocation,
                  version.manifestPath,
                  promoted ? now : null,
                  version.description,
                  now,
                  now,
               ],
            );
         } catch (err) {
            if (isUniqueViolation(err)) {
               throw new PackageVersionError(
                  "VERSION_CONFLICT",
                  `Version ${versionId} of package ${packageName} is already published.`,
               );
            }
            throw err;
         }
         if (promoted) {
            await movePointer(
               tx,
               environmentId,
               packageName,
               current,
               versionId,
               now,
            );
            row = await tx.get<Row>(`SELECT * FROM versions WHERE ${KEY}`, [
               environmentId,
               packageName,
               versionId,
            ]);
         }
         return { version: mapRow(row!), promoted };
      });
   }

   async setLatest(
      environmentId: string,
      packageName: string,
      versionId: string,
      onlyIf?: PromoteRule,
   ): Promise<boolean> {
      return this.db.transaction(async (tx) => {
         const target = await tx.get<Row>(
            `SELECT archive_status FROM versions WHERE ${KEY}`,
            [environmentId, packageName, versionId],
         );
         if (!target) throw versionNotFound(packageName, versionId);
         if (target.archive_status === "archive") {
            throw new PackageVersionError(
               "VERSION_ARCHIVED",
               `Version ${versionId} of package ${packageName} is archived; unarchive it before making it latest.`,
            );
         }
         const current = await readLatest(tx, environmentId, packageName);
         if (current === versionId) return false;
         if (onlyIf && !onlyIf(current)) return false;
         await movePointer(
            tx,
            environmentId,
            packageName,
            current,
            versionId,
            new Date().toISOString(),
         );
         return true;
      });
   }

   async setArchiveStatus(
      environmentId: string,
      packageName: string,
      versionId: string,
      status: VersionArchiveStatus,
   ): Promise<Version> {
      return this.db.transaction(async (tx) => {
         const target = await tx.get<Row>(
            `SELECT * FROM versions WHERE ${KEY}`,
            [environmentId, packageName, versionId],
         );
         if (!target) throw versionNotFound(packageName, versionId);
         if (target.archive_status === status) return mapRow(target);
         if (status === "archive") {
            const current = await readLatest(tx, environmentId, packageName);
            if (current === versionId) {
               throw new PackageVersionError(
                  "VERSION_IS_LATEST",
                  `Version ${versionId} is the latest version of package ${packageName}; make another version latest before archiving it.`,
               );
            }
            const active = await tx.get<{ n: number | bigint }>(
               "SELECT count(*) AS n FROM versions WHERE environment_id = ? AND package_name = ? AND archive_status = 'unarchive'",
               [environmentId, packageName],
            );
            if (Number(active?.n ?? 0) <= 1) {
               throw new PackageVersionError(
                  "VERSION_IS_LAST_ACTIVE",
                  `Version ${versionId} is the last version of package ${packageName} in service; delete the package to take it out of service.`,
               );
            }
         }
         const now = new Date().toISOString();
         const row = await tx.get<Row>(
            `UPDATE versions SET archive_status = ?, archived_at = ?, updated_at = ? WHERE ${KEY} RETURNING *`,
            [
               status,
               status === "archive" ? now : null,
               now,
               environmentId,
               packageName,
               versionId,
            ],
         );
         return mapRow(row!);
      });
   }

   async setManifestPath(
      environmentId: string,
      packageName: string,
      versionId: string,
      manifestPath: string | null,
   ): Promise<Version> {
      const row = await this.db.get<Row>(
         `UPDATE versions SET manifest_path = ?, updated_at = ? WHERE ${KEY} RETURNING *`,
         [
            manifestPath,
            new Date().toISOString(),
            environmentId,
            packageName,
            versionId,
         ],
      );
      if (!row) throw versionNotFound(packageName, versionId);
      return mapRow(row);
   }

   async deleteByPackage(
      environmentId: string,
      packageName: string,
   ): Promise<void> {
      await this.db.run(
         "DELETE FROM versions WHERE environment_id = ? AND package_name = ?",
         [environmentId, packageName],
      );
   }

   async deleteByEnvironmentId(environmentId: string): Promise<void> {
      await this.db.run("DELETE FROM versions WHERE environment_id = ?", [
         environmentId,
      ]);
   }
}

/**
 * The package's `latest`, read inside a transaction. A version is recorded
 * against an existing package row, so a missing one is a caller bug: throwing
 * rolls the transaction back rather than leaving a version no package owns.
 */
async function readLatest(
   tx: TransactionQueries,
   environmentId: string,
   packageName: string,
): Promise<string | null> {
   const pkg = await tx.get<{ latest_version: string | null }>(
      "SELECT latest_version FROM packages WHERE environment_id = ? AND name = ?",
      [environmentId, packageName],
   );
   if (!pkg) {
      throw new Error(
         `Package ${packageName} has no registry row; record the package before its versions.`,
      );
   }
   return pkg.latest_version ?? null;
}

/** Point `latest` at `next`, stamping when each side changed. */
async function movePointer(
   tx: TransactionQueries,
   environmentId: string,
   packageName: string,
   previous: string | null,
   next: string,
   now: string,
): Promise<void> {
   await tx.run(
      "UPDATE packages SET latest_version = ?, updated_at = ? WHERE environment_id = ? AND name = ?",
      [next, now, environmentId, packageName],
   );
   await tx.run(
      `UPDATE versions SET promoted_at = ?, updated_at = ? WHERE ${KEY}`,
      [now, now, environmentId, packageName, next],
   );
   if (previous !== null) {
      await tx.run(
         `UPDATE versions SET demoted_at = ?, updated_at = ? WHERE ${KEY}`,
         [now, now, environmentId, packageName, previous],
      );
   }
}

function versionNotFound(packageName: string, versionId: string) {
   return new PackageVersionError(
      "VERSION_NOT_FOUND",
      `Package ${packageName} has no version ${versionId}.`,
   );
}

function mapRow(row: Row): Version {
   const text = (value: unknown): string | null =>
      value === null || value === undefined ? null : String(value);
   const date = (value: unknown): Date | null =>
      value === null || value === undefined ? null : new Date(value as string);
   return {
      versionId: row.version_id as string,
      environmentId: row.environment_id as string,
      packageName: row.package_name as string,
      dirName: row.dir_name as string,
      contentHash: row.content_hash as string,
      sourceLocation: text(row.source_location),
      manifestPath: text(row.manifest_path),
      archiveStatus: row.archive_status as VersionArchiveStatus,
      archivedAt: date(row.archived_at),
      promotedAt: date(row.promoted_at),
      demotedAt: date(row.demoted_at),
      description: text(row.description),
      gitCommitSha: text(row.git_commit_sha),
      gitRef: text(row.git_ref),
      createdAt: new Date(row.created_at as string),
      updatedAt: new Date(row.updated_at as string),
   };
}
