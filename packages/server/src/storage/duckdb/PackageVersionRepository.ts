// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import {
   DuplicatePackageVersionError,
   PackageVersion,
   PackageVersionArchiveStatus,
   PackageVersionUpdate,
} from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

/**
 * DuckDB-backed repository for published package versions (`package_versions`).
 *
 * A version row is created once, at publish, and never re-created: a second
 * insert of the same (environment, package, version) is refused with
 * {@link DuplicatePackageVersionError}. What changes afterwards is limited to
 * {@link PackageVersionUpdate}: lifecycle state and manifest binding, never the
 * content identity (`content_hash`, `dir_name`, `source_location`).
 */
export class PackageVersionRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
   }

   async list(
      environmentId: string,
      packageName: string,
   ): Promise<PackageVersion[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM package_versions WHERE environment_id = ? AND package_name = ? ORDER BY created_at",
         [environmentId, packageName],
      );
      return rows.map(mapRow);
   }

   async listByEnvironment(environmentId: string): Promise<PackageVersion[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM package_versions WHERE environment_id = ? ORDER BY package_name, created_at",
         [environmentId],
      );
      return rows.map(mapRow);
   }

   async get(
      environmentId: string,
      packageName: string,
      version: string,
   ): Promise<PackageVersion | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM package_versions WHERE environment_id = ? AND package_name = ? AND version = ?",
         [environmentId, packageName, version],
      );
      return row ? mapRow(row) : null;
   }

   async create(
      version: Omit<PackageVersion, "id" | "createdAt" | "updatedAt">,
   ): Promise<PackageVersion> {
      const iso = new Date().toISOString();
      try {
         const rows = await this.db.all<Record<string, unknown>>(
            `INSERT INTO package_versions (id, environment_id, package_name, version, dir_name, content_hash, source_location, manifest_location, archive_status, archived_at, description, git_commit_sha, git_ref, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            RETURNING *`,
            [
               this.generateId(),
               version.environmentId,
               version.packageName,
               version.version,
               version.dirName,
               version.contentHash,
               version.sourceLocation,
               version.manifestLocation,
               version.archiveStatus,
               version.archivedAt ? version.archivedAt.toISOString() : null,
               version.description,
               version.gitCommitSha,
               version.gitRef,
               iso,
               iso,
            ],
         );
         return mapRow(rows[0]);
      } catch (err) {
         if (isUniqueViolation(err)) {
            throw new DuplicatePackageVersionError(
               version.packageName,
               version.version,
            );
         }
         throw err;
      }
   }

   async update(
      id: string,
      updates: PackageVersionUpdate,
   ): Promise<PackageVersion> {
      const setClauses: string[] = [];
      const params: unknown[] = [];
      if (updates.archiveStatus !== undefined) {
         setClauses.push("archive_status = ?");
         params.push(updates.archiveStatus);
      }
      if (updates.archivedAt !== undefined) {
         setClauses.push("archived_at = ?");
         params.push(
            updates.archivedAt ? updates.archivedAt.toISOString() : null,
         );
      }
      if (updates.manifestLocation !== undefined) {
         setClauses.push("manifest_location = ?");
         params.push(updates.manifestLocation);
      }
      setClauses.push("updated_at = ?");
      params.push(new Date().toISOString());
      params.push(id);
      const rows = await this.db.all<Record<string, unknown>>(
         `UPDATE package_versions SET ${setClauses.join(", ")} WHERE id = ? RETURNING *`,
         params,
      );
      if (rows.length === 0) {
         throw new Error(`Package version with id ${id} not found`);
      }
      return mapRow(rows[0]);
   }

   async deleteByPackage(
      environmentId: string,
      packageName: string,
   ): Promise<void> {
      await this.db.run(
         "DELETE FROM package_versions WHERE environment_id = ? AND package_name = ?",
         [environmentId, packageName],
      );
   }

   async deleteByEnvironmentId(environmentId: string): Promise<void> {
      await this.db.run(
         "DELETE FROM package_versions WHERE environment_id = ?",
         [environmentId],
      );
   }
}

function mapRow(row: Record<string, unknown>): PackageVersion {
   const text = (value: unknown): string | null =>
      value === null || value === undefined ? null : String(value);
   return {
      id: row.id as string,
      environmentId: row.environment_id as string,
      packageName: row.package_name as string,
      version: row.version as string,
      dirName: row.dir_name as string,
      contentHash: row.content_hash as string,
      sourceLocation: text(row.source_location),
      manifestLocation: text(row.manifest_location),
      archiveStatus: row.archive_status as PackageVersionArchiveStatus,
      archivedAt: row.archived_at ? new Date(row.archived_at as string) : null,
      description: text(row.description),
      gitCommitSha: text(row.git_commit_sha),
      gitRef: text(row.git_ref),
      createdAt: new Date(row.created_at as string),
      updatedAt: new Date(row.updated_at as string),
   };
}

/**
 * DuckDB reports a UNIQUE / PRIMARY KEY violation as a constraint error whose
 * message names the duplicate key. The only unique key on this table besides
 * the generated id is (environment_id, package_name, version), so any such
 * violation on insert is a duplicate version.
 */
function isUniqueViolation(err: unknown): boolean {
   if (!(err instanceof Error)) return false;
   return /duplicate key|unique constraint/i.test(err.message);
}
