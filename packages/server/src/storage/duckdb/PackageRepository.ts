// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { Package } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";
import { newId, Row, rowTimestamps, updateRow } from "./rows";

export class PackageRepository {
   constructor(private db: DuckDBConnection) {}

   async listPackages(environmentId: string): Promise<Package[]> {
      const rows = await this.db.all<Row>(
         "SELECT * FROM packages WHERE environment_id = ? ORDER BY name",
         [environmentId],
      );
      return rows.map(mapToPackage);
   }

   async getPackageById(id: string): Promise<Package | null> {
      const row = await this.db.get<Row>(
         "SELECT * FROM packages WHERE id = ?",
         [id],
      );
      return row ? mapToPackage(row) : null;
   }

   async getPackageByName(
      environmentId: string,
      name: string,
   ): Promise<Package | null> {
      const row = await this.db.get<Row>(
         "SELECT * FROM packages WHERE environment_id = ? AND name = ?",
         [environmentId, name],
      );
      return row ? mapToPackage(row) : null;
   }

   async createPackage(
      pkg: Omit<Package, "id" | "createdAt" | "updatedAt">,
   ): Promise<Package> {
      const id = newId();
      const now = new Date();

      await this.db.run(
         `INSERT INTO packages (id, environment_id, name, description, manifest_path, metadata, created_at, updated_at)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
         [
            id,
            pkg.environmentId,
            pkg.name,
            pkg.description || null,
            pkg.manifestPath,
            pkg.metadata ? JSON.stringify(pkg.metadata) : null,
            now.toISOString(),
            now.toISOString(),
         ],
      );

      return {
         id,
         ...pkg,
         createdAt: now,
         updatedAt: now,
      };
   }

   async updatePackage(
      id: string,
      updates: Partial<Package>,
   ): Promise<Package> {
      const existing = await this.getPackageById(id);
      if (!existing) {
         throw new Error(`Package with id ${id} not found`);
      }

      await updateRow(this.db, "packages", id, {
         name: updates.name,
         description: updates.description,
         manifest_path: updates.manifestPath,
         metadata:
            updates.metadata !== undefined
               ? JSON.stringify(updates.metadata)
               : undefined,
      });

      return this.getPackageById(id) as Promise<Package>;
   }

   async deletePackage(id: string): Promise<void> {
      await this.db.run("DELETE FROM packages WHERE id = ?", [id]);
   }

   async deletePackagesByEnvironmentId(id: string): Promise<void> {
      await this.db.run("DELETE FROM packages WHERE environment_id = ?", [id]);
   }
}

function mapToPackage(row: Row): Package {
   return {
      id: row.id as string,
      environmentId: row.environment_id as string,
      name: row.name as string,
      description: row.description as string | undefined,
      manifestPath: row.manifest_path as string,
      metadata: row.metadata ? JSON.parse(row.metadata as string) : undefined,
      ...rowTimestamps(row),
   };
}
