import { logger } from "../../logger";
import { Package } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

export class PackageRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
   }

   private now(): Date {
      return new Date();
   }

   async listPackages(environmentId: string): Promise<Package[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM packages WHERE environment_id = ? ORDER BY name",
         [environmentId],
      );
      return rows.map(this.mapToPackage);
   }

   async getPackageById(id: string): Promise<Package | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM packages WHERE id = ?",
         [id],
      );
      return row ? this.mapToPackage(row) : null;
   }

   async getPackageByName(
      environmentId: string,
      name: string,
   ): Promise<Package | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM packages WHERE environment_id = ? AND name = ?",
         [environmentId, name],
      );
      return row ? this.mapToPackage(row) : null;
   }

   async createPackage(
      pkg: Omit<Package, "id" | "createdAt" | "updatedAt">,
   ): Promise<Package> {
      const id = this.generateId();
      const now = this.now();

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

      // Read-after-write: fail loudly if the INSERT silently did not land.
      const verified = await this.getPackageByName(pkg.environmentId, pkg.name);
      if (!verified) {
         logger.error(
            `createPackage("${pkg.name}"): INSERT returned success but row is not visible on read-back (id=${id}; name=${pkg.name})`,
         );
         throw new Error(
            `Failed to create package (id=${id}; name=${pkg.name})`,
         );
      }

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

      const now = this.now();
      const setClauses: string[] = [];
      const params: unknown[] = [];

      if (updates.name !== undefined) {
         setClauses.push(`name = ?`);
         params.push(updates.name);
      }
      if (updates.description !== undefined) {
         setClauses.push(`description = ?`);
         params.push(updates.description);
      }
      if (updates.manifestPath !== undefined) {
         setClauses.push(`manifest_path = ?`);
         params.push(updates.manifestPath);
      }
      if (updates.metadata !== undefined) {
         setClauses.push(`metadata = ?`);
         params.push(JSON.stringify(updates.metadata));
      }

      setClauses.push(`updated_at = ?`);
      params.push(now.toISOString());
      params.push(id);

      await this.db.run(
         `UPDATE packages SET ${setClauses.join(", ")} WHERE id = ?`,
         params,
      );

      // Read-after-write: fail loudly if the UPDATE silently no-ops.
      const verified = await this.getPackageById(id);
      if (!verified) {
         logger.error(
            `updatePackage(${id}): UPDATE returned success but row is not visible on read-back (id=${id}; name=${existing.name})`,
         );
         throw new Error(
            `Failed to update package (id=${id}; name=${existing.name})`,
         );
      }
      return verified;
   }

   async deletePackage(id: string): Promise<void> {
      await this.db.run("DELETE FROM packages WHERE id = ?", [id]);
   }

   async deletePackagesByEnvironmentId(id: string): Promise<void> {
      await this.db.run("DELETE FROM packages WHERE environment_id = ?", [id]);
   }

   private mapToPackage(row: Record<string, unknown>): Package {
      return {
         id: row.id as string,
         environmentId: row.environment_id as string,
         name: row.name as string,
         description: row.description as string | undefined,
         manifestPath: row.manifest_path as string,
         metadata: row.metadata
            ? JSON.parse(row.metadata as string)
            : undefined,
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }
}
