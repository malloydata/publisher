import { Mutex } from "async-mutex";
import { Package } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

export class PackageRepository {
   /**
    * `swapPackageDirectory` is a SELECT-then-INSERT/UPDATE compound operation;
    * DuckDB's connection-level mutex serializes individual statements but does
    * not bracket the read-modify-write. This per-process mutex makes the
    * compound operation atomic so two concurrent swaps for the same package
    * cannot both observe the same `oldDirectoryPath` and miss orphaning the
    * loser's directory.
    */
   private swapMutex = new Mutex();

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

      return this.getPackageById(id) as Promise<Package>;
   }

   async deletePackage(id: string): Promise<void> {
      await this.db.run("DELETE FROM packages WHERE id = ?", [id]);
   }

   async deletePackagesByEnvironmentId(id: string): Promise<void> {
      await this.db.run("DELETE FROM packages WHERE environment_id = ?", [id]);
   }

   /**
    * Atomic upsert of a package's `manifest_path` (the directory that holds
    * its contents on disk). Returns the directory the row previously pointed
    * at so the caller can schedule it for sweep — `null` if this insert is
    * creating the row for the first time.
    *
    * Holding `swapMutex` makes the read-old + write-new sequence indivisible
    * within this Node process, which is the ordering point that replaces the
    * old per-package filesystem mutex.
    */
   async swapPackageDirectory(args: {
      environmentId: string;
      name: string;
      newDirectoryPath: string;
      description?: string;
      metadata?: Record<string, unknown>;
   }): Promise<{ id: string; oldDirectoryPath: string | null }> {
      return this.swapMutex.runExclusive(async () => {
         const existing = await this.getPackageByName(
            args.environmentId,
            args.name,
         );
         const now = this.now().toISOString();
         const metadataJson =
            args.metadata !== undefined ? JSON.stringify(args.metadata) : null;
         if (existing) {
            const oldDirectoryPath = existing.manifestPath || null;
            await this.db.run(
               `UPDATE packages
                SET manifest_path = ?, description = COALESCE(?, description),
                    metadata = COALESCE(?, metadata), updated_at = ?
                WHERE id = ?`,
               [
                  args.newDirectoryPath,
                  args.description ?? null,
                  metadataJson,
                  now,
                  existing.id,
               ],
            );
            return { id: existing.id, oldDirectoryPath };
         }

         const id = this.generateId();
         await this.db.run(
            `INSERT INTO packages (id, environment_id, name, description, manifest_path, metadata, created_at, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
            [
               id,
               args.environmentId,
               args.name,
               args.description ?? null,
               args.newDirectoryPath,
               metadataJson,
               now,
               now,
            ],
         );
         return { id, oldDirectoryPath: null };
      });
   }

   async listPackageDirectoryPaths(
      environmentId: string,
   ): Promise<Set<string>> {
      const rows = await this.db.all<{ manifest_path: string | null }>(
         "SELECT manifest_path FROM packages WHERE environment_id = ?",
         [environmentId],
      );
      const paths = new Set<string>();
      for (const row of rows) {
         if (row.manifest_path) paths.add(row.manifest_path);
      }
      return paths;
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
