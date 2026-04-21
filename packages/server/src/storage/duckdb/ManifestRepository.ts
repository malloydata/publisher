import { ManifestEntry } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

/**
 * Repository for build manifest entries stored in the `build_manifests` table.
 *
 * A build manifest records the materialized table produced by a specific build
 * of a package, linking it back to the Malloy source and connection that
 * generated it. Manifests are keyed by (environment, package, build) and enable
 * downstream consumers to discover which physical tables correspond to a given
 * package version.
 */
export class ManifestRepository {
   constructor(private db: DuckDBConnection) {}

   private generateId(): string {
      return `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;
   }

   /** Returns all manifest entries for a package, most recent first. */
   async listEntries(
      environmentId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM main.build_manifests WHERE environment_id = ? AND package_name = ? ORDER BY created_at DESC",
         [environmentId, packageName],
      );
      return rows.map(this.mapToEntry);
   }

   /** Looks up the manifest entry for a specific build, or null if none exists. */
   async getEntryByBuildId(
      environmentId: string,
      packageName: string,
      buildId: string,
   ): Promise<ManifestEntry | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM main.build_manifests WHERE environment_id = ? AND package_name = ? AND build_id = ?",
         [environmentId, packageName, buildId],
      );
      return row ? this.mapToEntry(row) : null;
   }

   /**
    * Inserts a new manifest entry, or updates the existing one if a row with
    * the same (environment, package, build) triple already exists. Uses INSERT ON
    * CONFLICT to avoid the TOCTOU race of SELECT-then-INSERT/UPDATE.
    */
   async upsertEntry(
      entry: Omit<ManifestEntry, "id" | "createdAt" | "updatedAt">,
   ): Promise<ManifestEntry> {
      const id = this.generateId();
      const now = new Date();
      const iso = now.toISOString();

      const rows = await this.db.all<Record<string, unknown>>(
         `INSERT INTO main.build_manifests (id, environment_id, package_name, build_id, table_name, source_name, connection_name, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (environment_id, package_name, build_id)
         DO UPDATE SET table_name = EXCLUDED.table_name,
                       source_name = EXCLUDED.source_name,
                       connection_name = EXCLUDED.connection_name,
                       updated_at = EXCLUDED.updated_at
         RETURNING *`,
         [
            id,
            entry.environmentId,
            entry.packageName,
            entry.buildId,
            entry.tableName,
            entry.sourceName,
            entry.connectionName,
            iso,
            iso,
         ],
      );

      return this.mapToEntry(rows[0]);
   }

   /** Deletes a single manifest entry by ID. */
   async deleteEntry(id: string): Promise<void> {
      await this.db.run("DELETE FROM main.build_manifests WHERE id = ?", [id]);
   }

   /** Removes all manifest entries belonging to an environment (used on environment deletion). */
   async deleteEntriesByEnvironmentId(environmentId: string): Promise<void> {
      await this.db.run(
         "DELETE FROM main.build_manifests WHERE environment_id = ?",
         [environmentId],
      );
   }

   /** Removes all manifest entries for a specific package. */
   async deleteEntriesByPackage(
      environmentId: string,
      packageName: string,
   ): Promise<void> {
      await this.db.run(
         "DELETE FROM main.build_manifests WHERE environment_id = ? AND package_name = ?",
         [environmentId, packageName],
      );
   }

   /** Maps a raw DB row (snake_case columns) to a {@link ManifestEntry}. */
   private mapToEntry(row: Record<string, unknown>): ManifestEntry {
      return {
         id: row.id as string,
         environmentId: row.environment_id as string,
         packageName: row.package_name as string,
         buildId: row.build_id as string,
         tableName: row.table_name as string,
         sourceName: row.source_name as string,
         connectionName: row.connection_name as string,
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }
}
