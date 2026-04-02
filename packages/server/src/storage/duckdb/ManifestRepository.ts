import { ManifestEntry } from "../DatabaseInterface";
import { DuckDBConnection } from "./DuckDBConnection";

/**
 * Repository for build manifest entries stored in the `build_manifests` table.
 *
 * A build manifest records the materialized table produced by a specific build
 * of a package, linking it back to the Malloy source and connection that
 * generated it. Manifests are keyed by (project, package, build) and enable
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
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         "SELECT * FROM build_manifests WHERE project_id = ? AND package_name = ? ORDER BY created_at DESC",
         [projectId, packageName],
      );
      return rows.map(this.mapToEntry);
   }

   /** Looks up the manifest entry for a specific build, or null if none exists. */
   async getEntryByBuildId(
      projectId: string,
      packageName: string,
      buildId: string,
   ): Promise<ManifestEntry | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM build_manifests WHERE project_id = ? AND package_name = ? AND build_id = ?",
         [projectId, packageName, buildId],
      );
      return row ? this.mapToEntry(row) : null;
   }

   /**
    * Inserts a new manifest entry, or updates the existing one if a row with
    * the same (project, package, build) triple already exists. The unique
    * constraint on the table guarantees at most one entry per build.
    */
   async upsertEntry(
      entry: Omit<ManifestEntry, "id" | "createdAt" | "updatedAt">,
   ): Promise<ManifestEntry> {
      const existing = await this.getEntryByBuildId(
         entry.projectId,
         entry.packageName,
         entry.buildId,
      );

      if (existing) {
         const now = new Date();
         await this.db.run(
            `UPDATE build_manifests SET table_name = ?, source_name = ?, connection_name = ?, updated_at = ? WHERE id = ?`,
            [
               entry.tableName,
               entry.sourceName,
               entry.connectionName,
               now.toISOString(),
               existing.id,
            ],
         );
         return {
            ...existing,
            tableName: entry.tableName,
            sourceName: entry.sourceName,
            connectionName: entry.connectionName,
            updatedAt: now,
         };
      }

      const id = this.generateId();
      const now = new Date();

      await this.db.run(
         `INSERT INTO build_manifests (id, project_id, package_name, build_id, table_name, source_name, connection_name, created_at, updated_at)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
         [
            id,
            entry.projectId,
            entry.packageName,
            entry.buildId,
            entry.tableName,
            entry.sourceName,
            entry.connectionName,
            now.toISOString(),
            now.toISOString(),
         ],
      );

      return {
         id,
         ...entry,
         createdAt: now,
         updatedAt: now,
      };
   }

   /** Looks up the most recent manifest entry for a source by name. */
   async getEntryBySourceName(
      projectId: string,
      packageName: string,
      sourceName: string,
   ): Promise<ManifestEntry | null> {
      const row = await this.db.get<Record<string, unknown>>(
         "SELECT * FROM build_manifests WHERE project_id = ? AND package_name = ? AND source_name = ? ORDER BY created_at DESC LIMIT 1",
         [projectId, packageName, sourceName],
      );
      return row ? this.mapToEntry(row) : null;
   }

   /** Deletes a single manifest entry by ID. */
   async deleteEntry(id: string): Promise<void> {
      await this.db.run("DELETE FROM build_manifests WHERE id = ?", [id]);
   }

   /** Removes all manifest entries for a given package within a project. */
   async deleteEntriesByPackage(
      projectId: string,
      packageName: string,
   ): Promise<void> {
      await this.db.run(
         "DELETE FROM build_manifests WHERE project_id = ? AND package_name = ?",
         [projectId, packageName],
      );
   }

   /** Removes all manifest entries belonging to a project (used on project deletion). */
   async deleteEntriesByProjectId(projectId: string): Promise<void> {
      await this.db.run(
         "DELETE FROM build_manifests WHERE project_id = ?",
         [projectId],
      );
   }

   /** Maps a raw DB row (snake_case columns) to a {@link ManifestEntry}. */
   private mapToEntry(row: Record<string, unknown>): ManifestEntry {
      return {
         id: row.id as string,
         projectId: row.project_id as string,
         packageName: row.package_name as string,
         buildId: row.build_id as string,
         tableName: row.table_name as string,
         sourceName: (row.source_name as string) || null,
         connectionName: (row.connection_name as string) || null,
         createdAt: new Date(row.created_at as string),
         updatedAt: new Date(row.updated_at as string),
      };
   }
}
