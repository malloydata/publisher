import { logger } from "../../logger";
import {
   BuildManifest,
   ManifestEntry,
   ManifestStore,
} from "../DatabaseInterface";
import { DuckDBConnection } from "../duckdb/DuckDBConnection";

/**
 * DuckLake-backed ManifestStore used in orchestrated mode.
 *
 * Reads and writes manifest entries through a DuckLake catalog attached to
 * the publisher's internal DuckDB. All workers sharing the same DuckLake
 * catalog see the same manifest, enabling multi-worker coordination.
 *
 * The catalog is attached by {@link StorageManager} before this store is
 * instantiated. The first worker to call {@link bootstrapSchema} creates the
 * `build_manifests` table idempotently. Schema ownership lives in the
 * publisher so that DDL and code co-evolve in the same repo.
 *
 * **Scope: manifest sync only.** Orchestrated mode only shares manifest
 * state across workers. The active-materialization lock (the unique index
 * on `materializations.active_key`) still lives in each worker's local
 * DuckDB, so two workers can run builds concurrently and race on physical
 * table names, staging table names, and manifest writes. Until a shared
 * build lease lives in the DuckLake catalog, deployments running
 * orchestrated mode must ensure builds are externally single-writer (e.g.,
 * one designated build worker, or an external job scheduler). Other
 * workers should only call `manifest/reload` to pick up manifests produced
 * by the build worker.
 */
export class DuckLakeManifestStore implements ManifestStore {
   private readonly table: string;

   constructor(
      private db: DuckDBConnection,
      catalogName: string,
   ) {
      this.table = `${catalogName}.build_manifests`;
   }

   /**
    * Idempotently creates the `build_manifests` table and indices in the
    * DuckLake catalog. Safe to call from every worker on startup.
    */
   async bootstrapSchema(): Promise<void> {
      await this.db.run(`
         CREATE TABLE IF NOT EXISTS ${this.table} (
            id VARCHAR,
            project_id VARCHAR NOT NULL,
            package_name VARCHAR NOT NULL,
            build_id VARCHAR NOT NULL,
            table_name VARCHAR NOT NULL,
            source_name VARCHAR NOT NULL,
            connection_name VARCHAR NOT NULL,
            created_at TIMESTAMP NOT NULL,
            updated_at TIMESTAMP NOT NULL
         )
      `);
      logger.info(`DuckLake manifest table bootstrapped: ${this.table}`);
   }

   async getManifest(
      projectId: string,
      packageName: string,
   ): Promise<BuildManifest> {
      const rows = await this.db.all<Record<string, unknown>>(
         `SELECT * FROM ${this.table} WHERE project_id = ? AND package_name = ? ORDER BY created_at DESC`,
         [projectId, packageName],
      );
      const manifest: BuildManifest = { entries: {}, strict: false };
      for (const row of rows) {
         const buildId = row.build_id as string;
         // Rows are ordered newest-first; keep only the latest per build_id
         // to handle rare duplicates from cross-worker races.
         if (!manifest.entries[buildId]) {
            manifest.entries[buildId] = {
               tableName: row.table_name as string,
            };
         }
      }
      return manifest;
   }

   /**
    * Insert a manifest entry. If a row with the same `build_id` already
    * exists (retry after crash), the duplicate is harmless:
    * {@link getManifest} deduplicates by build_id keeping the newest row.
    */
   async writeEntry(
      projectId: string,
      packageName: string,
      buildId: string,
      tableName: string,
      sourceName: string,
      connectionName: string,
   ): Promise<void> {
      const now = new Date().toISOString();
      const id = `${Date.now()}-${Math.random().toString(36).substring(2, 11)}`;

      await this.db.run(
         `INSERT INTO ${this.table} (id, project_id, package_name, build_id, table_name, source_name, connection_name, created_at, updated_at)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
         [
            id,
            projectId,
            packageName,
            buildId,
            tableName,
            sourceName,
            connectionName,
            now,
            now,
         ],
      );
   }

   async deleteEntry(id: string): Promise<void> {
      await this.db.run(`DELETE FROM ${this.table} WHERE id = ?`, [id]);
   }

   async listEntries(
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      const rows = await this.db.all<Record<string, unknown>>(
         `SELECT * FROM ${this.table} WHERE project_id = ? AND package_name = ? ORDER BY created_at DESC`,
         [projectId, packageName],
      );
      return rows.map(this.mapToEntry);
   }

   private mapToEntry(row: Record<string, unknown>): ManifestEntry {
      return {
         id: row.id as string,
         projectId: row.project_id as string,
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
