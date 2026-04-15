import { logger } from "../logger";
import {
   BuildManifest,
   ManifestEntry,
   ManifestStore,
} from "../storage/DatabaseInterface";
import { ProjectStore } from "./project_store";

/**
 * Manages build manifests that map source names to materialized table names.
 *
 * A build manifest is produced during materialization: each entry records which
 * DuckDB table backs a given Malloy source.
 *
 * Two-phase lifecycle:
 *  1. **Persist** – `writeEntry` stores individual entries to the DB as they
 *     are produced during a build.
 *  2. **Load** – `loadManifest` reads the manifest from the DB and recompiles
 *     all models in the package so the Malloy Runtime resolves persist
 *     references to the materialized tables.
 *
 * All manifest operations delegate to the active {@link ManifestStore}, which
 * is either the local DuckDB store (standalone) or DuckLake (orchestrated).
 */
export class ManifestService {
   constructor(private projectStore: ProjectStore) {}

   private get manifestStore(): ManifestStore {
      return this.projectStore.storageManager.getManifestStore();
   }

   async getManifest(
      projectId: string,
      packageName: string,
   ): Promise<BuildManifest> {
      return this.manifestStore.getManifest(projectId, packageName);
   }

   async writeEntry(
      projectId: string,
      packageName: string,
      buildId: string,
      tableName: string,
      sourceName: string,
      connectionName: string,
   ): Promise<void> {
      await this.manifestStore.writeEntry(
         projectId,
         packageName,
         buildId,
         tableName,
         sourceName,
         connectionName,
      );
   }

   async deleteEntry(id: string): Promise<void> {
      await this.manifestStore.deleteEntry(id);
   }

   /**
    * Load the manifest from storage and activate it on the package so
    * subsequent queries resolve persist references to materialized tables.
    * This is what `POST .../manifest/load` calls (used by orchestrated
    * workers that need to pick up manifest state produced elsewhere).
    */
   async loadManifest(
      projectId: string,
      packageName: string,
      projectName: string,
   ): Promise<BuildManifest> {
      const manifest = await this.getManifest(projectId, packageName);

      const project = await this.projectStore.getProject(projectName, false);
      const pkg = await project.getPackage(packageName, false);
      pkg.setBuildManifest(manifest.entries);

      logger.info("Loaded manifest", {
         projectId,
         packageName,
         entryCount: Object.keys(manifest.entries).length,
      });

      return manifest;
   }

   /**
    * Get the manifest entries from the active store for inspection.
    * Routes through ManifestStore so DuckLake mode reads the shared
    * catalog instead of the (empty) local DuckDB table.
    */
   async listEntries(
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      return this.manifestStore.listEntries(projectId, packageName);
   }
}
