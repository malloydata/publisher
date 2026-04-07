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

   private get repository() {
      return this.projectStore.storageManager.getRepository();
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
      entry: {
         tableName: string;
         sourceName?: string;
         connectionName?: string;
      },
   ): Promise<void> {
      await this.manifestStore.writeEntry(
         projectId,
         packageName,
         buildId,
         entry,
      );
   }

   async getEntryBySourceName(
      projectId: string,
      packageName: string,
      sourceName: string,
   ): Promise<ManifestEntry | null> {
      return this.manifestStore.getEntryBySourceName(
         projectId,
         packageName,
         sourceName,
      );
   }

   async deleteEntry(id: string): Promise<void> {
      await this.manifestStore.deleteEntry(id);
   }

   /**
    * Load the manifest from storage and recompile all models in the package
    * so queries resolve persist references to materialized tables.
    * This is what `POST .../manifest/load` calls.
    */
   async loadManifest(
      projectId: string,
      packageName: string,
      projectName: string,
   ): Promise<BuildManifest> {
      const manifest = await this.getManifest(projectId, packageName);

      const project = await this.projectStore.getProject(projectName, false);
      const pkg = await project.getPackage(packageName, false);
      await pkg.reloadAllModels(manifest.entries);

      logger.info("Loaded manifest and recompiled models", {
         projectId,
         packageName,
         entryCount: Object.keys(manifest.entries).length,
      });

      return manifest;
   }

   /**
    * Get the manifest entries from the DB for inspection.
    */
   async listEntries(
      projectId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      return this.repository.listManifestEntries(projectId, packageName);
   }
}
