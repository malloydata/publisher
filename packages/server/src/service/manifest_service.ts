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
 *  2. **Reload** – `reloadManifest` reads the manifest from the DB and
 *     recompiles all models in the package so the Malloy Runtime resolves
 *     persist references to the materialized tables.
 *
 * All manifest operations delegate to the active {@link ManifestStore}, which
 * is either the local DuckDB store (standalone) or DuckLake (orchestrated).
 */
export class ManifestService {
   constructor(private projectStore: ProjectStore) {}

   private manifestStoreFor(projectId: string): ManifestStore {
      return this.projectStore.storageManager.getManifestStore(projectId);
   }

   async getManifest(
      projectId: string,
      packageName: string,
   ): Promise<BuildManifest> {
      return this.manifestStoreFor(projectId).getManifest(
         projectId,
         packageName,
      );
   }

   async writeEntry(
      projectId: string,
      packageName: string,
      buildId: string,
      tableName: string,
      sourceName: string,
      connectionName: string,
   ): Promise<void> {
      await this.manifestStoreFor(projectId).writeEntry(
         projectId,
         packageName,
         buildId,
         tableName,
         sourceName,
         connectionName,
      );
   }

   async deleteEntry(projectId: string, entryId: string): Promise<void> {
      await this.manifestStoreFor(projectId).deleteEntry(entryId);
   }

   /**
    * Read the manifest from storage and reload it onto this worker's
    * package so subsequent queries resolve persist references to
    * materialized tables. This is what `POST .../manifest/reload` calls
    * (used by orchestrated workers that need to pick up manifest state
    * produced elsewhere). Despite the name, nothing is loaded *into*
    * storage — the worker pulls the manifest down and recompiles.
    */
   async reloadManifest(
      projectId: string,
      packageName: string,
      projectName: string,
   ): Promise<BuildManifest> {
      const manifest = await this.getManifest(projectId, packageName);

      const project = await this.projectStore.getProject(projectName, false);
      const pkg = await project.getPackage(packageName, false);
      await pkg.reloadAllModels(manifest.entries);

      logger.info("Reloaded manifest and recompiled models", {
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
      return this.manifestStoreFor(projectId).listEntries(
         projectId,
         packageName,
      );
   }
}
