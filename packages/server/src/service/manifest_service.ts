import { logger } from "../logger";
import {
   BuildManifest,
   ManifestEntry,
   ManifestStore,
} from "../storage/DatabaseInterface";
import { EnvironmentStore } from "./environment_store";

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
   constructor(private environmentStore: EnvironmentStore) {}

   private manifestStoreFor(environmentId: string): ManifestStore {
      return this.environmentStore.storageManager.getManifestStore(
         environmentId,
      );
   }

   async getManifest(
      environmentId: string,
      packageName: string,
   ): Promise<BuildManifest> {
      return this.manifestStoreFor(environmentId).getManifest(
         environmentId,
         packageName,
      );
   }

   async writeEntry(
      environmentId: string,
      packageName: string,
      buildId: string,
      tableName: string,
      sourceName: string,
      connectionName: string,
   ): Promise<void> {
      await this.manifestStoreFor(environmentId).writeEntry(
         environmentId,
         packageName,
         buildId,
         tableName,
         sourceName,
         connectionName,
      );
   }

   async deleteEntry(environmentId: string, entryId: string): Promise<void> {
      await this.manifestStoreFor(environmentId).deleteEntry(entryId);
   }

   /**
    * Read the manifest from storage and reload it onto this worker's
    * package so subsequent queries resolve persist references to
    * materialized tables. This is what `POST .../manifest?action=reload` calls
    * (used by orchestrated workers that need to pick up manifest state
    * produced elsewhere). Despite the name, nothing is loaded *into*
    * storage — the worker pulls the manifest down and recompiles.
    */
   async reloadManifest(
      environmentId: string,
      packageName: string,
      environmentName: string,
   ): Promise<BuildManifest> {
      const manifest = await this.getManifest(environmentId, packageName);

      const environment = await this.environmentStore.getEnvironment(
         environmentName,
         false,
      );
      const pkg = await environment.getPackage(packageName, false);
      await pkg.reloadAllModels(manifest.entries);

      logger.info("Reloaded manifest and recompiled models", {
         environmentId,
         packageName,
         entryCount: Object.keys(manifest.entries).length,
      });

      return manifest;
   }

   /**
    * Get the manifest entries from the active store for inspection.
    * Routes through ManifestStore so DuckLake mode reads the shared
    * catalog instead of the local DuckDB table.
    */
   async listEntries(
      environmentId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      return this.manifestStoreFor(environmentId).listEntries(
         environmentId,
         packageName,
      );
   }
}
