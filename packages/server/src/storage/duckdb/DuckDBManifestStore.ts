import {
   BuildManifest,
   ManifestEntry,
   ManifestStore,
   ResourceRepository,
} from "../DatabaseInterface";

/**
 * DuckDB-backed ManifestStore that delegates to the local ResourceRepository.
 *
 * In standalone mode this is the active store. Orchestrated deployments swap
 * in a DuckLake-backed implementation that reads/writes manifests through the
 * shared catalog instead of the local DuckDB build_manifests table.
 */
export class DuckDBManifestStore implements ManifestStore {
   constructor(private repository: ResourceRepository) {}

   /**
    * Assembles a {@link BuildManifest} by folding all manifest rows for the
    * given package into a build-ID-keyed map. `strict: false` lets the Malloy
    * runtime fall back to executing the underlying query when a persist
    * reference has no manifest entry (e.g. before the first materialization).
    */
   async getManifest(
      environmentId: string,
      packageName: string,
   ): Promise<BuildManifest> {
      const entries = await this.repository.listManifestEntries(
         environmentId,
         packageName,
      );
      const manifest: BuildManifest = {
         entries: {},
         strict: false,
      };
      for (const entry of entries) {
         manifest.entries[entry.buildId] = { tableName: entry.tableName };
      }
      return manifest;
   }

   async writeEntry(
      environmentId: string,
      packageName: string,
      buildId: string,
      tableName: string,
      sourceName: string,
      connectionName: string,
   ): Promise<void> {
      await this.repository.upsertManifestEntry({
         environmentId,
         packageName,
         buildId,
         tableName,
         sourceName,
         connectionName,
      });
   }

   async deleteEntry(id: string): Promise<void> {
      await this.repository.deleteManifestEntry(id);
   }

   async listEntries(
      environmentId: string,
      packageName: string,
   ): Promise<ManifestEntry[]> {
      return this.repository.listManifestEntries(environmentId, packageName);
   }
}
