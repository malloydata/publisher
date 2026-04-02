import {
   BuildManifest,
   ManifestStore,
   ResourceRepository,
} from "../DatabaseInterface";

/**
 * DuckDB-backed ManifestStore that delegates to the local ResourceRepository.
 *
 * In standalone mode this is the active store. Orchestrated deployments will
 * swap in a DuckLake-backed implementation that reads/writes manifests through
 * the shared catalog instead of the local DuckDB build_manifests table.
 */
export class DuckDBManifestStore implements ManifestStore {
   constructor(private repository: ResourceRepository) {}

   /**
    * Assembles a {@link BuildManifest} by folding all manifest rows for the
    * given package into a build-ID-keyed map. The `strict: true` flag tells
    * the Malloy runtime to only resolve persist references that appear in
    * the manifest, rejecting unknown build IDs at compile time.
    */
   async getManifest(
      projectId: string,
      packageName: string,
   ): Promise<BuildManifest> {
      const entries = await this.repository.listManifestEntries(
         projectId,
         packageName,
      );
      const manifest: BuildManifest = {
         entries: {},
         strict: true,
      };
      for (const entry of entries) {
         manifest.entries[entry.buildId] = { tableName: entry.tableName };
      }
      return manifest;
   }

   /** Persists a single manifest entry, creating or updating the row for the given build ID. */
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
      await this.repository.upsertManifestEntry({
         projectId,
         packageName,
         buildId,
         tableName: entry.tableName,
         sourceName: entry.sourceName || null,
         connectionName: entry.connectionName || null,
      });
   }
}
