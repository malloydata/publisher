// DuckLake catalog format compatibility check. Lives at `src/` root so
// both `service/` (user-facing connections) and `storage/` (materialization
// catalog) can pre-flight a catalog's recorded version without bringing in
// the other layer's helpers. See CLAUDE.md's "Two parallel DuckLake/PG
// attach paths" note.
//
// The Publisher Docker image bakes a specific DuckLake extension version at
// build time (see Dockerfile). If a catalog's on-disk format version is
// newer than what the baked extension supports, ATTACH fails deep inside
// DuckDB with a generic 500. The preflight catches that case and surfaces
// it as a non-retryable 422 instead.

// Catalog format versions the currently-baked DuckLake extension can read.
// The extension is paired with duckdb@1.5.3 (see Dockerfile's DUCKDB_VERSION
// and packages/server/package.json's @duckdb/node-api pin). The catalog format
// bumped from the 0.3 line to "1.0" at the duckdb 1.4.x -> 1.5.0 transition,
// so the 1.5.x extension produces and reads "1.0" catalogs. The earlier 0.3
// line stays listed because the 1.5.x extension still attaches those catalogs
// (it upgrades the format forward on write); dropping them would reject
// existing on-disk catalogs that have not yet been touched by a 1.5.x write.
export const SUPPORTED_CATALOG_VERSIONS: readonly string[] = [
   "0.1",
   "0.2",
   "0.3-dev1",
   "0.3",
   "1.0",
];

export function isCatalogVersionSupported(version: string): boolean {
   return SUPPORTED_CATALOG_VERSIONS.includes(version);
}
