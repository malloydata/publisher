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

// Versions the currently-baked DuckLake extension (0.3 line, paired with
// duckdb@1.4.4) can read. When the Publisher upgrades to the DuckLake 1.0
// line (separate PR, requires duckdb@1.5+), extend this list to include
// "1.0" and later format versions.
export const SUPPORTED_CATALOG_VERSIONS: readonly string[] = [
   "0.1",
   "0.2",
   "0.3-dev1",
   "0.3",
];

export function isCatalogVersionSupported(version: string): boolean {
   return SUPPORTED_CATALOG_VERSIONS.includes(version);
}
