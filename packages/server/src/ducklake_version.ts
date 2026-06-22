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

// Catalog format versions the currently-baked DuckLake extension attaches
// without migration. The extension is paired with duckdb@1.5.3 (see the
// Dockerfile's DUCKDB_VERSION and packages/server/package.json's
// @duckdb/node-api pin), which produces and attaches the "1.0" catalog format.
//
// The 1.5.x extension does NOT silently attach the older 0.3-line formats: a
// plain ATTACH of a 0.1/0.2/0.3-dev1/0.3 catalog fails with "catalog version
// mismatch ... the extension requires version 1.0" unless the caller opts into
// AUTOMATIC_MIGRATION. The Publisher attach paths do not pass that flag, so
// those versions are intentionally absent here -- listing them would let the
// preflight wave through a catalog that then fails deep inside DuckDB, which
// is exactly the 500 this preflight exists to convert into a clean 422.
export const SUPPORTED_CATALOG_VERSIONS: readonly string[] = ["1.0"];

export function isCatalogVersionSupported(version: string): boolean {
   return SUPPORTED_CATALOG_VERSIONS.includes(version);
}
