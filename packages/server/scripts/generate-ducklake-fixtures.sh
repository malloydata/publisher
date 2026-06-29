#!/usr/bin/env bash
#
# Regenerate the DuckLake old-format catalog fixtures used by
# tests/integration/ducklake_migration.test.ts.
#
# The migration test must attach catalogs written in each historical DuckLake
# catalog format and confirm the current baked engine migrates them forward
# without losing data. The test runner's engine only writes the newest format,
# so the older-format catalogs are committed as fixtures and regenerated here
# with the matching old DuckDB CLI.
#
# Catalog format is set by the DuckDB engine version, not by DuckLake directly.
# The observed mapping (verified via `SELECT value FROM ducklake_metadata
# WHERE key='version'`):
#
#     DuckDB 1.4.1 .. 1.4.4   -> catalog format "0.3"
#     DuckDB 1.5.0 .. 1.5.1   -> catalog format "0.4"
#     DuckDB 1.5.2 .. 1.5.3   -> catalog format "1.0"
#
# One fixture per distinct format is enough; patch releases within a format
# line produce identical formats. Each fixture seeds the SAME rows so the test
# can assert one expected result set regardless of source format.
#
# Requirements: the DuckDB CLIs listed in FIXTURES must be installed under
# ~/.duckdb/cli/<version>/duckdb. Install a specific version with:
#     DUCKDB_VERSION=<version> bash -c "curl -L https://install.duckdb.org | bash"
#
# Usage (run from packages/server):
#     bash scripts/generate-ducklake-fixtures.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SERVER_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
FIXTURE_ROOT="${SERVER_DIR}/tests/fixtures/ducklake"

CLI_BASE="${HOME}/.duckdb/cli"

# "<duckdb_cli_version>:<expected_catalog_format>". The directory name is
# derived from the catalog format so the test can label rows by format.
FIXTURES=(
   "1.4.4:0.3"
   "1.5.1:0.4"
   "1.5.2:1.0"
)

# Identical seed data in every fixture. The migration test asserts this exact
# result set after migrating each fixture forward. ducklake_flush_inlined_data
# forces the rows out to an on-disk parquet file; without it the 1.5.x engines
# keep small inserts inlined in the catalog and write no data file, so the
# fixture would not exercise the parquet data path a real catalog has.
SEED_SQL="CREATE TABLE dl.items AS SELECT * FROM (VALUES (1,'alpha'),(2,'beta'),(3,'gamma')) AS t(id, label); CALL ducklake_flush_inlined_data('dl');"

echo "Regenerating DuckLake fixtures under ${FIXTURE_ROOT}"
rm -rf "${FIXTURE_ROOT}"
mkdir -p "${FIXTURE_ROOT}"

for entry in "${FIXTURES[@]}"; do
   cli_version="${entry%%:*}"
   catalog_format="${entry##*:}"
   cli="${CLI_BASE}/${cli_version}/duckdb"

   if [[ ! -x "${cli}" ]]; then
      echo "ERROR: DuckDB CLI ${cli_version} not found at ${cli}" >&2
      echo "Install it with: DUCKDB_VERSION=${cli_version} bash -c \"curl -L https://install.duckdb.org | bash\"" >&2
      exit 1
   fi

   dest="${FIXTURE_ROOT}/v${catalog_format}_from_duckdb_${cli_version}"
   mkdir -p "${dest}"

   # Create from inside the destination so the recorded data_path stays
   # relative ("data/") and the fixture is relocatable.
   (
      cd "${dest}"
      "${cli}" -c "
         INSTALL ducklake;
         LOAD ducklake;
         ATTACH 'ducklake:catalog.ducklake' AS dl (DATA_PATH 'data/');
         ${SEED_SQL}
      " >/dev/null
   )

   recorded="$("${cli}" "${dest}/catalog.ducklake" \
      -noheader -list \
      -c "SELECT value FROM ducklake_metadata WHERE key='version';")"

   if [[ "${recorded}" != "${catalog_format}" ]]; then
      echo "ERROR: DuckDB ${cli_version} wrote catalog format '${recorded}', expected '${catalog_format}'" >&2
      echo "The version-to-format mapping in this script is stale; update FIXTURES." >&2
      exit 1
   fi

   echo "  ${dest#"${SERVER_DIR}/"}  (format ${recorded}, written by DuckDB ${cli_version})"
done

echo "Done."
