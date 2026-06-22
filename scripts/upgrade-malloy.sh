#!/bin/bash

if [ $# -gt 1 ]; then
  echo "Usage: $0 [NEW_VERSION]"
  echo "If NEW_VERSION is omitted, the latest version from npm is used."
  exit 1
fi

if [ $# -eq 1 ]; then
  NEW_VERSION=$1
else
  NEW_VERSION=$(npm view @malloydata/malloy version 2>/dev/null)
  if [ -z "$NEW_VERSION" ]; then
    echo "Failed to fetch latest version from npm"
    exit 1
  fi
  echo "Latest @malloydata/malloy from npm: $NEW_VERSION"
fi

# @malloydata/malloy-explorer is not always published for every @malloydata/malloy release;
# when missing, use the current latest on npm.
set_malloy_explorer_version_spec() {
  local published
  if published=$(npm view "@malloydata/malloy-explorer@${NEW_VERSION}" version 2>/dev/null) && [ -n "$published" ]; then
    MALLOY_EXPLORER_VERSION_SPEC="^${NEW_VERSION}"
    echo "  malloy-explorer: $MALLOY_EXPLORER_VERSION_SPEC (published for $NEW_VERSION)"
  else
    MALLOY_EXPLORER_VERSION=$(npm view @malloydata/malloy-explorer version 2>/dev/null)
    if [ -z "$MALLOY_EXPLORER_VERSION" ]; then
      echo "Failed to fetch @malloydata/malloy-explorer version from npm" >&2
      exit 1
    fi
    MALLOY_EXPLORER_VERSION_SPEC="${MALLOY_EXPLORER_VERSION}"
    echo "  malloy-explorer: $MALLOY_EXPLORER_VERSION_SPEC (no ${NEW_VERSION} on npm; using latest published)"
  fi
}

set_malloy_explorer_version_spec

version_spec_for() {
  local pkg=$1
  if [ "$pkg" = "@malloydata/malloy-explorer" ]; then
    echo "$MALLOY_EXPLORER_VERSION_SPEC"
  else
    echo "^${NEW_VERSION}"
  fi
}

# Update @malloydata dependencies in all packages/*/package.json files
for package_json in packages/*/package.json; do
  # Extract all @malloydata package names from this file
  malloy_packages=$(grep -o '"@malloydata/[^"]*"' "$package_json" | tr -d '"' | sort -u)

  if [ -n "$malloy_packages" ]; then
    echo "editing $package_json"
    while IFS= read -r pkg; do
      v=$(version_spec_for "$pkg")
      sed -i '' "s|\"$pkg\": *\"[^\"]*\"|\"$pkg\": \"$v\"|g" "$package_json"
    done <<< "$malloy_packages"
  fi
done

# Update @malloydata resolutions in root package.json
echo "editing package.json (resolutions)"
malloy_resolutions=$(grep -o '"@malloydata/[^"]*"' package.json | tr -d '"' | sort -u)
if [ -n "$malloy_resolutions" ]; then
  while IFS= read -r pkg; do
    v=$(version_spec_for "$pkg")
    sed -i '' "s|\"$pkg\": *\"[^\"]*\"|\"$pkg\": \"$v\"|g" package.json
  done <<< "$malloy_resolutions"
fi

echo "Updated all @malloydata/* packages to ^$NEW_VERSION (except @malloydata/malloy-explorer; see above)."

# Pull the new versions so bun.lock records the @duckdb/node-api version the
# new Malloy resolves to, then align the DuckDB build-time knobs (CLI install,
# Dockerfile) to it. A Malloy bump can move that engine to a new DuckDB
# version; syncing here keeps the baked CLI/extensions on the same version the
# runtime reads, with no manual follow-up step.

# Record the DuckDB engine version before the bump so we can tell afterward
# whether the Malloy upgrade moved it. "|| true" because on a fresh tree the
# lock may not yet resolve; an empty value just means "treat as changed".
DUCKDB_VERSION_BEFORE=$(bun scripts/duckdb-version.js 2>/dev/null || true)

echo "Pulling new versions with 'bun install'..."
bun install

echo "Syncing DuckDB build version to Malloy's @duckdb/node-api engine..."
bun scripts/sync-duckdb-version.js --write

DUCKDB_VERSION_AFTER=$(bun scripts/duckdb-version.js 2>/dev/null || true)

# A DuckDB engine change can change the DuckLake catalog *format* the engine
# writes (e.g. duckdb 1.4.x wrote "0.3", 1.5.0-1.5.1 write "0.4", 1.5.2+ write
# "1.0"). The migration matrix test attaches a committed fixture for each
# historical format and asserts the new engine migrates it forward without
# data loss. When the engine moves, that fixture set must be regenerated and
# the new current-format fixture committed, or the test's current-format guard
# fails. Surface that here rather than letting it surprise CI.
if [ "$DUCKDB_VERSION_BEFORE" != "$DUCKDB_VERSION_AFTER" ]; then
  echo ""
  echo "=============================================================================="
  echo "DuckDB engine version changed: ${DUCKDB_VERSION_BEFORE:-<unknown>} -> ${DUCKDB_VERSION_AFTER:-<unknown>}"
  echo ""
  echo "This may change the DuckLake catalog format the engine writes. Regenerate"
  echo "the DuckLake migration fixtures and commit them so the migration test can"
  echo "validate safe forward-migration from every historical format:"
  echo ""
  echo "  1. Install any DuckDB CLIs the generator needs that you do not have:"
  echo "       DUCKDB_VERSION=<version> bash -c \"curl -L https://install.duckdb.org | bash\""
  echo "  2. Regenerate the fixtures (adds a fixture for the new current format):"
  echo "       (cd packages/server && bash scripts/generate-ducklake-fixtures.sh)"
  echo "  3. Validate the migration matrix against the new engine:"
  echo "       (cd packages/server && bun test tests/integration/ducklake_migration.test.ts)"
  echo "  4. COMMIT the regenerated fixtures under"
  echo "       packages/server/tests/fixtures/ducklake/"
  echo "     so CI can validate DuckLake catalog migration on the new version."
  echo "=============================================================================="
else
  echo "DuckDB engine version unchanged (${DUCKDB_VERSION_AFTER:-<unknown>}); DuckLake migration fixtures are still current."
fi
