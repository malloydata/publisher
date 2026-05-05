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
echo "Run 'bun install' to pull the new versions"
