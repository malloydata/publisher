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
  echo "Latest version from npm: $NEW_VERSION"
fi

# Update @malloydata dependencies in all packages/*/package.json files
for package_json in packages/*/package.json; do
  # Extract all @malloydata package names from this file
  malloy_packages=$(grep -o '"@malloydata/[^"]*"' "$package_json" | tr -d '"' | sort -u)

  if [ -n "$malloy_packages" ]; then
    echo "editing $package_json"
    while IFS= read -r pkg; do
      # Update any version string for this package to the new version
      sed -i '' "s|\"$pkg\": *\"[^\"]*\"|\"$pkg\": \"^$NEW_VERSION\"|g" "$package_json"
    done <<< "$malloy_packages"
  fi
done

# Update @malloydata resolutions in root package.json
echo "editing package.json (resolutions)"
malloy_resolutions=$(grep -o '"@malloydata/[^"]*"' package.json | tr -d '"' | sort -u)
if [ -n "$malloy_resolutions" ]; then
  while IFS= read -r pkg; do
    sed -i '' "s|\"$pkg\": *\"[^\"]*\"|\"$pkg\": \"^$NEW_VERSION\"|g" package.json
  done <<< "$malloy_resolutions"
fi

echo "Updated all @malloydata packages to ^$NEW_VERSION"
echo "Run 'bun install' to pull the new versions"
