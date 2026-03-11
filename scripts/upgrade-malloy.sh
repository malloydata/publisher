#!/bin/bash

if [ $# -ne 1 ]; then
  echo "Usage: $0 NEW_VERSION"
  exit 1
fi

NEW_VERSION=$1

# Extract current version from package.json
OLD_VERSION=$(grep '"@malloydata/malloy"' packages/server/package.json | grep -o '"^[0-9]\+\.[0-9]\+\.[0-9]\+"' | tr -d '"')

echo "$OLD_VERSION -> ^$NEW_VERSION"
if [ -z "$OLD_VERSION" ]; then
    echo "Old Version not detected in packages/server/package.json"
    exit 1
fi

# Replace version in all packages/*/package.json files
for package in packages/*/package.json; do
  echo "editing $package"
  sed -i '' '/"@malloydata\// s/"\'${OLD_VERSION//./\\.}'"/"\^'${NEW_VERSION//./\\.}'\"/' $package
done

# Update @malloydata resolutions in root package.json
echo "editing package.json (resolutions)"
sed -i '' '/"@malloydata\// s/"\^[0-9][0-9]*\.[0-9][0-9]*\.[0-9][0-9]*"/"\^'"${NEW_VERSION//./\\.}"'"/' package.json