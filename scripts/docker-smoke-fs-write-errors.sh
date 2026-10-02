#!/usr/bin/env bash
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# A write the uid-1000 image cannot make must say why, not answer a bare 500.
#
# Run by build.yml's `docker_smoke_test` job against the image it just built
# from the root Dockerfile, and runnable locally the same way:
#
#   scripts/docker-smoke-fs-write-errors.sh malloy-publisher:smoke-test
#
# Since 0.9.0 the server runs as uid 1000 (#1273), so every mount it writes to
# has to be writable by that user. This reproduces how an orchestrated
# deployment loads packages: an orchestrator running as root writes package
# zips into a mount it shares with the server, then asks the server to load
# each one with POST /environments/{env}/packages. The server extracts a zip
# into a sibling directory beside it, after an `rm -rf` of any earlier one, so a
# package mount is a write mount. Three shapes:
#
#   S3   Publish a new version: the zip's sibling cannot be created (mkdir).
#   S3b  Re-publish over a directory a root-run 0.8.x server left behind: the
#        `rm -rf` before the extract cannot remove it.
#   S2   A `publisher_data` volume left root-owned by a 0.8.x.
#
# All three answer {"code":500,"message":"Internal server error."} today, with
# the EACCES only in the container log, and S3's failure is not in /status
# either. The assertions require the errno and refuse the generic body. They do
# not pin wording, or whether the path is named.

set -euo pipefail

IMAGE="${1:-malloy-publisher:smoke-test}"
GENERIC='Internal server error.'
S3_PORT="${S3_PORT:-4101}"
S2_PORT="${S2_PORT:-4102}"
PACKAGES_VOLUME=fsw-smoke-packages
DATA_VOLUME=fsw-smoke-data
S3_CONTAINER=fsw-smoke-s3
S2_CONTAINER=fsw-smoke-s2

# One package, published as successive versions, each its own zip.
ENV_NAME=analytics
PKG=orders
NEW_VERSION=v2
OLD_VERSION=v1

work=$(mktemp -d)
failures=0

# On Linux -- CI -- the mount is the real thing: a bind whose host source does
# not exist yet, which the Docker daemon creates as root, as it does for any
# bind it has to create. Docker Desktop maps bind-mount ownership and would hide
# the failure, so off Linux a root-owned named volume stands in for it.
if [ "$(uname -s)" = Linux ]; then
   PACKAGES_MOUNT="$work/packages:/tmp/packages"
else
   docker volume create "$PACKAGES_VOLUME" >/dev/null
   PACKAGES_MOUNT="$PACKAGES_VOLUME:/tmp/packages"
fi

cleanup() {
   for c in "$S3_CONTAINER" "$S2_CONTAINER"; do
      if [ "$failures" -gt 0 ]; then
         echo "--- $c logs (tail) ---"
         docker logs "$c" 2>&1 | tail -40 || true
      fi
      docker rm -f "$c" >/dev/null 2>&1 || true
   done
   docker volume rm -f "$PACKAGES_VOLUME" "$DATA_VOLUME" >/dev/null 2>&1 || true
   # The bind source is root-owned, so the runner user cannot remove it.
   docker run --rm --user 0 -v "$work:/w" "$IMAGE" rm -rf /w/packages >/dev/null 2>&1 || true
   rm -rf "$work"
}
trap cleanup EXIT

fail() {
   echo "✗ $*"
   failures=$((failures + 1))
}

wait_serving() {
   local container=$1 port=$2
   for i in $(seq 1 90); do
      if ! docker ps -q -f "name=^${container}$" | grep -q .; then
         echo "✗ $container exited"
         docker logs "$container" 2>&1 | tail -40 || true
         exit 1
      fi
      state=$(curl -sf "http://localhost:${port}/api/v0/status" | jq -r '.operationalState // empty' || true)
      if [ "$state" = serving ]; then
         echo "  $container serving after ${i}s"
         return 0
      fi
      sleep 1
   done
   echo "✗ $container did not reach 'serving' within 90s"
   exit 1
}

# assert_names_errno <label> <json body>
assert_names_errno() {
   local label=$1 body=$2 message
   message=$(echo "$body" | jq -r '.message // empty' 2>/dev/null || true)
   echo "  $label -> $body"
   if [ "$message" = "$GENERIC" ]; then
      fail "$label: answered the generic body, not the cause"
   elif ! echo "$message" | grep -Eqi 'EACCES|permission denied'; then
      fail "$label: response does not name EACCES ('${message:-<no message>}')"
   else
      echo "✓ $label names the errno"
   fi
}

# assert_in_load_errors <label> <port> <package>
assert_in_load_errors() {
   local label=$1 port=$2 package=$3 status entry
   status=$(curl -s "http://localhost:${port}/api/v0/status")
   entry=$(echo "$status" | jq -c --arg env "$ENV_NAME" --arg pkg "$package" \
      '[.loadErrors // [] | .[] | select(.environment == $env and .package == $pkg)][0] // empty')
   if [ -z "$entry" ]; then
      fail "$label: the failed add is not in /status loadErrors ($(echo "$status" | jq -c '.loadErrors // "absent"'))"
   elif ! echo "$entry" | jq -r .message | grep -Eqi 'EACCES|permission denied'; then
      fail "$label: /status loadErrors entry does not name EACCES ($entry)"
   else
      echo "✓ $label failed add is in /status loadErrors"
   fi
}

# publish <port> <version>: the orchestrator's load call.
publish() {
   local port=$1 version=$2
   curl -s -X POST -H 'Content-Type: application/json' \
      -d "{\"name\":\"${PKG}-${version}\",\"location\":\"/tmp/packages/${PKG}-${version}.zip\"}" \
      "http://localhost:${port}/api/v0/environments/${ENV_NAME}/packages"
}

# A one-model DuckDB package: `seed` as a directory (readable is all a
# directory location needs), `orders` as the zip the orchestrator uploads.
mkdir -p "$work/src"
for name in seed "$PKG"; do
   mkdir -p "$work/src/$name"
   printf '{"name":"%s","description":"one-model DuckDB package"}' "$name" \
      >"$work/src/$name/publisher.json"
   printf 'source: s is duckdb.sql("SELECT 1 AS x")\n' >"$work/src/$name/model.malloy"
done
(cd "$work/src/$PKG" && zip -q -r "../$PKG.zip" .)

# The orchestrator's side, as root: upload both versions' zips, and leave the
# directory a root-run 0.8.x server extracted the old version into.
docker run --rm --user 0 -v "$PACKAGES_MOUNT" -v "$work/src:/src:ro" "$IMAGE" sh -c "
   set -e
   cp -r /src/seed /tmp/packages/seed
   for v in $NEW_VERSION $OLD_VERSION; do
      cp /src/$PKG.zip /tmp/packages/${PKG}-\$v.zip
   done
   mkdir -p /tmp/packages/${PKG}-$OLD_VERSION
   cp -r /src/$PKG/. /tmp/packages/${PKG}-$OLD_VERSION/
   chown -R 0:0 /tmp/packages
   chmod -R u=rwX,go=rX /tmp/packages
   ls -lan /tmp/packages"

echo "S3: publish into a root-owned shared package mount"
docker run -d --name "$S3_CONTAINER" -p "$S3_PORT:4000" -v "$PACKAGES_MOUNT" "$IMAGE" >/dev/null
wait_serving "$S3_CONTAINER" "$S3_PORT"
code=$(curl -s -o "$work/env.json" -w '%{http_code}' -X POST \
   -H 'Content-Type: application/json' \
   -d "{\"name\":\"$ENV_NAME\",\"packages\":[{\"name\":\"seed\",\"location\":\"/tmp/packages/seed\"}],\"connections\":[]}" \
   "http://localhost:${S3_PORT}/api/v0/environments")
if [ "$code" != 200 ]; then
   fail "S3 setup: creating the environment from a readable directory returned $code: $(cat "$work/env.json")"
else
   assert_names_errno "S3 publish ${PKG}-$NEW_VERSION" "$(publish "$S3_PORT" "$NEW_VERSION")"
   assert_in_load_errors "S3" "$S3_PORT" "${PKG}-$NEW_VERSION"
   echo "S3b: re-publish over a directory a root-run server left behind"
   assert_names_errno "S3b publish ${PKG}-$OLD_VERSION" "$(publish "$S3_PORT" "$OLD_VERSION")"
   assert_in_load_errors "S3b" "$S3_PORT" "${PKG}-$OLD_VERSION"
fi

echo "S2: a publisher_data volume left root-owned by a 0.8.x"
# Not left empty: Docker re-seeds an EMPTY volume from the image's directory
# on every mount, ownership included, which would quietly hand it back to uid
# 1000. A real 0.8.x volume holds the environment it wrote, so this one does.
docker volume create "$DATA_VOLUME" >/dev/null
docker run --rm --user 0 -v "$DATA_VOLUME:/publisher/publisher_data" "$IMAGE" \
   sh -c "mkdir -p '/publisher/publisher_data/$ENV_NAME' && chown -R 0:0 /publisher/publisher_data && chmod -R 755 /publisher/publisher_data"
printf '{"environments":[{"name":"%s","packages":[{"name":"seed","location":"/tmp/packages/seed"}],"connections":[]}]}' "$ENV_NAME" \
   >"$work/publisher.config.json"
chmod 644 "$work/publisher.config.json"
docker run -d --name "$S2_CONTAINER" -p "$S2_PORT:4000" \
   -v "$DATA_VOLUME:/publisher/publisher_data" \
   -v "$PACKAGES_MOUNT" \
   -v "$work/publisher.config.json:/publisher/publisher.config.json:ro" \
   "$IMAGE" >/dev/null
wait_serving "$S2_CONTAINER" "$S2_PORT"
assert_names_errno "S2 GET /environments/$ENV_NAME/packages/seed/models" \
   "$(curl -s "http://localhost:${S2_PORT}/api/v0/environments/${ENV_NAME}/packages/seed/models")"

if [ "$failures" -gt 0 ]; then
   echo "✗ $failures filesystem-write assertion(s) failed"
   exit 1
fi
echo "✓ every write the uid-1000 server could not make named its errno"
