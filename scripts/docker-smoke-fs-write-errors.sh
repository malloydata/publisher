#!/usr/bin/env bash
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# A filesystem access the uid-1000 image cannot make must say why, not answer
# a bare 500 or sit silently in a state that reads as "still starting"; and a
# package location the server only reads must not need to be writable.
#
# Run by build.yml's `docker_smoke_test` job against the image it just built
# from the root Dockerfile, and runnable locally the same way:
#
#   scripts/docker-smoke-fs-write-errors.sh malloy-publisher:smoke-test
#
# The server runs as uid 1000 (#1273), so every mount it writes to has to be
# writable by that user, and every file it reads readable by it. The
# shapes below are how an orchestrated deployment meets that: an orchestrator
# running as root writes package zips into a mount it shares with the server
# and asks the server to load each with POST /environments/{env}/packages.
# Credentials arrive as a file bind at GOOGLE_APPLICATION_CREDENTIALS.
#
# Package mount: a zip is extracted into the server's own data directory, so
# the mount only has to be readable and nothing beside a zip is touched.
#   S3   Publish a new version from a root-owned mount: it loads.
#   S3b  Publish a zip next to a directory of the same name (one a root-run
#        0.8.x left behind, or the operator's own): it loads, and the
#        directory is left as it was.
#   S3c  The same mount bound read-only: it loads.
#   S4   The same zip declared in the config at boot: it loads.
# Server data
#   S2   A publisher_data volume left root-owned by a 0.8.x: names EACCES.
#   S6   A read-only root filesystem (Kubernetes readOnlyRootFilesystem): the
#        server cannot open publisher.db, and /status must say so.
#   S7   An unreadable publisher.config.json: /status must say so.
# Credentials
#   C1   GOOGLE_APPLICATION_CREDENTIALS names a file uid 1000 cannot read
#        (a 0600 file owned by someone else): a gs:// package add must name
#        EACCES, and so must the BigQuery connection test.
#   C2   GOOGLE_APPLICATION_CREDENTIALS names a directory, which is what a bind
#        of a host path that does not exist produces: both must say it is a
#        directory, not that the file "does not exist".
#
# The assertions require the cause and refuse the generic body. They do not pin
# wording. Every assertion runs before the script fails, so one run reports all
# of them; only a server that never reaches serving aborts the run.

set -euo pipefail

IMAGE="${1:-malloy-publisher:smoke-test}"
GENERIC='Internal server error.'
BASE_PORT="${BASE_PORT:-4100}"
PREFIX=fsw-smoke
PACKAGES_VOLUME=$PREFIX-packages
DATA_VOLUME=$PREFIX-data
CREDS_VOLUME=$PREFIX-creds
CREDS_DIR_VOLUME=$PREFIX-creds-dir
CONFIG_VOLUME=$PREFIX-config
VOLUMES=("$PACKAGES_VOLUME" "$DATA_VOLUME" "$CREDS_VOLUME" "$CREDS_DIR_VOLUME" "$CONFIG_VOLUME")
CREDS=/var/secrets/google/credentials.json

ENV_NAME=analytics
PKG=orders
NEW_VERSION=v2
OLD_VERSION=v1
GCS_LOCATION=gs://publisher-smoke-no-such-bucket/orders.zip

work=$(mktemp -d)
failures=0
containers=()
next_port=$BASE_PORT

# On Linux -- CI -- the package mount is the real thing: a bind whose host
# source does not exist yet, which the Docker daemon creates as root, as it
# does for any bind it has to create. Docker Desktop maps bind-mount ownership
# and would hide the failure, so off Linux a root-owned named volume stands in.
if [ "$(uname -s)" = Linux ]; then
   PACKAGES_MOUNT="$work/packages:/tmp/packages"
else
   docker volume create "$PACKAGES_VOLUME" >/dev/null
   PACKAGES_MOUNT="$PACKAGES_VOLUME:/tmp/packages"
fi

cleanup() {
   for c in "${containers[@]}"; do
      if [ "$failures" -gt 0 ]; then
         echo "--- $c logs (tail) ---"
         docker logs "$c" 2>&1 | tail -25 || true
      fi
      docker rm -f "$c" >/dev/null 2>&1 || true
   done
   docker volume rm -f "${VOLUMES[@]}" >/dev/null 2>&1 || true
   # The bind source is root-owned, so the runner user cannot remove it.
   docker run --rm --user 0 -v "$work:/w" "$IMAGE" rm -rf /w/packages >/dev/null 2>&1 || true
   rm -rf "$work"
}
trap cleanup EXIT

fail() {
   echo "✗ $*"
   failures=$((failures + 1))
}

pass() {
   echo "✓ $*"
}

as_root() {
   docker run --rm --user 0 "$@"
}

# start <name> [docker run args...]: start a server in the background. The
# port it publishes is $port afterwards (not a subshell, so the container is
# recorded for cleanup).
start() {
   local name=$1
   shift
   port=$next_port
   next_port=$((next_port + 1))
   docker run -d --name "$PREFIX-$name" -p "$port:4000" "$@" "$IMAGE" >/dev/null
   containers+=("$PREFIX-$name")
}

# wait_state <name> <state...>: wait until /status on $port reports one of the
# states; print the state reached, or nothing on timeout or exit.
wait_state() {
   local name=$1
   shift
   for _ in $(seq 1 60); do
      if ! docker ps -q -f "name=^$PREFIX-${name}$" | grep -q .; then
         return 0
      fi
      state=$(curl -sf "http://localhost:${port}/api/v0/status" | jq -r '.operationalState // empty' 2>/dev/null || true)
      for want in "$@"; do
         if [ "$state" = "$want" ]; then
            echo "$state"
            return 0
         fi
      done
      sleep 1
   done
}

wait_serving() {
   if [ "$(wait_state "$1" serving)" != serving ]; then
      echo "✗ $1 did not reach 'serving'"
      docker logs "$PREFIX-$1" 2>&1 | tail -40 || true
      exit 1
   fi
}

# assert_names <label> <regex> <json body>: the body's message (or
# errorMessage) is not the generic body and matches the regex.
assert_names() {
   local label=$1 pattern=$2 body=$3 message
   message=$(echo "$body" | jq -r '.message // .errorMessage // empty' 2>/dev/null || true)
   echo "  $label -> $body"
   if [ "$message" = "$GENERIC" ]; then
      fail "$label: answered the generic body, not the cause"
   elif ! echo "$message" | grep -Eqi "$pattern"; then
      fail "$label: response does not match /$pattern/ ('${message:-<no message>}')"
   else
      pass "$label names the cause"
   fi
}

# assert_in_load_errors <label> <package> <regex>
assert_in_load_errors() {
   local label=$1 package=$2 pattern=$3 status entry
   status=$(curl -s "http://localhost:${port}/api/v0/status")
   entry=$(echo "$status" | jq -c --arg env "$ENV_NAME" --arg pkg "$package" \
      '[.loadErrors // [] | .[] | select(.environment == $env and .package == $pkg)][0] // empty')
   if [ -z "$entry" ]; then
      fail "$label: $package is not in /status loadErrors ($(echo "$status" | jq -c '.loadErrors // "absent"'))"
   elif ! echo "$entry" | jq -r .message | grep -Eqi "$pattern"; then
      fail "$label: /status loadErrors entry does not match /$pattern/ ($entry)"
   else
      pass "$label: $package is in /status loadErrors with its cause"
   fi
}

# assert_status_names <label> <regex>: /status says why, anywhere in its body,
# rather than only on stderr.
assert_status_names() {
   local label=$1 pattern=$2 status
   status=$(curl -s "http://localhost:${port}/api/v0/status" || true)
   echo "  $label /status -> $(echo "$status" | jq -c '{operationalState, initialized, initError, loadErrors, emptyReason}' 2>/dev/null || echo "$status")"
   if echo "$status" | grep -Eqi "$pattern"; then
      pass "$label: /status names the cause"
   else
      fail "$label: /status does not say why the server cannot start (no /$pattern/)"
   fi
}

create_env() {
   local code
   code=$(curl -s -o "$work/env.json" -w '%{http_code}' -X POST \
      -H 'Content-Type: application/json' \
      -d "{\"name\":\"$ENV_NAME\",\"packages\":[{\"name\":\"seed\",\"location\":\"/tmp/packages/seed\"}],\"connections\":[]}" \
      "http://localhost:${port}/api/v0/environments")
   if [ "$code" != 200 ]; then
      fail "setup on :$port: creating the environment from a readable directory returned $code: $(cat "$work/env.json")"
      return 1
   fi
}

# add_package <name> <location>: the orchestrator's load call.
add_package() {
   curl -s -X POST -H 'Content-Type: application/json' \
      -d "{\"name\":\"$1\",\"location\":\"$2\"}" \
      "http://localhost:${port}/api/v0/environments/${ENV_NAME}/packages"
}

bigquery_test() {
   curl -s -X POST -H 'Content-Type: application/json' \
      -d '{"name":"bq","type":"bigquery","bigqueryConnection":{}}' \
      "http://localhost:${port}/api/v0/connections/test"
}

write_config() {
   printf '%s' "$2" >"$work/$1"
   chmod 644 "$work/$1"
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

# populate <mount>: the orchestrator's side, as root. Upload both versions'
# zips, and leave a directory named like the old one's zip, as a root-run 0.8.x
# extracting beside it did, with a file of its own in it.
populate() {
   as_root -v "$1" -v "$work/src:/src:ro" "$IMAGE" sh -c "
      set -e
      cp -r /src/seed /tmp/packages/seed
      for v in $NEW_VERSION $OLD_VERSION; do cp /src/$PKG.zip /tmp/packages/$PKG-\$v.zip; done
      mkdir -p /tmp/packages/$PKG-$OLD_VERSION
      cp -r /src/$PKG/. /tmp/packages/$PKG-$OLD_VERSION/
      echo mine >/tmp/packages/$PKG-$OLD_VERSION/keep.txt
      chown -R 0:0 /tmp/packages
      chmod -R u=rwX,go=rX /tmp/packages"
}
populate "$PACKAGES_MOUNT"

write_config boot-zip.json "{\"environments\":[{\"name\":\"$ENV_NAME\",\"packages\":[{\"name\":\"seed\",\"location\":\"/tmp/packages/seed\"},{\"name\":\"$PKG-$NEW_VERSION\",\"location\":\"/tmp/packages/$PKG-$NEW_VERSION.zip\"}],\"connections\":[]}]}"
write_config seed.json "{\"environments\":[{\"name\":\"$ENV_NAME\",\"packages\":[{\"name\":\"seed\",\"location\":\"/tmp/packages/seed\"}],\"connections\":[]}]}"

# assert_loads <label> <json body>: the add answered with the package.
assert_loads() {
   local label=$1 body=$2
   if echo "$body" | jq -e '.name' >/dev/null 2>&1; then
      pass "$label loads"
   else
      fail "$label did not load: $body"
   fi
}

# assert_mount_untouched <label> <mount>: the mount holds exactly what the
# orchestrator put there, and the directory beside the old version's zip still
# holds its own file.
assert_mount_untouched() {
   local label=$1 mount=$2 listing
   listing=$(as_root -v "$mount" "$IMAGE" sh -c \
      "cd /tmp/packages && ls -1A . | tr '\n' ' ' && cat $PKG-$OLD_VERSION/keep.txt")
   if [ "$listing" = "$PKG-$OLD_VERSION $PKG-$OLD_VERSION.zip $PKG-$NEW_VERSION.zip seed mine" ]; then
      pass "$label: nothing written beside the zips"
   else
      fail "$label: the package mount changed ($listing)"
   fi
}

echo "== Package mount"
echo "S3/S3b: runtime publish from a root-owned shared package mount"
start s3 -v "$PACKAGES_MOUNT"
wait_serving s3
if create_env; then
   assert_loads "S3 publish $PKG-$NEW_VERSION" \
      "$(add_package "$PKG-$NEW_VERSION" "/tmp/packages/$PKG-$NEW_VERSION.zip")"
   assert_loads "S3b publish $PKG-$OLD_VERSION" \
      "$(add_package "$PKG-$OLD_VERSION" "/tmp/packages/$PKG-$OLD_VERSION.zip")"
   assert_mount_untouched S3 "$PACKAGES_MOUNT"
fi

echo "S3c: the same mount bound read-only"
start s3c -v "$PACKAGES_MOUNT:ro"
wait_serving s3c
if create_env; then
   assert_loads "S3c publish $PKG-$OLD_VERSION" \
      "$(add_package "$PKG-$OLD_VERSION" "/tmp/packages/$PKG-$OLD_VERSION.zip")"
fi

echo "S4: a boot-time zip in the root-owned mount"
start s4 -v "$PACKAGES_MOUNT" -v "$work/boot-zip.json:/publisher/publisher.config.json:ro"
wait_serving s4
code=$(curl -s -o "$work/s4.json" -w '%{http_code}' \
   "http://localhost:${port}/api/v0/environments/${ENV_NAME}/packages/$PKG-$NEW_VERSION/models")
if [ "$code" = 200 ]; then
   pass "S4: the boot-time zip serves"
else
   fail "S4: a boot-time zip does not load ($code: $(cat "$work/s4.json"); loadErrors $(curl -s "http://localhost:${port}/api/v0/status" | jq -c '.loadErrors // "absent"'))"
fi
assert_mount_untouched S4 "$PACKAGES_MOUNT"

echo "== Server data"
echo "S2: a publisher_data volume left root-owned by a 0.8.x"
# Not left empty: Docker re-seeds an EMPTY volume from the image's directory
# on every mount, ownership included, which would quietly hand it back to uid
# 1000. A real 0.8.x volume holds the environment it wrote, so this one does.
docker volume create "$DATA_VOLUME" >/dev/null
as_root -v "$DATA_VOLUME:/publisher/publisher_data" "$IMAGE" \
   sh -c "mkdir -p '/publisher/publisher_data/$ENV_NAME' && chown -R 0:0 /publisher/publisher_data && chmod -R 755 /publisher/publisher_data"
start s2 -v "$DATA_VOLUME:/publisher/publisher_data" -v "$PACKAGES_MOUNT" \
   -v "$work/seed.json:/publisher/publisher.config.json:ro"
wait_serving s2
assert_names "S2 GET /environments/$ENV_NAME/packages/seed/models" 'EACCES|permission denied' \
   "$(curl -s "http://localhost:${port}/api/v0/environments/${ENV_NAME}/packages/seed/models")"

echo "S6: a read-only root filesystem"
start s6 --read-only --tmpfs /tmp
wait_state s6 serving initializing >/dev/null
sleep 3
assert_status_names S6 'EROFS|read-only'

echo "S7: an unreadable publisher.config.json"
docker volume create "$CONFIG_VOLUME" >/dev/null
as_root -v "$CONFIG_VOLUME:/cfg" "$IMAGE" sh -c \
   'printf "{\"environments\":[]}" >/cfg/publisher.config.json && chown 0:0 /cfg/publisher.config.json && chmod 600 /cfg/publisher.config.json'
start s7 -v "$CONFIG_VOLUME:/cfg" -e PUBLISHER_CONFIG_PATH=/cfg/publisher.config.json
wait_state s7 serving initializing >/dev/null
sleep 3
assert_status_names S7 'EACCES|permission denied'

echo "== Credentials"
echo "C1: GOOGLE_APPLICATION_CREDENTIALS names a file uid 1000 cannot read"
docker volume create "$CREDS_VOLUME" >/dev/null
as_root -v "$CREDS_VOLUME:/var/secrets/google" "$IMAGE" sh -c \
   "printf '{\"type\":\"service_account\",\"project_id\":\"smoke\",\"client_email\":\"smoke@smoke.iam.gserviceaccount.com\",\"private_key\":\"x\"}' >$CREDS && chown 0:0 $CREDS && chmod 600 $CREDS"
start c1 -v "$PACKAGES_MOUNT" -v "$CREDS_VOLUME:/var/secrets/google" -e "GOOGLE_APPLICATION_CREDENTIALS=$CREDS"
wait_serving c1
if create_env; then
   assert_names "C1 publish from $GCS_LOCATION" 'EACCES|permission denied' \
      "$(add_package "$PKG-gcs" "$GCS_LOCATION")"
fi
assert_names "C1 BigQuery connection test" 'EACCES|permission denied' "$(bigquery_test)"

echo "C2: GOOGLE_APPLICATION_CREDENTIALS names a directory"
docker volume create "$CREDS_DIR_VOLUME" >/dev/null
start c2 -v "$PACKAGES_MOUNT" -v "$CREDS_DIR_VOLUME:$CREDS" -e "GOOGLE_APPLICATION_CREDENTIALS=$CREDS"
wait_serving c2
if create_env; then
   assert_names "C2 publish from $GCS_LOCATION" 'director|EISDIR' \
      "$(add_package "$PKG-gcs" "$GCS_LOCATION")"
fi
assert_names "C2 BigQuery connection test" 'director|EISDIR' "$(bigquery_test)"

if [ "$failures" -gt 0 ]; then
   echo "✗ $failures filesystem assertion(s) failed"
   exit 1
fi
echo "✓ every package mount loaded without being written to, and every access the server could not make named its cause"
