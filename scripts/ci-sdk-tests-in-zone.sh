#!/usr/bin/env bash
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# Runs the SDK's date-handling specs (`test:tz`) in one time zone, for
# cross-platform-tests.yml. Used by the Linux client job on every PR and by the
# macOS and Windows legs, so the zone handling below has one copy.
#
# usage: ci-sdk-tests-in-zone.sh <IANA zone> <Windows zone name>
#
# Runners are UTC, the one zone where a local-vs-UTC date bug is invisible.
# Reverting GivenInput's `dayjs.utc` to `dayjs` fails 0 tests under UTC, 3
# under a zone ahead of it and 1 under a zone behind it. Both directions are
# needed, and they are not redundant: a date fixture only crosses midnight for
# zones on one side of UTC, so an ahead zone and a behind zone fail *different*
# tests. Any zone ahead scores the same as any other, and likewise behind, so
# the workflow's two are representatives rather than special: do not add more
# for symmetry, and do not swap one for a zone picked for being unusual.
#
# Extra runs rather than a matrix entry: a matrix would double the whole job,
# and setting TZ on the main run would trade away UTC coverage instead of
# adding to it. Only the specs that handle a date run here (7 SDK spec files);
# src/timezone_runs.spec.ts fails when one is missing from `test:tz`, so a new
# one cannot quietly skip these runs.
#
# The zone the runtime actually resolved is checked before testing, and the
# run fails if the zone did not take effect. Without that, a run would be
# green, fast and detecting nothing, the same silhouette as a suite collecting
# no tests at all.
#
# Linux and macOS take the zone from TZ. Bun on Windows ignores an IANA TZ and
# resolves UTC, so there the machine's own zone is switched with tzutil, which
# a newly started bun reads, and set back on exit, pass or fail, so the server
# suites that follow still run in the zone they always have.

set -euo pipefail

WANT_ZONE="$1"
WINDOWS_ZONE="$2"
export WANT_ZONE

if [ "${RUNNER_OS:-}" = "Windows" ]; then
  original=$(MSYS_NO_PATHCONV=1 tzutil /g | tr -d '\r')
  restore_zone() {
    echo "restoring the Windows time zone to $original"
    MSYS_NO_PATHCONV=1 tzutil /s "$original"
  }
  trap restore_zone EXIT
  echo "switching the Windows time zone from $original to $WINDOWS_ZONE"
  MSYS_NO_PATHCONV=1 tzutil /s "$WINDOWS_ZONE"
else
  export TZ="$WANT_ZONE"
fi

bun -e 'const want=process.env.WANT_ZONE, got=Intl.DateTimeFormat().resolvedOptions().timeZone; console.log("resolved timezone:", got); if (got !== want) { console.error(`Zone did not take effect: wanted ${want}, got ${got}. This run would be a duplicate UTC run detecting nothing.`); process.exit(1); }'
bun run --cwd packages/sdk test:tz
