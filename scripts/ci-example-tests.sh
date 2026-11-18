#!/usr/bin/env bash
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# Runs the example packages' tests (`test:examples`) for cross-platform-tests.yml
# and fails on any skip. Used by the Linux client job and the macOS and Windows
# legs.
#
# The storefront data app's filter encoding is checked against Malloy's own
# filter grammar, under node rather than bun, because the page is a no-build ES
# module the browser loads directly, and node is the runtime closest to that:
# no bundler, no transform, no test-runner globals.
#
# Two ways this could pass while testing nothing, both closed here rather than
# left to a comment.
#
# The script NAMES its test files instead of globbing them: `node --test` on a
# glob that matches nothing exits 0 and reports "tests 0", so a moved or renamed
# directory would turn this green while running none of it. Named explicitly, a
# missing file exits 1. The cost is that a new test file has to be added to the
# script by hand.
#
# And a skip counts toward the total, so an expected-count comment cannot tell
# a skipped case from a run one. One suite skips itself off en-US, where its
# currency assertions do not hold, and CI is en-US, so any skip here means the
# runner is not what we think it is. No count is written down: the total has
# changed often, and every written-down figure went stale.

# -e explicitly, not left to the caller's shell: without it a FAILING suite
# exits 0, because the grep succeeds and runs last.
set -eo pipefail

log="${RUNNER_TEMP:-.}/examples-tests.log"
bun run test:examples 2>&1 | tee "$log"
grep -q "skipped 0" "$log" || {
  echo "::error::example tests reported a skip; this suite must run in full on CI"
  exit 1
}
