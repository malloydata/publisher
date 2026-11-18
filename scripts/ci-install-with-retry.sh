#!/usr/bin/env bash
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# Runs a package install command, retrying it against the SAME registry when it
# fails: `scripts/ci-install-with-retry.sh bun install --frozen-lockfile`.
#
# Credible's donated runners send npm and bun installs through a pull-through
# mirror (the job-started hook prints which), and that mirror intermittently
# answers a tarball GET with 401 or 403 well inside its credentials' lifetime.
# A second try a few seconds later succeeds. This retries the command exactly
# as given: it adds no flag, changes no registry and has no fallback to any
# other one, so a retried install resolves the same lockfile from the same
# place as the first try would have.
#
# A retry that rescued the job is reported as a ::warning:: annotation, so a
# mirror that is getting worse stays visible on the run summary instead of
# being absorbed silently. When every attempt fails, the last attempt's output
# is the last thing in the log and the script exits with its status.
#
# CI_INSTALL_RETRY_DELAYS overrides the waits between attempts, in seconds
# (default "15 45": three attempts in all). Tests set it to "0 0".

set -euo pipefail

if [ "$#" -eq 0 ]; then
  echo "usage: $0 <install command> [args...]" >&2
  exit 2
fi

read -r -a delays <<< "${CI_INSTALL_RETRY_DELAYS:-15 45}"
attempts=$(( ${#delays[@]} + 1 ))

for (( attempt = 1; attempt <= attempts; attempt++ )); do
  echo "ci-install-with-retry: attempt ${attempt} of ${attempts}: $*"
  status=0
  "$@" || status=$?
  if [ "$status" -eq 0 ]; then
    echo "ci-install-with-retry: succeeded on attempt ${attempt} of ${attempts}"
    if [ "$attempt" -gt 1 ]; then
      echo "::warning title=Package install retried::'$*' failed $(( attempt - 1 )) time(s) before succeeding on attempt ${attempt} of ${attempts}; see the earlier attempts' output in this step for the registry's errors"
    fi
    exit 0
  fi
  if [ "$attempt" -lt "$attempts" ]; then
    delay=${delays[$(( attempt - 1 ))]}
    echo "ci-install-with-retry: attempt ${attempt} exited ${status}; retrying in ${delay}s"
    sleep "$delay"
  fi
done

echo "::error title=Package install failed::'$*' failed all ${attempts} attempts; the last exited ${status}, and its output is directly above"
exit "$status"
