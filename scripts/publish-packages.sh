#!/usr/bin/env bash
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# The body of release.yml's `publish-packages` job. Extracted from the
# workflow because a `run:` block is one GitHub expression template and the
# limit is 21000 characters; this body is past it. Every input arrives
# through the step's `env:`, so there is nothing interpolated here.

set -euo pipefail

echo "## Independently-versioned packages" >> "$GITHUB_STEP_SUMMARY"

# Skip on a prerelease. These packages carry their own versions, which
# have no hyphen, so their publish workflows would tag them `latest`
# even though the release cutting them is a prerelease. gh-release
# skips prereleases for the same reason.
#
# Checked here rather than as a job-level `if:` so that it announces
# itself. A skipped job writes no summary and no annotation, and a
# skipped job still reports success, so gating it that way would make
# a green release that published nothing completely invisible.
if [[ "$NEW_VERSION" == *-* ]]; then
  echo "::notice title=Independently-versioned packages::Skipped for prerelease version ${NEW_VERSION}. Publish these from an ordinary release, or dispatch skills-npm.yml, then create-malloy-package-npm.yml, then python-sdk.yml on main."
  echo "Skipped for prerelease version \`${NEW_VERSION}\`. Publish these from an ordinary release, or dispatch their workflows on main." >> "$GITHUB_STEP_SUMMARY"
  exit 0
fi

# Wait for the version to land on npm rather than watching the child
# run: a child whose publish job was skipped reports success, so the
# run's conclusion would not catch it, and asking the registry has no
# correlation race with a concurrent manual dispatch.
#
# Size the budget against the whole child run, not one job of it. Its
# publish job needs check_pack first (a full frozen-lockfile install of
# the monorepo, a build, and the package's tests, which has taken over
# six minutes on its own), and then repeats the install in the publish
# job itself, with no cache in either. Add dispatch queue time and npm
# CDN visibility lag. Timing out early is expensive in the wrong way:
# the release goes red for a publish that in fact succeeded, and
# because the failure stops the sequence the packages after it are
# never dispatched at all.
#
# A wall-clock deadline rather than an attempt count, so npm latency
# cannot stretch the budget past the job's timeout-minutes and cost us
# the diagnostic below.
POLL_BUDGET_SECONDS=1500
POLL_SLEEP=15
# How many consecutive unexpected registry answers end the wait. One
# is not enough: a single 5xx or DNS blip says almost nothing about
# whether the child published, and giving up on it would reintroduce
# exactly the failure the budget comment above is about, with the
# remaining packages never dispatched.
POLL_MAX_REGISTRY_ERRORS=3

# Registry and API output can contain anything, including a line starting with
# "::" that the runner would parse as a workflow command. Prefix every
# line with a non-whitespace marker: the runner trims leading
# whitespace before it looks for "::", so indenting would not help.
echo_untrusted_output() {
  printf '%s\n' "$1" | sed 's/^/[npm] /'
}

# The sha this job verified its versions against. Fixed for the whole
# job by definition: it is what the checkout above contains, and
# nothing here commits or pulls. Re-reading it per package would
# return the same value and fix nothing, which is why the staleness
# problem is solved by scoping the comparison (below) rather than by
# moving this line.
if ! CHECKOUT_SHA="$(git rev-parse HEAD)"; then
  echo "::error title=Independently-versioned packages::could not read this checkout's sha, so nothing can be verified against main. No package was dispatched."
  echo "- nothing dispatched: could not read the checkout's sha" >> "$GITHUB_STEP_SUMMARY"
  exit 1
fi

# The two registries differ in exactly two places — how a manifest is
# read and how the registry is asked — so those are the only two things
# parameterised. Everything else in publish_pkg below (the watched-path
# guard, the "main moved" compare, the dispatch, the poll loop, the
# summary lines) is registry-agnostic and must stay that way: a second
# copy of this function for PyPI is how the two would drift.
#
# Prints "<name> <version> <slug>" on success, nothing on failure.
# The slug is what goes in a URL; for npm it is unused and echoes the
# name back so both kinds print three fields.
read_manifest() {
  local kind="$1" dir="$2"
  case "$kind" in
    npm)
      # `node -p` prints the string "undefined" with exit 0 for a
      # missing field; the caller checks for it, which is why this
      # does not try to.
      node -p "const p=require('./packages/${dir}/package.json'); \
               [p.name, p.version, p.name].join(' ')" 2>&1
      ;;
    pypi)
      # tomllib, the same read build-python-sdk.sh and the bump check
      # use, so the three cannot disagree about where the version
      # lives. The slug is PEP 503 normalisation.
      #
      # tomllib is stdlib from Python 3.11, and this job installs
      # NOTHING on purpose — it holds actions: write, so the fewer
      # third-party actions and downloads in it the better. That makes
      # the interpreter an assumption, so assert it instead of
      # provisioning it: ubuntu-24.04 ships 3.12 as python3 today, and
      # without this check a runner image that moved back below 3.11
      # would report "could not read its manifest", which reads as a
      # corrupt pyproject.toml and sends someone to the wrong file.
      #
      # Failing here rather than up front is deliberate. It costs the
      # npm packages nothing: they are dispatched before this and
      # re-running the job skips whatever already published, so the
      # blast radius is one undispatched package with an accurate
      # message, not two blocked publishes.
      if ! python3 -c 'import tomllib' 2>/dev/null; then
        echo "python3 reports $(python3 -V 2>&1) and has no tomllib; reading pyproject.toml needs Python 3.11 or newer on the runner"
        return 1
      fi
      # One line on purpose. A multi-line `python3 -c '...'` has to sit
      # at this run: block's BASE indentation for YAML to strip it back
      # to column 0, which inside a nested shell function means source
      # lines dedented out of the block that contains them — it reads as
      # a mistake and one wrong space either ends the block scalar early
      # or hands Python an IndentationError. python-sdk.yml can indent
      # its copy because that one is at the top level of its step.
      python3 -c 'import re, sys, tomllib; p = tomllib.load(open("packages/" + sys.argv[1] + "/pyproject.toml", "rb"))["project"]; print(p["name"], p["version"], re.sub(r"[-_.]+", "-", p["name"]).lower())' "$dir" 2>&1
      ;;
  esac
}

# Three-way, and the three ways are the whole point: published, free,
# and "the registry did not answer" — which is not a green light. Both
# branches echo the raw registry answer for echo_untrusted_output.
#   0 = this version is published
#   1 = this version is free
#   2 = no usable answer
registry_has() {
  local kind="$1" name="$2" version="$3" slug="$4" answer
  case "$kind" in
    npm)
      # Exit 0 AND non-empty output. npm answers some queries with exit
      # 0 and nothing on stdout, which is not a yes.
      if answer="$(npm view "${name}@${version}" version --prefer-online 2>&1)" \
         && [ -n "$answer" ]; then
        printf '%s' "$answer"; return 0
      fi
      printf '%s' "$answer"
      case "$answer" in *E404*) return 1 ;; *) return 2 ;; esac
      ;;
    pypi)
      # PyPI has no `npm view`; the JSON endpoint's STATUS CODE is the
      # answer, so the code is read rather than the exit status and
      # there is no --fail (a 404 is an answer, not a failure). A
      # transport failure makes curl print 000, which lands in the
      # "did not answer" branch, and `|| answer=000` catches the case
      # where curl fails before printing anything at all.
      answer="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 20 \
        "https://pypi.org/pypi/${slug}/${version}/json" 2>&1)" || answer=000
      printf 'HTTP %s' "$answer"
      case "$answer" in 200) return 0 ;; 404) return 1 ;; *) return 2 ;; esac
      ;;
    # NOT optional, and not defensive. A `case` that matches nothing
    # succeeds, so without this an unrecognised kind returns 0, which
    # the caller reads as "already published": it prints the notice,
    # writes "was already published, skipped" to the job summary, and
    # returns 0. The package is silently never dispatched and the
    # release goes green. That is the one fail-OPEN outcome this whole
    # function is written to avoid, and a typo or a fourth package is
    # all it takes.
    *)
      printf 'unknown registry kind: %s' "$kind"
      return 2
      ;;
  esac
}

publish_pkg() {
  local dir="$1" wf="$2" reg="$3" name version slug spec out rc
  local deadline live_sha errors
  local changed file_count relevant file path
  local -a paths
  # Read through `if !`, and validate. A bare assignment under set -e
  # dies with a raw Node stack trace, no annotation and no summary
  # line, which is the silent failure the summary lines exist to
  # prevent. And `node -p` prints the string "undefined" with exit 0
  # for a missing field, so a manifest that lost its name would build
  # the spec `undefined@0.1.3`; the package named `undefined` exists on
  # npm, so that spec can resolve and be read as "already published".
  if ! out="$(read_manifest "$reg" "$dir")" || [ -z "$out" ]; then
    echo_untrusted_output "$out"
    echo "::error title=${dir}::could not read the ${reg} manifest for packages/${dir}, so ${wf} was not dispatched"
    echo "- \`${dir}\` NOT dispatched: could not read its manifest" >> "$GITHUB_STEP_SUMMARY"
    return 1
  fi
  # `tail -n 1`, because both read_manifest arms end in `2>&1` so a
  # failure has something to show — and node and python write warnings
  # there on a SUCCESSFUL run too (ExperimentalWarning,
  # DeprecationWarning). A bare `read` takes the FIRST line, so one
  # warning becomes the name and version: both non-empty, neither the
  # literal "undefined", so both guards below pass, the spec is
  # nonsense, the registry 404s on it (reading as "free"), the child is
  # dispatched and may publish correctly, and then the poll asks about
  # the bogus spec for the full budget and reddens a release that
  # worked. The npm steps in this repo use `tail -n 1` for exactly this.
  read -r name version slug <<<"$(printf '%s\n' "$out" | tail -n 1)"
  # "undefined" is checked because `node -p` prints exactly that, with
  # exit 0, for a missing field — and the package literally named
  # `undefined` exists on npm, so that spec can resolve and be read as
  # "already published".
  if [ -z "$name" ] || [ "$name" = "undefined" ] \
     || [ -z "$version" ] || [ "$version" = "undefined" ] || [ -z "$slug" ]; then
    echo "::error title=${dir}::the ${reg} manifest for packages/${dir} declares name='${name}' version='${version}'; refusing to publish an unnamed or unversioned package. ${wf} was not dispatched."
    echo "- \`${dir}\` NOT dispatched: its manifest has no usable name or version" >> "$GITHUB_STEP_SUMMARY"
    return 1
  fi
  # Written the way someone would paste it into the matching installer,
  # so a summary line is directly actionable.
  case "$reg" in
    npm) spec="${name}@${version}" ;;
    pypi) spec="${name}==${version}" ;;
  esac

  # The paths this package's PUBLISHED content is built from, which is
  # not the same as the paths it lives in.
  #
  # skills packs the repo-root skills/ tree as well as its own
  # directory, so it has both.
  #
  # create-malloy-package ships dist/ and templates/ built from its own
  # directory, PLUS a dependency range its publish job bakes in by
  # reading packages/skills/package.json from main at dispatch time.
  # That file therefore belongs in its list even though its contents
  # never enter the tarball. Scoped to the one file rather than the
  # skills directory: a skill edit cannot change what the scaffolder
  # publishes, only a version change can.
  #
  # The child workflow file is in both lists because a dispatch runs
  # whatever definition main holds at dispatch time, not the one this
  # job read.
  # skills additionally watches the root install inputs, and the
  # asymmetry with the scaffolder is deliberate. Its dist/ is emitted
  # by `tsc`, whose version bun resolves from the root lockfile, so a
  # lockfile-only bump changes the published bytes. The scaffolder's
  # dist/ comes from `bun build --packages external`, where the bun
  # version is pinned by BUN_VERSION in its own (watched) workflow file
  # and nothing from node_modules enters the bundle.
  # python-client is GENERATED from api-doc.yaml by build-python-sdk.sh,
  # so the OpenAPI spec is a published-content input exactly the way the
  # skills tree is for skills: an api-doc.yaml-only commit changes the
  # wheel with nothing in packages/python-client/ touched. Its own bump
  # check watches the same two paths.
  case "$dir" in
    skills) paths=("skills/" "packages/skills/" "bun.lock" "package.json") ;;
    create-malloy-package) paths=("packages/${dir}/" "packages/skills/package.json") ;;
    python-client) paths=("packages/${dir}/" "api-doc.yaml") ;;
    *) paths=("packages/${dir}/") ;;
  esac
  paths+=(".github/workflows/${wf}")

  # The whole guard rests on the trailing-slash convention below, and
  # both ways of getting it wrong disable an entry silently rather than
  # loudly: a directory written without its slash becomes an exact-file
  # comparison that can never fire, and a file written with one becomes
  # a prefix that can never fire. Check the convention against the
  # checkout instead of trusting whoever edits the list next.
  for path in "${paths[@]}"; do
    case "$path" in
      */) [ -d "${path%/}" ] && continue ;;
      *) [ -f "$path" ] && continue ;;
    esac
    echo "::error title=${name}::watched path '${path}' does not match its kind in this checkout (a trailing slash means a directory, none means a file). ${wf} was NOT dispatched, because an entry that cannot match silently stops guarding."
    echo "- \`${spec}\` NOT dispatched: watched path '${path}' is malformed" >> "$GITHUB_STEP_SUMMARY"
    return 1
  done

  # Three outcomes, and the third is why this is not a boolean:
  # published means there is nothing to do, free means carry on, and
  # "the registry did not answer" is NOT a green light. `|| rc=$?`
  # rather than a bare call, because a non-zero return under set -e
  # would kill the step before the case below could read it.
  out="$(registry_has "$reg" "$name" "$version" "$slug")" && rc=0 || rc=$?
  case "$rc" in
    0)
      echo "::notice title=${name}::${spec} is already published; nothing to do"
      echo "- \`${spec}\` was already published, skipped" >> "$GITHUB_STEP_SUMMARY"
      return 0
      ;;
    1) : ;; # the version is free, carry on
    *)
      echo_untrusted_output "$out"
      echo "::error title=${name}::could not ask the ${reg} registry whether ${spec} is taken"
      echo "- \`${spec}\` NOT dispatched: the ${reg} registry did not answer whether it is already published" >> "$GITHUB_STEP_SUMMARY"
      return 1
      ;;
  esac

  # Fail closed on a content change, not just a version change: skills
  # publishes the repo-root skills/ tree, so a commit that leaves the
  # version untouched can still change what ships under it, and npm
  # versions are immutable.
  #
  # Scoped to this package's paths rather than to any movement at all.
  # A later package's check runs after the earlier ones finished a full
  # dispatch-and-wait, up to POLL_BUDGET_SECONDS, so comparing bare shas
  # failed the scaffolder whenever anything at all merged inside that
  # window, including changes that could not affect it. That is a
  # likely event on a busy day and it stops the sequence.
  # `if !` rather than a bare assignment. Under set -e a failing
  # command substitution kills the step outright, before the check
  # below can run, so a gh crash here produced no annotation and an
  # empty job summary: the exact silent failure the summary lines
  # exist to prevent. The regex below only catches a gh that SUCCEEDS
  # and returns something that is not a sha.
  if ! live_sha="$(gh api "repos/${GITHUB_REPOSITORY}/commits/main" --jq .sha 2>&1)"; then
    echo_untrusted_output "$live_sha"
    echo "::error title=${name}::could not reach the GitHub API to read main's current sha (logged above); ${wf} was not dispatched"
    echo "- \`${spec}\` NOT dispatched: could not reach the API to read main's sha" >> "$GITHUB_STEP_SUMMARY"
    return 1
  fi
  if [[ ! "$live_sha" =~ ^[0-9a-f]{40}$ ]]; then
    # Through the prefixer, not inlined: this is the branch where the
    # value FAILED validation, so it is exactly the case where it may
    # not be a bare sha. Same reasoning as the count below.
    echo_untrusted_output "$live_sha"
    echo "::error title=${name}::could not read main's current sha as 40 hex characters (logged above); ${wf} was not dispatched"
    echo "- \`${spec}\` NOT dispatched: could not read main's sha" >> "$GITHUB_STEP_SUMMARY"
    return 1
  fi

  if [ "$live_sha" != "$CHECKOUT_SHA" ]; then
    # Every branch from here fails closed. A wrong "proceed" publishes
    # an immutable version whose content nobody verified; a wrong
    # "stop" costs a re-run.
    #
    # One request, and deliberately NOT --paginate. The compare
    # endpoint paginates its `commits` array; `files` is capped at 300
    # and comes back whole on every page, so --paginate would emit the
    # same filenames once per commit page. That inflates the count
    # below and would trip the cap check on a wide-enough release
    # window: a false failure of exactly the kind this whole change
    # exists to remove.
    #
    # The count comes from the same response as the names, in one
    # --jq program, so the two cannot disagree and it stays a single
    # round trip. Line prefixes because filenames are untrusted text.
    # The `type` assertion is not decoration. Without it a response
    # carrying no `files` array yields `COUNT 0` from `null | length`
    # before it errors, and the whole point of this block is that
    # "zero relevant files" means "go ahead and publish".
    # `previous_filename` as well as `filename`, and that is not
    # belt and braces. The compare API reports a rename under its NEW
    # path only, so moving packages/skills/foo.md to docs/foo.md
    # changes what the skills tarball contains while reporting a path
    # that matches nothing here. Emitting both makes a rename OUT of a
    # watched directory count, which is the fail-closed direction. A
    # deletion already reports the old path, so it was never affected.
    # `.status` is checked FIRST and is not defensive padding. This is
    # a three-dot compare, so it is merge-base relative: if main was
    # rewritten and live_sha is no longer a descendant of the
    # checkout, `files` describes merge-base to live_sha and simply
    # omits every commit that existed at checkout and no longer does.
    # A rewind returns an empty list. Both read as "nothing relevant
    # changed", which is the one wrong answer that publishes.
    # Requiring "ahead" makes anything other than a fast-forward abort.
    if ! changed="$(gh api "repos/${GITHUB_REPOSITORY}/compare/${CHECKOUT_SHA}...${live_sha}" --jq 'if .status != "ahead" then error("main is not a fast-forward ahead of this checkout (status=\(.status)); the file list would be incomplete") elif (.files | type) != "array" then error("compare response has no files array") else ("COUNT \(.files | length)"), (.files[] | "FILE \(.filename)"), (.files[] | select(.previous_filename) | "FILE \(.previous_filename)") end' 2>&1)"; then
      echo_untrusted_output "$changed"
      echo "::error title=${name}::main moved from ${CHECKOUT_SHA} to ${live_sha} and the compare API did not answer, so whether it touched ${dir} is unknown. ${wf} was NOT dispatched."
      echo "- \`${spec}\` NOT dispatched: main moved and the compare API did not answer" >> "$GITHUB_STEP_SUMMARY"
      return 1
    fi

    file_count="$(printf '%s\n' "$changed" | sed -n 's/^COUNT //p')"
    if [[ ! "$file_count" =~ ^[0-9]+$ ]]; then
      # Print the value through the prefixer rather than inlining it in
      # the annotation. It is derived from filenames, a filename may
      # contain a newline, and two forged COUNT lines would otherwise
      # put attacker text at the start of a line inside this ::error,
      # where the runner reads it as a workflow command.
      echo_untrusted_output "$file_count"
      echo "::error title=${name}::could not read a single numeric changed-file count from the compare of ${CHECKOUT_SHA}...${live_sha} (logged above), so a change to ${dir} cannot be ruled out. ${wf} was NOT dispatched."
      echo "- \`${spec}\` NOT dispatched: unreadable compare response" >> "$GITHUB_STEP_SUMMARY"
      return 1
    fi

    # The compare endpoint caps files at 300 and signals that only by
    # returning exactly that many, so a diff at the cap may be
    # truncated and a relevant file may be missing from it.
    if [ "$file_count" -ge 300 ]; then
      echo "::error title=${name}::main moved from ${CHECKOUT_SHA} to ${live_sha} across at least 300 files, which is the compare API's cap, so the file list may be truncated and cannot rule out a change to ${dir}. ${wf} was NOT dispatched."
      echo "- \`${spec}\` NOT dispatched: compare diff hit the 300-file cap" >> "$GITHUB_STEP_SUMMARY"
      return 1
    fi

    relevant=""
    while IFS= read -r file; do
      # Only FILE lines; the COUNT line above shares this stream.
      case "$file" in FILE\ *) file="${file#FILE }" ;; *) continue ;; esac
      [ -n "$file" ] || continue
      # A trailing slash means "this directory and everything under
      # it"; anything else is one exact file. Without the distinction
      # a bare filename also matches its own neighbours by prefix, so
      # packages/skills/package.json.bak would count as a change to
      # packages/skills/package.json.
      for path in "${paths[@]}"; do
        case "$path" in
          # A directory entry also has to match the directory's own
          # path with the slash stripped. Replace the skills/ tree with
          # a file or a symlink and git reports the blob at `skills`,
          # which the prefix form alone would miss.
          */) case "$file" in
                "$path"* | "${path%/}") relevant="${relevant}  ${file}"$'\n'; break ;;
              esac ;;
          *) [ "$file" = "$path" ] && { relevant="${relevant}  ${file}"$'\n'; break; } ;;
        esac
      done
    done <<< "$changed"

    if [ -n "$relevant" ]; then
      # Deliberately does not claim nothing was published: on the second
      # package, the first may already have been dispatched and confirmed.
      # The job summary records what actually shipped.
      echo "::error title=${name}::main moved from ${CHECKOUT_SHA} to ${live_sha} during this release and touched files ${spec} is built from, so ${wf} would not publish what this job verified. It was NOT dispatched; see the job summary for what already shipped. Re-run THIS JOB (Re-run failed jobs), or dispatch ${wf} on main directly. Do not re-run the whole release: that bumps and republishes sdk/app/server."
      printf '%s\n' "$relevant" | sed 's/^/[changed] /'
      echo "- \`${spec}\` NOT dispatched: main moved under its own paths" >> "$GITHUB_STEP_SUMMARY"
      return 1
    fi

    echo "::notice title=${name}::main moved from ${CHECKOUT_SHA} to ${live_sha} during this release, but nothing under ${paths[*]} changed, so ${spec} is still what ${wf} will publish"
  fi

  # The dispatch API returns no run id, and picking one out of
  # gh run list right afterwards can select an older or concurrent
  # run, so link the filtered workflow view rather than a wrong run.
  local runs_url="https://github.com/${GITHUB_REPOSITORY}/actions/workflows/${wf}?query=branch%3Amain"

  echo "::notice title=${name}::dispatching ${wf} on main to publish ${spec} (${runs_url})"
  # Aborting here is right: polling 25 minutes for a run that was never
  # created helps nobody. But under `set -e` a bare command would exit
  # the step with only gh's stderr and the notice above to go on, which
  # is the one exit from this function with no explanation of its own.
  if ! out="$(gh workflow run "$wf" --repo "$GITHUB_REPOSITORY" --ref main 2>&1)"; then
    echo_untrusted_output "$out"
    echo "::error title=${name}::could not dispatch ${wf} on main, so ${spec} was not published and nothing is waiting for it. Check that the workflow exists on main and still declares workflow_dispatch, then dispatch it yourself at ${runs_url}."
    echo "- \`${spec}\` NOT dispatched: the dispatch API call failed" >> "$GITHUB_STEP_SUMMARY"
    return 1
  fi

  deadline=$((SECONDS + POLL_BUDGET_SECONDS))
  errors=0
  while [ "$SECONDS" -lt "$deadline" ]; do
    sleep "$POLL_SLEEP"
    # Keep the same three-way distinction the check above makes. A
    # registry that cannot answer is not the same as a child that did
    # not publish, and reporting it as the latter would send the
    # operator to a child run they will find green. Require the
    # condition to persist, though: a single blip is not evidence, and
    # giving up on one would strand the later packages undispatched.
    # This is the more dangerous of the two query sites: an
    # unanswered registry read as success would print "published",
    # write "published" to the job summary, and return 0, so on a later
    # package the release ends GREEN asserting a publish that never
    # happened. rc=2 is counted as "did not answer", not as a yes and
    # not as a no.
    out="$(registry_has "$reg" "$name" "$version" "$slug")" && rc=0 || rc=$?
    case "$rc" in
      0)
        echo "::notice title=${name}::${spec} published"
        echo "- \`${spec}\` published" >> "$GITHUB_STEP_SUMMARY"
        return 0
        ;;
      1) errors=0 ;;
      *)
        errors=$((errors + 1))
        echo_untrusted_output "$out"
        if [ "$errors" -ge "$POLL_MAX_REGISTRY_ERRORS" ]; then
          echo "::error title=${name}::the ${reg} registry stopped answering while waiting for ${spec} (${errors} consecutive unusable answers); the ${wf} run may have published it. Check ${runs_url} and the registry before re-dispatching."
          echo "- \`${spec}\` dispatched, then the ${reg} registry stopped answering; check ${runs_url}" >> "$GITHUB_STEP_SUMMARY"
          return 1
        fi
        ;;
    esac
  done

  echo "::error title=${name}::${spec} did not appear on ${reg} within $((POLL_BUDGET_SECONDS / 60))m. Check ${runs_url}. If that run failed, fix it and dispatch ${wf} on main; do not re-run the whole release, which bumps and republishes sdk/app/server."
  echo "- \`${spec}\` dispatched but did not appear on ${reg}, see ${runs_url}" >> "$GITHUB_STEP_SUMMARY"
  return 1
}

# Order is not optional for the first two. create-malloy-package pins
# its skills dependency from the monorepo and then asks the registry
# whether that range resolves, so skills has to be ON NPM before the
# scaffolder is dispatched. Dispatching both at once races and fails the
# scaffolder.
#
# python-client has no such coupling — nothing depends on it and it
# depends on nothing here — so it is last only because it is the one
# whose first publish is still unproven. A failure there leaves the two
# npm packages already published, which the job summary records.
publish_pkg skills skills-npm.yml npm
publish_pkg create-malloy-package create-malloy-package-npm.yml npm
publish_pkg python-client python-sdk.yml pypi
