#!/usr/bin/env bash
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# The body of release.yml's `gh-release` stamp step. Extracted from the
# workflow because a `run:` block is one GitHub expression template and the
# limit is 21000 characters; this body is past it. Every input arrives
# through the step's `env:`, so there is nothing interpolated here.

set -euo pipefail

git config --local user.email "${ACTOR}@users.noreply.github.com"
git config --local user.name "${ACTOR}"

# The same three files `prepare` bumps on the release branch. Reset
# here so `main` ends the release declaring what shipped.
PKGS="packages/sdk/package.json packages/app/package.json packages/server/package.json"

# The branch name is unchanged even though the branch now carries the
# version reset as well. It is the identifier the publisher-release
# skill and CONTEXT.md both tell a releaser to look for
# (`git ls-remote --heads origin 'refs/heads/release-notes-stamp-*'`),
# and renaming it would break that recovery path to buy nothing.
BRANCH="release-notes-stamp-${VERSION}"
STAMP_LOG="$(mktemp)"
FAILURE=""
STAMPED=0
VERSIONED=0
# Distinct from VERSIONED=0, which is also the no-op value. Declared
# here as well as reset per attempt below, so a path that never enters
# the loop cannot trip `set -u` on it in the summary.
VERSION_FAILED=0
# Read by the summary, the annotation and the commit body, but assigned
# deep inside the loop next to the commit — so declare it here too,
# for the same reason.
VERSION_NOTE=""
OUTCOME=""

# This step prepares a branch and hands a human a link. It deliberately
# does neither of the two more automatic things, and the reasons are
# worth keeping because both look better than they are.
#
# NOT `git push origin main`, which is what this step did until 0.0.250
# and which never once worked: main is protected and requires a pull
# request, so every attempt was rejected with GH006, retry included.
# 0.0.249 is the worked example — six sections on its release page, none
# stamped. A bypass cannot be granted from inside the workflow; it is a
# change to the branch protection itself, by someone with admin.
#
# NOT `gh pr create` either, though that was the obvious replacement.
# Opening a PR with GITHUB_TOKEN requires the repo/org setting "Allow
# GitHub Actions to create and approve pull requests", which is OFF by
# default, and reading it needs admin — so from here it is unverifiable.
# Depending on an unverifiable permission is exactly how the GH006 bug
# got written: the old code assumed a push it was never allowed to make,
# and the comment above it admitted the assumption was unconfirmed.
# Repeating that shape would have failed the same way, silently, at the
# next release.
#
# A branch push, by contrast, is PROVEN on this token: `prepare` pushes
# `release/sdk-<version>` every single release. And letting a human open
# the PR is not merely safer, it is better — a PR opened by a person
# triggers `pull_request`, so its checks actually run and any maintainer
# can merge it. A bot-authored PR never triggers workflows, so its
# required checks would sit at `expected` forever and only an admin
# could merge. The cost is one click, and it buys back the check suite.
#
# Each git command is checked rather than left to `set -e`. A bare
# failure would exit the step, and `continue-on-error` would then report
# it as a success with nothing in the summary — the silent skip this
# whole step exists to end. The two passes cover a transient fetch,
# stamp or push failure; nothing here has a side effect that a second
# attempt could duplicate, which is what makes retrying safe now that
# no PR is created.
for pass in 1 2; do
  # Names the actual reason, before it is cleared. A retry is usually a
  # flaky fetch, but a stamp or a push can fail into it too.
  if [ "$pass" = 2 ]; then
    echo "::notice title=Release notes::${FAILURE}; retrying"
  fi
  FAILURE=""
  # Reset per attempt, beside FAILURE and for the same reason: pass 1
  # failing the reset must not make pass 2's summary claim it failed
  # when the retry wrote the version fine.
  VERSION_FAILED=0

  # A fresh main, not this checkout: this job is on the release branch,
  # and main may have moved since the release started.
  git fetch origin main || { FAILURE="could not fetch main"; continue; }
  # `-f` is what makes the retry correct, not just tidy. If pass 1 died
  # after `stamp` rewrote RELEASE_NOTES.md but before the commit landed,
  # $BRANCH already points at origin/main, so a plain `checkout -B` is a
  # no-op that PRESERVES that edit — and pass 2's stamp then finds no
  # `## [Unreleased]` headings left, returns 0, and reports "nothing to
  # stamp" over a release that had narrative. Forcing restores the file.
  git checkout -f -B "$BRANCH" origin/main || { FAILURE="could not check out ${BRANCH}"; continue; }

  if ! STAMPED="$(node scripts/release-notes.mjs stamp "$VERSION" --titles "$TITLES_FILE" 2>"$STAMP_LOG")"; then
    sed 's/^/[release-notes] /' "$STAMP_LOG"
    FAILURE="release-notes.mjs stamp failed (logged above)"
    continue
  fi
  # Non-empty on success means a section that reached the release page
  # was edited or removed on main during the release window, so there is
  # no heading left to stamp. Nothing to undo — the narrative is already
  # public — but it should not pass unremarked.
  if [ -s "$STAMP_LOG" ]; then
    sed 's/^/[release-notes] /' "$STAMP_LOG"
    echo "::warning title=Release notes::a section on the release page is no longer in RELEASE_NOTES.md on main (logged above) and was not stamped"
  fi
  # Reset main's declared sdk/app/server version to what just shipped,
  # on this same branch. `main` has never carried the released version:
  # release branches are never merged back, so packages/sdk/package.json
  # sat 41 patches behind npm and could not be read by anything.
  #
  # Here and not in `prepare`, for a reason that is easy to undo by
  # accident. `prepare` stages exactly these three files and commits
  # them; if `main` already declared the version being released there
  # would be no diff, `git commit` would exit non-zero, and that step
  # would die under `set -euo pipefail` before pushing anything — and
  # once this reset works, `main` declaring the released version is the
  # NORMAL state. So the reset has to happen after the release, where a
  # no-op is allowed.
  #
  # Which is exactly why VERSIONED is read and not ignored: on this
  # branch a zero is ordinary (a re-run, or main already reset) and must
  # not commit, while the notes may have nothing to stamp either. Every
  # combination of the two has to be a valid outcome, including both
  # zero.
  # A warning, NOT a `continue`. This half must never be able to lose
  # the other: `stamp` has already run and may have rewritten headings,
  # so abandoning the iteration here throws away a successful stamp and
  # leaves main's headings reading [Unreleased] — the exact state this
  # whole step exists to prevent, and the one the summary below calls
  # the expensive half. Measured, with a version field missing from
  # main's app manifest: the stamp succeeded, this returned non-zero,
  # and the step pushed nothing at all. The retry was pure waste too,
  # since a malformed manifest fails identically on the second pass.
  #
  # VERSIONED=0 is safe rather than merely convenient: the staging
  # below is conditional on it, so a partial write is left unstaged and
  # the `checkout -f -B` at the top of the next iteration discards it.
  #
  # Unquoted $PKGS, as the `git add` below is: the word splitting is
  # the point.
  # shellcheck disable=SC2086
  if ! VERSIONED="$(node scripts/set-version.mjs "$VERSION" $PKGS 2>"$STAMP_LOG")"; then
    sed 's/^/[set-version] /' "$STAMP_LOG"
    echo "::warning title=Release notes::could not reset main's declared version (logged above); continuing with the release notes only. main will keep declaring its previous version, which prepare's max(npm, declared) floor tolerates."
    VERSIONED=0
    # A SEPARATE flag, because VERSIONED=0 has two meanings and only
    # one of them is "main already declared this version". The summary
    # lines below stated that reason as fact, so a run where the reset
    # FAILED emitted the warning above AND a summary saying main already
    # declared it — the summary contradicting a warning from the same
    # run, which is exactly what the comment below this block warns
    # about for the notes half. The summary is the artifact CONTEXT.md
    # and the release skill both send a releaser to read.
    VERSION_FAILED=1
  fi

  if [ "$STAMPED" = "0" ] && [ "$VERSIONED" = "0" ]; then
    # Nothing to stamp and nothing to reset. Two different states for
    # the notes half, and conflating them is how a summary line ends up
    # contradicting a warning the same run already wrote. A non-empty
    # titles file means narrative DID reach the release page and none of
    # it could be stamped here; empty means there was none to begin with
    # — or that `extract` failed, since that path deliberately truncates
    # the file.
    if [ -s "$TITLES_FILE" ]; then
      OUTCOME="unstamped"
    else
      OUTCOME="nothing"
    fi
    break
  fi

  # Stage only what changed, so the commit's contents match what the
  # summary below claims. Staging an unchanged file is harmless, but
  # staging RELEASE_NOTES.md when nothing was stamped would make a
  # version-only commit read as a notes commit in `git show --stat`.
  if [ "$STAMPED" != "0" ]; then
    git add RELEASE_NOTES.md || { FAILURE="could not stage RELEASE_NOTES.md"; continue; }
  fi
  if [ "$VERSIONED" != "0" ]; then
    # shellcheck disable=SC2086
    git add $PKGS || { FAILURE="could not stage the version files"; continue; }
  fi

  # One stable title whatever the branch carries, with the detail in
  # the body. The publisher-release skill's optional PR search matches
  # on it; its primary check is `git ls-remote` for the branch name,
  # which is title-independent. At least one of the two counts is
  # non-zero by the break above, so `git commit` cannot be handed an
  # empty diff here — and if that ever stops holding it fails and is
  # reported as a failure, which is the honest outcome.
  #
  # Same ambiguity as the summary LEAD, and this one outlives the run:
  # a reader doing archaeology on `git log` months later has no job
  # summary and no annotation to cross-check the 0 against.
  VERSION_NOTE=""
  if [ "$VERSION_FAILED" = 1 ]; then
    VERSION_NOTE=" (the reset FAILED rather than finding nothing to do)"
  fi
  git commit -s \
    -m "chore(release): stamp ${VERSION} on main" \
    -m "${STAMPED} release-note section(s) stamped, ${VERSIONED} version file(s) reset.${VERSION_NOTE}" \
    || { FAILURE="could not commit the stamp"; continue; }

  # NOT forced. A stamp branch already on the remote may carry a
  # human's conflict resolution — the documented fix when main has
  # moved under RELEASE_NOTES.md — and force-pushing the freshly
  # recomputed stamp over it would discard that work with no warning.
  # So a rejected push is inspected rather than overridden: a branch
  # that already exists is left exactly as it is and reported.
  if git push origin "$BRANCH" 2>"$STAMP_LOG"; then
    OUTCOME="pushed"
    break
  fi
  sed 's/^/[git push] /' "$STAMP_LOG"
  if git ls-remote --exit-code --heads origin "$BRANCH" >/dev/null 2>&1; then
    OUTCOME="existing"
    break
  fi
  FAILURE="could not push ${BRANCH} (logged above)"
done

# Every outcome writes a summary line, including the no-op ones. An
# empty "Release notes" section would be indistinguishable from this
# step dying before it wrote anything, and the whole point of the
# annotations is that a skipped stamp is visible without opening logs.
COMPARE_URL="${COMPARE_BASE}/main...${BRANCH}?expand=1"
echo "## 📝 Release notes and version" >> "$GITHUB_STEP_SUMMARY"

# Any OTHER stamp branch still on the remote, which is now a much more
# likely and much more costly situation than it was.
#
# Before the version reset, a release with no narrative pushed no branch
# at all, so an outstanding stamp branch meant someone had skipped a
# narrative release. Now VERSIONED is 3 on essentially every release, so
# EVERY release pushes one — and two outstanding branches both cut from
# origin/main edit the same three version lines, which is an
# unavoidable 3-way conflict on whichever is merged second. Measured in
# both merge orders.
#
# That matters beyond tidiness: faced with "conflicted, and its version
# is stale anyway", the natural move is to CLOSE it — which silently
# discards the notes half, the half that re-appends this release's
# narrative to every subsequent release page. So name it here rather
# than leaving a releaser to work it out from a conflict marker.
STALE_BRANCHES=""
if ! STALE_BRANCHES="$(git ls-remote --heads origin 'refs/heads/release-notes-stamp-*' 2>/dev/null \
      | sed 's#.*refs/heads/##' | grep -vx "$BRANCH" || true)"; then
  STALE_BRANCHES=""
fi
if [ -n "$STALE_BRANCHES" ]; then
  echo "::warning title=Release notes::an earlier stamp branch is still unmerged ($(tr '\n' ' ' <<<"$STALE_BRANCHES")); merge the OLDEST first or the newer one conflicts in the three version files"
  {
    echo "⚠️ **An earlier stamp PR is still open.** These branches are still on the remote:"
    echo
    while IFS= read -r stale; do echo "- \`${stale}\`"; done <<<"$STALE_BRANCHES"
    echo
    echo "**Merge them oldest-first, before this release's.** Each stamp branch is cut from \`origin/main\` and rewrites the same three \`package.json\` version lines, so whichever is merged second will conflict there. Resolve in favour of the **newer** version and keep both sets of \`RELEASE_NOTES.md\` edits."
    echo
    echo "Do not close a conflicted stamp PR to clear it. The version half is recoverable — \`prepare\` derives its floor from \`max(npm latest, main declared)\` either way — but the notes half is not: those headings still read \`[Unreleased]\`, so every later release re-appends that release's narrative to its own page."
  } >> "$GITHUB_STEP_SUMMARY"
  echo >> "$GITHUB_STEP_SUMMARY"
fi

if [ -n "$FAILURE" ]; then
  echo "::warning title=Release notes::${FAILURE}, so RELEASE_NOTES.md on main is NOT stamped and main's declared version is NOT reset. The heading still reads [Unreleased], which means the NEXT release re-appends this release's narrative to its own page, and so does every release after it. Recovery is in the publisher-release skill, step 3 — do NOT run an unscoped stamp, it rewrites every [Unreleased] section including ones this release did not ship."
  {
    echo "⚠️ **Not stamped on \`main\`** — ${FAILURE}."
    echo
    echo "\`RELEASE_NOTES.md\` still reads \`[Unreleased]\`, so the next release will re-append this release's narrative to its page. \`main\`'s \`packages/{sdk,app,server}/package.json\` were not reset either, so they still declare whatever they did before this release."
    echo
    echo "The version half is the cheaper of the two to leave: nothing reads it at runtime, and \`prepare\` derives its floor from \`max(npm latest, main declared)\`, so it is correct whether or not this landed. The notes half is not — see below."
    echo
    echo "Check whether the branch reached the remote anyway — if \`release-notes-stamp-${VERSION}\` is there, the stamp is computed correctly and only needs a PR: [open it](${COMPARE_URL}). If it is not, follow step 3 of the \`publisher-release\` skill. **Do not run \`release-notes.mjs stamp\` without \`--titles\`**: unscoped it stamps every \`[Unreleased]\` section, including ones this release did not ship."
  } >> "$GITHUB_STEP_SUMMARY"
  exit 0
fi

case "$OUTCOME" in
  pushed|existing)
    # The annotation and the summary line are emitted separately on
    # purpose: redirecting the whole if/else to $GITHUB_STEP_SUMMARY
    # captures the `::notice` too, so GitHub never sees the annotation
    # and the summary carries a raw `::notice` line instead.
    if [ "$OUTCOME" = existing ]; then
      echo "::notice title=Release notes::${BRANCH} was already on the remote and was left untouched; open ${COMPARE_URL}"
      LEAD="Branch \`${BRANCH}\` was **already on the remote** and has been left exactly as it was, in case it carries a fix someone pushed. Check it, then open the PR."
    else
      # The annotation carries the same count and so the same
      # ambiguity; VERSION_NOTE is the one built for the commit body.
      echo "::notice title=Release notes::pushed ${BRANCH} stamping ${STAMPED} section(s) and resetting ${VERSIONED} version file(s) as ${VERSION}${VERSION_NOTE}; open ${COMPARE_URL}"
      LEAD="Pushed \`${BRANCH}\`, stamping ${STAMPED} release-note section(s) and resetting ${VERSIONED} version file(s) to \`${VERSION}\`."
      # "resetting 0 version file(s)" carries the same two meanings the
      # rarer branches below were fixed for — nothing to reset, or the
      # reset failed — and THIS is the branch that fires whenever there
      # is narrative to stamp, so it is the one most people read. It
      # reports an ambiguous count rather than asserting a false reason,
      # which is milder, but the ::warning is easy to scroll past.
      if [ "$VERSION_FAILED" = 1 ]; then
        LEAD="${LEAD} The 0 there is because the version reset FAILED (see the warning above), not because there was nothing to reset — \`main\` still declares whatever it did before this release."
      fi
    fi
    {
      echo "$LEAD"
      echo
      echo "👉 **[Open the pull request](${COMPARE_URL}) and merge it before the next release.**"
      echo
      echo "This last step is a human's on purpose. A PR opened by a person triggers \`pull_request\`, so its checks run and any maintainer can merge it; one opened by this workflow would never trigger them, leaving required checks at \`expected\` and an admin as the only person who could merge. Left unopened, the headings on \`main\` still read \`[Unreleased]\` — which is exactly what the next release's \`extract\` matches, so ${VERSION}'s narrative lands on the next release's page too, and on every one after that. \`main\` also keeps declaring the pre-release version, which is the state this reset exists to end."
      # The `unstamped` case below cannot be reached when there is also
      # a version to reset: a non-zero VERSIONED carries the commit, so
      # the outcome is `pushed` and that whole block is skipped. Say it
      # here instead, or a branch that stamped NO narrative reads as an
      # unqualified success and the ::warning above it looks incidental.
      if [ "$STAMPED" = "0" ] && [ -s "$TITLES_FILE" ]; then
        echo
        echo "⚠️ **Note: this branch resets the version but stamps no narrative.** This release's page carries hand-written sections, yet none of the headings it recorded are still \`[Unreleased]\` on \`main\` — they were edited or removed while the release ran. See the warning above for which. Merging this PR does not fix that; check those sections on \`main\` say what they should and carry the version that shipped them."
      fi
    } >> "$GITHUB_STEP_SUMMARY"
    ;;
  unstamped)
    # Narrative reached the release page, and not one of its headings
    # is still `[Unreleased]` on main — they were edited or removed
    # during the release window. The `missing` warning above named
    # them. Nothing to undo, but this is not a no-op and must not read
    # like one.
    echo "::warning title=Release notes::this release's narrative is on its page but none of it could be stamped on main; the headings it recorded are no longer there"
    {
      echo "⚠️ **Narrative shipped, nothing stamped.** This release's page carries hand-written sections, but none of the headings it recorded are still \`[Unreleased]\` on \`main\` — they were edited or removed while the release ran. See the warning above for which."
      echo
      if [ "$VERSION_FAILED" = 1 ]; then
        echo "No branch was pushed. The version reset also FAILED (see the warning above), so \`main\` still declares whatever it did before this release — \`prepare\`'s \`max(npm, declared)\` floor tolerates that, so it is the cheaper half to leave. Check that those sections on \`main\` say what they should, and carry the version that actually shipped them."
      else
      echo "No branch was pushed, and \`main\` already declared \`${VERSION}\` so there was no version to reset either. Check that those sections on \`main\` say what they should, and carry the version that actually shipped them."
      fi
    } >> "$GITHUB_STEP_SUMMARY"
    ;;
  nothing)
    # The annotation asserts the same reason the summary does, so it
    # is conditional for the same reason.
    if [ "$VERSION_FAILED" = 1 ]; then
      echo "::notice title=Release notes::nothing to stamp on main; the version reset failed (see the warning above)"
    else
      echo "::notice title=Release notes::nothing to stamp and no version to reset on main"
    fi
    {
      if [ "$VERSION_FAILED" = 1 ]; then
        echo "Nothing to stamp — no \`[Unreleased]\` headings were recorded as shipping in this release, so its page carries the generated PR list only. The version reset FAILED (see the warning above), so \`main\` still declares whatever it did before this release, and no branch was pushed."
      else
      echo "Nothing to stamp — no \`[Unreleased]\` headings were recorded as shipping in this release, so its page carries the generated PR list only. \`main\` already declared \`${VERSION}\`, so there was no version to reset either, and no branch was pushed."
      fi
      echo
      echo "If the step above warned that it could not read \`RELEASE_NOTES.md\`, that is the reason for the first half, and the narrative did not reach the release page either — this line does not rule that out."
    } >> "$GITHUB_STEP_SUMMARY"
    ;;
esac
