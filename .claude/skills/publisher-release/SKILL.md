---
name: publisher-release
description: Cut a Malloy Publisher release, and write the RELEASE_NOTES.md entries a release ships. Use when asked to release, cut a release, ship a version, or publish Publisher to npm/Docker — and when a change needs a release note, or you are deciding whether it does and what version to stamp on it.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Releasing Publisher

A release is one `workflow_dispatch` of `Release (NPM + Docker)`
(`.github/workflows/release.yml`), followed by one pull request: the
`release/v<version>` branch the release published from, merged back into
`main`. That branch already carries the version bump and the stamped release
notes, so **you never create a branch or stamp anything by hand**. You open the
PR from the branch the workflow pushed.

Read `.github/workflows/CONTEXT.md` (path from the repo root) before acting. It carries the publishing rules that are not guessable from the
YAML, and it is the authority when this file and it disagree.

## Release notes: when to write one, and what number goes on it

`RELEASE_NOTES.md` is not written at release time. **The PR that changes the
behaviour writes the note**, in the same PR, as a `## [Unreleased]` section —
which is the only way it gets written by someone who knows what changed. The
release then carries it to users on its own: `prepare` stamps every
`[Unreleased]` heading with the release's version on the release branch, and
`gh-release` puts those sections on the release page. Writing the section is
the whole job; merging the release branch back is the one thing the release
cannot do for itself.

### Does this change need one?

`gh-release` already attaches an auto-generated "What's Changed" list of merged
PRs. **That list is sufficient for a routine patch**, so a note is not a per-PR
chore. Write one when the PR list would leave a reader unable to act — when a
user or operator has to *do* something, or would otherwise draw a wrong
conclusion from an unchanged-looking system:

- A **breaking change**, or anything needing a migration step. Mark the heading
  `(BREAKING)`.
- A **new or removed API field, endpoint, or response shape** — anything that
  breaks a strict generated client, or that consumers should move onto.
- A **deprecation**: what still works, for how long, and what to move to.
- **Changed meaning of an existing signal** — a metric label, a counter, an
  error code. A value that keeps its name and changes its basis is the case
  most worth writing, because nothing else will surface it.
- **A silent failure that is now visible**, or a bug whose symptom users have
  been living with. Say who was affected and how to tell.
- A **new capability with a cost or a limit** worth knowing before adopting it.

Skip it for refactors, tests, docs, CI, and internal changes with no observable
effect. When unsure, ask whether a reader upgrading blind could be surprised. If
not, the PR list covers it.

Write it for the person upgrading: what changed, what breaks, what to do. The
existing sections are the house style — prose, not bullets-only, and specific
about the failure mode.

### What number goes on it

Three states, in order:

1. **`## [Unreleased] — <what changed>`** while the change sits on `main`
   unreleased. This is what an authoring PR writes. Always.
2. **Amend that same section in place** if a follow-up PR lands before the
   release and changes the same behaviour. Do not add a second section — the two
   ship together as one story, and the reader has never seen the first version.
   #1024 (pre-aggregation off by default) and #1030 (on by default) are one
   section for exactly this reason.
3. **`## [<version>] — <what changed>`** once a release has shipped it. **CI
   writes this** — `prepare` stamps it on `release/v<version>`, and it
   reaches `main` when that branch's PR merges (step 6). You do not stamp by
   hand, and you do not guess the number in advance.

Once a section is stamped, it is history and does not get rewritten. A follow-up
that changes that behaviour opens a **new** `[Unreleased]` section referencing
the shipped version by number, the way the build-failures section names 0.0.245
and 0.0.246 when describing what 0.0.247 changed.

Never write a version number into a note yourself. Release numbers are assigned
at dispatch time and a release can fail. A failed release's stamp is harmless
only because its branch is never merged; the next dispatch cuts a new branch
from `main`, which still reads `[Unreleased]`.

## The versioning policy

**`0.MINOR.PATCH` for every published package, while pre-1.0.** It is the policy,
not yet the state: `skills` follows it, `sdk`/`app`/`server` are still on `0.0.x`
until `0.2.0` is cut, `create-malloy-package` versions on its own `0.0.x` line, computed at release time, and the Python client
declares `0.1.0` — sharing `0.1.x` with `skills`. Nothing enforces the *shape* of
the number for any of them — skills and create-malloy-package resolve their next
version from npm `latest` at release time, and moving either onto a new minor
line is a judgement call made with an explicit `-f version=`, not something CI
demands.

- **MINOR = a breaking change. PATCH = everything else.** A breaking change is
  one that turns a package which loaded or built yesterday into one that does
  not: a refused annotation, a refused `#@ persist` combination, a load-time
  failure on syntax that previously passed. It also covers a removed endpoint or
  response field, like 0.0.242's `/pages` → `/data-apps` rename.
- **This is what makes a range usable.** `^0.0.250` resolves to
  `>=0.0.250 <0.0.251` — exactly one version — and `~0.0.250` to
  `>=0.0.250 <0.1.0`, a range whose upper half does not exist. So on `0.0.x` no
  consumer can say "patches yes, breaking no". `^0.2.0` resolves to
  `>=0.2.0 <0.3.0`, which they can. (On any `0.x` line `^` and `~` are
  identical — both stop at the next minor.)
- **No `1.x`.** Publisher is preview, so a major is a deliberate decision made
  with an explicit `-f version=`; there is no `major` option on the dispatch. If
  it ever happens, note `@malloy-publisher/sdk@1.0.1` **is** published (an old
  off-line publish `latest` moved off long ago) while `1.0.0` is free, and `app`
  and `server` have no `1.x` at all — so a 1.x line has to start at `1.0.0` and
  skip `1.0.1`, or start at `1.0.2`.
- **The `sdk`/`app`/`server` train has NOT moved yet.** Its `latest` is still
  `0.0.250` and the policy takes effect at its first minor release, intended to
  be **`0.2.0`** — not `0.1.0`, because `skills` already occupies `0.1.x` and two
  trains sharing a minor invites reading one for the other. Until that release is
  cut, a default dispatch derives `0.0.251`, `0.0.252`, … and that is correct.
  **Do not "correct" it with an explicit version.** `skills` is already on
  `0.1.x` and already follows the policy.

Read a release note's own claims rather than its heading when deciding. A section
can be titled `(BREAKING)` while its bullets say *"this cannot affect an existing
package"*, and the reverse happens too. Propose the level and get the
maintainer's call rather than inferring it from the diff alone — a narrow break
may still be judged patch-worthy, and that judgement is theirs.

### How to bump

`prepare` derives the version from **npm's `latest`**, so a routine release needs
no version input at all:

```bash
# a patch — the default
gh workflow run release.yml --repo malloydata/publisher --ref main

# a breaking release
gh workflow run release.yml --repo malloydata/publisher --ref main -f bump=minor
```

The floor is `max(npm latest, main declared)` by version sort, and it **fails
closed**: if the registry will not answer, `prepare` refuses rather than falling
back to the file. An explicit `-f version=` still wins outright over `bump`, and
is what you want for a major, for a prerelease, or to name the number exactly.

Two things worth knowing about the derived path:

- The chosen level is applied **once**. If that number's release branch or tag
  already exists, the walk past it is by *patch* — so `bump=minor` colliding with
  an existing `0.3.0` gives `0.3.1`, not `0.4.0`.
- `bump=minor` is computed off npm's current `latest`, which is not always the
  number the policy wants. Off a `0.0.x` floor a minor bump lands on `0.1.0` —
  the line `skills` occupies. That is why the pending move to `0.2.0` needs an
  explicit version rather than `bump=minor`.

An earlier version of this section said *"`prepare`'s default version is not
trustworthy — always pass `-f version=`"*. That was true and is no longer:
#1037 changed the floor to `max(npm latest, main declared)` with fail-closed
behaviour, so the hole it warned about — a default run walking up from `main`'s
stale number, finding a gap below `latest`, and moving the dist-tag **backwards**
with nothing failing — is closed. Passing an explicit version is now a choice,
not a precaution.

## The version trains

| Packages | Version | Decided by | Missing bump caught by |
| --- | --- | --- | --- |
| `sdk`, `app`, `server` | lockstep | `release.yml` itself, on a `release/v<v>` branch | n/a — the release sets it |
| `skills` | its own line, `package.json` carries a fixed `0.0.0-dev` placeholder | release time, from npm's own state (`scripts/independent-version.mjs`); you, for a minor | n/a — nothing is committed ahead of time to forget |
| `create-malloy-package` | its own line, same placeholder | release time, the same way | n/a, same reason |
| `malloy-publisher-sdk` (Python) | its own line | you, by hand, on `main` | `python-sdk.yml` PR check — but see below |

**Skills and create-malloy-package no longer have a version to forget.** Nothing
is bumped ahead of a release: both packages' `package.json` declare `0.0.0-dev`
in the repo, and what actually publishes is decided at release time by
`scripts/independent-version.mjs`, called from `scripts/publish-packages.sh`, and
written into the manifest at publish time. So the pre-release hand-audit that
used to live in step 2 below (comparing `main`'s declared version against npm)
no longer applies to either package — there is nothing on `main` to compare.

The two are decided differently:

- **skills** publishes when its published content changed since npm `latest`'s
  `gitHead` — a diff of `skills/`, `packages/skills/`, `bun.lock` and the root
  `package.json`, excluding `skills/README.md` and
  `packages/skills/src/*.spec.ts`. Changed publishes one patch above the
  highest published plain version; unchanged skips.
- **create-malloy-package** publishes on every non-prerelease release,
  unconditionally, because it bakes the server's npm `latest` into every
  workspace it scaffolds — a release changes what it ships even when its own
  directory did not. The only skip is a re-run of the same release, detected by
  its published `publisherServer` field already matching.

The publish version is one patch above the HIGHEST published plain version
(`npm view <pkg> versions --json`), not `latest` plus one — a `latest` dist-tag
rolled back by hand after a bad release must not make this recompute a version
that's already published. `latest`'s `gitHead` stays the content-diff baseline
above; only the version arithmetic reads the full versions list.

A hand dispatch of either child workflow with no `version` input publishes one
patch above the highest published version, the same guard the old PR check
used to enforce, now run once at dispatch time. **A minor or major bump is a
hand dispatch with `-f version=`.** After a hand-dispatched skills minor,
hand-dispatch the scaffolder too — it depends on skills' version — or wait for
the next release, which republishes the scaffolder anyway.

One caveat: the **Python** check has nothing to *catch* until the first publish
lands, because `malloy-publisher-sdk` is not on PyPI at all and a project-level
404 is its pass. (It can still go red on an unreadable `pyproject.toml`, a
version that is not `major.minor.patch`, or a registry that answers neither 200
nor 404.) It starts enforcing for real the moment a version is up there, so treat
the Python version as unenforced only until then.

**Its PyPI publish is paused, though.** The `publish_pkg python-client` line at
the end of `scripts/publish-packages.sh` is commented out, so no release
dispatches `python-sdk.yml` today. Before restoring that line, read *The first
PyPI publish* in `.github/workflows/CONTEXT.md`: `PYPI_TOKEN` has to be
account-scoped for a first upload (a project-scoped token cannot exist for a
project that does not), the name has to still be free, and `0.1.0` is what
ships — PyPI filenames can never be reused, so move the version before that
run if it is going to move at all. A failure there is cheap: it is dispatched
last, nothing depends on it, and re-running the job skips whatever already
published.

`main`'s `packages/sdk/package.json` used to lag npm permanently. It no longer
does: merging the release PR (step 6) brings those three files to the version
that shipped, and CI's `Release sync` check fails while a shipped
release's PR is unmerged. `prepare` still takes the max of npm and the file for
its floor rather than trusting either. Ask npm when you want to know what is
published.

## Order, and why each step is where it is

### 1. Establish where things actually stand

```bash
git fetch --tags origin
npm view @malloy-publisher/server version              # the real current version
git log --oneline "v$(npm view @malloy-publisher/server version)..origin/main"
```

That commit list is what this release ships. If it is empty, there is nothing to
release.

### 2. Skills and create-malloy-package need no pre-flight bump check

Nothing is committed ahead of time for either package, so there is nothing to
confirm here: `scripts/independent-version.mjs` reads npm's own state at release
time and decides publish/skip/version from it, not from what `main` declares.
Skip straight to *Sanity-check the notes* below.

If you want to see what the release will decide before dispatching, ask npm the
same questions the release does:

```bash
npm view @malloy-publisher/skills dist-tags.latest
npm view "@malloy-publisher/skills@$(npm view @malloy-publisher/skills dist-tags.latest)" gitHead
git diff --quiet "<that gitHead>" origin/main -- skills/ packages/skills/ bun.lock package.json \
  ':!skills/README.md' ':!packages/skills/src/*.spec.ts' && echo unchanged || echo changed

npm view @malloy-publisher/create-malloy-package dist-tags.latest
npm view "@malloy-publisher/create-malloy-package@$(npm view @malloy-publisher/create-malloy-package dist-tags.latest)" publisherServer
```

`changed` (or any diff not `unchanged`) means skills will publish this release;
`unchanged` means it skips. create-malloy-package publishes regardless, unless
its `publisherServer` already equals the version this release is about to ship —
that only happens on a re-run.

Also check the Python client. Its PyPI publish is paused (work in progress): the
release does not dispatch python-sdk.yml until the `publish_pkg python-client`
line at the end of scripts/publish-packages.sh is restored, so this reading does
not change between releases for now. Its version is in `pyproject.toml`, not a
`package.json`, and PyPI answers 404 for the whole project until the first upload
lands — so "PyPI: 404" here is the expected reading today and NOT a reason to
skip the comparison next time.

```bash
printf 'python-client: PyPI %s, main %s\n' \
  "$(curl -sS --max-time 20 https://pypi.org/pypi/malloy-publisher-sdk/json \
     | python3 -c 'import json,sys; print(json.load(sys.stdin)["info"]["version"])' \
     2>/dev/null || echo 404)" \
  "$(python3 -c 'import tomllib; print(tomllib.load(open("packages/python-client/pyproject.toml","rb"))["project"]["version"])')"
```

#### The scaffolder's server pin is derived now — do not set it by hand

`create-malloy-package` pins the server its generated workspaces run
(`SERVER_VERSION` in `packages/create-malloy-package/src/scaffold.ts`). **That pin
is substituted at publish time** from npm's current `@malloy-publisher/server`
`latest`, in the runner's working tree only, so the value committed to the repo is
a dev default and there is nothing to bump before a release. Its publish job then
reads the line back, confirms it matches the registry, and confirms that server
still documents `--host`.

This is what 0.0.250 was about, and it is worth knowing why the old instruction
existed: the pin was hand-maintained and the publish job refused to ship unless it
equalled npm's `latest`, so a release that forgot it went red 25 minutes in, after
`skills` had already published. Setting it in the pre-release PR is now wrong
rather than merely unnecessary — the substitution overwrites it, and a committed
value that happens to differ is not a problem to fix.

The derivation is only correct because `publish-packages` waits for `publish-npm`.
It did not until 0.0.250, and on `needs: prepare` alone this job races the server
publish, so the substituted value would be a release behind and every generated
workspace would pin the previous server. **If that `needs:` is ever narrowed back
to `prepare` alone, this breaks silently rather than loudly.**

The `needs:` is not enough on its own, because npm can take minutes to show a
version after it publishes. In 0.7.0 `latest` read 0.7.0 seven minutes after
`publish-npm` finished. So `publish-packages.sh` also waits, up to 15 minutes, for
the server's `latest` to read the new version before dispatching the scaffolder.
It then passes that version to the scaffolder as its `server_version` input,
because another runner can still read the old `latest` for a few minutes; the
scaffolder waits until its own `latest` matches and refuses to pin anything else.
If the wait gives up, neither the scaffolder nor python-client was dispatched.
Once `latest` reads the new version, re-run the `publish-packages` job (Re-run
failed jobs), not the whole release.

### 3. Check the last release merged back, and read what ships

`prepare` stamps `RELEASE_NOTES.md` itself and `gh-release` reads it back, so
there is nothing to paste. Whether the last release merged back is CI's job:
`release-sync.yml` compares npm's `latest` with `packages/sdk/package.json`
on every PR, every push to `main`, and after every release run. Read its
result rather than repeating the comparison:

```bash
gh api repos/malloydata/publisher/commits/main/check-runs \
  --jq '.check_runs[] | select(.name == "Release sync") | "\(.conclusion) \(.html_url)"'
# the narrative this release will stamp and ship
git fetch origin
NOTES="$(mktemp)" && git show origin/main:RELEASE_NOTES.md > "$NOTES"
RELEASE_NOTES_FILE="$NOTES" node scripts/release-notes.mjs extract | grep '^## '
```

**If the check failed, do not dispatch.** The previous release's PR has not
merged, so `main` still reads `[Unreleased]` for sections that release already
shipped, and this release would stamp and publish them again. The check's error
names the version; finish step 6 for it, which is also the only thing that turns
the check green. `release.yml` does not run this check itself, so nothing stops
a dispatch but you.

What you are checking in the extract is that the sections listed are the ones this release
actually ships. A section describes work merged to `main`, so anything sitting
there goes out with this release whether or not it was written for it. Nothing
listed is fine and common — the generated PR list carries a routine patch.

If a section is present that should **not** ship yet, the work behind it is
already on `main` and the note is telling the truth; the fix is a release, not
an edit.

### 4. Dispatch

`prepare` derives the version from npm's `latest` and fails closed if the registry
will not answer, so a routine patch needs no input. See *How to bump* above for
the policy behind the choice.

```bash
# a patch
gh workflow run release.yml --repo malloydata/publisher --ref main

# a breaking release
gh workflow run release.yml --repo malloydata/publisher --ref main -f bump=minor

# an exact number: a major, a prerelease, or a line change like 0.0.x -> 0.2.0
gh workflow run release.yml --repo malloydata/publisher --ref main -f version=<next>
```

**Do not merge to `main` while it runs.** `publish-packages` aborts if `main`
moves under a watched path mid-release. A `RELEASE_NOTES.md`-only merge is not
watched, but the window is short — just wait. **Do not push to
`release/v<version>` while it runs either**: `npm-sdk.yml` and
`docker-image.yml` check it out by name, so a push mid-run can publish two
different commits under one version.

### 5. Verify what actually shipped

A green tick is not evidence. Read the run's **job summary**, which names each
independently-versioned package as published or skipped and why.

```bash
npm view @malloy-publisher/server version
npm view @malloy-publisher/skills version
gh release view "v<version>" --repo malloydata/publisher
```

### 6. Open the release PR, and get it merged

This is the one step the release cannot finish itself, and as the agent running
this skill **you open the PR**. The branch already exists and already holds
everything: `prepare` committed the three `packages/{sdk,app,server}/package.json`
versions and the stamped `RELEASE_NOTES.md` headings to `release/v<version>`
as one commit. Do not create another branch and do not stamp anything by hand.

Open it once the run has finished, for any release whose sdk reached npm
(`npm view @malloy-publisher/sdk version` reads it). That includes one whose
`gh-release` then failed: it shipped, and `release-sync.yml` stays red until
this PR merges. A release that never reached npm has nothing to merge back:
leave its branch alone (the next dispatch skips past it) and dispatch again.
A prerelease or a `+build` version never gets a PR.

**First confirm the branch is there and holds only the release commit**, against
`origin/main`:

```bash
V=<version>
git fetch origin
# 1. prepare pushed it
git ls-remote --exit-code --heads origin "release/v$V"
# 2. one commit ahead of main, titled chore(release): $V
git log --oneline "origin/main..origin/release/v$V"
# 3. only the three manifests and RELEASE_NOTES.md
git diff --stat "origin/main...origin/release/v$V"
# 4. the [$V] sections it stamped, which should match the release page
NOTES="$(mktemp)" && git show "origin/release/v$V:RELEASE_NOTES.md" > "$NOTES"
RELEASE_NOTES_FILE="$NOTES" node scripts/release-notes.mjs extract "$V" | grep '^## '
# 5. no PR for it yet
gh pr list --repo malloydata/publisher --head "release/v$V" --state all
```

Read them in order, and stop at the first surprise:

1. **No branch:** `prepare` failed before pushing, so nothing published. Read
   the run; there is nothing to merge back.
2. **More than one commit:** someone pushed to the release branch. Read those
   commits before opening anything.
3. **Any other file:** stop and show the user. The release branch should carry
   nothing else.
4. **No sections:** normal for a routine patch. Otherwise compare with
   `gh release view "v$V" --repo malloydata/publisher`.
5. **A PR exists:** use it rather than opening a second. If it was closed
   unmerged, reopen it.

Then open it against `main`:

```bash
gh pr create --repo malloydata/publisher --base main --head "release/v$V" \
  --title "chore(release): $V" \
  --body "Merges the v$V release branch back into main: sets sdk, app and server to $V and stamps the RELEASE_NOTES.md sections v$V shipped.

Release: https://github.com/malloydata/publisher/releases/tag/v$V"
```

The title matters: the repo squash-merges, so it is what `main`'s history keeps.
The workflow does not open this PR itself because a PR opened with
`GITHUB_TOKEN` triggers no workflows: `main`'s required checks would never run
and only an admin could merge it. Opened on the user's credentials, the checks
run and any maintainer can merge.

**`main` requires the branch to be up to date, and `RELEASE_NOTES.md` usually
conflicts.** Any PR merged since the release was cut makes the branch stale. A new
`[Unreleased]` section lands directly above the first stamped heading, which git
treats as one conflicting hunk. Merge `main` in locally, with a sign-off because
the DCO check is required:

```bash
git fetch origin
git switch -c "release/v$V" "origin/release/v$V"
git merge --signoff origin/main
# Only if the merge stopped on a RELEASE_NOTES.md conflict: keep BOTH sides,
# main's new [Unreleased] sections exactly as they are and this branch's [$V]
# headings, then conclude it. The package.json version lines never conflict;
# only release PRs change them.
#   git add RELEASE_NOTES.md && git commit -s --no-edit
node scripts/release-notes.mjs extract "$V" | grep '^## '   # exactly v$V's sections
node scripts/release-notes.mjs extract | grep '^## '        # only what merged since
git push origin "release/v$V"
```

The two `extract` lines are the check that the resolution is right: the first
must list the same sections as the release page, the second only sections
merged after the release was cut. Pushing to the release branch is safe once the
run has finished, since the tag pins the published commit.

Ask the user to merge the PR, or merge it if they asked you to. Merging deletes
`release/v$V` (the repo deletes head branches on merge); the tag keeps the
commit. Then confirm:

```bash
git fetch origin main
git show origin/main:packages/sdk/package.json | grep '"version"'   # $V
gh release view "v$V" --repo malloydata/publisher --json body -q .body | head -40
```

If the release page is missing narrative that `[$V]` sections on the branch
carry, `gh-release` logged a `could not read RELEASE_NOTES.md` warning. Add the
output of `extract "$V"` to the page by hand with `gh release edit`.

Left unmerged, `release-sync.yml` fails on `main` and on every PR, naming
this version; merging this PR is the only fix. Confirm it went green on `main`
after the merge (the first command in step 3).

## If it fails

- **Only `publish-packages` is red** → the sdk/app/server release completed and
  the tag exists; that job sits outside `gh-release`'s `needs`. Use *Re-run
  failed jobs*, which re-enters that job alone and skips whatever already landed.
  **Do not re-run the release** — it walks the version forward and burns three
  npm versions.

  Dispatching the children by hand also works, but it is *not* the same thing and
  the skip does not come with it. Only `publish-packages` asks the registry and
  skips; each child carries a **Verify this version is not already published**
  step that `exit 1`s on a version npm already holds. So dispatching a child that
  already published fails the run rather than no-opping. Ask npm first and
  dispatch only the package still missing, keeping skills before the scaffolder:

  ```bash
  npm view @malloy-publisher/skills version
  npm view @malloy-publisher/create-malloy-package version
  ```
- **"main moved during this release"** → expected and retryable, same recovery.
  It can fire on the second package after the first already published, so read
  the job summary rather than assuming nothing shipped.
- **Anything after `npm-sdk.yml` published** → not resumable at the same version.
  npm versions are immutable; move forward.

## Prereleases

Any hyphen in the version skips `gh-release` *and* both independently-versioned
packages, because their own versions carry no hyphen and would take over the
`latest` tag. Ship those from an ordinary release.
