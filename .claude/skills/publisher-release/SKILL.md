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
(`.github/workflows/release.yml`). Everything else in this skill exists because
work has to land on `main` **before** that dispatch, or be written **after** it
completes — and the step that is always skipped is the last one.

Read `.github/workflows/CONTEXT.md` (path from the repo root) before acting. It carries the publishing rules that are not guessable from the
YAML, and it is the authority when this file and it disagree.

## Release notes: when to write one, and what number goes on it

`RELEASE_NOTES.md` is not written at release time. **The PR that changes the
behaviour writes the note**, in the same PR, as a `## [Unreleased]` section —
which is the only way it gets written by someone who knows what changed. The
release then carries it to users on its own: `gh-release` appends every
`[Unreleased]` section to the release page, then pushes a branch stamping those
headings with the version that shipped them and prints a link to open it as a PR.
Writing the section is the whole job; opening and merging that PR is the one
thing the release cannot do for itself.

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
   writes this** — `gh-release` pushes a `release-notes-stamp-<version>` branch
   after the release is cut, and a human opens and merges it. You do not stamp by
   hand, and you do not guess the number in advance.

Once a section is stamped, it is history and does not get rewritten. A follow-up
that changes that behaviour opens a **new** `[Unreleased]` section referencing
the shipped version by number, the way the build-failures section names 0.0.245
and 0.0.246 when describing what 0.0.247 changed.

Never write a version number into a note yourself *in advance*. Release numbers
are assigned at dispatch time and a release can fail, so a section stamped early
can name a version that does not exist — which is exactly why the stamp runs
after `gh release create` succeeds and not before. Stamping a version that has
**already** shipped is a different act and sometimes necessary, because the
stamp branch needs a human to open and merge, and can be missed; step 3 covers
it, including why the obvious command for it is destructive.

## Which number to bump

**A breaking change should take a minor bump** — a change that turns a package
which loaded or built yesterday into one that does not: a refused annotation, a
refused `#@ persist` combination, a load-time failure on syntax that previously
passed. Everything else is a patch.

That is the direction, not yet the record. **Every release on this line has been
a patch**, including 0.0.242's `/pages` → `/data-apps` rename, which was
breaking. So do not infer the bump from the diff on your own: propose it and get
the maintainer's call. A narrow break may still be judged patch-worthy, and that
judgement is theirs.

Read the release note's own claims rather than its heading, either way. A
section can be titled `(BREAKING)` while its individual bullets say *"this
cannot affect an existing package"*, and the reverse happens too.

### `prepare`'s default version is not trustworthy — always pass `-f version=`

`prepare`'s default patch-bumps **`main`'s `packages/sdk/package.json`**, which
lags npm permanently because release branches are never merged back, then walks
forward until it finds a version whose release branch and tag are both free.
Three things are wrong with that, in rising order of consequence:

- **It grows.** `main` never advances, so the walk restarts from the same old
  number every release and costs one `ls-remote` per step, forever.
- **It stops at the first *free* number, not above the *highest*.** It is correct
  today only because the tag sequence happens to be gapless, so "first free" and
  "above the highest" are the same number. Nothing enforces that.
- **It never asks npm**, which is the only authority on what is actually
  published — so a free branch-and-tag pair is all it takes, whatever the
  registry holds.

The two combine badly, and **passing an explicit version is what opens the
hole.** Dispatch `0.0.250` while the walk would have reached `0.0.248`, and
`0.0.248`/`0.0.249` become permanent gaps below the high-water mark. The next
run dispatched *without* a version walks up, finds `0.0.248` free of branch and
tag, publishes it, and `npm publish` moves the `latest` dist-tag **backwards** to
a version older than the one before it. Nothing fails; the release goes green.

Deleting a stale release branch whose tag was never cut opens a gap the same
way. That one is likelier to fail loudly — if npm already holds the version, the
publish 403s on immutability — but it fails loudly only by luck, and only if the
version got as far as npm.

The durable fix is to derive the floor from `npm view @malloy-publisher/sdk
version` rather than from `main`. Until that lands, pass the version explicitly
on every dispatch.

**If the line ever moves off `0.0.x`,** the second problem stops being
hypothetical: with `0.1.0` published, a default run walks up from `main`'s old
number, finds `0.0.248` free, and publishes below `latest`. Reset the floor by
setting `main`'s sdk/app/server to the version that just shipped — in the
**post-release** PR, never before. `prepare` runs `set_version`, `git add` and
`git commit -s` over those three files only; if `main` already carries the
version being released there is no diff, `git commit` exits non-zero, and the
step dies under `set -euo pipefail` before anything is pushed.

## The three version trains

| Packages | Version | Bumped by |
| --- | --- | --- |
| `sdk`, `app`, `server` | lockstep | `release.yml` itself, on a `release/sdk-<v>` branch never merged back |
| `skills` | its own line | you, by hand, on `main` |
| `create-malloy-package` | its own line | you, by hand, on `main` |

`main`'s `packages/sdk/package.json` lags npm permanently — release branches are
never merged back. Never treat it as the current version; ask npm.

## Order, and why each step is where it is

### 1. Establish where things actually stand

```bash
git fetch --tags origin
npm view @malloy-publisher/server version              # the real current version
git log --oneline "v$(npm view @malloy-publisher/server version)..origin/main"
```

That commit list is what this release ships. If it is empty, there is nothing to
release.

### 2. Bump the independently-versioned packages, if their content changed

`publish-packages` decides purely on the version in `main`'s `package.json`, and
**a change that lands without a bump is skipped while the release stays green.**
Nothing else in CI requires the bump, so nothing else will catch it.

Check each against the commit that last set its version, over the paths its
*published content* is built from — which is not the same as where it lives:

- `skills` → `skills/`, `packages/skills/`, `bun.lock`, root `package.json`
- `create-malloy-package` → `packages/create-malloy-package/`, and
  `packages/skills/package.json` (its publish job bakes the skills version into
  its dependency range)

**First ask whether it is already bumped.** A push only runs `check_pack`, so a
merged bump sits on `main` while npm still reports the old version — which is
the normal state between a release-prep merge and the dispatch. Skip this whole
step when `main` is already ahead:

```bash
npm view @malloy-publisher/skills version                       # what is published
node -p "require('./packages/skills/package.json').version"     # what main declares
```

Different means it is bumped and pending; leave it alone. Only when they
**match** does the content question arise:

```bash
vb=$(git log --format=%h -S"\"version\": \"$(npm view @malloy-publisher/skills version)\"" \
       -- packages/skills/package.json | tail -1)
git log --oneline "$vb..origin/main" -- skills/ packages/skills/ bun.lock package.json
```

Any output there means bump `packages/skills/package.json`. Same shape for the
scaffolder. This must be **merged to `main` before the dispatch**: the release
reads `main`, not the release branch.

#### The scaffolder also pins a server version, and it is checked

`create-malloy-package` hard-pins the server its generated workspaces run
(`SERVER_VERSION` in `packages/create-malloy-package/src/scaffold.ts`), and its
publish workflow **refuses to ship unless that pin equals npm's `latest`**. A
stale pin is therefore not a cosmetic lag; it fails the publish 25 minutes into a
release, after `skills` has already gone out:

```bash
sed -n 's/^export const SERVER_VERSION = "\(.*\)";$/\1/p' \
  packages/create-malloy-package/src/scaffold.ts
npm view @malloy-publisher/server version
```

Set the pin to **the version this release is about to publish**, in the same
pre-release PR, and confirm that version still honours `--host` (the guard's own
instruction — check `--host` is still parsed in `packages/server/src/server.ts`).

This works only because `publish-packages` waits for `publish-npm`. It did not
until 0.0.250, and the two orderings made the pin unsatisfiable: the guard ran
before the server reached npm, so pinning the outgoing version failed against a
`latest` one release behind, and pinning the previous version failed once npm
caught up. If that `needs:` is ever narrowed back to `prepare` alone, the
scaffolder stops being publishable alongside a server bump at all.

Do not skip the equality check and read the log alone. Anchored at the commit
that set npm's version, the log still reports a bump that has already merged,
and "any output means bump" then walks the version a second time for nothing.

### 3. Sanity-check the notes

`gh-release` reads `RELEASE_NOTES.md` itself: it appends every `## [Unreleased]`
section to the release page, then pushes a branch stamping those headings. There
is nothing to paste, so this step is a read and one check.

```bash
grep -n '^## \[Unreleased\]' RELEASE_NOTES.md
node scripts/release-notes.mjs extract | head -40
# a previous release's stamp, still waiting on a human
git ls-remote --heads origin 'refs/heads/release-notes-stamp-*'
```

What you are checking is that the sections listed are the ones this release
actually ships. A section describes work merged to `main`, so anything sitting
there goes out with this release whether or not it was written for it. Nothing
listed is fine and common — the generated PR list carries a routine patch.

If a section is present that should **not** ship yet, the work behind it is
already on `main` and the note is telling the truth; the fix is a release, not
an edit.

**A previous release's stamp left unmerged is the one that will bite you**, which
is what the third command is for. The stamp cannot land itself: `main` requires a
pull request, so the release pushes the branch and stops. Until someone opens and
merges it, its sections still read `[Unreleased]` and *this* release re-appends
the previous release's narrative to its own page.

`git ls-remote` rather than `gh pr list --search`, deliberately. That search is
full text, not title-scoped, so it matches any PR whose body merely discusses the
stamp — including release-prep PRs, which all explain the mechanism. The branch
name is exact, and it also catches a branch that was pushed but never opened,
which the PR search cannot see at all. If you want the PR too:
`gh pr list --repo malloydata/publisher --search '"stamp the release notes shipped in" in:title' --state open`.

So: if the branch is there, open and merge it before dispatching.

#### If the branch is gone too

Then stamp by hand — the one case where you write a version number into a note
yourself, because the release that shipped it is already public and its number is
no longer a guess. **Scope it first.** The bare command is destructive:

```bash
# DESTRUCTIVE — rewrites EVERY [Unreleased] section in the file
node scripts/release-notes.mjs stamp <version>
```

Unscoped it stamps every `[Unreleased]` heading, including the ones *this*
release is about to ship. Those get labelled with a version that never contained
them and vanish from the next release's page — the exact failure `--titles` was
added to prevent. And you cannot just pass `--titles`: that file lives in
`$RUNNER_TEMP` and died with the run.

Re-establish the scope by comparing the shipped release page against the file.
The page carries two `## ` headings of its own — `## Release v<version>` from the
job's header and `## What's Changed` from `--generate-notes` — so filter those or
you are comparing 8 lines against 6 and never get a match:

```bash
gh release view "v<version>" --repo malloydata/publisher --json body -q .body   | grep '^## ' | grep -vE '^## (Release v|What'"'"'s Changed)' | sort
node scripts/release-notes.mjs extract | grep '^## ' | sort
```

Both then print stripped titles, so the sets are directly comparable. If they are
**identical**, every remaining section belongs to that release and the unscoped
stamp is safe. If the file has extras, write just the shipped headings — verbatim
from `RELEASE_NOTES.md`, `[Unreleased]` marker included — into a file and pass
`--titles <that file>`. Either way, commit on a branch and open a PR.

0.0.249 is the worked example, and it is why this section exists: it put all six
of its sections on its release page and stamped none of them, because the step
still pushed straight to a protected `main`. The unscoped stamp was safe there
only because the two sets matched once filtered — six sections on the page, the
same six in the file.

### 4. Dispatch

Pass the version explicitly — always, and not merely to know the number in
advance. Left to itself `prepare` patch-bumps `main`'s stale sdk version and
walks forward until it finds a free branch and tag, which is not the same as a
version above `latest`; see *`prepare`'s default version is not trustworthy*.

```bash
gh workflow run release.yml --repo malloydata/publisher --ref main -f version=<next>
```

**Do not merge to `main` while it runs.** `publish-packages` aborts if `main`
moves under a watched path mid-release. A `RELEASE_NOTES.md`-only merge is not
watched, but the window is short — just wait.

### 5. Verify what actually shipped

A green tick is not evidence. Read the run's **job summary**, which names each
independently-versioned package as published or skipped and why.

```bash
npm view @malloy-publisher/server version
npm view @malloy-publisher/skills version
gh release view "v<version>" --repo malloydata/publisher
```

### 6. Open and merge the stamp

Half verification, half the one task the release genuinely cannot finish itself.
Two things `gh-release` did, both visible without leaving the run:

- The release page carries the narrative under the generated header. The job
  logs `attached N narrative section(s)`, or `no [Unreleased] sections` when
  there were none.
- A `release-notes-stamp-<version>` branch is pushed, and the job summary's
  *Release notes* section carries a compare link to open it as a PR.

**Follow that link, open the PR, merge it.** The run stops at a branch on
purpose: a PR opened by a person triggers `pull_request`, so its checks run and
any maintainer can merge it, where one opened by the workflow would trigger no
workflows at all and only an admin could ever merge it. One click buys back the
check suite. Left unopened it costs exactly what a failed stamp used to — the
next release re-appends this release's narrative to its own page, and so does the
one after.

```bash
gh release view "v<version>" --repo malloydata/publisher --json body -q .body | head -40
git ls-remote --heads origin 'refs/heads/release-notes-stamp-*'
```

**Wait for `publish-packages` to finish before you merge it.** Merging moves
`main`, and while that job is still polling npm any movement pushes it off its
fast path onto the compare API — which aborts the dispatch outright if the API
does not answer or the diff hits its 300-file cap. `RELEASE_NOTES.md` being
outside the paths it watches saves the *verdict*, not the request. You are
already past step 5, so waiting for that job to go green costs nothing and
closes the window instead of documenting it.

The stamp step is `continue-on-error`, deliberately: the release is already
public and correct by then, and reddening a finished release over a docs commit
would send someone hunting a publishing problem that does not exist. So a missing
branch is a real possibility — **read the job summary rather than assuming**. It
always writes a line, including `No [Unreleased] sections to stamp` when there
was nothing to do, so silence there means the step died and not that the release
had no narrative. Re-running the job does not help: `gh release create` fails on
the existing tag, and the stamp step is skipped behind it. Recover by hand as in
step 3.

A **prerelease** skips the stamp, matching the rest of the job.

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
