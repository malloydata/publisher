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

## The versioning policy

**`0.MINOR.PATCH` for every published package, while pre-1.0.** It is the policy,
not yet the state: `skills` follows it, `sdk`/`app`/`server` are still on `0.0.x`
until `0.2.0` is cut, `create-malloy-package` is on `0.0.8`, and the Python client
declares `0.1.0` — sharing `0.1.x` with `skills`. The bump checks enforce *ahead of
the registry*, not the shape of the number, so moving those two is a judgement call
on their next release rather than something CI will demand.

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

| Packages | Version | Bumped by | Missing bump caught by |
| --- | --- | --- | --- |
| `sdk`, `app`, `server` | lockstep | `release.yml` itself, on a `release/sdk-<v>` branch | n/a — the release sets it |
| `skills` | its own line | you, by hand, on `main` | `skills-npm.yml` PR check |
| `create-malloy-package` | its own line | you, by hand, on `main` | `create-malloy-package-npm.yml` PR check |
| `malloy-publisher-sdk` (Python) | its own line | you, by hand, on `main` | `python-sdk.yml` PR check — but see below |

**A forgotten bump is now a red PR check**, not a silent skip at release time.
Each check asks the same thing — *is the declared version ahead of the registry* —
scoped to the paths that package's published content is built from, and skips
when this PR touched none of them. So the pre-release hand-audit that used to
live in step 2 below is mostly gone.

Two caveats on that:

- The **Python** check has nothing to catch today, because `malloy-publisher-sdk`
  is not on PyPI at all: a project-level 404 is its pass. (It can still go red on
  an unreadable `pyproject.toml` or a registry that answers neither 200 nor 404 —
  it just cannot currently catch a missing bump.) `python-sdk.yml`'s `publish` job
  is gated on a `refs/tags/sdk-python-*` ref while its `on: push` names
  `branches:` and no `tags:`, so a tag push never triggers the workflow and that
  job has never run. Note it is *reachable*, not sealed: the workflow has a
  `workflow_dispatch`, and `gh workflow run python-sdk.yml --ref
  sdk-python-0.1.0` names a tag ref, satisfies the gate, and would publish to
  PyPI for the first time. Treat the Python version as unenforced, and that
  command as a decision rather than a check.
- `bun.lock` and the root `package.json` change what `skills` publishes (its
  `dist/` is emitted by `tsc`, whose version bun resolves from the lockfile) but
  are **not** in `skills-npm.yml`'s trigger, so a lockfile-only PR never reaches
  the check. That one case still needs the manual look in step 2.

`main`'s `packages/sdk/package.json` used to lag npm permanently. It no longer
should: the post-release stamp PR resets those three files to the version that
shipped. But that PR needs a human to merge it, so **`main` is truthful only if
the last one landed** — which is why `prepare` still takes the max of npm and the
file rather than trusting either. Ask npm when you want to know what is
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

### 2. Confirm the independently-versioned packages are bumped

`publish-packages` decides purely on the version in `main`'s `package.json`, and
**a change that lands without a bump is skipped while the release stays green.**
That used to be caught by nothing, so this step was a hand-audit of two packages.
It is now a PR check on each of them, so this step is a confirmation rather than
an investigation:

```bash
for p in skills create-malloy-package; do
  printf '%s: npm %s, main %s\n' "$p" \
    "$(npm view "@malloy-publisher/$p" version)" \
    "$(node -p "require('./packages/$p/package.json').version")"
done
```

`main` ahead of npm is the normal, healthy state between a release-prep merge and
the dispatch: the bump is merged and pending. Equal is fine too — it means
nothing that ships in those packages changed. Either way, the bump must be
**merged to `main` before the dispatch**, because the release reads `main`, not
the release branch.

The one gap the checks do not cover: `bun.lock` and the root `package.json`
change what `skills` publishes — its `dist/` is emitted by `tsc`, whose version
bun resolves from the lockfile — but neither is in `skills-npm.yml`'s trigger, so
a lockfile-only PR never reaches the check. If this release contains a dependency
bump and `skills` is at npm's version, look:

```bash
vb=$(git log --format=%h -S"\"version\": \"$(npm view @malloy-publisher/skills version)\"" \
       -- packages/skills/package.json | tail -1)
git log --oneline "$vb..origin/main" -- bun.lock package.json
```

Output there means bump `packages/skills/package.json`.

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
the previous release's narrative to its own page — and `main` still declares the
version before last. The second is harmless (`prepare` takes the max of npm and
the file), the first is not.

`git ls-remote` rather than `gh pr list --search`, deliberately. That search is
full text, not title-scoped, so it matches any PR whose body merely discusses the
stamp — including release-prep PRs, which all explain the mechanism. The branch
name is exact, and it also catches a branch that was pushed but never opened,
which the PR search cannot see at all. If you want the PR too:
`gh pr list --repo malloydata/publisher --search '"stamp" in:title' --state open`.
The commit title is `chore(release): stamp <version> on main`; 0.0.250 used
`docs: stamp the release notes shipped in <version>`, so search on `stamp` rather
than either full phrase.

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

The lost branch carried the version reset too, so add it to the same commit —
this one has no destructive edge, and it is a no-op if `main` already declares
the version:

```bash
node scripts/set-version.mjs <version> \
  packages/sdk/package.json packages/app/package.json packages/server/package.json
```

0.0.249 is the worked example, and it is why this section exists: it put all six
of its sections on its release page and stamped none of them, because the step
still pushed straight to a protected `main`. The unscoped stamp was safe there
only because the two sets matched once filtered — six sections on the page, the
same six in the file.

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
  *Release notes and version* section carries a compare link to open it as a PR.

**That branch now carries two things**, despite its name: the stamped
`RELEASE_NOTES.md` headings *and* `main`'s three `packages/{sdk,app,server}/
package.json` files reset to the version that shipped. The summary line names
both counts, and either can legitimately be zero — nothing to stamp, or `main`
already declaring that version. When both are zero no branch is pushed at all,
and the summary says so.

The name is unchanged on purpose: it is the identifier this skill and
`CONTEXT.md` both tell you to look for, and renaming it would break the recovery
below to buy nothing.

**Follow that link, open the PR, merge it.** The run stops at a branch on
purpose: a PR opened by a person triggers `pull_request`, so its checks run and
any maintainer can merge it, where one opened by the workflow would trigger no
workflows at all and only an admin could ever merge it. One click buys back the
check suite. Left unopened it costs exactly what a failed stamp used to — the
next release re-appends this release's narrative to its own page, and so does the
one after — and `main` keeps declaring the pre-release version.

Of those two, **the notes half is the one that compounds**; the version half is
self-correcting, because `prepare` takes `max(npm latest, main declared)` and so
derives the right floor whether or not this PR landed. That is exactly why that
floor is not "simplified" to reading the file.

```bash
gh release view "v<version>" --repo malloydata/publisher --json body -q .body | head -40
git ls-remote --heads origin 'refs/heads/release-notes-stamp-*'
# after merging: main should now declare what shipped
git fetch origin main && git show origin/main:packages/sdk/package.json | grep '"version"'
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
