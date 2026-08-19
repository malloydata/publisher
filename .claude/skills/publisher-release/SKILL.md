---
name: publisher-release
description: Cut a Malloy Publisher release, and write the RELEASE_NOTES.md entries a release ships. Use when asked to release, cut a release, ship a version, or publish Publisher to npm/Docker — and when a change needs a release note, or you are deciding whether it does and what version to stamp on it.
---

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
`[Unreleased]` section to the release page and stamps the heading back on
`main`. Writing the section is the whole job.

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
   does this** — `gh-release` commits the stamp to `main` after the release is
   cut. You do not stamp by hand, and you do not guess the number in advance.

Once a section is stamped, it is history and does not get rewritten. A follow-up
that changes that behaviour opens a **new** `[Unreleased]` section referencing
the shipped version by number, the way the build-failures section names 0.0.245
and 0.0.246 when describing what 0.0.247 changed.

Never write a version number into a note yourself. Release numbers are assigned
at dispatch time and a release can fail, so a section stamped in advance can name
a version that does not exist — which is exactly why the stamp runs after
`gh release create` succeeds and not before.

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

Do not skip the equality check and read the log alone. Anchored at the commit
that set npm's version, the log still reports a bump that has already merged,
and "any output means bump" then walks the version a second time for nothing.

### 3. Sanity-check the notes

`gh-release` reads `RELEASE_NOTES.md` itself: it appends every `## [Unreleased]`
section to the release page and commits the heading back to `main` stamped with
the version that shipped it. There is nothing to paste and nothing to stamp by
hand, so this step is a read, not a task.

```bash
grep -n '^## \[Unreleased\]' RELEASE_NOTES.md
node scripts/release-notes.mjs extract | head -40
```

What you are checking is that the sections listed are the ones this release
actually ships. A section describes work merged to `main`, so anything sitting
there goes out with this release whether or not it was written for it. Nothing
listed is fine and common — the generated PR list carries a routine patch.

If a section is present that should **not** ship yet, the work behind it is
already on `main` and the note is telling the truth; the fix is a release, not
an edit.

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

### 6. Confirm the notes landed

`gh-release` does this now, so the step is verification rather than work. Two
things it did, both visible without leaving the run:

- The release page carries the narrative under the generated header. The job
  logs `attached N narrative section(s)`, or `no [Unreleased] sections` when
  there were none.
- `main` carries a `docs: stamp the release notes shipped in <version>` commit.

```bash
gh release view "v<version>" --repo malloydata/publisher --json body -q .body | head -40
git fetch -q origin && git log --oneline -1 origin/main
```

The stamp step is `continue-on-error`, deliberately: the release is already
public and correct by then, and reddening a finished release over a docs commit
would send someone hunting a publishing problem that does not exist. So a
missing stamp is a real possibility and costs one commit — check rather than
assume. The next release stamps it anyway.

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
