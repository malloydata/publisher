<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# GitHub Actions Workflows, AI Guide

CI and release machinery. Read the YAML for mechanics; this covers what is not visible there: which
workflow owns what, and the publishing rules that bite.

## CI

`build.yml` is the entry point (pushes and PRs). It does not run the test suites itself: it holds
`lint` (typecheck), `lint_format` (prettier, eslint, and the DuckDB/pg version-consistency guards),
`runtime_test` (starts the built server bin under both `npx` with the Node shebang and a direct
`bun run` on the bundle), `docker_smoke_test`, and `cross_platform_test`, which calls
`cross-platform-tests.yml`.

`cross-platform-tests.yml` is `workflow_call`-only and is where the tests actually run, across Linux,
macOS, and Windows: the server unit and integration suites, the skills package tests, and the
create-malloy-package tests plus its e2e suite on Linux. It is reached on every PR through
`build.yml`.

The two npm publish workflows below do re-run their own package's suite, against the exact ref being
published, because a PR run proves nothing about the ref a dispatch actually targets. What they do
not repeat is the server suites and the Linux-only e2e suite.

`app-playwright.yml` runs the browser suite on PRs. `python-sdk.yml` builds the Python client on PRs
and pushes, and now publishes it too: it is the third train `publish-packages` dispatches. Its publish
job has never actually run yet, so read *The first PyPI publish* below before cutting the release that
would be its first.

**None of the three publish workflows is purely a publish workflow any more, so a red check on a PR
that touches one package can come from its own file rather than from `build.yml`.** Know which job:

- `skills-npm.yml`'s `check_pack` runs on PRs and on pushes to `main` that touch `skills/**`,
  `packages/skills/**`, or the workflow file, and it now also carries the version-bump check.
- `create-malloy-package-npm.yml` used to be genuinely dispatch-only. It now has a `pull_request`
  trigger, but *only* for its `check_version` job — `check_pack` and `publish` are both gated to
  `workflow_dispatch`, so no PR pays for that install and the dispatch path is unchanged.
- `python-sdk.yml`'s `check_version` runs on PRs touching `api-doc.yaml`,
  `packages/python-client/**`, or the workflow file.

Each package's machinery stays in its own file: that is why the bump check is written three times
rather than extracted into a reusable workflow. The two npm copies are kept textually parallel so
they can be diffed; the PyPI one differs in registry and in where it reads the version.

Two workflows hold real warehouse credentials, and both should be treated as such when edited:
`connection-integration-tests.yml` (the full connection matrix) and `k6-tests.yml`, which passes
`BQ_PRESTO_TRINO_KEY` and runs on pushes as well as on demand.

## Publishing

There are three npm trains and one PyPI train, and they do not share a version.

| Packages | Registry | Version | Published by | Missing bump caught by |
|---|---|---|---|---|
| `@malloy-publisher/sdk`, `app`, `server` | npm | Lockstep, set by `release.yml` | `npm-sdk.yml`, called from `release.yml` | n/a — the release sets it |
| `@malloy-publisher/skills` | npm | Its own line | `skills-npm.yml` | a `pull_request` check in `skills-npm.yml` |
| `@malloy-publisher/create-malloy-package` | npm | Its own line | `create-malloy-package-npm.yml` | a `pull_request` check in the same file |
| `malloy-publisher-sdk` | PyPI | Its own line | `python-sdk.yml` | a `pull_request` check in the same file |

### The versioning policy

**`0.MINOR.PATCH` for every published package, while pre-1.0. MINOR is a breaking
change; PATCH is everything else.**

**Not yet in force on the `sdk`/`app`/`server` line.** As of this writing that
train's `latest` is `0.0.250`; the policy takes effect at its first minor release,
which is **intended to be `0.2.0`** — skipping `0.1.x` because `skills` occupies
it and two trains sharing a minor invites reading one for the other. Until that
release is cut, `prepare` will keep deriving `0.0.251`, `0.0.252`, … and that is
correct; do not "fix" it with an explicit version. The move itself needs
`-f version=0.2.0`, because a minor bump computed from a `0.0.x` floor lands on
`0.1.0`. `skills` has always been on `0.1.x` and so already follows the policy.

Two of the other three do not, and the checks in Part 1 do not make them: they
enforce *ahead of the registry*, not the shape of the number. `create-malloy-package`
is on `0.0.8`, so `^` pins it exactly, the same trap the policy exists to close;
and `malloy-publisher-sdk` (Python) declares `0.1.0`, sharing `0.1.x` with `skills` —
the very collision the sdk/app/server line skips `0.1.x` to avoid. Both are cheap
to move, on their next release, and neither is urgent: the scaffolder has no
dependents, and the Python client has no published versions at all.

`bun.lock` is a fourth reader of these numbers and is deliberately left alone. It
records `0.0.209` for the three workspace packages, so once the stamp PR resets
`main` it will disagree with the manifests it locked. That is benign — the entries
are workspace links, not registry resolutions, and `bun install --frozen-lockfile`
does not compare them — so the stamp does not touch the lockfile, which keeps the
release out of the business of regenerating it. If a future bun makes that a hard
error, the fix is to add `bun.lock` to `PKGS`' sibling staging in the stamp step,
not to make `prepare` run an install.

The point of the policy is that a consumer can write a range. Verified with
`semver.validRange`:

| Range | Resolves to |
|---|---|
| `^0.0.250` | `>=0.0.250 <0.0.251` — exactly one version |
| `~0.0.250` | `>=0.0.250 <0.1.0` |
| `^0.2.0` | `>=0.2.0 <0.3.0` |

So on `0.0.x` there is no way to say "patches yes, breaking no": `^` pins a single
version, and `~` spans a range whose upper half does not exist. On `0.MINOR.PATCH`
a minor means breaking, so `^0.2.0` is a genuinely compatible range. (Note `~` and
`^` are *identical* on a `0.x` line — both stop at the next minor — so `^` is a
convention here, not a wider range.)

**There is no `major` on the dispatch, deliberately.** Publisher is preview, so a
major is a decision to be made with an explicit `-f version=` rather than a
dropdown choice. One registry fact worth knowing before anyone does: on
`@malloy-publisher/sdk`, **`1.0.1` is published** (an old off-line publish that
`latest` long since moved off) while `1.0.0` is free; `app` and `server` have no
`1.x` at all. So `1.0.1` is burned on one of the three, and a 1.x line would have
to start at `1.0.0` and skip `1.0.1`, or start at `1.0.2`. `prepare`'s floor
comment covers the related trap: `npm view … version` reads the `latest` dist-tag,
not the highest version, and "correcting" it to the maximum would make the next
release `1.0.2` and take the whole line with it.

`release.yml`'s `bump` input chooses `patch` (default) or `minor` off the same npm
floor; `-f version=` still wins outright. The level is applied once and the walk
past an already-taken number is by patch, so `bump=minor` colliding with an
existing `0.3.0` gives `0.3.1` rather than abandoning the `0.3.x` line. Note a
minor bump is computed from npm's current `latest`, so off a `0.0.x` floor it
lands on `0.1.0` — which is why the pending move to `0.2.0` needs an explicit
version.

Prereleases keep the hyphen convention: `npm-sdk.yml` routes a hyphenated version
to the `next` dist-tag and `docker-image.yml` withholds `latest`, so a prerelease
never takes over either.

### A forgotten bump is a red PR check

It used to be invisible until release time — `publish-packages` skips a package
whose version is already on npm and the release stays green — and it is the first
of the *two skips* below. Each of the three published trains now carries a
`pull_request` check asking the same question: **is the declared version ahead of
the registry.** Not "did this PR change the version": between a release-prep merge
and the dispatch `main` is legitimately ahead and untouched, so a did-it-change
check would redden every PR opened in that window. Two registry questions per
check, because a version can be free and still *below* `latest`, and publishing
that moves the dist-tag backwards.

Each check is scoped to the paths that package's published content is built from,
and **that list is deliberately narrower than both the workflow's own `paths:`
trigger and `publish_pkg`'s "main moved" guard.** Both of those watch the publish
workflow file, correctly, because a dispatch runs whatever definition `main`
holds — but that is a question about which code runs, while this is a question
about the tarball. Editing a publish workflow changes no published byte, so
demanding a bump for it would force a content-free publish and make every future
edit to the check require a release. Scoping fails **closed**: if the base ref
will not resolve or the diff will not compute, the check runs rather than skipping.

Two gaps in that coverage, both named rather than assumed:

- `bun.lock` and the root `package.json` change what `skills` publishes (its
  `dist/` is emitted by `tsc`, whose version bun resolves from the lockfile) and
  are in `publish_pkg`'s guard for that reason, but they are not in
  `skills-npm.yml`'s `paths:` trigger, so a lockfile-only PR never reaches the
  check. Widening the trigger would run a full install, build and test suite on
  every dependency bump. The `publisher-release` skill's step 2 covers this case
  by hand.
- The **PyPI** check has nothing to *catch* until the first publish lands,
  because nothing is on PyPI and a project-level 404 is the pass. It is not
  incapable of failing — an unreadable `pyproject.toml`, a version that is not
  `major.minor.patch`, or a registry that answers neither 200 nor 404 all redden
  it. **This is the one entry here with an expiry date**: the moment
  `malloy-publisher-sdk` has a version on PyPI, this check starts enforcing
  exactly like its npm siblings, and the 404 branch stops being the path every
  run takes. See *The first PyPI publish* below.
- `BUN_VERSION` in `create-malloy-package-npm.yml` pins the bundler `prepack`
  runs to emit `dist/index.js`, the only JS in that tarball — so bumping it does
  change published bytes, and the check reports "no published scaffolder content
  changed". Same shape as the `bun.lock` entry above and accepted for the same
  reason: watching the whole workflow file to catch one line would demand a
  release for every comment edit, and the consequence is bounded — the tarball
  stays one bundler behind until the next real release, not broken. Hoisting
  `BUN_VERSION` somewhere watched is what would close it.

**None of these three checks may become a required status check.** They all live
behind path-filtered `pull_request` triggers, so a PR touching none of those
paths produces no check at all — and a required check that never runs sits at
`expected` forever, blocking merge on every unrelated PR. `skills-npm.yml` always
had this shape; it now applies three times. Requiring them means first giving
each an always-run gate job that reports success when the paths did not match.

`release.yml` is `workflow_dispatch`-only and is the single place a release starts. Its `prepare` job
bumps sdk/app/server, commits to a fresh `release/sdk-<version>` branch, and pushes it; `npm-sdk.yml`
and `docker-image.yml` are then called with that ref. `publish-packages` triggers the other three
trains (see below). `gh-release` cuts the tag, and only after npm and Docker have both succeeded.

`gh-release` also owns the release notes. It appends every `## [Unreleased]` section of
`RELEASE_NOTES.md` to the release body (via `scripts/release-notes.mjs`) and then pushes a
`release-notes-stamp-<version>` branch stamping those headings with the shipped version, printing a
compare link for a human to open as a PR. Both used to be manual post-release steps and were reliably
skipped — 0.0.243 through 0.0.247 shipped with none of their narrative.

**That branch carries the version reset too, despite its name**: `scripts/set-version.mjs` over the
three `packages/{sdk,app,server}/package.json` files, so `main` ends the release declaring what
shipped. Either half can legitimately be zero — nothing to stamp, or `main` already at that version —
and when both are, no branch is pushed and the summary says so. The name is unchanged because it is
the identifier the `publisher-release` skill and this file both tell a releaser to look for.

**The version reset cannot move into `prepare`,** which is the obvious place for it. `prepare` stages
exactly those three files and commits them, so once this reset works — meaning `main` already declares
the version being released — there would be no diff, `git commit` would exit non-zero, and that step
would die under `set -euo pipefail` before pushing anything. It has to happen after the release, where
a no-op is allowed, which is why `set_version` was extracted into a script both jobs can call.

**Only the first half is automatic, and the reasons the second half stops at a branch are the whole
story of this step.** It has been written three ways; two of them are wrong and look right.

*Not a push to `main`,* which is what it did until 0.0.250 and which never once worked. `main` is
protected and requires a pull request, so every attempt was rejected with `GH006`, retry included.
0.0.249 is the worked example: six sections on its release page, none stamped, and nobody noticed
until the next release was being cut.

*Not `gh pr create` either,* which is the obvious replacement and was the first fix attempted.
Opening a PR with `GITHUB_TOKEN` requires the repo/org setting **"Allow GitHub Actions to create and
approve pull requests", which is off by default**, and reading that setting needs admin — so from a
`maintain` account it cannot be confirmed. Depending on an unverifiable permission is precisely how
the `GH006` bug was written in the first place: that code assumed a push it was never allowed to
make, and the comment above it admitted the assumption was unconfirmed. The same shape would have
failed the same way, silently, at the next release.

*A branch push, then.* It is **proven** on this token — `prepare` pushes `release/sdk-<version>`
every release — and needs no permission beyond the `contents: write` the job already has. Letting a
person open the PR is not merely the safe option, it is the better one: a PR opened by a human
triggers `pull_request`, so its checks run and **any maintainer can merge it**, where a bot-authored
PR triggers no workflows at all, leaving required checks at `expected` forever and an admin as the
only possible merger. The cost is one click, and it buys back the check suite.

There is a cheaper fix available to an admin, and it is worth checking before anyone assumes this
shape is permanent. Classic protection exposes *"Allow specified actors to bypass required pull
requests"* (`required_pull_request_reviews.bypass_pull_request_allowances`), which accepts apps and
may accept `github-actions`; a ruleset with a bypass actor is the other route, and enabling the
Actions-can-create-PRs setting would at least allow the bot-PR shape. None of the three is verifiable
from a `maintain` account, which is why none is assumed here. Each also trades away something real:
a bypass widens the surface flagged under *Hardening that is not in place yet*, since `release.yml`
carries no ref guard, and a GitHub App token adds a stored credential to a repo that deliberately has
none for npm.

Two things about that pair are easy to get wrong.

**The two halves must agree on which sections shipped.** `extract` runs against the release branch's
snapshot of the file; `stamp` runs minutes later against whatever `main` has become, and recent
releases take 4–10 minutes. So `extract --titles` writes the exact heading lines it consumed to a
file under `$RUNNER_TEMP`, and `stamp --titles` rewrites only those. Unscoped, a section merged
inside that window was stamped with a version it never shipped in — off that release's page, and off
every later one, because its heading no longer said `[Unreleased]`. The titles file lives in
`$RUNNER_TEMP` and not the repo because the stamp step checks out a fresh branch from `origin/main`,
which would discard an in-tree file first.

**A stamp that does not land is not cheap, and "opened a PR" is not "landed."** The stamp step is
`continue-on-error` — by then the release is public and correct, and a failed docs commit must not
redden it — but the cost is *not* merely a heading that still reads `[Unreleased]` for the next
release to pick up. That heading is exactly what the next release's `extract` matches, so the next
release re-appends **this** release's narrative to its own page, and so does the one after that,
until a human notices. That is why every failure path in the step emits a `::warning` and a
job-summary line naming the fix, rather than relying on `continue-on-error` alone.

Ending at a branch shrinks that failure window without closing it, and moves the last inch of it
onto a person: the consequence is identical whether the branch was never pushed or was pushed and
never opened. **So the stamp is part of releasing, not paperwork after it** — open and merge it before
the next dispatch, and check for a stale one before dispatching. Three things about that:

- **Every outcome writes a job-summary line, including the no-ops.** "No sections to stamp" and
  "already stamped" say so explicitly, because an empty *Release notes and version* section in the summary is
  indistinguishable from the step dying before it wrote anything — and telling a releaser to "read
  the summary" is useless if silence is ambiguous.
- **The branch is pushed without `--force`.** A stamp branch already on the remote may carry a
  human's conflict resolution, which is the documented fix when `main` moved under
  `RELEASE_NOTES.md`; recomputing the stamp and forcing over it would discard that silently. A
  rejected push is therefore inspected, and an existing branch is reported and left alone.
- **A re-run of `gh-release` is not a recovery path for a missed stamp.** `gh release create` has no
  `--clobber`, so on a re-run it fails on the existing tag, and the stamp step's implicit `success()`
  means it never runs at all. Recovery is manual; step 3 of the `publisher-release` skill has it.

One trap in that manual recovery, because the obvious command is destructive: `release-notes.mjs
stamp <version>` **without `--titles` rewrites every `[Unreleased]` section in the file**, including
ones the *upcoming* release is about to ship — mislabelling them and erasing them from the next
release's page, which is the exact failure `--titles` was added to prevent. The titles file lives in
`$RUNNER_TEMP` and does not survive the run, so a manual stamp has to re-establish the scope; the
skill spells out how.

`scripts/release-notes.mjs` and `scripts/set-version.mjs` both have unit coverage —
`release-notes.spec.ts` and `set-version.spec.ts` — run by `build.yml`'s `lint_format` job on every PR
(`bun run test:scripts`). The notes spec also smoke-runs `extract` against the real
`RELEASE_NOTES.md`, so a heading it cannot read a title from fails the PR that wrote it rather than the
release that would silently drop the narrative; the version spec does the equivalent, asserting the
replace still finds the `"version"` field in all three real manifests at their real paths, so a
rename or a reformat that moved it out of reach fails a PR rather than a release. The release path
itself is dispatch-only and cannot be exercised in CI, so the shell in the changed steps was verified
out of band instead: each `run:` body extracted from the YAML and executed against a scratch repo with
`npm`, `npx` and `curl` stubbed, which is how the bump checks, the server-pin substitution, the
`--host` check and the stamp step were each covered. That harness is a one-off and is **not** in this
repo — it is worth rebuilding rather than trusting, because a later edit to any of those bodies has
nothing automated behind it.

The stamp step itself can no longer collide with `publish-packages`: it pushes a branch, so `main`
does not move while that job polls npm. But the path list still matters, because the release does not
end when the run does — the operator is told to open and merge the stamp PR next, and
`publish-packages` can still be inside its poll budget when they do. That merge does move `main`
mid-release, and it is harmless for one reason only: `RELEASE_NOTES.md` is in none of the paths that
guard watches. Adding a watched path that `RELEASE_NOTES.md` matches would turn every narrative
release into an aborted package dispatch.

npm publishing uses **GitHub Actions OIDC trusted publishing**, not a stored token. There is no
`NPM_TOKEN` in this repo and one should not be added back. The Docker and PyPI paths do use secrets
(`DOCKERHUB_TOKEN`, `PYPI_TOKEN`); npm does not.

### Rules that bite (not guessable from the YAML)

The first two rules below are npm's behaviour, not ours, and they drive a one-way-door decision, so
they carry sources. Check them before acting on them: npm can change either unilaterally, and
everything in this file that follows from them changes with it.

- **One trusted publisher per package.** npm allows a single entry per package, so a package cannot
  be publishable from two workflows. Sources: npm's
  [trusted publishing docs](https://docs.npmjs.com/trusted-publishers), and
  `malloydata/malloy`'s own `.github/workflows/CONTEXT.md`, which records the concrete check and the
  **409** the registry returns on a second registration for the same package.
- **Matching is on the top-level caller workflow filename**, not a reusable workflow it calls. npm's
  [trusted publishing docs](https://docs.npmjs.com/trusted-publishers) describe this as a mismatch
  hazard, and `malloydata/malloy`'s `CONTEXT.md` states the same rule verbatim ("Matching is on the
  top-level caller workflow filename, not a reusable workflow it calls"), independently arrived at.
  This is why `@malloy-publisher/sdk`, `app`, and
  `server` have entries naming `release.yml` even though the `npm publish` calls live in
  `npm-sdk.yml`.
- **`release.yml` dispatches the three independently-versioned packages, it does not call them.**
  `publish-packages` triggers `skills-npm.yml`, `create-malloy-package-npm.yml` and `python-sdk.yml`
  through the Actions API, so each runs as its own top-level run and keeps its own npm entry naming
  its own file. Converting either npm one to `workflow_call` would force its npm entry to be
  re-pointed at `release.yml` and would immediately break its manual dispatch. That is a one-way
  door, because there is only one entry per package. `python-sdk.yml` uploads with a token rather
  than OIDC, so it is not subject to that constraint — which is a reason not to rely on it, not a
  reason to treat it differently: it is dispatched the same way so there is one mechanism to reason
  about.
- **The two registries differ in exactly two places.** `publish_pkg` is parameterised on how a
  manifest is read (`read_manifest`) and how the registry is asked (`registry_has`, three-way:
  published / free / did not answer). Everything else — the watched-path guard, the "main moved"
  compare, the dispatch, the poll loop, the summary lines — is registry-agnostic and must stay that
  way. A second copy of that function for PyPI is how the two would drift, and the npm one carries a
  dozen fail-closed decisions that were each paid for.
- **`timeout-minutes` on `publish-packages` tracks the package count.** It is three poll budgets plus
  margin now. Keep it above N x `POLL_BUDGET_SECONDS`, or the LAST package timing out gets killed by
  the job cap before it can print which package failed — the one diagnostic that says whether a
  publish happened.
- **Dispatch the children on `main`, never on the release branch.** `prepare` bumps only sdk/app/
  server, so a release branch's copies of these two packages are identical to `main`, and both
  children guard on `github.ref == 'refs/heads/main'`. Dispatched on a release branch their publish
  jobs skip, and a skipped job reports success, so a release would go green having published nothing.
- **Order is forced: skills before create-malloy-package.** The scaffolder pins its skills dependency
  from the monorepo and then asks the registry whether that range resolves, so skills has to be on
  npm first. Dispatching both at once fails the scaffolder.
- **OIDC auto-enables provenance, which requires `repository.url`** in each published `package.json`.
  A package missing it fails to publish with a `422 ... provenance` error. This is the likely failure
  when adding a new package.
- **`main` carries the released sdk/app/server version only if the last stamp PR merged.** Release
  branches are still never merged back; instead `gh-release`'s stamp branch resets those three
  `package.json` files to the shipped version, and a human merges it. Before that mechanism `main` sat
  41 patches behind npm. So the file is now *usually* truthful and never *reliably* so, which is why
  `prepare`'s floor is `max(npm latest, main declared)` and fails closed when the registry will not
  answer. **Do not "simplify" that to reading the file** — an unmerged stamp PR is exactly the thing
  that gets missed, and the max is correct either way. `prepare` then walks forward from the floor
  until it finds a version whose release branch and tag are both free.
- **Publish steps need scripts enabled.** `prepack` is what copies `skills/` into the skills package
  and what builds the scaffolder's `dist/`. A publish run with `--ignore-scripts`, or with
  `ignore-scripts=true` in the publisher's own npmrc, ships a package with the contents missing and
  exits 0. The derived `SERVER_VERSION` now rides on this too: the substitution edits `src/scaffold.ts`,
  and only `prepack` turns that into the `dist/index.js` npm actually ships (`dist/` is gitignored, so
  there is no stale copy to fall back to). Nothing asserts the substituted value reached `dist/` —
  scripts-disabled would ship no `dist/` at all rather than an old pin, which is why this is a footnote
  and not a check.

### The first PyPI publish

`python-client` is the third train `publish-packages` dispatches, and it got there by replacing a
publish gate that had never run rather than by repairing it. The old job was gated
`if: startsWith(github.ref, 'refs/tags/sdk-python-')` while `on: push` named `branches: [main]` and no
`tags:`, so a tag push did not trigger the workflow at all and the job was unreachable. It now carries
the same gate as its two siblings, `workflow_dispatch` on `refs/heads/main`.

**The tag trigger was not restored, and that is not tidiness.** A tag pointed at a commit already on
`main` pushes no new commits, so the `paths:` filter on that `push` has no changed files to match and
would filter the run out — the trigger would look repaired and still not fire, which is the exact bug
being fixed. Restoring it for real means giving up the `paths:` filter and running the uv install and
client regeneration on every push to `main`. One mechanism is the better trade, and it is the point of
this whole file.

**The publish job did not build a usable package, and that was invisible because it could never run.**
Two independent reasons, both now fixed, both worth knowing because either one alone ships a wheel that
installs cleanly and cannot be imported:

- The job did `pip install build && python -m build` on a checkout. Almost none of this package is in
  git — `packages/python-client/.gitignore` is `malloy_publisher_sdk/*` with
  `!malloy_publisher_sdk/__init__.py` — so a checkout holds exactly one tracked python file, and that
  file's line 6 imports `.client`, which is generated from `api-doc.yaml` and never committed.
  `needs: build` does not help: that job regenerates on its own runner and shares no artifact. The job
  now runs `scripts/build-python-sdk.sh` with `BUILD_PACKAGE=true`, which is the one script that knows
  how to produce this package.
- **Running the generator is still not enough**, which is the part that is easy to miss. The build
  backend is hatchling, and hatchling's default file selection *consults `.gitignore`* — so the
  generated client was excluded from the artifacts even when sitting on disk. Measured: a build with
  `client.py` and `api/` present shipped a wheel containing exactly `__init__.py`. `pyproject.toml` now
  sets `[tool.hatch.build] ignore-vcs = true`, plus explicit `packages`/`include` for the wheel and
  sdist targets so that switching the ignore rules off does not start packing `.venv/`, `dist/` and
  `__pycache__`. An explicit `packages` **alone does not fix it** — the ignore rules still apply.

**A third tool reads `.gitignore` here, and it is worth knowing before you trust a green build.**
`build-python-sdk.sh` runs `black`, `ruff` and `pyright` over the generated client at steps 6 and 7 —
except ruff also honours `.gitignore`, so inside a git checkout it silently skips the entire generated
tree. Measured on one tree with one ruff (0.16.4): **929 errors outside a git repo, `All checks
passed!` inside one.** CI is a checkout, so it reports the latter and the release is not at risk; but
"lint ran" does not mean "the generated client was linted". Left as-is deliberately — turning it on
surfaces ~929 findings and is a separate decision — and recorded because it is the same trap as the
packaging bug above, one directory that is deliberately gitignored while being the package's entire
content.

The guard against both is `Verify the wheel is importable before uploading it`, which installs the
built wheel into a throwaway venv and imports it, from `/tmp` so the source tree cannot satisfy the
import. It asserts the artifact rather than the intent, because "we remembered to regenerate" is what
failed, and it runs BEFORE twine — a check afterwards would only name the version you had burned.

**Nothing has been published yet, so the first release that reaches this step is still the one run
where the path has never been proven against the real registry.** Three preconditions, none of which
CI can check for you:

- **`PYPI_TOKEN` has to be an ACCOUNT-scoped token for the first upload.** A project-scoped token
  cannot exist for a project that does not exist. After the first publish, mint a project-scoped one
  and replace the secret — the account-scoped token can upload to every project the owner has, which
  is not a permission this workflow needs twice.
- **The name has to still be free.** `malloy-publisher-sdk` 404s today, and PyPI names are
  first-come; nothing reserves it in the meantime.
- **`0.1.0` is what would ship**, because that is what `packages/python-client/pyproject.toml`
  declares. Note it shares its minor with `@malloy-publisher/skills`' `0.1.x` — different registries,
  so not a collision, but see the versioning policy above on why the sdk train skips `0.1.x`. If it is
  going to move, move it *before* the first publish: PyPI filenames can never be reused, so `0.1.0`
  is spent the moment it uploads.

The failure is cheap and recoverable, which is why this is a note and not a gate. `python-client` is
dispatched last, and nothing depends on it; a failure there leaves the two npm packages already
published, says so in the job summary, and re-running `publish-packages` skips whatever landed.

`publish-packages` is also the only job that reads `pyproject.toml`, and it installs nothing on
purpose because it holds `actions: write`. So it uses the runner's own `python3` for `tomllib`
(stdlib from 3.11; ubuntu-24.04 ships 3.12) and asserts that rather than provisioning it — without the
assertion, an image that moved `python3` back below 3.11 would report "could not read its manifest",
which reads as a corrupt file and sends you to the wrong place.

### One package that looks publishable and is not

**`@malloydata/publisher-cli` at `0.0.1` is unpublished but not `private`.** It is in a different npm
scope (`@malloydata`, not `@malloy-publisher`) and no workflow publishes it, so it looks shippable
while nothing ships it. Decide separately whether to mark it `private: true` or bring it into
`publish-packages` — and if the latter, it needs the whole *Adding a published package* dance below,
plus a bump check like the other three.

For completeness: `api-doc.yaml`'s `info.version: v0` is the REST API version behind the `/api/v0`
path prefix, not a package version. It is unrelated to any of this and should not be bumped with a
release.

### Adding a published package

Publish the first version by hand: npm will not accept a trusted-publisher entry until the package
exists. Then add the `repository` field, register the trusted publisher naming that package's own
workflow file, and wire it into `release.yml`'s `publish-packages` so a release picks it up.

### Recovering a failed release

The two independently-versioned packages are safe to retry **through `publish-packages`**: that job
asks the registry first and skips a version that is already published, so re-running it republishes
nothing. The skip lives in that job, not in the children — each child's "Verify this version is not
already published" step `exit 1`s on a version npm already holds, so a hand-dispatched child that
already published fails rather than no-opping.

**If `publish-packages` is the only red job, do not re-run the release.** It sits outside
`gh-release`'s `needs`, so it neither blocked nor undid the tag: the sdk/app/server release completed
and only the two dispatches are outstanding. Use "Re-run failed jobs", which re-enters that job alone
against a now-settled `main` and skips whatever already landed. Dispatching `skills-npm.yml` and then
`create-malloy-package-npm.yml` on `main` by hand also works, in that order, but ask npm which of the
two is still missing and dispatch only that one — per the note above, a child whose version is already
published fails its own guard. Re-running `release.yml` instead walks the version forward and burns
three npm versions for nothing.

The "main moved during this release" abort is an expected, retryable failure of this kind. It fails
before dispatching the package it names, but note it can fire on the second package after the first
has already published, so read the job summary for what actually shipped rather than assuming the
registry is untouched. Re-running `publish-packages` is safe either way, because it skips a version
that is already on npm; a hand dispatch is safe only for the package that has not published yet.

One thing the derived `SERVER_VERSION` changed about that hand dispatch: it is now **quiet** where it
used to be loud. Dispatching `create-malloy-package-npm.yml` resolves the pin from whatever the server's
`latest` is at that moment, so a dispatch fired before `publish-npm` has landed the new server
publishes a scaffolder pinned a release behind, and passes. The old hand-maintained pin failed the run
instead. So order still matters even though nothing enforces it: confirm the server version is on npm
(`npm view @malloy-publisher/server dist-tags.latest`) before dispatching the scaffolder by hand.

It fires only when `main` moved **under the paths that package's published content is built from**, not
on any movement at all: the second package is checked after the first has finished a full
dispatch-and-wait, so a bare sha comparison failed it whenever anything merged inside that window,
including changes that could not affect it. Those paths are not simply "where the package lives".
`create-malloy-package` includes `packages/skills/package.json`, because its publish job reads the
skills version from `main` at dispatch time and bakes it into the published dependency range, even
though no skills file enters its tarball. Every uncertain answer still aborts: an unanswered compare
API, and a diff at that API's 300-file cap, where the list may be truncated.

**Two skips are how a green release ships nothing, and both name themselves in the job summary.**
Read that summary rather than the run's green tick if you expected a publish.

1. **The version was not bumped.** `publish-packages` decides purely on the version in `main`'s
   `package.json`, so if a change lands in `skills/` or `packages/create-malloy-package/` without a
   version bump, the release skips that package and stays green. This is now much harder to reach: each
   package carries a `pull_request` bump check (see *A forgotten bump is a red PR check*). It is not
   impossible — a lockfile-only change to `skills` is outside that check's trigger — so the skip and
   its summary line stay.
2. **The release was a prerelease.** Any hyphen in the release version skips both packages, because
   their own versions carry no hyphen and their publish workflows would tag them `latest`.
   `gh-release` skips prereleases for the same reason. Publish these from an ordinary release, or
   dispatch their workflows on `main` directly.

Those two are skips. The scaffolder used to have a third failure that was not a skip but a hard red,
costing a full poll budget to discover: it refused to publish unless the `SERVER_VERSION` it pins in
`src/scaffold.ts` equalled `@malloy-publisher/server`'s npm `latest`, and nothing set that pin, so a
release that forgot it failed 25 minutes in, *after* `skills` had already published.

**That pin is now derived at publish time**, the same way the `workspace:*` skills dependency is:
`create-malloy-package-npm.yml` reads npm's current server `latest`, substitutes it into the runner's
working tree only, asserts the substitution took, and then verifies it. The value committed to the
repo is a dev default — what a `bun run` from a clone boots — and **setting it in a pre-release PR is
now wrong rather than merely unnecessary**, because the substitution overwrites it.

Three things about that pair are load-bearing.

*The verification step is not a tautology.* The resolve step writes; the verify step reads the line
back with the same expression the old hand-policed guard used and re-asks the registry. So it still
catches everything it did before, plus a file that grew a second `SERVER_VERSION` line and a `latest`
that moved between the two steps. The protection it exists for is unchanged: a scaffolder published
ahead of its pinned server generates workspaces whose `npm start` cannot resolve at all, under a
version that can never be replaced.

*`sed` exits 0 when it matches nothing,* so `set -e` does not catch a substitution that did not apply
— the trap the skills-dep step in the same file already names. The resolve step therefore asserts the
exact line it wrote. Without that it exits 0 announcing `SERVER_VERSION <unreadable> -> <latest>`.

*The `--host` check had to become automatic.* The old guard's error message told a human to "confirm
the new server still honours `--host`", and deriving the pin removes that prompt. Nothing else covers
the thing that matters here, which is the **published** server this scaffolder is about to pin. The
scaffolder's e2e suite does boot a server with `--host 127.0.0.1` and assert no LAN address answers
(`packages/create-malloy-package/tests/e2e/scaffold.e2e.spec.ts:350-395`), run on every PR through
`cross-platform-tests.yml` — but against the **repo's own build**, which is the one place the flag
could not regress unnoticed anyway. The server itself has no unit test asserting the flag is parsed
(`server.ts:112`), only the usage text at `:157`. The flag is not arbitrary — the server accepts an unknown flag without a word and falls back
to binding `0.0.0.0`, so a release that renames or drops it does not fail a generated workspace, it
silently makes it serve an unauthenticated REST API and MCP endpoint on every interface while its own
`package.json`, `AGENTS.md` and `README` all still say `127.0.0.1`. The job now runs the resolved
version's `--help`, which prints usage and `process.exit(0)`s before binding a port, and requires
`--host` in the option column **with an argument placeholder and a description after it**. Anchored
that tightly, not on a bare substring, because two near misses both pass a loose match: a server that
replaced the flag and merely named it in a wrapped description line ("`--host` is deprecated; use
`--bind`"), and one that kept the name but dropped its argument, which would leave a generated
workspace's address behind as a stray token.

All of this only works because `publish-packages` waits for `publish-npm`, which it did not until
0.0.250. Before that this job raced the server publish, and *no* pin could satisfy the old guard: the
outgoing version failed against an npm still a release behind, the previous one failed once npm caught
up. 0.0.250 is the worked example — the guard ran at 15:07:55 against a `latest` that did not become
0.0.250 until 15:09:13. **Narrowing that `needs:` back to `prepare` alone now breaks the derivation
silently** rather than loudly: the substituted value would simply be a release behind, and every
generated workspace would pin the previous server.

sdk/app/server are not re-runnable at the same version today. `prepare` walks the version forward
whenever a release branch or tag exists, so a release that fails after `npm-sdk.yml` has published
cannot be resumed and the retry burns three new versions. npm versions are immutable, so there is no
repair beyond moving forward.

### Hardening that is not in place yet

There are **three** trusted-publisher entries, not two: `release.yml` (for sdk, app, and server),
`skills-npm.yml`, and `create-malloy-package-npm.yml`. Any hardening has to cover all three.

The `github.ref == 'refs/heads/main'` guards on the two child publish jobs are a guardrail, not a
boundary: npm matches repo plus filename and never a ref, so anyone who can dispatch can push a
branch whose copy of the workflow drops the line. **`release.yml` carries no ref guard at all**, and
its `prepare` job branches from whatever ref it was dispatched on and passes that branch to
`npm-sdk.yml`, so a branch dispatch publishes that branch's sdk/app/server under the real names. That
is the largest of the three surfaces, so it matters most there.

The control that survives all of it is binding each trusted-publisher entry to a GitHub Environment
whose deployment branches are restricted to `main`, which npm verifies from the OIDC token. If that
is added, land the `environment:` key in the workflow **before** setting the environment name on npm,
never after, or every publish fails on a claim mismatch that looks nothing like its cause. Configure
it with branch restrictions only and no required reviewers, since a pending approval would stall the
release's wait for the package to appear on npm.
