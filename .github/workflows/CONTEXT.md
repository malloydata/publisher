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
`bun run` on the bundle), `docker_smoke_test`, `python_sdk_build` (calls `python-sdk.yml`),
`python_sdk_smoke_test` (drives a server built from the PR through the built Python SDK wheel; see
*Publishing to PyPI*), and `cross_platform_test`, which calls `cross-platform-tests.yml`.

`cross-platform-tests.yml` is `workflow_call`-only and is where the tests actually run, across Linux,
macOS, and Windows: the server unit and integration suites, the skills package tests, and the
create-malloy-package tests plus its e2e suite on Linux. It is reached on every PR through
`build.yml`.

The two npm publish workflows below do re-run their own package's suite, against the exact ref being
published, because a PR run proves nothing about the ref a dispatch actually targets. What they do
not repeat is the server suites and the Linux-only e2e suite.

`app-playwright.yml` runs the browser suite on PRs. `python-sdk.yml` is `workflow_call`-only (plus a
manual dispatch): it builds and verifies the Python client, for `build.yml` on every PR and for
`release.yml`, which publishes it to PyPI from a job of its own. Nothing has been uploaded yet, so read *Publishing to PyPI* below before cutting the
release that would be its first.

**Not every publish workflow is purely a publish workflow, so a red check on a PR that touches one
package can come from its own file rather than from `build.yml`.** Know which job:

- `skills-npm.yml`'s `check_pack` runs on PRs and on pushes to `main` that touch `skills/**`,
  `packages/skills/**`, or the workflow file. It only builds and tests the package; skills carries
  no PR-time version check any more (see *Skills and create-malloy-package version at release
  time* below).
- `create-malloy-package-npm.yml` is genuinely dispatch-only: `workflow_dispatch` is its only
  trigger. Its `pull_request` trigger and `check_version` job are both gone.
- `python-sdk.yml` has no PR trigger and no version check: the Python SDK releases in lockstep
  with sdk/app/server, so `prepare` sets its version and there is nothing for a PR to bump.

Two workflows hold real warehouse credentials, and both should be treated as such when edited:
`connection-integration-tests.yml` (the full connection matrix) and `k6-tests.yml`, which passes
`BQ_PRESTO_TRINO_KEY` and runs on pushes as well as on demand.

## Publishing

There are three npm trains and one PyPI train. The PyPI train shares sdk/app/server's version;
the other two npm trains each have their own.

| Packages | Registry | Version | Published by | Missing bump caught by |
|---|---|---|---|---|
| `@malloy-publisher/sdk`, `app`, `server` | npm | Lockstep, set by `release.yml` | `npm-sdk.yml`, called from `release.yml` | n/a — the release sets it |
| `@malloy-publisher/skills` | npm | Decided at release time by `scripts/independent-version.mjs`; `package.json` carries a fixed `0.0.0-dev` placeholder | `skills-npm.yml` | n/a — nothing to forget, the decision reads npm itself |
| `@malloy-publisher/create-malloy-package` | npm | Decided at release time the same way; `package.json` also carries `0.0.0-dev` | `create-malloy-package-npm.yml` | n/a, same reason |
| `malloy-publisher-sdk` | PyPI | Lockstep with sdk/app/server, set by `release.yml` in `pyproject.toml` | `release.yml`'s `publish-python-sdk`, built by `python-sdk.yml` | n/a — the release sets it |

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
is on `0.0.8`, so `^` pins it exactly, the same trap the policy exists to close.
It is cheap to move, on its next release, and not urgent: the scaffolder has no
dependents. `malloy-publisher-sdk` (Python) follows sdk/app/server's number, so it
follows whatever that line does.

`bun.lock` is a fourth reader of these numbers and is deliberately left alone. It
records `0.0.209` for the three workspace packages, so once a release PR merges
`main` disagrees with the manifests it locked. That is benign — the entries
are workspace links, not registry resolutions, and `bun install --frozen-lockfile`
does not compare them — so `prepare` does not touch the lockfile, which keeps the
release out of the business of regenerating it. If a future bun makes that a hard
error, the fix is to rewrite those `bun.lock` entries beside `PKGS` in `prepare`,
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

**The `next` dist-tag was retired on 2026-08-26, on npm and on Docker Hub.** It
had pointed at `0.0.198-dev6` since 2026-05-21 while 51 stable versions shipped
past it, because a dist-tag only moves when a publish uses it and no prerelease
had been cut since. So `@malloy-publisher/server@next` silently installed a
three-month-old build, and `ms2data/malloy-publisher:next` served the matching
image. Nothing in this repo, in the Credible service, or in the published docs
installed from either. Three consequences, and the first one decides what
automation is possible here at all:

- **npm OIDC cannot move a dist-tag.** Trusted publishing authenticates
  `npm publish` and `npm stage publish` only; `npm dist-tag add|rm` needs a
  granular token, which this repo deliberately does not have (see the OIDC note
  below). So CI can move `next` only by publishing a real prerelease under it.
  Re-pointing `next` at an existing stable release is not available without
  reintroducing `NPM_TOKEN`, which is not a trade worth making for a tag nothing
  reads. The same follows for any other dist-tag surgery: it is a human org
  member acting by hand, never a workflow.
- **The routing was left in place, so this is self-healing.** `npm-sdk.yml`
  still sends any hyphenated version to `next`, and `docker-image.yml` still
  tags `:next` on a prerelease. The first prerelease after this recreates both
  tags, correctly, with no change here.
- **`docs/deployment.md` now states there is no pre-release channel today.**
  That sentence is true only while the above holds, so a release that cuts a
  prerelease has to update it in the same PR.

### Skills and create-malloy-package version at release time

There is no PR-time bump to forget for these two: nothing is committed ahead of time, so there is
nothing for a check to compare against. `packages/skills/package.json` and
`packages/create-malloy-package/package.json` both carry a fixed `0.0.0-dev` placeholder version in
the repo; what actually publishes is decided by `scripts/independent-version.mjs`, called from
`scripts/publish-packages.sh` (a release) or from the child workflow's own "Resolve version to
publish" step (a hand dispatch), and written into the manifest at publish time by
`scripts/set-version.mjs`, in the runner's working tree only.

The two packages are decided differently:

- **skills** publishes when its published content changed since npm `latest`'s `gitHead`: a
  `git diff --quiet <gitHead> HEAD` over `skills/`, `packages/skills/`, `bun.lock` and the root
  `package.json`, excluding `skills/README.md` and `packages/skills/src/*.spec.ts` (neither reaches
  the tarball). Changed publishes one patch above the highest published plain version; unchanged
  skips. `gitHead` is what `npm publish` stamped onto the currently-published version's manifest,
  which is why `release.yml`'s checkout now uses `fetch-depth: 0` — an arbitrary past commit a
  shallow clone would not have.
- **create-malloy-package** publishes on every non-prerelease release, because it bakes the
  server's npm `latest` into every workspace it scaffolds, so a release changes what it ships even
  when nothing in its own directory did. The only skip is a re-run: if npm `latest`'s
  `publisherServer` field already equals this release's version, a previous attempt of the same
  release already got it out.

The publish version is one patch above the HIGHEST published plain `x.y.z` version
(`npm view <pkg> versions --json`), not `latest` plus one: `latest` can be moved backwards by hand
(`npm dist-tag add <pkg>@<older> latest`) after a bad release while the newer version stays
published, and computing off `latest` alone would recompute that already-published version and
abort — under `set -e`, before the scaffolder is ever dispatched, so a server release could ship
with no scaffolder pinning it. `latest` and its `gitHead` stay the content-diff baseline above,
since that's what users actually get; only the version arithmetic moved to the versions list.

A hand dispatch of either child workflow (`gh workflow run skills-npm.yml` /
`create-malloy-package-npm.yml`, no `version` input) publishes one patch above the highest
published version, the same "free and above the ceiling" guard the old PR check used to enforce,
now run once at dispatch time instead of on every PR. **A minor or major bump is a hand dispatch
with `-f version=`** — nothing computes one automatically. After a hand-dispatched skills minor,
hand-dispatch the scaffolder too (it depends on skills' version), or just wait for the next
release, which republishes the scaffolder anyway.

The old PR-time bump check's `bun.lock` gap is closed by this move, not merely inherited: `bun.lock`
and the root `package.json` are in the skills diff's watched paths directly (they change what
`dist/` becomes, since it's emitted by `tsc` and bun resolves that from the lockfile), where before
they were outside `skills-npm.yml`'s `paths:` trigger and a lockfile-only PR never reached the
check at all. A lockfile-only change now counts as "changed" and triggers a publish on its own.

The Python SDK had a PR-time bump check of the same kind (`check_version` in `python-sdk.yml`),
and it is gone for a different reason: the SDK no longer has a version of its own. `prepare`
stamps the release version into `packages/python-client/pyproject.toml` with the three
`package.json` files, so a PR has nothing to bump and a release always has a new number to upload.

`release.yml` is `workflow_dispatch`-only and is the single place a release starts. Its `prepare` job
bumps sdk/app/server and the Python SDK, stamps the release notes, commits both to a fresh `release/v<version>`
branch cut from `main`, and pushes it; `npm-sdk.yml` and `docker-image.yml` are then called with that
ref. `build-python-sdk` calls `python-sdk.yml` with it too, and `publish-python-sdk` uploads what that
built (see *Publishing to PyPI*). `publish-packages` triggers the two other npm trains (see below).
`gh-release` cuts the tag, and
only after npm and Docker have both succeeded.

**The release branch is also the post-release PR.** `prepare` rewrites every `## [Unreleased]`
heading in `RELEASE_NOTES.md` as `## [<version>]` (`scripts/release-notes.mjs stamp`) in the same
commit that sets the three `packages/{sdk,app,server}/package.json` versions and
`packages/python-client/pyproject.toml`'s (`scripts/set-version.mjs`, which sets a pyproject's
`[project]` version). `gh-release` then puts the sections headed `[<version>]` on the release
page (`release-notes.mjs extract <version>`) and prints a compare link. A person opens
`release/v<version>` into `main` as a PR, titled `chore(release): <version>`, and merges it; that
one PR is the whole post-release step. It does **not** touch `packages/skills/package.json` or
`packages/create-malloy-package/package.json`: those two carry a fixed `0.0.0-dev` placeholder and
their published version is decided at release time from npm's own state (see *Skills and
create-malloy-package version at release time* above). Both halves used to be manual post-release
steps and were reliably skipped: 0.0.243 through 0.0.247 shipped with none of their narrative.

Why this shape, and not the ones it replaced:

- **One tree.** The stamp, the version and the narrative on the release page all come from the
  commit npm and Docker published. An earlier design extracted from the release branch and stamped a
  fresh branch cut from `main` minutes later, which needed a `--titles` artifact to keep the two
  agreeing about which sections shipped. A section merged to `main` while a release runs is simply
  not on the release branch, stays `[Unreleased]`, and ships next time.
- **A release that fails stamps nothing that matters.** The stamped branch reaches `main` only by a PR
  someone opens after the release succeeded. A failed release leaves its branch unmerged, and the
  next dispatch walks past it (`version_taken`) and cuts a new one from `main`, which still reads
  `[Unreleased]`.
- **Not a push to `main`.** `main` is protected and requires a pull request; until 0.0.250 the stamp
  step pushed there and every attempt was rejected with `GH006`.
- **Not `gh pr create` from the workflow.** Opening a PR with `GITHUB_TOKEN` needs the off-by-default
  "Allow GitHub Actions to create and approve pull requests" setting, which only an admin can read.
  Worse, a bot-authored PR triggers no workflows, so `main`'s required checks sit at `expected` and
  only an admin can merge it. A PR opened by a person (or by an agent on that person's credentials,
  which is what the `publisher-release` skill does) runs its checks and any maintainer can merge it.

**`release-sync.yml` fails while a release's PR is unmerged.** It compares npm's `latest` for
`@malloy-publisher/sdk` with the version `packages/sdk/package.json` declares, on every PR, every
push to `main`, and right after each `release.yml` run (`workflow_run`, against `main`'s head, so
`main` reads red as soon as there is a release PR to merge). It is its own workflow rather than a
`build.yml` job because it is not about the build: it changes state when npm does, not when the
code does. `main`'s version moves only when a release PR merges, so npm ahead
of it means a release published and its PR has not merged. The next release would then stamp that
release's still-`[Unreleased]` sections a second time under the new number. The fix is to merge
`release/v<npm latest>` into `main`, which is also the only thing that clears the check; the
release PR itself passes because its tree declares the new version. It reads npm rather than the
git tags because a release whose npm publish succeeded and whose `gh-release` failed has no tag
but did ship.

**Expect it red on unrelated PRs.** Because it runs on every PR, contributors see it fail from the
moment a release publishes until that release's PR merges, on changes that have nothing to do with
the release. That is deliberate, and it is meant to be a **required status check** on `main`, so
in that window nothing else merges and the release PR is the next thing to land. It gates merges,
not dispatches: `release.yml` does not consult it.

**A prerelease is never stamped.** `prepare` skips the stamp for any version with a `-` or `+`, so
`release-notes.mjs extract <prerelease>` finds nothing (it accepts the version, and there is no
`[<prerelease>]` heading to match), and `main`'s `[Unreleased]` sections stay unstamped and ship
with the next ordinary release. A prerelease gets no GitHub release page in any case.

**Merging it back usually needs a conflict resolved.** `main` requires branches to be up to date, and
a new `[Unreleased]` section merged during or after the release is normally inserted directly above
the first stamped heading. Git treats those adjacent lines as one hunk, so `RELEASE_NOTES.md`
conflicts. Keep both sides: `main`'s new `[Unreleased]` sections as they are, and the release branch's
`[<version>]` headings. The version lines do not conflict, since only release PRs change them.
**Do not push to the release branch until the release has finished**, because `npm-sdk.yml` and
`docker-image.yml` check out the branch by name, not by commit.

**`delete_branch_on_merge` is on, so merging the PR deletes `release/v<version>`.** The tag keeps
the commit, `version_taken` checks the tag as well as the branch, and the release page's
"Release branch" line is the only thing left pointing at a name that no longer exists.

`scripts/release-notes.mjs` and `scripts/set-version.mjs` both have unit coverage —
`release-notes.spec.ts` and `set-version.spec.ts` — run by `build.yml`'s `lint_format` job on every PR
(`bun run test:scripts`). The notes spec also smoke-runs `extract` against the real
`RELEASE_NOTES.md`, so a heading it cannot read a title from fails the PR that wrote it rather than the
release that would silently drop the narrative; the version spec does the equivalent, asserting the
replace still finds the `"version"` field in all three real manifests at their real paths, so a
rename or a reformat that moved it out of reach fails a PR rather than a release. The release path
itself is dispatch-only and cannot be exercised in CI, so the shell in the changed steps was verified
out of band instead: each `run:` body extracted from the YAML and executed against a scratch repo with
`npm`, `npx` and `curl` stubbed, which is how the version-resolution steps, the server-pin
substitution, the `--host` check, `prepare`'s stamp and `release-sync.yml`'s comparison were each covered. That harness is a one-off and
is **not** in this repo — it is worth rebuilding rather than trusting, because a later edit to any of
those bodies has nothing automated behind it.

Merging the release PR cannot collide with `publish-packages`: it changes only
`packages/{sdk,app,server}/package.json` and `RELEASE_NOTES.md`, neither of which
`publish_indep_pkg`'s "main moved" guard watches for skills or create-malloy-package. The reason to
wait for the run to finish before touching the PR is the release branch itself (above), not this
guard.

npm and PyPI publishing both use **GitHub Actions OIDC trusted publishing**, not a stored token.
There is no `NPM_TOKEN` or `PYPI_TOKEN` in this repo and neither should be added back. Only the
Docker path uses a secret (`DOCKERHUB_TOKEN`).

### Docker image signing

`docker-image.yml`'s `merge` job signs the published manifest list with cosign, keyless, after
`imagetools create` pushes it, then verifies the signature before the job ends. Things that are not
obvious from the YAML:

- **The digest comes from `imagetools create --metadata-file`, not from a tag.** A signature binds to
  one digest; resolving `:latest` or the version tag afterwards would sign whatever the tag points at
  by then.
- **The certificate names `docker-image.yml`, not `release.yml`.** Fulcio's subject for GitHub
  Actions is `job_workflow_ref`, the reusable workflow, so the identity a consumer verifies is
  `https://github.com/malloydata/publisher/.github/workflows/docker-image.yml@refs/heads/main`.
  Renaming or moving `docker-image.yml` changes that identity and breaks every consumer pinned to
  it; treat the filename as a public contract.
- **`id-token: write` has to be granted twice.** The `merge` job requests it and `release.yml`'s
  `publish-docker` job must grant it, since a called workflow cannot hold more than its caller
  grants. Dropping it from the caller fails `publish-docker` before any job starts.
- **One signature covers the attestations.** The manifest list references each platform image and
  its BuildKit SBOM and provenance attestation manifests by digest, so signing the list binds all of
  them. The attestations themselves are not separately signed.
- **A signing failure fails the release after the tags are already pushed.** The image is public
  but unsigned, and `gh-release` does not run. Re-run the failed jobs: `imagetools create` re-pushes
  the same list from the same platform digests and the signing steps run again. A Sigstore outage
  (Fulcio or Rekor) shows up here the same way.
- **The `@refs/heads/main` in the identity is what makes a branch dispatch distinguishable.**
  `release.yml` has no ref guard (see *Hardening that is not in place yet*), so a release dispatched
  from a branch is signed with that branch's ref. The in-workflow verify accepts any ref and prints
  the identity it found; consumers pinning `@refs/heads/main` reject the branch-built image.

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
- **`release.yml` dispatches the two independently-versioned npm packages, it does not call them.**
  `publish-packages` triggers `skills-npm.yml` and `create-malloy-package-npm.yml` through the
  Actions API, so each runs as its own top-level run and keeps its own npm entry naming its own
  file. Converting either to `workflow_call` would force its npm entry to be re-pointed at
  `release.yml` and would immediately break its manual dispatch. That is a one-way door, because
  there is only one entry per package.
- **PyPI's matching rule is the opposite of npm's.** PyPI matches the file that defines the
  uploading job, and refuses a reusable workflow outright, so the Python SDK is not dispatched from
  `publish-packages` but uploaded by `release.yml`'s own `publish-python-sdk` job. See *Publishing
  to PyPI*.
- **`timeout-minutes` on `publish-packages` tracks the package count.** It is two poll budgets plus
  margin now. Keep it above N x `POLL_BUDGET_SECONDS`, or the LAST package timing out gets killed by
  the job cap before it can print which package failed — the one diagnostic that says whether a
  publish happened.
- **Dispatch the children on `main`, never on the release branch.** `prepare` bumps only sdk/app/
  server, so a release branch's copies of these two packages are identical to `main`, and both
  children guard on `github.ref == 'refs/heads/main'`. Dispatched on a release branch their publish
  jobs skip, and a skipped job reports success, so a release would go green having published nothing.
- **Order is forced: skills before create-malloy-package.** The scaffolder pins its skills dependency
  from the version skills just published (or skills' npm `latest`, when skills was skipped) and then
  asks the registry whether that range resolves, so skills has to be decided, and confirmed on npm if
  it published, before the scaffolder is dispatched. Dispatching both at once fails the scaffolder.
- **OIDC auto-enables provenance, which requires `repository.url`** in each published `package.json`.
  A package missing it fails to publish with a `422 ... provenance` error. This is the likely failure
  when adding a new package.
- **`main` carries the released sdk/app/server version only once that release's PR merges.** Before
  release branches were merged back, `main` sat 41 patches behind npm. `release-sync.yml`
  reports when it lags, but nothing stops a dispatch, so `prepare`'s floor is
  still `max(npm latest, main declared)` and fails closed when the registry will not answer. **Do
  not "simplify" that to reading the file** — an unmerged release PR leaves the file behind npm, and
  the max is correct either way. `prepare` then walks forward from the floor until it finds a
  version whose release branch and tag are both free.
- **Publish steps need scripts enabled.** `prepack` is what copies `skills/` into the skills package
  and what builds the scaffolder's `dist/`. A publish run with `--ignore-scripts`, or with
  `ignore-scripts=true` in the publisher's own npmrc, ships a package with the contents missing and
  exits 0. The derived `SERVER_VERSION` now rides on this too: the substitution edits `src/scaffold.ts`,
  and only `prepack` turns that into the `dist/index.js` npm actually ships (`dist/` is gitignored, so
  there is no stale copy to fall back to). Nothing asserts the substituted value reached `dist/` —
  scripts-disabled would ship no `dist/` at all rather than an old pin, which is why this is a footnote
  and not a check.

### Publishing to PyPI

`malloy-publisher-sdk` is published by two jobs in `release.yml`, at the same level as `publish-npm`
and `publish-docker`, and not by `publish-packages`:

- **`build-python-sdk`** calls `python-sdk.yml` on the release branch, which carries the version
  `prepare` stamped into `pyproject.toml`. Its `build` job generates the client, runs the tests,
  builds the wheel and sdist, verifies them and uploads `dist/` as the `python-client-dist`
  artifact. It holds no publishing credential.
- **`publish-python-sdk`** is a plain job in `release.yml`, with `environment: pypi` and
  `permissions: id-token: write` and nothing else. It downloads the artifact, reads the name and
  version from the wheel's own `METADATA`, refuses a version that is not this release's (a stamp
  that did not reach the wheel), skips one already on PyPI (a re-run after the upload landed), and
  otherwise uploads with `pypa/gh-action-pypi-publish`, pinned by SHA. It runs no repository code
  and installs nothing.

Both skip a prerelease or a `+build` version of sdk/app/server, by the same `[-+]` test `prepare`
uses for the release-notes stamp; `publish-python-sdk` admits only a plain major.minor.patch.
Neither is in `gh-release`'s `needs`, for the same reason `publish-packages` is not.

**The upload waits for `publish-npm` and `publish-docker`; the build does not.** A PyPI filename
can never be reused, so a release abandoned after a failed npm or Docker publish, in favour of the
next version, would otherwise leave `malloy-publisher-sdk==X` on PyPI for a server `X` that never
shipped. With the wait, PyPI only ever holds versions whose server published. The build runs beside
npm and Docker, so the wait costs only the upload itself.

**The smoke test is `build.yml`'s, on every PR, not the release's**, the same split as the Docker
image (`build.yml`'s `docker_smoke_test` on every PR; `docker-image.yml` only builds and pushes).
`python_sdk_build` calls `python-sdk.yml`, the same build the release runs, then
`python_sdk_smoke_test` installs that wheel into a clean venv for every Python the package declares,
starts a server built from the PR, and drives it through the SDK
(`packages/python-client/scripts/smoke-test-wheel.py`): status, environments, packages, every
model, the compiled models and a query, each answer parsed into the generated models. Every PR,
not only SDK ones, because a server change can break the SDK's wire format without touching
`api-doc.yaml`. So a green `main` has built, verified and exercised the same kind of artifact a
release uploads, and the release does not wait on a server build to upload it.

**Trusted publishing (OIDC), not a token.** There is no `PYPI_TOKEN`, and one should not be added
back. The trusted publisher on PyPI is: project `malloy-publisher-sdk`, owner `malloydata`,
repository `publisher`, workflow **`release.yml`**, environment **`pypi`**. Three things about it are
not guessable:

- **PyPI matches the file that defines the uploading job, not the top-level workflow.** Warehouse
  looks the publisher up by the token's `job_workflow_ref` claim (`lookup_by_claims` in
  `warehouse/oidc/models/github.py`). That is the opposite of npm's rule (see *Rules that bite*
  below), which is why the npm entries for sdk/app/server name `release.yml` while the `npm publish`
  runs in `npm-sdk.yml`. For PyPI, the entry names `release.yml` because the upload job is written in
  `release.yml`. Moving `publish-python-sdk` into another file breaks publishing until the PyPI entry
  is changed to match.
- **The upload cannot live in a reusable workflow.** PyPI does not support trusted publishing from
  one, which is why this is two jobs where `publish-npm` and `publish-docker` are one `uses:` each.
  It is also why the upload is not a job in `python-sdk.yml` called from here.
- **The split is the security model, not tidiness.** `id-token: write` lets any step in a job mint
  an upload token. `build` runs uv, the generator, black, ruff, pyright, pytest and the wheel's
  runtime dependencies; in a job holding id-token a compromise of any of them could upload anything
  under the project's name. PyPI's own guidance is this shape: build in one job, publish the artifact
  from another that does nothing else.

**The environment is the boundary.** `release.yml` has no ref guard (see *Hardening that is not in
place yet*), and PyPI does not check the ref. It does refuse a token whose `environment` claim is
not `pypi` (case-insensitively), so a branch copy of `release.yml` that drops `environment:` cannot
publish. Restrict the `pypi` environment's deployment branches to `main`, with no required
reviewers, and a release dispatched from any other branch never starts the job either. GitHub
creates the environment on first use if it does not exist, with no restrictions at all, so **create
it, restricted, before registering the pending publisher**. In the other order, anyone with write
access could dispatch `release.yml` from a branch, have GitHub create `pypi` unrestricted on the
spot, and publish without review on `main`. An admin creates it with:

```bash
gh api -X PUT repos/malloydata/publisher/environments/pypi \
  -F 'deployment_branch_policy[protected_branches]=false' \
  -F 'deployment_branch_policy[custom_branch_policies]=true'
gh api -X POST repos/malloydata/publisher/environments/pypi/deployment-branch-policies \
  -f name=main -f type=branch
# Expect one policy, `main`:
gh api repos/malloydata/publisher/environments/pypi/deployment-branch-policies \
  --jq '.branch_policies[].name'
```

A custom policy naming `main`, not "protected branches": that option admits every branch with a
protection rule, which is wider than `main`.

#### What the first publish needed, and why it took this long

The first three attempts (September 2026, under the old dispatched `python-sdk.yml` publish job)
failed with `403 Forbidden from https://upload.pypi.org/legacy/`. #1257 paused the publish and
listed token scope as the likely cause. The log says otherwise: the step's environment shows
`TWINE_PASSWORD:` **empty**, where a set secret prints as `***`, and the repository has no
`PYPI_TOKEN` secret at all. twine sent an empty password. Trusted publishing removes the secret
entirely.

Preconditions CI cannot check, for the release that carries the first upload, in this order:

- **The `pypi` environment, restricted to `main`**, created as above before anything on PyPI
  names it.
- **A pending publisher whose project name matches exactly**: `malloy-publisher-sdk`, `malloydata`,
  `publisher`, `release.yml`, `pypi`, with the environment named rather than left as "(Any)", which
  would make the restriction above irrelevant. A pending publisher creates the
  project on first upload, under the name it was registered with. A pending publisher registered as
  `malloy-publisher` mints a token without complaint and the upload then fails with "Non-user
  identities cannot create new projects". `publish-python-sdk` checks the wheel's name before
  uploading for that reason.
- **The name is not reserved by the pending publisher.** PyPI: a pending publisher "does not create
  a project or reserve a project's name until it is actually used to publish". `malloy-publisher-sdk`
  404s today; anyone can take it before the first upload.
- **The first upload is the release's version**, because `prepare` stamps it into
  `pyproject.toml` (lockstep with sdk/app/server). `main` declares the last released version, as
  the three `package.json` files do, and the release PR merges each new one back. `Release sync`
  (`release-sync.yml`) fails a PR whose `pyproject.toml` version differs from
  `packages/sdk/package.json`'s, so the two cannot drift apart on `main` the way they did when
  #1496 moved sdk/app/server to 0.10.3 while this file still said 0.10.2.

#### Packaging traps the guards exist for

**The publish path did not build a usable package, and that was invisible because it could never
run.** Two independent reasons, both fixed, both worth knowing because either one alone ships a
wheel that installs cleanly and cannot be imported:

- The old job did `pip install build && python -m build` on a checkout. Almost none of this package
  is in git — `packages/python-client/.gitignore` is `malloy_publisher_sdk/*` with
  `!malloy_publisher_sdk/__init__.py` — so a checkout holds exactly one tracked python file, and that
  file's line 6 imports `.client`, which is generated from `api-doc.yaml` and never committed.
  `build` runs `scripts/build-python-sdk.sh` with `BUILD_PACKAGE=true`, which is the one script that
  knows how to produce this package.
- **Running the generator is still not enough**, which is the part that is easy to miss. The build
  backend is hatchling, and hatchling's default file selection *consults `.gitignore`* — so the
  generated client was excluded from the artifacts even when sitting on disk. Measured: a build with
  `client.py` and `api/` present shipped a wheel containing exactly `__init__.py`. `pyproject.toml`
  sets `[tool.hatch.build] ignore-vcs = true`, plus explicit `packages`/`include` for the wheel and
  sdist targets so that switching the ignore rules off does not start packing `.venv/`, `dist/` and
  `__pycache__`. An explicit `packages` **alone does not fix it** — the ignore rules still apply.

Two more, found by the smoke test the first time it ran:

- **`requires-python` said `>=3.9`, and the wheel cannot be imported on 3.9.** The generated client
  annotates with PEP 604 unions (`Error | list[Package] | None`) and no
  `from __future__ import annotations`, so 3.9 raises `TypeError: unsupported operand type(s) for |`
  on the first import, after pip installed it without complaint. Every job here ran 3.11. The floor
  is now 3.10, and the smoke test runs the floor plus every `Programming Language :: Python :: 3.N`
  classifier, read from `pyproject.toml`, so the metadata cannot claim a version nothing ran.
- **`__version__` was never set.** `build-python-sdk.sh` step 5 was a sed replacing an existing
  `__version__ = "..."` line in the generated `__init__.py`, which has none; sed matched nothing and
  exited 0. It now appends the line and asserts it, and the smoke script checks `__version__` against
  the installed distribution's metadata.

**More tools read `.gitignore` here, and it is worth knowing before you trust a green build.**
`build-python-sdk.sh` runs `black`, `ruff` and `pyright` over the generated client at steps 6 and 7,
and **two of the three silently skip all of it** inside a git checkout, because both honour
`.gitignore`:

- **ruff.** Measured on one tree with one ruff (0.16.4): **929 errors outside a git repo, `All checks
  passed!` inside one**, nothing else changed.
- **black.** Its own line in any green run of this workflow is `7 files left unchanged` — which is
  exactly the tracked set (`malloy_publisher_sdk/__init__.py` plus the six files under `tests/`) and
  **zero** files from the generated tree.
- **pyright is the exception**: it resolves into the generated code regardless, and reports against
  its types (`list[Connection] | Unset`) from `tests/`. Note step 7 ends in `|| true`, so those
  findings never fail the build either — a green run here has four outstanding pyright errors.

CI is a checkout, so it reports the clean numbers and the release is not at risk; but "lint and format
ran" does not mean the generated client was linted or formatted. Left as-is deliberately — turning it
on surfaces ~929 findings and is a separate decision — and recorded because it is the same trap as the
packaging bug above, one directory that is deliberately gitignored while being the package's entire
content, and now three independent tools quietly deciding what that means.

The guards against all of this run in `python-sdk.yml`'s `build` and `build.yml`'s smoke test, before
the artifact can reach
`publish-python-sdk`: `Verify the wheel is importable before uploading it` installs the built wheel
into a throwaway venv and imports it from `/tmp`, and checks the sdist contains the generated client;
the smoke test, on every PR, uses it against a real server on every declared Python. They assert the
artifact rather than the intent, because "we remembered to regenerate" is what failed, and they run
before the upload — a check afterwards would only name the version you had burned.

**Running `build-python-sdk.sh` locally edits tracked files.** It regenerates
`malloy_publisher_sdk/__init__.py` without its license header, black and ruff may reformat `tests/`
with a different local version, and step 9's `rm -rf build/` deletes the tracked
`build/lib/mcp_client.py`. Restore those before committing (`git checkout --` on the three paths).

### One package that looks publishable and is not

**`@malloydata/publisher-cli` at `0.0.1` is unpublished but not `private`.** It is in a different npm
scope (`@malloydata`, not `@malloy-publisher`) and no workflow publishes it, so it looks shippable
while nothing ships it. Decide separately whether to mark it `private: true` or bring it into
`publish-packages` — and if the latter, it needs the whole *Adding a published package* dance below,
plus a version-decision path like skills and create-malloy-package's, or a place in `prepare`'s
lockstep stamp like `malloy-publisher-sdk`'s.

For completeness: `api-doc.yaml`'s `info.version: v0` is the REST API version behind the `/api/v0`
path prefix, not a package version. It is unrelated to any of this and should not be bumped with a
release.

### Adding a published package

Publish the first version by hand: npm will not accept a trusted-publisher entry until the package
exists. Then add the `repository` field, register the trusted publisher naming that package's own
workflow file, and wire it into `release.yml`'s `publish-packages` so a release picks it up.

### Recovering a failed release

**A red `build-python-sdk` or `publish-python-sdk` is retried with "Re-run failed jobs"**, never by
re-running the release. The publish job asks PyPI first and skips a version that is already there,
so a retry after an upload that did land is a no-op; a retry of the publish job alone reuses the
verified artifact (`python-client-dist`, kept 7 days).

The two independently-versioned npm packages are safe to retry **through `publish-packages`**: that job
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
instead. A release waits for this itself (see below), but a hand dispatch does not: confirm the server
version is on npm (`npm view @malloy-publisher/server dist-tags.latest`) before dispatching the
scaffolder by hand.

It fires only when `main` moved **under the paths that package's published content is built from**, not
on any movement at all: the second package is checked after the first has finished a full
dispatch-and-wait, so a bare sha comparison failed it whenever anything merged inside that window,
including changes that could not affect it. `create-malloy-package` no longer watches
`packages/skills/package.json`: its publish job now takes the skills version to depend on as a
dispatch input (`skills_version`, set from what skills just published, or its npm `latest` when
skills was skipped) rather than reading that file from `main`, so a skills publish with no
scaffolder-relevant change no longer retriggers this guard. Every uncertain answer still aborts: an
unanswered compare API, and a diff at that API's 300-file cap, where the list may be truncated.

**Two kinds of skip are how a green release ships nothing (or ships less than expected), and both
name themselves in the job summary.** Read that summary rather than the run's green tick if you
expected a publish.

1. **Nothing was decided to publish.** `independent-version.mjs` can return `skip` for a real reason,
   not a bug: skills skips when nothing in its watched paths changed since npm `latest`'s `gitHead`,
   and the scaffolder skips only on a re-run where its published `publisherServer` already equals
   this release. Either skip is expected and reported by name in the summary; it is not the same
   failure mode a forgotten bump used to be, because there is no bump to forget any more.
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

That `needs:` is not enough on its own, because npm can take minutes to show a version after
`npm publish` returns. In 0.7.0, `publish-npm` finished at 19:09 and `latest` read 0.7.0 only at
19:16, so the scaffolder read 0.6.0. So `publish-packages.sh` also waits, up to 15 minutes, for the
server's `latest` to read the version the release shipped before it dispatches the scaffolder
(`wait_for_server_latest`). If `latest` never gets there, the scaffolder is not dispatched, `set -e`
ends the job, and the job says to re-run it once `latest` reads the new version.

That wait proves only the release runner's view of npm. The registry is cached per CDN edge
(`cache-control: public, max-age=300`), so the scaffolder's runners can read the previous `latest` for
a few more minutes. So the release also dispatches the scaffolder with `server_version` set to the
version it shipped. Both scaffolder jobs wait for their own `latest` to read it, and the pin step
refuses anything else. A hand dispatch leaves the input empty and pins `latest` as before.

The `--host` check runs `npx @malloy-publisher/server@<pin>` from an empty temporary directory, not
the checkout. In the checkout, npx uses the workspace's own unbuilt `packages/server` whenever the pin
equals the version that file declares, and fails with `malloy-publisher: not found`. That failed the
0.5.1 and 0.7.0 scaffolder publishes. In 0.7.0 it was also the only thing that stopped a scaffolder
pinned to the previous server from shipping.

sdk/app/server are not re-runnable at the same version today. `prepare` walks the version forward
whenever a release branch or tag exists, so a release that fails after `npm-sdk.yml` has published
cannot be resumed and the retry burns three new versions. npm versions are immutable, so there is no
repair beyond moving forward.

### Hardening that is not in place yet

There are **three** npm trusted-publisher entries, not two: `release.yml` (for sdk, app, and server),
`skills-npm.yml`, and `create-malloy-package-npm.yml`. Any hardening has to cover all three.
PyPI's one entry (`release.yml`, for `malloy-publisher-sdk`) is bound to the `pypi` environment,
and that environment's deployment branches are restricted to `main` (created 2026-10-29, ahead of
the PyPI pending publisher; see *Publishing to PyPI* for why that order matters).

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
