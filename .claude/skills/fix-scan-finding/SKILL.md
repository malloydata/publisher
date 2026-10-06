---
name: fix-scan-finding
description: >-
  Fix a CRITICAL Trivy finding that is failing CI in this repo (a vulnerability, misconfiguration,
  or secret from security-scan.yml or image-scan.yml), or add, review, or retire an entry in
  .trivyignore.yaml. Drives the decision the gate forces: fix it upstream, or accept it with an
  expiry. Read it before the first scanner command or security-motivated dependency bump, including
  on a gate that is already green. Trigger on "CVE", "trivy", "trivyignore", "code scanning", "scan
  finding", "vulnerability", "image scan", or a red Trivy check on a pull request. NOT for choosing
  what CI scans or for routine dependency bumps with no finding behind them.
---
<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Fixing a security-scan finding

**Audience:** a contributor or agent working a CVE, Trivy, or code-scan finding in this repository --
fixing a red CRITICAL gate, or verifying a finding on a gate that is already green. No prior Trivy
expertise assumed.
**Prerequisites:** `trivy` on PATH (`brew install trivy`, or see the Trivy install docs), `bun` at
the version in the root `package.json` `engines`, and Docker with `buildx` for image findings. No
cloud credentials are needed -- everything here is local and read-only against public registries.

## What CI scans

| Workflow | Job | Reads |
|---|---|---|
| `.github/workflows/security-scan.yml` | `Trivy filesystem scan (vulnerabilities)` | every lockfile in the tree |
| `.github/workflows/security-scan.yml` | `Trivy config scan (Dockerfiles)` | the Dockerfiles only: Trivy's misconfiguration scanner does not read GitHub Actions workflows |
| `.github/workflows/security-scan.yml` | `Trivy secret scan` | the working tree |
| `.github/workflows/image-scan.yml` | `Trivy image scan (built from source, <platform>)` | the `Dockerfile` built from the PR, once per platform the release publishes (`linux/amd64`, `linux/arm64`) |
| `.github/workflows/image-scan.yml` | `Trivy image scan (published latest, <platform>)` | `ms2data/malloy-publisher:latest` on both platforms, weekly schedule and dispatch |

Each job uploads its full SARIF to Code Scanning, then runs a second scan scoped to CRITICAL with
`exit-code: 1`. CRITICAL fails the job, and the secret gate fails on HIGH as well: a committed
credential is an incident whatever its rating. GitHub's own "Trivy" Code Scanning check on a pull
request is set, in the repository's code scanning settings, to fail only on critical alerts. It
treats every alert in a file the PR touches as new, and a lockfile is one file, so at a high
threshold every dependency PR went red on alerts already open on `main`. If that check goes red on
high again, the setting was changed; that is the fix, not a lockfile edit. GitHub rates an alert
by the SARIF's CVSS score, not Trivy's label, so the two can disagree: a Trivy MEDIUM can show as
high, and a Trivy HIGH with CVSS 9.0 or more is critical to GitHub and still fails the check. HIGHs
still land in Code Scanning, and one with a reachable fix is still worth fixing. Every job reads
the same `.trivyignore.yaml`. On a pull request from a fork the SARIF upload is skipped (the token
is read-only), but the gate still runs, so read the job log there.

## The rule

**Prefer the upstream fix. Accept only what you cannot fix.** In order:

1. **Refresh the lockfile** if the manifest already allows a patched version. No ignore entry.
2. **Move the parent** that pins the vulnerable version, when a newer parent release in range fixes
   it. One move, N CVEs.
3. **Force the transitive version** with a root `package.json` `resolutions` entry only when 2 is
   genuinely blocked -- in range where possible, and only if it actually clears the finding
   (verify; see the traps below).
4. **Remove the component** from the image when nothing at runtime uses it.
5. **Accept with `expired_at`** when no reachable fix exists, with a `statement:` that is the review
   record.

Do not skip to 5 because it is quick. Do not do 2 or 3 without the delta check in step B.

## A. Establish the real finding set

Do not trust the GitHub Code Scanning alert list. GitHub maps Trivy's CVE severity onto its own
scale, so genuine CRITICALs can appear there as `high`. Always scan locally with the gate's flags:

```bash
trivy fs . --scanners vuln --severity CRITICAL \
  --ignorefile .trivyignore.yaml \
  --skip-dirs node_modules --skip-dirs .venv \
  --skip-dirs '**/dist' --skip-dirs '**/build' \
  --format json --quiet > /tmp/crit.json
jq -r '.Results[]? | .Target as $t | (.Vulnerabilities // [])[]
  | "\($t)\t\(.PkgName)@\(.InstalledVersion)\t\(.VulnerabilityID)\tfix=\(.FixedVersion)"' \
  /tmp/crit.json | sort -u
```

- **Quote the `**/...` globs.** Unquoted, zsh expands them, Trivy prints its usage text, and the run
  looks like a failure of the tree rather than of the command.
- **Confirm the scan actually covered files.** A Report Summary with no rows, or JSON with no
  `Results` key at all (what Trivy emits for a directory with no lockfiles), means nothing was scanned and `exit=0` proves nothing. Running from the wrong directory is
  the usual cause. A gate that scanned nothing is the one failure mode that looks exactly like
  success.
- **For image findings, build what CI builds.** CI builds and scans both `linux/amd64` and
  `linux/arm64`, with the DuckDB version derived from Malloy. A local build covers only the platform
  you ask for, and the per-architecture base images and prebuilt binaries differ enough that the two
  report different findings. Build the platform whose job is red (`linux/amd64` or `linux/arm64`),
  and pass `APT_REFRESH` as the CI builds do, or a stale cached `apt-get upgrade` layer can report
  Debian packages CI has already upgraded:
  ```bash
  docker buildx build --platform <platform> --load \
    --build-arg DUCKDB_VERSION=$(node scripts/duckdb-version.js) \
    --build-arg APT_REFRESH=$(date -u +%F) -t publisher:scan .
  trivy image publisher:scan --scanners vuln --severity CRITICAL --ignorefile .trivyignore.yaml
  ```
- **Run once more with `--include-dev-deps`.** Trivy skips devDependencies in Node lockfiles by
  default, and so does the gate. On this repo the flag has surfaced CRITICALs in dev tooling in the
  root `bun.lock` (`dompurify`, `ejs`, `shell-quote`) that the gate does not see. A
  finding that appears only with the flag is not gating, but it is real.

Then group by root cause before touching anything -- see
[references/root-cause-grouping.md](references/root-cause-grouping.md). Several CVEs against one
lockfile are usually *one* lagging parent, not several pins.

## B. Before any version move: the delta check

Never raise a pinned version or add a resolution without checking what the old version was
protecting. **If a pin carries a comment, that comment is the spec** -- read it, then verify whether
it still holds against the target version rather than assuming either way.

1. **Read the pin rationale.** A pinned version or `resolutions` entry often carries a reason in a
   nearby comment or in the commit that added it: `git log -S'"<pkg>": "<ver>"' -- package.json`.
2. **Check the version line, not only the fixed version.** A fix published only on a new major is
   unreachable when every parent release still declares the old major. Check what the parent's
   latest release declares, and what the registry actually has:
   ```bash
   npm view <pkg> dist-tags
   npm view <parent>@latest dependencies.<pkg>
   ```
   **Always check the latest public release before accepting a finding as unfixable.** The newest
   version on the registry may carry the fix even when the scanner's "Fixed Version" or your
   current major suggests otherwise, and a release may have shipped since the advisory was written.
   When the fix exists only on a release that is a breaking change (a new major, or a parent that
   has to move with it), do not decide alone: tell the user what the migration would involve --
   which APIs or behavior change, which files and consumers are affected, whether the impact is
   runtime or build-time only, and what testing it needs -- and let them choose between migrating
   now and accepting with an expiry until the migration is scheduled. orval is the worked example:
   npm had 8.38.0 with the fix, but forcing it broke the only consumer, so the evidence went to the
   user as a choice rather than a silent accept.
3. **Test the change against the real consumer**, empirically. When a resolution moves a consumer
   across a major, run the consumer's own call against both versions in scratch installs and diff
   the output. The worked example is in [references/ecosystems.md](references/ecosystems.md) (one
   package on two majors).
4. **Look for held-back companions.** Grep for comments referencing the pinned version -- a package
   "held back to track X" must move with X or it is silently mismatched.
5. **Check what consumes the output** if the move could change behavior at a boundary: generated
   code, a wire format, a file another tool reads. A patch bump usually cannot; a major can.

Then classify and act:

- **No behavioral impact** -> apply, build, test, done.
- **Impact found** -> state the specific impact, the blast radius, and the migration steps, and
  **get the user's approval before migrating**. Do not silently absorb a migration into a CVE fix.
  Present it as a choice (move and migrate now / review the diff first / investigate deeper / stay
  and accept) with the consequence of each.
- **Build-time-only impact** (codegen, test tooling) still needs approval, but say so plainly -- it
  is a much smaller ask than a runtime change, and mislabeling it either way wastes the decision.

## C. Apply the fix

Match the ecosystem; details and commands, including the Bun traps that make a targeted refresh
rewrite half the tree, are in [references/ecosystems.md](references/ecosystems.md).

Whatever you touch, the **stale-pin comment is part of the diff**. A pin comment that survives its
own removal becomes a lie the next reader believes -- rewrite it to describe the new state and the
invariant that still matters, not the change you made.

Run builds, image builds, and test suites with `run_in_background: true`; an image build for a
platform other than your machine's runs under emulation and takes minutes.

## D. Verify

The test suite is the oracle for "did I break it"; the scanner is the oracle for "did I fix it".
Both, in that order, and neither substitutes for the other.

1. **Re-run the step A scan and read what remains.** A move that clears four of five CVEs is a
   partial fix, and the fifth is the one that matters.
2. **Run what exercises the moved dependency:** `bun run test` for server dependencies;
   `bun install --frozen-lockfile && bun run generate-clients` in `packages/server/k6-tests` for its
   codegen; an image build of the red job's platform (step A) for anything in the `Dockerfile`.
3. **Do not trust a codegen exit code.** `bun run generate-clients` exits 0 even when it generates
   no files. Count the output files, and diff them against the previous version.
4. **For a Dockerfile change, confirm the component is gone from the built image,** for example
   `docker run --rm --entrypoint sh publisher:scan -c 'cd /publisher && bun why esbuild'`, then
   re-scan the image.

## E. If it cannot be fixed: accept it properly

An acceptance has three parts, and skipping any one of them is how a suppression becomes permanent by
accident:

1. **A path-scoped entry.** A bare `id:` suppresses the finding repo-wide, including in a lockfile
   added next month that inherits the same vulnerable dependency. `paths:` resolve against the scan
   root, which is the repository root: `packages/server/k6-tests/bun.lock`, not `k6-tests/bun.lock`.
   An entry with the wrong prefix parses fine and silently never matches. Verify the scoping holds:
   copy the affected `bun.lock` and `package.json` to a new directory, re-scan, and confirm the
   finding still fires there. Delete the copy.
2. **`expired_at`** on anything that will be fixed (`yyyy-mm-dd`; Trivy stops honouring the entry
   that day and the gate reds again). Omit it **only** for a genuine won't-fix, where an expiry would
   red the gate on a date certain for something nobody intends to change -- that is recurring noise,
   not a forcing function. A risk-accepted entry must name the compensating control, not assert
   safety.
3. **A `statement:`** that says the package and version, how it is reached, why the vulnerable path
   is not exercised here, and what upgrade retires the entry. The statement in `.trivyignore.yaml`,
   plus the PR description, is the review record -- an entry without one is an unexplained
   suppression. Follow the shape of the entries already in the file rather than inventing a new one.

**OS-package (Debian) findings in an image scan cannot be path-scoped.** They carry no package
path, so no `paths:` value matches them: tested on the built image with CVE-2019-16224
(`liblmdb0`), a bare `id:` suppressed it, while `paths: ["nonexistent"]`,
`paths: ["usr/share/doc/liblmdb0/copyright"]`, and `paths: ["var/lib/dpkg/status"]` all left it
firing. So an OS-package acceptance must be a bare `id:` entry. That is safe here because the
filesystem scan never reports OS packages, so the entry cannot hide a lockfile finding; to make up
for the missing scope, the `statement:` must name the Debian package and the image it was found in.
Prefer fixing these in the `Dockerfile` first (see [references/ecosystems.md](references/ecosystems.md)):
Trivy `Status: fixed` means `apt-get upgrade` clears it; `Status: affected` means Debian has no fix
yet, so first ask what installed the package (`apt-cache rdepends --installed <pkg>` inside the
image) -- removing an unused parent package is option 4 of the rule, and is how this repo's unfixed
`liblmdb0` and `libxml2` findings were cleared rather than accepted.

## The traps

- **A printed "Fixed Version" can be unreachable through the parent.** orval's 11 CRITICALs in
  `packages/server/k6-tests` are fixed only in 8.21.0+ (npm `latest` is 8.38.0), but
  `@grafana/openapi-to-k6` 0.3.2 and its latest, 0.4.1, both declare `orval ^7.5.0`. Forcing orval
  8.38.0 makes `bun run generate-clients` fail under both ("Cannot read properties of undefined", no
  files written). So the finding is unfixable on this line, not un-upgraded, and was accepted with
  `expired_at`. Always check `npm view <pkg> dist-tags` and test forcing the fixed version against
  the real consumer before accepting -- and before planning a move around it.
- **Forcing a version can look like a fix and not be one.** A resolution to the newest 7.x of
  orval cleared the one finding 7.x fixes and left the 8.x-only ones standing. Always re-scan after
  a resolution; never assume the version you chose covers every CVE you targeted.
- **`bun update <transitive-pkg>` adds it as a direct dependency.** Run against `basic-ftp` and
  `protobufjs`, it wrote both into the root `package.json` `dependencies` at their latest majors
  (6.2.1 and 8.8.0) instead of refreshing them in range. Use `resolutions` for a transitive package.
- **Deleting a lockfile entry does not re-resolve just that entry.** Removing stale entries from
  `bun.lock` and running `bun install --lockfile-only` re-resolved most of the tree,
  moving unrelated packages such as the whole AWS SDK. That is not a targeted fix.
- **Bun `resolutions` are top-level only.** A nested key such as `"snowflake-sdk/fast-xml-parser"`
  is accepted and silently ignored; the lockfile does not change.
- **Build-time-only dependencies are still reported.** A CVE in codegen or test tooling
  (`packages/server/k6-tests`, `e2e`, `examples/data-app` each have their own lockfile) is not in the
  image. It still fails the filesystem gate, so fix it or accept it with that reasoning stated -- do
  not argue it away without an entry.
- **A devDependencies-only CVE is invisible without `--include-dev-deps`.** Neither the gate nor
  the default local scan passes that flag. When sweeping one vulnerable chain repo-wide, re-run with
  it before concluding a lockfile is unaffected -- a clean default scan is not evidence the chain is
  absent, only that nothing in it is a production dependency.
- **A clean `bun.lock` scan can still hide nested copies.** After this repo's lockfile refresh
  reported zero HIGH, the image scan of the same tree still found nested `lodash` 4.17.21,
  `ip-address` 10.0.1 and `ws` 5.2.4 that the filesystem scan never listed. Scan the built image
  before calling a Node finding gone, or list every lockfile key still at the vulnerable version.
- **A production dependency's peers ship.** `@vitejs/plugin-react` in `packages/app` `dependencies`
  pulled `vite`, and through it `esbuild` (with a Go stdlib CVE in its binary), into the image
  despite `bun install --production`. `bun why <pkg>` inside the built image names the path.
- **Your architecture is not the whole gate.** CI scans the image on both published platforms, and
  a local build scans only the one you built; the other can carry findings yours does not. Build
  the red job's `--platform` before concluding an image finding is gone.
- **Yesterday's quiet CVE can red you today.** A newly published advisory against a version you
  already had will fail a gate that was green last week, with no change on your side. A red gate is
  not evidence your branch introduced the finding -- check the advisory date before hunting your diff.
  The weekly scheduled runs exist to surface exactly this.
- **Unfixed criticals are not exempt.** The gates do not set `ignore-unfixed`, so a critical with no
  upstream patch blocks like any other. When there is nothing to upgrade to, the answer is an
  explicit path-scoped `.trivyignore.yaml` entry with a `statement:` -- a reviewed acceptance, not a
  silent one.
- **Renaming a scan job changes its check name.** Status checks match by job name. If a maintainer
  has made a `Trivy ...` check required in branch protection, a rename leaves the old required check
  pending forever. Ask a maintainer before renaming one.

## Finishing

State plainly which findings are **fixed** (with the version that fixed them), which are **accepted**
(with the expiry and why no fix is reachable), and which are **still open**. If a change needed
approval and you got it, say what was approved. Never report a finding as fixed on the strength of a
scan that covered no targets.

### The handoff is not done until CI is green

A green local run is necessary and not sufficient. A dependency move changes the resolved tree for
every consumer, and CI is the only place the image is built and scanned on both published
platforms the way the gate runs it, and the only place the full build and test matrix runs against the new lockfile. Local
runs also use your machine's `node_modules` and Docker cache, which can hold artifacts CI resolves
differently.

So end every fix by handing the user the PR step and **waiting for CI**:

1. Ask the user whether they want you to push the branch and open a PR against `main`, or would
   rather do it themselves. Give the exact commands either way, for example:
   ```bash
   git add <changed files>
   git commit -m "fix(deps): <what moved and which findings it clears>"
   git push -u origin <branch>
   gh pr create --repo malloydata/publisher --base main --title "<title>" --body-file <file>
   gh pr checks <pr-number> --repo malloydata/publisher --watch
   ```
   From a fork, push to the fork and open the PR against `malloydata/publisher` `main`.
2. Wait for the **`Trivy ...` checks** to pass on that PR. That is the gate the work exists to clear,
   and it re-runs against the PR head with `.trivyignore.yaml` as committed.
3. Wait for the **build and test jobs** to pass as well. They exercise the moved dependency; the
   scan does not.
4. If any of those go red, the fix is not done: read the failure, and treat a break in a
   *different* package as the expected shape of a shared-dependency regression rather than an
   unrelated flake.

Report the fix as **verified locally, pending CI** until those checks report. Do not describe a
dependency move as safe or complete on local evidence alone.
