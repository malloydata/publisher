<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Per-ecosystem fix mechanics

Lookup, not required reading. Find your ecosystem, apply, then return to
[SKILL.md](../SKILL.md) step D to verify.

Run every build, image build, and test command with `run_in_background: true`.

## Bun lockfiles (the root workspace)

The root `package.json` declares `workspaces: ["packages/*"]`, so one root `bun.lock` covers
`packages/server`, `packages/app`, `packages/sdk`, and the rest. Every command below was run against
this repository with Bun 1.3.13.

Check the manifest before deciding anything -- often the range already permits the fix and only the
lockfile is stale:

```bash
grep -n '"<pkg>"' packages/*/package.json package.json   # declared range, and who declares it
grep -n '"<pkg>' bun.lock                                 # resolved version, and which entries depend on it
```

`--lockfile-only` rewrites `bun.lock` without installing anything, which keeps each experiment
cheap. Copy `bun.lock` and `package.json` aside before each attempt so a bad result is one `cp` to
undo, and read `git diff --stat` after every step: the size of the diff is the first sign that a
command did more than you asked. Do not hand-edit a lockfile.

### A direct dependency behind its range

Run `bun update` in the workspace that declares it:

```bash
cd packages/server && bun update <direct-dep> --lockfile-only
```

It moves the resolved version within the range and also rewrites the declared floor
(`^4.7.8` -> `^4.7.9`). Keep that rewrite: it documents the minimum the fix needs.

### A transitive dependency behind its range

Add an in-range entry to the root `package.json` `resolutions` -- the mechanism this repo already
uses (`esbuild`, `pg`, the `@malloydata/*` pins) -- then refresh:

```json
"resolutions": {
  "basic-ftp": "^5.3.1",
  "protobufjs": "^7.5.5"
}
```

```bash
bun install --lockfile-only
```

Choose the floor from the fixed version and keep it inside every consumer's declared range, so no
consumer is moved across a major. Those two entries changed 42 lockfile lines.

Two approaches that look equivalent and are not:

- `bun update <transitive-pkg>` adds the package to the root `dependencies` at its latest major
  (`basic-ftp` 6.2.1, `protobufjs` 8.8.0), rather than refreshing it in range.
- Deleting the stale entries from `bun.lock` and running `bun install --lockfile-only` re-resolves
  most of the tree (about 1,900 changed lines; the AWS SDK moved from 3.962 to 3.1142).

Bun `resolutions` are **top-level only**. A nested key such as `"snowflake-sdk/fast-xml-parser"` is
accepted and silently ignored; the lockfile does not change. There is no per-consumer override.

### One package on two majors

A root resolution applies to every copy, so it can only be used when every consumer tolerates the
forced version. `fast-xml-parser` sat on two majors: 4.x via `snowflake-sdk` 2.3.1 (exact-pinned by
`@malloydata/db-snowflake`, so it cannot move without a Malloy bump), and exact 5.2.5 via
`@aws-sdk/xml-builder`.

1. **Reduce the problem first.** Move any consumer that has a newer release in range.
   `@google-cloud/storage` 7.22.0 had moved to `fast-xml-parser ^5`, so updating it in
   `packages/server` removed one 4.x consumer.
2. **Test each remaining consumer's actual call against both versions.** `snowflake-sdk` uses
   `XMLValidator` and `XMLParser` in `dist/lib/global_config.js`; `@aws-sdk/xml-builder` calls the
   parser with its own options in `dist-cjs/xml-parser.js`. Copy each call, with each package's
   exact options, into a script; run it against both versions in two scratch installs
   (`npm i fast-xml-parser@<ver>` in two directories); and diff the output across representative
   inputs. Identical output on 16 `snowflake-sdk` cases and 6 AWS response shapes is what justified
   the root resolution `"fast-xml-parser": "^5.5.6"`.
3. **Record the evidence in the PR description.** The resolution alone does not explain why the
   major move is safe.

### A production dependency that should not ship

`bun install --production` skips devDependencies but still installs the peers of production
dependencies. `@vitejs/plugin-react` in `packages/app` `dependencies` pulled `vite` and `esbuild`
(whose Go binary carries a stdlib CVE) into the image; `bun why esbuild` inside the image showed the
path. Moving the build-time package to `devDependencies` removes it and its peers from the image.
Confirm nothing imports it at runtime first: grep outside `node_modules` and `dist`.

## Standalone lockfiles

`packages/server/k6-tests`, `e2e`, and `examples/data-app` each have their own lockfile and are not
root workspaces. They fail the filesystem gate but never reach the image. Put `resolutions` in
**that directory's** `package.json` and run `bun install --lockfile-only` there. Then run what
consumes it; for `packages/server/k6-tests`:

```bash
cd packages/server/k6-tests && bun install --frozen-lockfile && bun run generate-clients
```

`bun run generate-clients` exits 0 even when it writes no files, so count the generated files. For a
codegen tool, also diff the generated output against the previous version: orval 7.18 -> 7.21
changed the generated clients from `import http from "k6/http"` to `import * as http from
"k6/http"`, which was verified safe with a one-line `k6 run` script printing `typeof http.get`.

Before accepting a finding here, confirm the fix is unreachable, not merely un-upgraded:
`npm view <pkg> dist-tags`, `npm view <parent>@latest dependencies.<pkg>`, and a real attempt at
forcing the fixed version through the consumer. The orval entries in `.trivyignore.yaml` are the
worked example.

## pip

If a finding lands in `packages/python-client`, raise the pin in its `pyproject.toml`, re-resolve,
and re-scan; the same rules apply.

## Dockerfile / config findings (`DS-*`, `AVD-DS-*`)

These are misconfigurations, not CVEs -- fix the instruction rather than a version. Common criticals
are a missing `USER`, an unpinned base tag, or added capabilities. The `Trivy config scan
(Dockerfiles, Actions)` job also reads the GitHub Actions workflows. Re-run locally with:

```bash
trivy config . --severity CRITICAL --ignorefile .trivyignore.yaml
```

## Image-layer findings

Findings that exist only in the built image, not in any lockfile. Build and scan `linux/amd64` as
CI does (SKILL.md step A) before concluding anything.

- **Debian packages frozen in the base layer.** The `oven/bun` base image's Debian packages lag
  behind Debian's own fixes. The fix is `apt-get upgrade` in the `Dockerfile`'s base stage
  (`base-deps`). A Trivy `Status: fixed` means an upgrade clears it (`bind9-dnsutils`,
  `libgnutls30t64`, `perl*` here).
- **Unfixed Debian packages (`Status: affected`): ask what installed them before accepting.**
  Debian has no fix yet, so no upgrade helps, but the package may be there only because of a parent
  nothing uses. Inside the image:
  ```bash
  docker run --rm --entrypoint sh publisher:scan -c 'apt-cache rdepends --installed <pkg>'
  ```
  `liblmdb0` (CVE-2019-16224, -16225, -16227) and `libxml2` (CVE-2026-6653) reached the image only
  via `bind9-libs` <- `bind9-dnsutils` (`dig`, `nslookup`); a grep found no caller in the server, so
  dropping `dnsutils` from the `Dockerfile`'s `apt-get install` fixed all four. That is option 4 of
  the rule (remove the component). Accept only what a runtime package genuinely needs, and then as a
  bare `id:` entry, because `paths:` never matches an OS package (SKILL.md step E).
- **Tools installed alongside a runtime that never uses them.** NodeSource `nodejs` bundles npm at
  `/usr/lib/node_modules/npm` with its own copy of `tar` (6.2.1 was a CRITICAL). The server runs
  under Bun and never calls npm or npx, so the fix is to remove npm from the image, not to accept
  it.
- **Prebuilt Go binaries report `stdlib` CVEs.** A Go binary is scanned against the Go version it
  was compiled with, so the finding moves only when the binary is rebuilt upstream. Find which
  package ships the binary (`bun why <pkg>` inside the image) and ask whether the image needs it at
  all -- the `esbuild` case above was fixed by removing it.

After any `Dockerfile` change, confirm the component is actually gone from the built image, then
re-scan the image.

## Secret findings

A committed credential is not fixed by an ignore entry. Rotate the credential first, then remove it
from the tree; history rewriting is a separate decision to raise with a maintainer.

A finding in a **local, gitignored** file (for example a credentials JSON or a `.env`) is not in the
repo at all -- CI checks out tracked content only, so it fails locally and passes in CI. Confirm with
`git ls-files --error-unmatch <file>` before treating it as real, and use `--skip-files` for the
local run rather than adding an ignore entry for something that was never committed.
