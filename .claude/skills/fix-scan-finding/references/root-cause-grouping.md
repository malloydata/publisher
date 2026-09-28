<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Grouping findings by root cause

Read this after establishing the finding set, before fixing anything. The point is to avoid making N
changes where the tree has one cause -- and to avoid the reverse error of assuming one cause when the
CVEs sit on different version lines.

## Why grouping comes first

Almost every dependency CVE is **transitive**: the vulnerable package is not declared in any
`package.json`. It arrives through a parent's dependency range, an exact pin inside a parent, a
codegen tool's own tree, or a base image. Fixing the *declared* thing is usually the wrong move
because there is nothing declared to fix.

In this repository a finding has one of four origins, and each is fixed in a different place:

| Origin | How it shows in the scan | Fixed in |
|---|---|---|
| Root workspace lockfile | `Target: bun.lock` (filesystem scan), Node.js rows (image scan) | the workspace `package.json`, or root `resolutions` |
| A standalone lockfile | `Target: packages/server/k6-tests/bun.lock`, `e2e/...`, `examples/data-app/...` | that directory's own `package.json` |
| Debian base layer | an OS-package target in the image scan | the `Dockerfile` base stage |
| A binary the `Dockerfile` installs or a package ships | a Go binary target, or a path such as `/usr/lib/node_modules/npm` | the `Dockerfile` step or the dependency that brings it in |

Standalone lockfiles are outside the root workspace, so nothing in them reaches the image. They
still fail the filesystem gate.

## How to group

1. **Dump to JSON and aggregate by `(Target, PkgName, InstalledVersion)`.** The same package at the
   same version in the filesystem scan and the image scan is one finding with two reports. Repeated
   versions across packages from the same upstream project are the signature of one parent.
2. **Find who declares it.** In `bun.lock`, each package entry lists its own dependencies; search for
   the vulnerable name inside other entries to find the edge, then check whether that parent is a
   workspace's direct dependency or itself transitive. Inside a built image, `bun why <pkg>` prints
   the whole path. Whether the parent is a devDependency, a production dependency, or a peer of one
   determines whether it can reach the image.
3. **Check whether the manifest already allows the fix.** If `package.json` requests `^4.7.8` and
   the fix is 4.7.9, there is no version decision to make -- only a stale lockfile.
4. **Check for exact pins.** An exact version inside a parent (`@aws-sdk/xml-builder` pins
   `fast-xml-parser` 5.2.5) means no refresh of the child can move it. Either the parent moves or a
   resolution forces the child.

## Worked example

The first run of the gates on this repository reported dozens of CRITICALs across the lockfiles and
the image, and they reduced to eight causes:

| Cause | Fix |
|---|---|
| Direct server dependencies behind their declared range | `bun update <dep> --lockfile-only` in `packages/server` |
| Transitive packages behind their range (`basic-ftp`, `protobufjs`) | root `resolutions`, in range (`^5.3.1`, `^7.5.5`) |
| `fast-xml-parser` on two majors (4.x via `snowflake-sdk`, exact 5.2.5 via `@aws-sdk/xml-builder`) | move `@google-cloud/storage` in range, test both remaining consumers against both versions, then one root resolution `^5.5.6` |
| orval 7.x via `@grafana/openapi-to-k6` in `packages/server/k6-tests` | fixes are 8.21.0+ only and no parent release admits 8.x; accepted with `expired_at` |
| Debian packages frozen in the `oven/bun` base layer, Trivy `Status: fixed` (`bind9-dnsutils`, `libgnutls30t64`, `perl*`) | `apt-get upgrade` in the `base-deps` stage |
| Unfixed Debian CVEs (`Status: affected`) in `liblmdb0` / `libxml2`, via `dnsutils` -> `bind9-libs` | `dnsutils` removed from the `Dockerfile`'s `apt-get install` (nothing calls `dig` or `nslookup`) |
| npm's bundled `tar` 6.2.1, from NodeSource `nodejs` | npm removed from the image |
| Go stdlib in the `esbuild` binary, via `@vitejs/plugin-react` | `@vitejs/plugin-react` moved to `devDependencies` in `packages/app` |

Pinning each package individually would have produced a larger, less correct diff: pins drift
silently from the parents that pull the packages in, and several of these findings were not fixed by
any version change at all.

**A major version on a transitive dependency is not automatically a migration.** Measure the real
surface before deciding: find every call site the consumer actually makes into the package and
test those calls against both versions. The `fast-xml-parser` row moved two consumers across a
major with zero behavioral change, and the evidence was a diff of their own calls, not the
package's changelog. Whose API it is, theirs or ours, decides whether a major move is risky.

## The grouping error to avoid

**Same package does not mean same fix.** `fast-xml-parser` appeared in both scans at two majors,
reached through different parents. Group by cause to plan the change; then re-scan to prove each
copy moved. Likewise, orval's findings share one package and one lockfile but split across version
lines: a resolution to the newest 7.x cleared the one 7.x fixes, and the rest exist only on 8.x.
The re-scan after the change is what proves it -- not the grouping, and not the fix version the
scanner prints.

## Deciding runtime reachability

Worth establishing per group, because it changes the argument you can make in a `statement:` (it
never changes whether the gate fires):

- Present in the image's runtime stage -> reachable, fix it.
- Only in a standalone lockfile, a build stage, a devDependency, or a codegen tool's own tree -> not
  in the published image. Still gate-failing; fix it where cheap, otherwise accept it with the
  reachability argument stated explicitly.

Do not use "it's only build tooling" to skip the entry. An unrecorded suppression is invisible.
