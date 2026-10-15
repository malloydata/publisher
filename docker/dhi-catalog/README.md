<!--
Copyright (c) Credible Data Inc.
SPDX-License-Identifier: MIT
-->

# Publisher in the Docker Hardened Images catalog

> What this is: the definition files that put `dhi.io/malloy-publisher` into Docker's
> [Hardened Images catalog](https://github.com/docker-hardened-images/catalog), kept here beside
> the source they build, and how a submission works. For the image this repository publishes
> itself (`ms2data/malloy-publisher`, built from the root `Dockerfile` on the same hardened base
> images), see [`packages/server/README.docker.md`](../../packages/server/README.docker.md).

## Two images, one base

The root `Dockerfile` builds `ms2data/malloy-publisher` **from** Docker Hardened Images
(`dhi.io/bun`, `dhi.io/git`, `dhi.io/amazoncorretto`). That image is hardened in the sense that
matters to a deployment: a runtime with no shell and no package manager, running as uid 65532,
with Docker's SBOM, provenance and VEX on every base layer and this repository's own SBOM,
provenance and cosign signature on the image.

A `dhi.io/malloy-publisher` is a different thing: an image **in** the catalog, built by Docker's
pipeline from a definition file, published under `dhi.io/`, rebuilt by Docker when a package it
contains is patched, and carrying Docker's own attestations end to end. The catalog does not take a
Dockerfile; it takes the definition under `image/malloy-publisher/` here, which describes the same
build in the catalog's own language (packages from the DHI Debian repository, pinned downloads,
pipeline steps, outputs with ownership, runtime settings, tests).

## Files

| Path                                      | What it is                                                                                                           |
| ----------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| `image/malloy-publisher/info.yaml`        | Catalog listing metadata (display name, one-line description, category, home URL).                                   |
| `image/malloy-publisher/overview.md`      | The listing's About text, in the catalog's house shape.                                                              |
| `image/malloy-publisher/guides.md`        | The listing's How-to-use text.                                                                                       |
| `image/malloy-publisher/logo.svg`         | The listing's logo (the Publisher console's).                                                                        |
| `image/malloy-publisher/debian-13/0.yaml` | The **runtime** variant: the definition. Edit this one.                                                              |
| `image/malloy-publisher/debian-13/0-dev.yaml` | The **dev** variant (shell, apt, root), generated from `0.yaml` by `render-dev.py`. Never edit by hand.           |
| `render-dev.py`                           | Renders `0-dev.yaml` from `0.yaml`: `python3 docker/dhi-catalog/render-dev.py docker/dhi-catalog/image/malloy-publisher/debian-13/0.yaml`. |

The layout under `image/` mirrors the catalog repository's, so a submission is a copy of that
directory into a fork of the catalog.

## Bumping the version

`0.yaml` pins a release, not a branch: `VERSION` (the tag), `COMMIT_SHA` (the commit the tag names,
so the tag cannot move under the build), `DUCKDB_VERSION` (the engine's version, which
`node scripts/duckdb-version.js` prints from the release's `bun.lock`) and its two CLI zip digests,
`ADBC_SNOWFLAKE_VERSION` and its two digests, and `BUN_VERSION` (the line `dhi.io/bun` carries;
verified against Bun's signed checksum list, so no digest to keep). Update them, update the `tags`
list, re-render the dev variant, and build.

## Building locally

The catalog's definitions are built by a BuildKit frontend Docker publishes, named on the first
line of each file (`# syntax=dhi.io/build:2-debian13`). From this directory:

```bash
docker buildx build . -f image/malloy-publisher/debian-13/0.yaml \
  --sbom=generator=dhi.io/scout-sbom-indexer:1 --provenance=1 \
  --tag malloy-publisher:dhi-catalog --load
```

Pulling the frontend and the DHI package repository needs no subscription (`docker login dhi.io`
is only required for the non-public entitlement). The build fetches from GitHub, npm,
extensions.duckdb.org and the RDS trust store, the same as the root `Dockerfile`.

This definition has been built that way end to end (2026-10-15, arm64): the frontend resolves
the packages and their source archives, verifies every `checksum:` (a wrong digest fails the build
with `digest mismatch`), runs the pipeline, and exports an image with an attestation manifest. The
image it produced ran as uid 65532 with no shell, loaded every baked extension and the ADBC driver
offline, cloned over HTTPS, reached `serving` and answered a query from a mounted package.

What a local build cannot do is sign: a catalog image's signature and its zero-CVE SLA come from
Docker's pipeline, which is the point of submitting rather than self-publishing.

Three things the build environment taught, each now in the definition: its JDK package registers
no `/usr/bin/java`, so the build step sets `JAVA_HOME` from `/usr/lib/jvm`; a build stage gets only
the packages it lists (`gpg` for the Bun checksum, `bunx` as a symlink); and the OpenAPI generator
downloads its jar from Maven Central at build time. That version is pinned in the repository's
`openapitools.json`, the build's cache keeps the jar between runs, and `scripts/generate-api-types.mjs`
retries a failed download, but a reviewer may still ask to see the jar as a checksummed `files:` entry.

## Submitting to the catalog

The catalog is Apache-2.0 and takes contributions by pull request
([CONTRIBUTING.md](https://github.com/docker-hardened-images/catalog/blob/main/CONTRIBUTING.md)).
There is no partner programme to join first. Two steps, in this order:

1. **Open an image request** with the catalog's
   [Image Request](https://github.com/docker-hardened-images/catalog/issues/new?template=image_request.yml)
   template. It asks for the name (`malloy-publisher`), the versions (`0.9.12`), and three links:
   the source repository (`https://github.com/malloydata/publisher`), the Dockerfile
   (`https://github.com/malloydata/publisher/blob/main/Dockerfile`) and a public image
   (`docker.io/ms2data/malloy-publisher:0.9.12`). The maintainers triage requests before a
   definition is reviewed; a request that already has a definition ready is the easy kind.
2. **Open the PR**: fork the catalog, copy `image/malloy-publisher/` from here into its `image/`
   directory, and submit. Contributions are made under Apache-2.0 (the definition files, not
   Publisher, which stays MIT and is referenced, not vendored). Expect the review to ask for a
   build from the catalog's own CI and to adjust conventions a local build cannot settle: whether
   the `tests` block ran (the build log does not show it), whether the maintainers want the
   OpenAPI generator jar vendored as a `files:` entry rather than fetched, and how they prefer
   git's closure handled (the frontend resolved `git` without `perl`, matching the root
   `Dockerfile`).

The image then lives at `dhi.io/malloy-publisher:<tag>`, which anyone can pull, and which an
organisation with a DHI subscription can mirror into its own namespace and customise.
