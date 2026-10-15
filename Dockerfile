# syntax=docker/dockerfile:1.4
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# Every stage starts from a Docker Hardened Image (dhi.io). The runtime stage is
# the `bun` runtime variant: no shell, no package manager, runs as `nonroot`
# (uid 65532). Everything that needs a shell runs in a `-dev` stage and is
# COPYd across. The images are free to pull (`docker login dhi.io` is not
# needed for the public catalog), rebuilt by Docker when Debian ships a fix,
# and carry their own SBOM, SLSA provenance and VEX; `apt-get upgrade` and the
# APT_REFRESH cache-buster the previous base needed have no job here.
#
# One Bun image for every bun stage ON PURPOSE. The adbc-driver stage compiles
# a shim with `-ldl` against the dev image's glibc and self-tests it there; the
# runtime image has to be the same build (same Debian release, same glibc) or
# the self-test proves nothing. `-dev` and runtime variants of one tag are
# built together from one definition, so pinning the tag pins both.
#
# dhi.io/bun carries only the current 1.x line (1.4 today); there is no 1.3
# tag. The server's floor is `bun >=1.4.3` (package.json engines), the same
# version, so the runtime, CI and a clone all run one Bun.
ARG BUN_IMAGE=dhi.io/bun:1.4.3-debian13
ARG GIT_IMAGE=dhi.io/git:2-debian13
ARG JAVA_IMAGE=dhi.io/amazoncorretto:21-debian13-dev

# Java for generate-api-types scripts. Same Debian as the bun images, so the
# JVM copied out of it below links against the builder's libraries.
FROM ${JAVA_IMAGE} AS java-base

# ADBC Snowflake driver + shim (ADBC-SHIM). Kept in its own stage so the
# compiler never reaches the runtime image and so a broken driver/shim pair
# fails the BUILD. Removal: see packages/server/adbc-shim/README.md.
#
# Downloaded directly rather than through the upstream installer's `curl … | sh`,
# and checked against the digest GitHub publishes: this is a 16MB native library
# dlopen'd into the server process, and a version tag is mutable — an asset can
# be re-uploaded under it. Pin the bytes, not the name.
#
# No `|| echo` fallback: a failed fetch FAILS THE BUILD. That tolerance is
# exactly what let this ship broken once. An image without the driver cannot
# answer a single Snowflake query and must not leave the builder reporting
# success.
#
# The shim (packages/server/adbc-shim/) is compiled here and installed under the
# driver's own file name, with the real driver renamed beside it; selftest.c
# dlopens the pair the way the driver manager will and fails the build if the
# chain does not come up. No network, no credentials.
#
# When bumping DUCKDB_VERSION, re-check this: the community extension moves with
# DuckDB and the driver ABI may move with it, and a mismatch surfaces only at
# query time.
#
# No `apt-get upgrade` here: the runtime stage cannot upgrade (no apt), so an
# upgrade would move THIS stage's glibc ahead of the one the shim runs against.
# The dev and runtime variants of ${BUN_IMAGE} share a glibc as built; the
# smoke test's driver-load step (build.yml, 4b) is what proves the shim loads
# in the shipped image.
FROM ${BUN_IMAGE}-dev AS adbc-driver
ARG ADBC_SNOWFLAKE_VERSION=1.12.0
ARG ADBC_SNOWFLAKE_SHA256_AMD64=9f3b44bd2c5d1a84acd1dadf7b9995e47bad78ca37f799c9e8460ac196fd319c
ARG ADBC_SNOWFLAKE_SHA256_ARM64=7648311005788d9576ee06ced1efd0f4f5849a5e70ef9946abad08588e312283
# gzip because the dev image has none and the driver ships as a .tar.gz.
RUN apt-get update && apt-get install -y --no-install-recommends curl gzip gcc libc6-dev && \
    rm -rf /var/lib/apt/lists/*
RUN mkdir -p /out && ADBC_ARCH="$(dpkg --print-architecture)" && \
    if [ "${ADBC_ARCH}" = "amd64" ]; then ADBC_SHA="${ADBC_SNOWFLAKE_SHA256_AMD64}"; \
    else ADBC_SHA="${ADBC_SNOWFLAKE_SHA256_ARM64}"; fi && \
    curl -fsSL --retry 8 --retry-delay 3 --retry-max-time 180 --retry-all-errors \
      -o /tmp/adbc-snowflake.tar.gz \
      "https://github.com/adbc-drivers/snowflake/releases/download/go/v${ADBC_SNOWFLAKE_VERSION}/snowflake_linux_${ADBC_ARCH}_v${ADBC_SNOWFLAKE_VERSION}.tar.gz" && \
    echo "${ADBC_SHA}  /tmp/adbc-snowflake.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/adbc-snowflake.tar.gz -C /tmp libadbc_driver_snowflake.so && \
    mv /tmp/libadbc_driver_snowflake.so /out/libadbc_driver_snowflake.real.so && \
    rm -f /tmp/adbc-snowflake.tar.gz
# ADBC-SHIM: compile + self-test. Without the shim this stage ends at the mv
# above, with the driver kept under its real name.
#
# adbc.h is the ABI the shim wraps, vendored byte-for-byte from
# apache/arrow-adbc@7f35429e (c/include/arrow-adbc/adbc.h). The digest makes a
# re-vendor deliberate: update it and the provenance line in the shim's README
# together, and re-read the AdbcDriver struct layout when you do.
ARG ADBC_HEADER_SHA256=b6ce3eb8394d4877af1693654d34bc11648a2dfb1f0eacc0c47d062ca69feb71
COPY packages/server/adbc-shim/ /src/adbc-shim/
RUN echo "${ADBC_HEADER_SHA256}  /src/adbc-shim/adbc.h" | sha256sum -c - && \
    gcc -O2 -Wall -Wextra -shared -fPIC -o /out/libadbc_driver_snowflake.so /src/adbc-shim/shim.c -ldl && \
    gcc -O2 -Wall -Wextra -o /tmp/selftest /src/adbc-shim/selftest.c -ldl && \
    /tmp/selftest /out/libadbc_driver_snowflake.so && \
    mkdir -p /tmp/stub && \
    gcc -O2 -Wall -Wextra -I/src/adbc-shim -shared -fPIC -o /tmp/stub/libadbc_driver_snowflake.real.so /src/adbc-shim/stub_driver.c && \
    ADBC_REAL_DRIVER=/tmp/stub/libadbc_driver_snowflake.real.so /tmp/selftest /out/libadbc_driver_snowflake.so

# DuckDB CLI, pinned to @duckdb/node-api (the query engine) so the CLI reads
# the same ~/.duckdb/extensions/v<version>/ dir the runtime bakes. CI passes
# --build-arg DUCKDB_VERSION derived from the lockfile (the source of truth);
# the default below is a fallback for plain `docker build`, kept in sync by
# scripts/sync-duckdb-version.js and enforced by the CI consistency check.
#
# Fetched as the release zip rather than through install.duckdb.org's
# `curl | bash`: that installer wants gzip and a shell profile to edit, and
# this stage only needs the one binary. The version check at the end is the
# verification: the binary must report the version the tag names.
#
# Staged under /out with the path it will have in the runtime image, under the
# home of `nonroot` (uid 65532), the user the server runs as, because DuckDB
# resolves ~/.duckdb from HOME.
FROM ${BUN_IMAGE}-dev AS duckdb-cli
ARG DUCKDB_VERSION=1.5.5
RUN DUCKDB_VERSION="${DUCKDB_VERSION}"; set -eu; \
    apt-get update && apt-get install -y --no-install-recommends curl unzip && \
    rm -rf /var/lib/apt/lists/*; \
    ARCH="$(dpkg --print-architecture)"; \
    CLI_DIR="/out/home/nonroot/.duckdb/cli/${DUCKDB_VERSION}"; \
    curl -fsSL --retry 8 --retry-delay 3 --retry-max-time 180 --retry-all-errors \
      -o /tmp/duckdb_cli.zip \
      "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-${ARCH}.zip"; \
    mkdir -p "${CLI_DIR}" && unzip -q /tmp/duckdb_cli.zip -d "${CLI_DIR}" && rm -f /tmp/duckdb_cli.zip; \
    chmod 0755 "${CLI_DIR}/duckdb"; \
    got="$("${CLI_DIR}/duckdb" -noheader -list -c 'SELECT version();')"; \
    test "${got}" = "v${DUCKDB_VERSION}" || { echo "duckdb CLI reports ${got}, expected v${DUCKDB_VERSION}" >&2; exit 1; }

# git, for package locations that are GitHub repositories (the server shells
# out to it through simple-git). The runtime image has no apt, so the binary
# and its libraries come from Docker's own hardened git image, which is built
# on the same Debian release from the same package repository.
#
# Not the whole git image: it carries curl, git-lfs and perl, and a blanket
# COPY of its root would also overwrite the runtime's /etc/passwd and dpkg
# database. Instead this stage walks git's dependency closure in the git
# image's dpkg database and copies exactly the packages the runtime image does
# not already have, each one's files AND its dpkg records, and appends their
# stanzas to a copy of the runtime image's package database. The result is an
# inventory the SBOM generator and a vulnerability scanner both see: a `git`
# that showed up in the filesystem but not in /var/lib/dpkg/status would be
# invisible to every scan of this image.
#
# Left out, with the reason: git-man (manual pages); perl and the three
# packages that exist for it (perl-base is already in the runtime; `git clone`,
# fetch and checkout are C, and the perl subcommands -- git-send-email,
# git-add --interactive -- are not something a server runs).
#
# The runtime image's package database is read from the SAME ${BUN_IMAGE} the
# final stage uses, so the appended status file describes the image it lands in.
FROM ${GIT_IMAGE} AS git
FROM ${BUN_IMAGE} AS runtime-base
FROM ${BUN_IMAGE}-dev AS git-closure
COPY --from=git / /git-root
COPY --from=runtime-base /var/lib/dpkg/status /tmp/runtime-status
RUN set -eu; \
    admin=/git-root/var/lib/dpkg; \
    grep '^Package: ' /tmp/runtime-status | cut -d' ' -f2 | sort -u > /tmp/have; \
    printf '%s\n' git-man perl perl-base perl-modules-5.40 libperl5.40 liberror-perl > /tmp/skip; \
    echo git > /tmp/want; : > /tmp/closure; \
    while [ -s /tmp/want ]; do \
      p=$(head -n1 /tmp/want); sed -i '1d' /tmp/want; \
      grep -qx "$p" /tmp/closure && continue; \
      grep -qx "$p" /tmp/have && continue; \
      grep -qx "$p" /tmp/skip && continue; \
      dpkg-query --admindir="$admin" -W -f '${Package}\n' "$p" >/dev/null 2>&1 \
        || { echo "git depends on $p, which ${GIT_IMAGE:-the git image} does not carry" >&2; exit 1; }; \
      echo "$p" >> /tmp/closure; \
      dpkg-query --admindir="$admin" -W -f '${Pre-Depends}, ${Depends}\n' "$p" \
        | tr ',' '\n' | sed -e 's/|.*//' -e 's/(.*//' -e 's/:any//' -e 's/^ *//' -e 's/ *$//' \
        | grep . >> /tmp/want || true; \
    done; \
    echo "git closure: $(tr '\n' ' ' < /tmp/closure)"; \
    mkdir -p /git-files/var/lib/dpkg/info; \
    : > /tmp/files; \
    while read -r p; do \
      dpkg-query --admindir="$admin" -L "$p" | while read -r f; do \
        [ "$f" = "/." ] && continue; \
        if [ -e "/git-root$f" ] || [ -L "/git-root$f" ]; then printf '%s\n' "${f#/}" >> /tmp/files; fi; \
      done; \
      cp "$admin/info/$p."* /git-files/var/lib/dpkg/info/; \
    done < /tmp/closure; \
    sort -u /tmp/files > /tmp/files.sorted; \
    tar -C /git-root --no-recursion -cf - -T /tmp/files.sorted | tar -C /git-files -xf -; \
    cp /tmp/runtime-status /git-files/var/lib/dpkg/status; \
    while read -r p; do dpkg-query --admindir="$admin" -s "$p"; echo; done < /tmp/closure \
      >> /git-files/var/lib/dpkg/status; \
    test -x /git-files/usr/bin/git && test -x /git-files/usr/lib/git-core/git-remote-https

# Builder stage
FROM ${BUN_IMAGE}-dev AS builder
COPY --from=java-base /usr/lib/jvm /usr/lib/jvm
ENV JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH
ENV NODE_ENV=production
WORKDIR /publisher

# Copy package files first for better layer caching
COPY package.json bun.lock api-doc.yaml ./
# patchedDependencies in package.json point at patches/; bun install refuses to
# run without the patch files, so they have to arrive before it does.
COPY patches/ ./patches/
COPY scripts/add-license-headers.mjs ./scripts/
COPY packages/server/package.json ./packages/server/package.json
COPY packages/app/package.json ./packages/app/package.json
COPY packages/sdk/package.json ./packages/sdk/package.json

# Install all workspace dependencies once (cached across builds)
RUN --mount=type=cache,target=/root/.bun/install/cache \
    bun install

# Build SDK first
COPY packages/sdk/ ./packages/sdk/
WORKDIR /publisher/packages/sdk
RUN --mount=type=cache,target=/root/.bun \
    bun run build

# Build app
WORKDIR /publisher/packages/app
COPY packages/app/ ./
RUN --mount=type=cache,target=/root/.bun \
    NODE_OPTIONS='--max-old-space-size=4096' bun run build:server

# Build server. Its build ends with bake-duckdb-extensions, which populates
# ~/.duckdb/extensions (/root here) through the same @duckdb/node-api engine
# the runtime uses; the dev image ships the CA certificates that fetch needs.
WORKDIR /publisher/packages/server
COPY packages/server/ ./
RUN --mount=type=cache,target=/root/.bun \
    bun run build:server-only

# Assembly: everything the runtime image holds, laid out under /out with its
# final paths and ownership, in a stage that has a shell. The runtime stage
# has none, so it can only COPY. Copying /out/ onto / is what carries the
# ownership across: a multi-stage COPY keeps the uid of every entry BELOW the
# path it copies and resets the path itself to root, so /publisher has to be
# copied as a child of something.
FROM ${BUN_IMAGE}-dev AS assemble
RUN apt-get update && apt-get install -y --no-install-recommends curl && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /out/publisher

# Copy built artifacts from builder
COPY --from=builder /publisher/package.json /publisher/bun.lock ./
COPY --from=builder /publisher/patches/ ./patches/
COPY --from=builder /publisher/packages/app/dist/ ./packages/app/dist/
COPY --from=builder /publisher/packages/app/package.json ./packages/app/package.json
COPY --from=builder /publisher/packages/server/dist/ ./packages/server/dist/
COPY --from=builder /publisher/packages/server/package.json ./packages/server/package.json
COPY --from=builder /publisher/packages/sdk/dist/ ./packages/sdk/dist/
COPY --from=builder /publisher/packages/sdk/package.json ./packages/sdk/package.json

# Install production-only deps. Workspace links in node_modules are relative,
# so the tree can move from /out/publisher to /publisher unchanged.
RUN --mount=type=cache,target=/root/.bun/install/cache \
    bun install --production

# Carry over the DuckDB extensions baked during the builder stage's
# `build:server-only`. They live in ~/.duckdb/extensions/v<version>/, which the
# runtime engine reads at INSTALL/LOAD time -- so the server finds them on disk
# and skips the network fetch. Copying the baked cache from the builder keeps a
# single bake mechanism (the server build) instead of re-running it here. The
# CLI (duckdb-cli stage) and runtime engine are pinned to the same DuckDB
# version, so all agree on one dir. The bake ran as root in the builder, so its
# cache is under /root there; here it lands in the runtime user's home, handed
# to that user below so an INSTALL at run time of an extension the bake did not
# cover can still write beside it.
COPY --from=builder /root/.duckdb/extensions /out/home/nonroot/.duckdb/extensions

# The Snowflake extension is a wrapper over the ADBC Snowflake driver, and
# `INSTALL snowflake FROM community` does NOT bring it — the extension ships
# alone, the driver is a separate artifact. Without it the extension installs,
# LOADs and answers snowflake_version() perfectly well, and then every
# snowflake_query() fails at run time with "ADBC Snowflake driver
# (libadbc_driver_snowflake.so) not found".
#
# The driver is downloaded, verified and wrapped in the `adbc-driver` stage
# above; what arrives here is two files (ADBC-SHIM — without the shim it is the
# one upstream file under its own name):
#
#   libadbc_driver_snowflake.so       the shim (packages/server/adbc-shim/), which
#                                     forwards every call to the real driver and
#                                     sets the two statement options that bound
#                                     the driver's read-ahead — see its README
#   libadbc_driver_snowflake.real.so  the upstream driver, unmodified
#
# The shim finds the real driver beside itself, so both go wherever the
# extension is. Placed HERE, after the extensions COPY above, rather than
# earlier: the extension resolves the driver by calling dladdr on ITSELF and
# looking beside the loaded object, so the driver has to sit next to whichever
# snowflake.duckdb_extension the runtime actually loads. That directory is
# discovered rather than reconstructed from a platform string.
#
# The closing count-match is the verification — one shim and one real driver
# for EVERY extension directory, not "at least one somewhere": a pipeline's
# status is its last command's, so a copy that failed in a non-final loop
# iteration would otherwise pass. snowflake_version() cannot serve as a check,
# being a scalar that never touches the driver.
COPY --from=adbc-driver /out/libadbc_driver_snowflake.so /out/libadbc_driver_snowflake.real.so /tmp/adbc/
RUN find /out/home/nonroot/.duckdb/extensions -name snowflake.duckdb_extension -printf '%h\n' \
      | while read -r d; do cp /tmp/adbc/libadbc_driver_snowflake.so /tmp/adbc/libadbc_driver_snowflake.real.so "$d/"; done && \
    rm -rf /tmp/adbc && \
    ext=$(find /out/home/nonroot/.duckdb/extensions -name snowflake.duckdb_extension | wc -l) && \
    shim=$(find /out/home/nonroot/.duckdb/extensions -name libadbc_driver_snowflake.so | wc -l) && \
    real=$(find /out/home/nonroot/.duckdb/extensions -name libadbc_driver_snowflake.real.so | wc -l) && \
    test "$ext" -gt 0 && test "$shim" -eq "$ext" && test "$real" -eq "$ext"

# ADBC-SHIM operator note. The shim is opt-in: with neither variable set it is a pure pass-through and
# the driver behaves exactly as upstream ships it. To bound how far the driver
# reads ahead of the consumer, set at deploy time:
#
#   ADBC_RESULT_QUEUE_SIZE=1        Arrow record batches buffered per result
#                                   stream (driver default 100). 1 keeps a slow
#                                   consumer — a CTAS into DuckLake on object
#                                   storage — from having the whole remaining
#                                   result resident: measured 3325 MiB → 286 MiB
#                                   on a 20M-row read, no measured cost to a
#                                   fast one.
#   ADBC_PREFETCH_CONCURRENCY       streams downloaded in parallel (driver
#                                   default 5). The throughput knob, not the
#                                   memory one — lowering it costs wall time.
#
# Deliberately not defaulted here, like PUBLISHER_DUCKLAKE_*: an image upgrade
# must not change what the driver does until an operator asks it to.

# Trust the Amazon RDS root CAs so Postgres->RDS connections verify the server
# certificate out of the box. Node/Bun ignore the OS trust store and use their
# own bundled CA set, so NODE_EXTRA_CA_CERTS is the load-bearing knob (it appends
# to that set). Fetched at build rather than vendored, so there is no committed
# cert to keep fresh: a CA rotation is picked up on the next image
# build/release, and every consumer of this image (the worker, the SSH-bastion
# connection path) inherits the trust anchor without re-mounting a CA.
RUN mkdir -p /out/etc/ssl/certs && \
    curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    -o /out/etc/ssl/certs/rds-global-bundle.pem

# The server runs as `nonroot` (uid 65532), the user the runtime image ships;
# nothing here runs as root. Every application file stays root-owned, so the
# server cannot modify one in place. The server root directory itself is owned
# by uid 65532, because publisher.db is created directly in it, and that lets
# the server rename aside and replace any of its top-level entries:
# package.json, bun.lock, and the packages/ and node_modules/ directories. Such
# a change lasts as long as the container. Some storage drivers, Docker's
# default overlayfs among them, refuse to rename a directory that comes from an
# image layer; that limit belongs to the driver, not this image.
#
# publisher_data/ and ducklake_data/ are created here, owned by that user,
# because Docker seeds a new named volume from the image's directory, ownership
# included: a volume mounted on either on first run is writable without a
# chown. ducklake_data/ is the mount point for a DuckLake storage destination
# whose bucketUrl is a local path; the server writes there only when one is
# configured. These are the only mount points the image prepares; a new volume
# at any path the image lacks starts root-owned. A volume an older image
# populated as uid 1000 (0.9.x) or root (0.8.x) is not writable either.
# packages/server/README.docker.md covers both.
#
# /etc/publisher is an empty directory outside the application tree, the
# documented place to mount credential files.
RUN mkdir -p /out/publisher/publisher_data /out/publisher/ducklake_data /out/etc/publisher && \
    chown 65532:65532 /out/publisher /out/publisher/publisher_data /out/publisher/ducklake_data && \
    chown -R 65532:65532 /out/home/nonroot

# Final image: the runtime variant. No shell, so no RUN; every file arrives by
# COPY from a stage above. `docker run --entrypoint <binary>` and `docker exec`
# of a binary both still work (coreutils and `bun` are in the image; `sh` is
# not), and `docker debug` attaches a toolbox shell to a running container.
FROM ${BUN_IMAGE} AS final

# OCI image metadata — surfaces in `docker inspect`, registry UIs
# (Docker Hub / GHCR), and Docker Desktop. The description is kept short
# (some tools truncate at 80–120 chars); the `documentation` URL points
# at the root README's Docker section for build/run/mount-path details.
LABEL org.opencontainers.image.title="Malloy Publisher" \
    org.opencontainers.image.description="Open-source semantic model server for Malloy (REST :4000, MCP :4040)." \
    org.opencontainers.image.source="https://github.com/malloydata/publisher" \
    org.opencontainers.image.documentation="https://github.com/malloydata/publisher#docker" \
    org.opencontainers.image.licenses="MIT"

# Order matters for one file: git-closure's /var/lib/dpkg/status is the
# runtime image's own database plus git's packages, and must land before
# anything else touches /var/lib/dpkg (nothing below does; keep it that way).
COPY --from=git-closure /git-files/ /
COPY --from=duckdb-cli /out/ /
COPY --from=assemble /out/ /

WORKDIR /publisher

# Runtime config
ARG DUCKDB_VERSION=1.5.5
ENV NODE_ENV=production
# Nobody starts an agent session inside the container, so the .mcp.json the
# server writes into its working directory could never be read. The git-working-
# tree guard that would normally cover /publisher cannot fire here, because
# .dockerignore excludes .git from the image. Left on, every boot would write a
# file into it, which matters to anyone bind-mounting a project at /publisher.
# Pass -e PUBLISHER_NO_MCP_CONFIG= to opt back in. Note the same emptiness is
# reachable by accident: `docker run -e PUBLISHER_NO_MCP_CONFIG` with no value,
# or a Compose `environment:` entry with none, deletes this ENV when the host
# does not have the variable, which re-enables the write.
ENV PUBLISHER_NO_MCP_CONFIG=1
ENV PATH="/home/nonroot/.duckdb/cli/${DUCKDB_VERSION}:$PATH"
# Set rather than left to USER, which derives HOME from /etc/passwd: run as
# `--user 0`, the server would otherwise resolve ~/.duckdb under /root and find
# none of the baked extensions.
ENV HOME=/home/nonroot
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/rds-global-bundle.pem
# Declare the runtime ports so `docker run -P` and Docker Desktop's
# port-preview surface them. The server listens on both (REST on 4000, MCP on
# 4040); this just makes them discoverable.
EXPOSE 4000 4040

# Pass --server_root explicitly so the zero-arg bundled-default trigger
# in server.ts (added for `npx @malloy-publisher/server` UX) does NOT fire
# inside the production container. Without this, a Docker image launched
# with no mounted config would try to clone the bundled DuckDB samples
# from GitHub at startup, blowing past the docker_smoke_test 90s timeout.
# Operators that want a config provide it at /publisher/publisher.config.json
# (mount as volume) or override CMD with --config <path>.
#
# Numeric, not `nonroot`: Kubernetes' runAsNonRoot can only verify a numeric
# USER, and refuses to start an image whose USER is a name unless the pod also
# sets runAsUser. The base image already sets 65532; stated here so a base
# that changed its default could not change what this image runs as.
USER 65532:65532
CMD ["bun", "run", "./packages/server/dist/server.mjs", "--server_root", "/publisher"]
