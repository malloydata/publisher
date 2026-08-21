# syntax=docker/dockerfile:1.4
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

# Java for generate-api-types scripts
FROM amazoncorretto:21.0.8 AS java-base

FROM oven/bun:1.3.13-slim AS base-deps

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates unzip git \
    openssl libcurl4 libssl3 dnsutils iputils-ping file && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# DuckDB CLI version, pinned to @duckdb/node-api (the query engine) so the
# CLI bakes extensions into the same ~/.duckdb/extensions/v<version>/ dir
# the runtime reads. CI passes --build-arg DUCKDB_VERSION derived from the
# lockfile (the source of truth); the default below is a fallback for plain
# `docker build`, kept in sync by scripts/sync-duckdb-version.js and enforced
# by the CI consistency check.
ARG DUCKDB_VERSION=1.5.3
RUN DUCKDB_VERSION=${DUCKDB_VERSION} bash -c "curl -L https://install.duckdb.org | bash" && \
    ln -s /root/.duckdb/cli/${DUCKDB_VERSION}/duckdb /usr/local/bin/duckdb && \
    duckdb -c "INSTALL snowflake FROM community; LOAD snowflake; SELECT snowflake_version();" || \
    echo "Snowflake verification skipped (offline build)" && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# Builder stage
FROM oven/bun:1.3.13-slim AS builder
COPY --from=java-base /usr/lib/jvm /usr/lib/jvm
ENV JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH
ENV NODE_ENV=production
WORKDIR /publisher

# CA certificates are required for the DuckDB extension bake (run by
# packages/server's build): without them @duckdb/node-api can't verify TLS to
# extensions.duckdb.org and every download fails with an SSL CA cert error.
# The bun:slim base ships without them.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy package files first for better layer caching
COPY package.json bun.lock api-doc.yaml ./
COPY scripts/add-license-headers.mjs ./scripts/
COPY packages/server/package.json ./packages/server/package.json
COPY packages/app/package.json ./packages/app/package.json
COPY packages/sdk/package.json ./packages/sdk/package.json
COPY packages/mcp-apps/package.json ./packages/mcp-apps/package.json

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

# Build the MCP Apps widgets. Its output is copied into the server bundle by
# packages/server/build.ts, so it has to run before the server build below.
WORKDIR /publisher/packages/mcp-apps
COPY packages/mcp-apps/ ./
RUN --mount=type=cache,target=/root/.bun \
    bun run build

# Build server
WORKDIR /publisher/packages/server
COPY packages/server/ ./
RUN --mount=type=cache,target=/root/.bun \
    bun run build:server-only

# Final image
FROM base-deps AS final
WORKDIR /publisher

# OCI image metadata — surfaces in `docker inspect`, registry UIs
# (Docker Hub / GHCR), and Docker Desktop. The description is kept short
# (some tools truncate at 80–120 chars); the `documentation` URL points
# at the root README's Docker section for build/run/mount-path details.
LABEL org.opencontainers.image.title="Malloy Publisher" \
    org.opencontainers.image.description="Open-source semantic model server for Malloy (REST :4000, MCP :4040)." \
    org.opencontainers.image.source="https://github.com/malloydata/publisher" \
    org.opencontainers.image.documentation="https://github.com/malloydata/publisher#docker" \
    org.opencontainers.image.licenses="MIT"

# Copy built artifacts from builder
COPY --from=builder /publisher/package.json /publisher/bun.lock ./
COPY --from=builder /publisher/packages/app/dist/ /publisher/packages/app/dist/
COPY --from=builder /publisher/packages/app/package.json /publisher/packages/app/package.json
COPY --from=builder /publisher/packages/server/dist/ /publisher/packages/server/dist/
COPY --from=builder /publisher/packages/server/package.json /publisher/packages/server/package.json
COPY --from=builder /publisher/packages/sdk/dist/ /publisher/packages/sdk/dist/
COPY --from=builder /publisher/packages/sdk/package.json /publisher/packages/sdk/package.json
# The builder's `bun install` rewrites bun.lock to match the workspace members
# present in the build context, and the final stage inherits that lockfile, so a
# member named in it whose manifest is missing here makes the frozen production
# install below fail. mcp-apps ships no runtime code (its output is inlined into
# packages/server/dist/mcp-apps), so only the manifest is needed, and its
# dependencies are devDependencies precisely so `--production` installs none.
COPY --from=builder /publisher/packages/mcp-apps/package.json /publisher/packages/mcp-apps/package.json

# Install production-only deps
RUN --mount=type=cache,target=/root/.bun/install/cache \
    bun install --production

# Carry over the DuckDB extensions baked during the builder stage's
# `build:server-only` (packages/server's build runs bake-duckdb-extensions).
# They live in ~/.duckdb/extensions/v<version>/, which the runtime engine reads
# at INSTALL/LOAD time -- so the server finds them on disk and skips the network
# fetch. Copying the baked cache from the builder keeps a single bake mechanism
# (the server build) instead of re-running it here. The CLI (base-deps) and
# runtime engine are pinned to the same DuckDB version, so all agree on one dir.
COPY --from=builder /root/.duckdb/extensions /root/.duckdb/extensions

# The Snowflake extension is a wrapper over the ADBC Snowflake driver, and
# `INSTALL snowflake FROM community` does NOT bring it — the extension ships
# alone, the driver is a separate artifact. Without it the extension installs,
# LOADs and answers snowflake_version() perfectly well, and then every
# snowflake_query() fails at run time with "ADBC Snowflake driver
# (libadbc_driver_snowflake.so) not found".
#
# Placed HERE, after the COPY above, rather than earlier: the extension resolves
# the driver by calling dladdr on ITSELF and looking beside the loaded object, so
# the driver has to sit next to whichever snowflake.duckdb_extension the runtime
# actually loads. That directory is discovered rather than reconstructed from a
# platform string — the CLI (base-deps) and the bake (@duckdb/node-api, in the
# builder) each write their own, and this is the layer that ships.
#
# Downloaded directly rather than through the upstream installer's `curl … | sh`,
# and checked against the digest GitHub publishes: this is a 16MB native library
# dlopen'd into the server process, and a version tag is mutable — an asset can
# be re-uploaded under it. Pin the bytes, not the name.
#
# No `|| echo` fallback, unlike the extension install above: a failed fetch FAILS
# THE BUILD. That tolerance is exactly what let this ship broken. An image without
# the driver cannot answer a single Snowflake query and must not leave the builder
# reporting success. The closing `test` is the verification — snowflake_version()
# cannot serve as one, being a scalar that never touches the driver.
#
# When bumping DUCKDB_VERSION, re-check this: the community extension moves with
# DuckDB and the driver ABI may move with it, and a mismatch surfaces only at
# query time.
ARG ADBC_SNOWFLAKE_VERSION=1.12.0
ARG ADBC_SNOWFLAKE_SHA256_AMD64=9f3b44bd2c5d1a84acd1dadf7b9995e47bad78ca37f799c9e8460ac196fd319c
ARG ADBC_SNOWFLAKE_SHA256_ARM64=7648311005788d9576ee06ced1efd0f4f5849a5e70ef9946abad08588e312283
RUN ADBC_ARCH="$(dpkg --print-architecture)" && \
    if [ "${ADBC_ARCH}" = "amd64" ]; then ADBC_SHA="${ADBC_SNOWFLAKE_SHA256_AMD64}"; \
    else ADBC_SHA="${ADBC_SNOWFLAKE_SHA256_ARM64}"; fi && \
    curl -fsSL --retry 8 --retry-delay 3 --retry-max-time 180 --retry-all-errors \
      -o /tmp/adbc-snowflake.tar.gz \
      "https://github.com/adbc-drivers/snowflake/releases/download/go/v${ADBC_SNOWFLAKE_VERSION}/snowflake_linux_${ADBC_ARCH}_v${ADBC_SNOWFLAKE_VERSION}.tar.gz" && \
    echo "${ADBC_SHA}  /tmp/adbc-snowflake.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/adbc-snowflake.tar.gz -C /tmp libadbc_driver_snowflake.so && \
    find /root/.duckdb/extensions -name snowflake.duckdb_extension -printf '%h\n' \
      | while read -r d; do cp /tmp/libadbc_driver_snowflake.so "$d/"; done && \
    rm -f /tmp/adbc-snowflake.tar.gz /tmp/libadbc_driver_snowflake.so && \
    test -n "$(find /root/.duckdb/extensions -name libadbc_driver_snowflake.so -print -quit)"

# Runtime config
ARG DUCKDB_VERSION=1.5.3
ENV NODE_ENV=production
# Nobody starts an agent session inside the container, so the .mcp.json the
# server writes into its working directory could never be read. The git-working-
# tree guard that would normally cover /publisher cannot fire here, because
# .dockerignore excludes .git from the image. Left on, every boot would write a
# root-owned file, which matters to anyone bind-mounting a project at /publisher.
# Pass -e PUBLISHER_NO_MCP_CONFIG= to opt back in. Note the same emptiness is
# reachable by accident: `docker run -e PUBLISHER_NO_MCP_CONFIG` with no value,
# or a Compose `environment:` entry with none, deletes this ENV when the host
# does not have the variable, which re-enables the write.
ENV PUBLISHER_NO_MCP_CONFIG=1
ENV PATH="/root/.duckdb/cli/${DUCKDB_VERSION}:$PATH"
RUN mkdir -p /etc/publisher

# Trust the Amazon RDS root CAs so Postgres->RDS connections verify the server
# certificate out of the box. Node/Bun ignore the OS trust store and use their
# own bundled CA set, so NODE_EXTRA_CA_CERTS is the load-bearing knob (it appends
# to that set). Fetched at build (curl is in base-deps) rather than vendored, so
# there is no committed cert to keep fresh: a CA rotation is picked up on the next
# image build/release, and every consumer of this image (the worker, the
# SSH-bastion connection path) inherits the trust anchor without re-mounting a CA.
RUN curl -fsSL https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem \
    -o /etc/ssl/certs/rds-global-bundle.pem
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
CMD ["bun", "run", "./packages/server/dist/server.mjs", "--server_root", "/publisher"]
