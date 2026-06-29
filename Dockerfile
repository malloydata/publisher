# syntax=docker/dockerfile:1.4

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
    org.opencontainers.image.description="Open-source semantic model server for Malloy (REST :4000, MCP :4040, agent MCP :4041)." \
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

# Runtime config
ARG DUCKDB_VERSION=1.5.3
ENV NODE_ENV=production
ENV PATH="/root/.duckdb/cli/${DUCKDB_VERSION}:$PATH"
RUN mkdir -p /etc/publisher
# Declare the runtime ports so `docker run -P` and Docker Desktop's
# port-preview surface them. The server already listens on all three (REST on
# 4000, core MCP on 4040, agent MCP on 4041); this just makes them discoverable.
EXPOSE 4000 4040 4041

# Pass --server_root explicitly so the zero-arg bundled-default trigger
# in server.ts (added for `npx @malloy-publisher/server` UX) does NOT fire
# inside the production container. Without this, a Docker image launched
# with no mounted config would try to clone the bundled DuckDB samples
# from GitHub at startup, blowing past the docker_smoke_test 90s timeout.
# Operators that want a config provide it at /publisher/publisher.config.json
# (mount as volume) or override CMD with --config <path>.
CMD ["bun", "run", "./packages/server/dist/server.mjs", "--server_root", "/publisher"]
