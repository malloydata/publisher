# syntax=docker/dockerfile:1.4

# Java for generate-api-types scripts
FROM amazoncorretto:21.0.8 AS java-base

FROM oven/bun:1.2.23-slim AS base-deps

RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates unzip git \
    openssl libcurl4 libssl3 dnsutils iputils-ping file && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

RUN curl -L https://install.duckdb.org | bash && \
    ln -s /root/.duckdb/cli/latest/duckdb /usr/local/bin/duckdb && \
    curl -sSL https://raw.githubusercontent.com/iqea-ai/duckdb-snowflake/main/scripts/install-adbc-driver.sh | bash && \
    ldconfig && \
    duckdb -c "INSTALL snowflake FROM community; LOAD snowflake; SELECT snowflake_version();" || \
    echo "Snowflake verification skipped (offline build)" && \
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# Builder stage
FROM oven/bun:1.2.23-slim AS builder
COPY --from=java-base /usr/lib/jvm /usr/lib/jvm
ENV JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH
ENV NODE_ENV=production
WORKDIR /publisher

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

# Runtime config
ENV NODE_ENV=production
ENV PATH="/root/.duckdb/cli/latest:$PATH"
RUN mkdir -p /etc/publisher
EXPOSE 4000

CMD ["bun", "run", "./packages/server/dist/server.js"]
