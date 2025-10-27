# The generate-api-types scripts require Java.
FROM amazoncorretto:21.0.8 AS java-base

FROM oven/bun:1.2.19-slim AS builder
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

# Install root dependencies first
RUN bun install

# Build SDK first (copy only SDK source)
COPY packages/sdk/ ./packages/sdk/
WORKDIR /publisher/packages/sdk
RUN bun install --frozen-lockfile
RUN bun run build

# Install app dependencies and build app
WORKDIR /publisher/packages/app
RUN bun install --frozen-lockfile
COPY packages/app/ ./
RUN NODE_OPTIONS='--max-old-space-size=4096' bun run build:server

# Install server dependencies and build server
WORKDIR /publisher/packages/server
RUN bun install --frozen-lockfile
COPY packages/server/ ./
RUN bun run build:server-only

# Production runtime
FROM debian:bookworm-slim AS runner
WORKDIR /publisher

# ---- Bun install (same version as builder) ----
RUN apt-get update && apt-get install -y curl ca-certificates unzip git && \
    curl -fsSL https://bun.sh/install | bash && \
    ln -s /root/.bun/bin/bun /usr/local/bin/bun && \
    rm -rf /var/lib/apt/lists/*

# --- System dependencies with correct OpenSSL stack ---
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openssl libcurl4 libssl3 dnsutils iputils-ping unzip file && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# ---- Copy built artifacts from builder ----
COPY --from=builder /publisher/package.json /publisher/bun.lock ./
COPY --from=builder /publisher/packages/app/dist/ /publisher/packages/app/dist/
COPY --from=builder /publisher/packages/app/package.json /publisher/packages/app/package.json
COPY --from=builder /publisher/packages/server/dist/ /publisher/packages/server/dist/
COPY --from=builder /publisher/packages/server/package.json /publisher/packages/server/package.json
COPY --from=builder /publisher/packages/sdk/dist/ /publisher/packages/sdk/dist/
COPY --from=builder /publisher/packages/sdk/package.json /publisher/packages/sdk/package.json

# --- Install production-only deps ---
RUN bun install --production

# --- DuckDB CLI ---
RUN curl -L https://install.duckdb.org | bash && \
    ln -s /root/.duckdb/cli/latest/duckdb /usr/local/bin/duckdb
ENV PATH="/root/.duckdb/cli/latest:$PATH"

# --- ADBC Snowflake driver ---
RUN curl -sSL https://raw.githubusercontent.com/iqea-ai/duckdb-snowflake/main/scripts/install-adbc-driver.sh | bash && \
    ldconfig && \
    echo "Verifying DuckDB Snowflake extension..." && \
    duckdb -c "INSTALL snowflake FROM community; LOAD snowflake; SELECT snowflake_version();" || \
    echo "Snowflake verification skipped (offline build)"

# --- Node 20 LTS runtime ---
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - && \
    apt-get install -y nodejs && \
    rm -rf /var/lib/apt/lists/*

# --- Runtime config ---
ENV NODE_ENV=production
RUN mkdir -p /etc/publisher
EXPOSE 4000

CMD ["node", "--require", "./packages/server/dist/instrumentation.js", "./packages/server/dist/server.js"]