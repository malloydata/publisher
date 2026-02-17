# The generate-api-types scripts require Java.
FROM amazoncorretto:21.0.8 AS java-base

# Production runtime — stable tooling layers first for max caching
FROM oven/bun:1.2.23-slim AS runner

# All system dependencies in a single layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl ca-certificates unzip git \
    openssl libcurl4 libssl3 dnsutils iputils-ping file && \
    update-ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# DuckDB CLI + Snowflake driver + Node 20 LTS
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
RUN bun install

# Build SDK first
COPY packages/sdk/ ./packages/sdk/
WORKDIR /publisher/packages/sdk
RUN bun run build

# Build app
WORKDIR /publisher/packages/app
COPY packages/app/ ./
RUN NODE_OPTIONS='--max-old-space-size=4096' bun run build:server

# Build server
WORKDIR /publisher/packages/server
COPY packages/server/ ./
RUN bun run build:server-only

# Final image — continue from the cached runner base
FROM runner AS final
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
RUN bun install --production

# Runtime config
ENV NODE_ENV=production
ENV PATH="/root/.duckdb/cli/latest:$PATH"
RUN mkdir -p /etc/publisher
EXPOSE 4000

CMD ["bun", "./packages/server/dist/server.js"]
