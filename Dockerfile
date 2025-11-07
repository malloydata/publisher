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

FROM oven/bun:1.2.19-slim AS runner
WORKDIR /publisher
COPY --from=builder /publisher/package.json /publisher/bun.lock ./
# Copy app runtime dependencies
COPY --from=builder /publisher/packages/app/dist/ /publisher/packages/app/dist/
COPY --from=builder /publisher/packages/app/package.json /publisher/packages/app/package.json
# Copy server runtime dependencies
COPY --from=builder /publisher/packages/server/dist/ /publisher/packages/server/dist/
COPY --from=builder /publisher/packages/server/package.json /publisher/packages/server/package.json
# Copy sdk runtime dependencies
COPY --from=builder /publisher/packages/sdk/dist/ /publisher/packages/sdk/dist/
COPY --from=builder /publisher/packages/sdk/package.json /publisher/packages/sdk/package.json
RUN bun install --production

ENV NODE_ENV=production
RUN mkdir -p /etc/publisher
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git
EXPOSE 4000
CMD ["node", "--require", "./packages/server/dist/instrumentation.js", "./packages/server/dist/server.js"]