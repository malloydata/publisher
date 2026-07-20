# Malloy Publisher Server

The Malloy Publisher Server is an Express.js server that provides an API for managing and accessing Malloy data models, packages, and queries

## Configuration

`publisher.config.json` lives in this directory. The repository root also contains a symlink (`/publisher.config.json` → `./packages/server/publisher.config.json`) so that running the server from either location picks up the same config. Edit one and you've edited both.

For the BigQuery-enabled variant, see [`publisher.config.example.bigquery.json`](./publisher.config.example.bigquery.json) and the [Quick Start in the repo root README](../../README.md#quick-start).

### Remaining deprecation warnings

Removing the unused `trino` CLI direct dep (it pulled in `@google-cloud/translate@0.7.x` → `request@2.x` → `har-validator` → `hawk` → `cryptiles`) cleaned the worst chain. About 25 `npm warn deprecated` lines remain on `npx @malloy-publisher/server` install, all upstream-owned:

- **npm CLI tooling**: `npmlog`, `gauge`, `are-we-there-yet`, `glob@7/8/10`, `rimraf@3`, `tar@6.2.1`, `inflight`, `@npmcli/move-file`, `node-domexception`, `querystring` — pulled in by npm itself and by `node-pre-gyp`/`node-gyp`. Not actionable from this repo.
- **`uuid@8.x` / `uuid@9.x`**: surfaced across multiple transitives (Malloy, AWS SDKs, others). Resolves when each upstream bumps to `uuid@11`.
- **`q@1.5.1`**: pulled in via `thrift` → `@databricks/sql` → `@malloydata/db-databricks`. Resolves when Databricks upgrades `@databricks/sql` past the thrift dep, or when we replace the Databricks driver.
- **`aws-sdk@2.1693.0`**: listed as a direct dep in `packages/server/package.json` but not imported anywhere in source — leftover, candidate for removal in a follow-up PR. The actual consumer is `@aws-sdk/client-s3` v3.

## K6 Test Presets

The Malloy Publisher Server includes several K6 test presets to help you test its performance and stability.

Below is a list of the available test presets:

### Smoke Test

Basic functionality test with minimal load.

- **File:**
  `./k6-tests/smoke-test.ts`
- **Virtual Users:** 1
- **Duration:** 1 minute
- **95th Percentile Response Time:** < 500ms
- **Error Rate:** < 1%

### Load Test

Testing system under normal load.

- **File:**
  `./k6-tests/load-test.ts`
- **Virtual Users:** 50
- **Duration:** 5 minutes
- **95th Percentile Response Time:** < 1s
- **Error Rate:** < 5%

### Stress Test

Testing system under extreme load.

- **File:**

   `./k6-tests/stress-test.ts`

- **Virtual Users:** 100
- **Duration:** 10 minutes
- **95th Percentile Response Time:** < 2s
- **Error Rate:** < 10%

### Spike Test

Testing system under sudden spikes of load.

- **File:**
  `./k6-tests/spike-test.ts`
- **Stages:**
   - 2 minutes ramp-up to 100 users
   - 1 minute at 100 users
   - 2 minutes ramp-down to 0 users
- **95th Percentile Response Time:** < 2s
- **Error Rate:** < 10%

### Breakpoint Test

Testing system to find its breaking point.

- **File:**
  `./k6-tests/breakpoint-test.ts`
- **Stages:**
   - 2 minutes at 50 users
   - 2 minutes at 100 users
   - 2 minutes at 150 users
   - 2 minutes at 200 users
   - 2 minutes ramp-down to 0 users
- **95th Percentile Response Time:** < 3s
- **Error Rate:** < 15%

### Soak Test

Testing system under sustained load.

- **File:**
  `./k6-tests/soak-test.ts`
- **Virtual Users:** 10
- **Duration:** 1 hour
- **95th Percentile Response Time:** < 1s
- **Error Rate:** < 1%

You can run these presets using the K6 testing tool to ensure your system performs well under different load conditions.

For example, this command will run a smoke test against your localhost:

```sh
k6 run ./k6-tests/smoke-test.ts --env PUBLISHER_URL=http://::1:4000
```

## OpenTelemetry Integration

The K6 tests can be configured to export metrics to OpenTelemetry collectors using the experimental OpenTelemetry output. This allows you to integrate K6 metrics with your observability stack.

```sh
# Build the publisher server
bun run build
# Replace this with an actual OTLP endpoint that you can use
MY_OTLP_ENDPOINT=http://monitoring.myserver.com:4318
# Start an instrumented publisher server
OTEL_EXPORTER_OTLP_ENDPOINT=${MY_OTLP_ENDPOINT} PACKAGE_ROOT=./malloy-samples bun start:instrumented
# Start an instrumented k6 smoke test
K6_OTEL_HTTP_EXPORTER_ENDPOINT=${MY_OTLP_ENDPOINT} K6_OTEL_GRPC_EXPORTER_INSECURE=true K6_OTEL_METRIC_PREFIX=k6_ k6 run ./k6-tests/smoke-test.ts --env PUBLISHER_URL=http://::1:4000
```

For more information on how to configure OpenTelemetry collectors, please refer to the official documentation: [K6 OpenTelemetry Integration](https://grafana.com/docs/k6/latest/results-output/real-time/opentelemetry/)

## MCP Prompt Capability

Publisher's MCP interface exposes the bundled agent **skills** (under [`skills/`](../../skills/)) as **LLM-ready prompts**, so hosts that ingest MCP but do not load skill files can pull the same guidance. For authoring or contributing skills, see [`docs/agent-skills/`](../../docs/agent-skills/).

List prompts:

```bash
mcp-client prompts/list
```

Get a prompt:

```bash
mcp-client prompts/get --name malloy-analysis
```

These calls return `messages` ready for your LLM chat completion.
