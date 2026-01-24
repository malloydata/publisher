# k6-tests

Performance and load testing suite for the Malloy Publisher API using k6.

## Installation

To install dependencies:

```bash
bun install
```

## Prerequisites

**Important:** Before running tests, ensure the publisher server is running with the following environment variables to limit log output:

```bash
DISABLE_RESPONSE_LOGGING=true
LOG_LEVEL=info  # or LOG_LEVEL=warn
```

Start the server before running any k6 tests.

## Available Tests

### Smoke Test

Basic functionality verification under minimal load:

```bash
bun run smoke-test              # Standard test
./scripts/run-with-otel.sh smoke-test/smoke-test.ts  # With OpenTelemetry export
```

### Load Tests

#### CRUD Load Tests

Specific tests for CRUD (Create, Read, Update, Delete) operations on individual resource types:

```bash
bun run load-test-crud-projects      # Projects CRUD load test
bun run load-test-crud-connections   # Connections CRUD load test
bun run load-test-crud-packages      # Packages CRUD load test
bun run load-test-crud               # Run all three CRUD tests sequentially
```

Run CRUD tests with OpenTelemetry export:

```bash
./scripts/run-with-otel.sh load-test/load-test-crud-projects.ts
./scripts/run-with-otel.sh load-test/load-test-crud-connections.ts
./scripts/run-with-otel.sh load-test/load-test-crud-packages.ts
```

#### Comprehensive Load Test

Focused on comprehensive API read operations and query execution. Tests listing, getting, and querying various resources (projects, packages, models, notebooks, connections, databases, queries, SQL sources) under normal load:

```bash
bun run load-test               # Standard test
./scripts/run-with-otel.sh load-test/load-test.ts  # With OTEL export
```

## OpenTelemetry & Prometheus Integration

Export k6 metrics to Prometheus via OpenTelemetry for visualization in Grafana.


**Usage:**
Use the `run-with-otel.sh` script to run any test with OpenTelemetry export:

```bash
./scripts/run-with-otel.sh <test-file> [additional-k6-args...]
```

Examples:
```bash
./scripts/run-with-otel.sh smoke-test/smoke-test.ts
./scripts/run-with-otel.sh load-test/load-test.ts --duration 5m
./scripts/run-with-otel.sh load-test/load-test-crud-projects.ts
```

## Environment Variables

### Test Configuration

- `K6_PUBLISHER_URL` - Base URL of the publisher service (default: `http://localhost:4000`)
- `K6_PROJECT_NAME` - Project name for tests (default: `malloy-samples`)
- `K6_AUTH_TOKEN` - Authorization token for API requests
- `GOOGLE_APPLICATION_CREDENTIALS` - BigQuery service account JSON (for BigQuery tests)
- `K6_USE_VERSION_ID` - Set to `"true"` to use version IDs in API calls - uses "latest" as versionId when set to true
- `K6_MAX_VIEWS_PER_MODEL` - Maximum views to test per model (default: `5`)
- `K6_DEBUG` - Set to `"true"` for verbose logging
- `K6_WHITELISTED_PACKAGES` - JSON array of package names to test
- `K6_AVAILABLE_PACKAGES` - JSON array of available package names

### OpenTelemetry Export (when using `run-with-otel.sh`)

These environment variables are automatically set by `run-with-otel.sh` but can be overridden:

- `K6_OTEL_GRPC_EXPORTER_ENDPOINT` - OTEL collector gRPC endpoint (default: `localhost:4317`)
- `K6_OTEL_GRPC_EXPORTER_INSECURE` - Disable TLS for plain gRPC (default: `true` - set automatically by script)
- `K6_OTEL_SERVICE_NAME` - Service name for metrics (default: `k6-load-test`)
- `K6_OTEL_EXPORTER_PROTOCOL` - Exporter protocol: `grpc` or `http/protobuf` (default: `grpc`)
- `K6_OTEL_HTTP_EXPORTER_ENDPOINT` - OTEL collector HTTP endpoint (default: `localhost:4318`)
- `K6_OTEL_FLUSH_INTERVAL` - How often k6 flushes internal metrics (default: `1s`)
- `K6_OTEL_EXPORT_INTERVAL` - How often k6 exports to OTEL collector (default: `2s` in script, `10s` otherwise)

## Setup & Build

### Clone Malloy Samples

Clone the malloy-samples repository (required for tests):

```bash
bun run clone-malloy-samples
```

This clones the [malloy-samples](https://github.com/credibledata/malloy-samples) repository into `./packages` directory.

### Client Generation

To regenerate API clients from OpenAPI spec:

```bash
bun run generate-clients
```

### Build

Run the full build process (cleanup, clone samples, and generate clients):

```bash
bun run build
```

## Cleanup

Remove generated files and dependencies:

```bash
bun run clean
```
