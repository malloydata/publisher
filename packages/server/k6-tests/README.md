# k6-tests

Performance and load testing suite for the Malloy Publisher API using k6.

## Installation

To install dependencies:

```bash
bun install
```

## Available Tests

### Smoke Test

Basic functionality verification under minimal load:

```bash
bun run smoke-test
```

### Load Tests

Individual CRUD operation load tests:

```bash
bun run load-test-projects      # Projects CRUD load test
bun run load-test-connections   # Connections CRUD load test
bun run load-test-packages      # Packages CRUD load test
```

Combined load test (runs all three):

```bash
bun run load-test
```

### Performance Tests

```bash
bun run spike-test        # Spike test - sudden traffic spikes
bun run stress-test       # Stress test - beyond normal capacity
bun run breakpoint-test   # Breakpoint test - find maximum capacity
bun run soak-test         # Soak test - sustained load over time
```

### Run All Tests

```bash
bun run all-tests
```

## Environment Variables

- `K6_PUBLISHER_URL` - Base URL of the publisher service (default: `http://localhost:4000`)
- `K6_PROJECT_NAME` - Project name for tests (default: `malloy-samples`)
- `K6_AUTH_TOKEN` - Authorization token for API requests
- `GOOGLE_APPLICATION_CREDENTIALS` - BigQuery service account JSON (for BigQuery tests)
- `K6_USE_VERSION_ID` - Set to `"true"` to use version IDs in API calls - uses "latest" as versionId when set to true
- `K6_MAX_VIEWS_PER_MODEL` - Maximum views to test per model (default: `5`)
- `K6_DEBUG` - Set to `"true"` for verbose logging
- `K6_WHITELISTED_PACKAGES` - JSON array of package names to test
- `K6_AVAILABLE_PACKAGES` - JSON array of available package names

## Client Generation

To regenerate API clients from OpenAPI spec:

```bash
bun run generate-clients
```

## Cleanup

Remove generated files and dependencies:

```bash
bun run clean
```

## Build

Compile TypeScript:

```bash
bun run build
```
