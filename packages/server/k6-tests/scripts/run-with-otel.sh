#!/bin/bash

# Helper script to run k6 tests with OpenTelemetry output to Prometheus
# Usage: ./run-with-otel.sh <test-file> [additional-k6-args...]

set -e

# Default OTEL collector endpoint (gRPC)
# Use 'localhost:4317' when running from host machine
# Use 'otel-collector:4317' when running from inside Docker (like prober service)
OTEL_ENDPOINT="${K6_OTEL_GRPC_EXPORTER_ENDPOINT:-localhost:4317}"
OTEL_SERVICE_NAME="${K6_OTEL_SERVICE_NAME:-k6-load-test}"
OTEL_EXPORTER_PROTOCOL="${K6_OTEL_EXPORTER_PROTOCOL:-grpc}"

# Get the test file (first argument)
TEST_FILE="${1}"
if [ -z "$TEST_FILE" ]; then
    echo "Usage: $0 <test-file> [additional-k6-args...]"
    echo "Example: $0 smoke-test/smoke-test.ts"
    echo "Example: $0 load-test/load-test-crud.ts --duration 5m"
    exit 1
fi

# Shift to get remaining arguments
shift

echo "=========================================="
echo "Running k6 test with OpenTelemetry export"
echo "=========================================="
echo "Test file: $TEST_FILE"
echo "OTEL endpoint: $OTEL_ENDPOINT"
echo "OTEL service name: $OTEL_SERVICE_NAME"
echo "OTEL exporter protocol: $OTEL_EXPORTER_PROTOCOL"
echo "=========================================="
echo ""

# Export k6 metrics to OpenTelemetry
export K6_OTEL_SERVICE_NAME="$OTEL_SERVICE_NAME"
export K6_OTEL_EXPORTER_PROTOCOL="$OTEL_EXPORTER_PROTOCOL"
export K6_OTEL_GRPC_EXPORTER_ENDPOINT="$OTEL_ENDPOINT"
export K6_OTEL_GRPC_EXPORTER_INSECURE="true"  # Disable TLS for plain gRPC
export K6_OTEL_FLUSH_INTERVAL="1s"
export K6_OTEL_EXPORT_INTERVAL="2s"
export K6_OTEL_METRIC_PREFIX="k6_"
# Run k6 with OTEL output
# Note: k6 v1.4.0+ uses "opentelemetry" (stable), "experimental-opentelemetry" is for backward compatibility
k6 run \
  --out "opentelemetry" \
  "$TEST_FILE" \
  "$@"
