#!/bin/bash
# Run k6 CRUD tests locally
# Usage: ./run_crud_test.sh [--port PORT] [--no-bigquery]

set -euo pipefail

# Defaults
PORT="${K6_PUBLISHER_PORT:-4002}"
PROJECT_NAME="${K6_PROJECT_NAME:-malloy-samples}"
NO_BIGQUERY=false

# Parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    --port) PORT="$2"; shift 2 ;;
    --no-bigquery) NO_BIGQUERY=true; shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

PUBLISHER_URL="http://localhost:${PORT}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Required env vars
export K6_PUBLISHER_URL="${PUBLISHER_URL}"
export K6_PROJECT_NAME="${PROJECT_NAME}"

# BigQuery credentials
if [ "$NO_BIGQUERY" = false ]; then
  if [ -z "${GOOGLE_APPLICATION_CREDENTIALS:-}" ]; then
    echo "Warning: GOOGLE_APPLICATION_CREDENTIALS is not set."
    echo "BigQuery packages will be skipped. Set it or use --no-bigquery to suppress this warning."
  else
    echo "BigQuery credentials: ${GOOGLE_APPLICATION_CREDENTIALS}"
    export GOOGLE_APPLICATION_CREDENTIALS
  fi
fi

echo "========================================"
echo "  k6 CRUD Tests"
echo "========================================"
echo "  Publisher URL:  ${PUBLISHER_URL}"
echo "  Project Name:   ${PROJECT_NAME}"
echo "  BigQuery:       $([ "$NO_BIGQUERY" = true ] && echo "disabled" || echo "${GOOGLE_APPLICATION_CREDENTIALS:-not set}")"
echo "========================================"

# Verify server is running
echo "Verifying server is running..."
response=$(curl -sf "${PUBLISHER_URL}/api/v0/projects" 2>&1) || {
  echo "Error: Server is not running at ${PUBLISHER_URL}"
  echo "Start the server first: cd packages/server && npx malloy-publisher --port ${PORT} --server_root ./"
  exit 1
}
echo "Server is up. Projects: ${response}"
echo ""

# Run the combined CRUD test
echo "Starting k6 CRUD tests..."
cd "${SCRIPT_DIR}"
k6 run --verbose load-test/load-test-crud.ts



# # Basic (assumes server running on port 4002)
# cd packages/server/k6-tests
# ./run_crud_test.sh

# # Custom port
# ./run_crud_test.sh --port 4000

# # Skip BigQuery
# ./run_crud_test.sh --no-bigquery

# # With BigQuery credentials
# GOOGLE_APPLICATION_CREDENTIALS=/path/to/creds.json ./run_crud_test.sh
