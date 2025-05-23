#!/usr/bin/env bash
cd "$(dirname "$0")/.."
set -euo pipefail

SDK_OUTPUT_DIR="malloy_publisher_sdk"

# Step 1: Environment setup
# Require uv to be available
if ! command -v uv &>/dev/null; then
  echo "❌  'uv' command not found. Install uv (https://astral.sh/uv) and re-run." >&2
  exit 1
fi

if [ ! -f .venv/bin/activate ]; then
  echo "Creating virtual environment with uv…"
  uv venv .venv
fi

source .venv/bin/activate

# Step 2: Install dependencies
echo "Installing dependencies..."
# Install editable with test extras using uv
uv pip install -e ".[dev,test]"

# Step 3: Validate OpenAPI spec
echo "Validating OpenAPI spec..."
uvx openapi-spec-validator ../../api-doc.yaml

# Step 4: Generate client
echo "Generating Python SDK into $SDK_OUTPUT_DIR/..."
# Clean previous generation if it exists in the target directory
rm -rf "$SDK_OUTPUT_DIR"
# No need to mkdir -p if overwrite is used and it creates the dir
uvx openapi-python-client generate \
    --path ../../api-doc.yaml \
    --config ./openapi-client.json \
    --meta none \
    --output-path "$SDK_OUTPUT_DIR" \
    --overwrite

# Step 5: Format generated code
echo "Formatting generated code..."
black "$SDK_OUTPUT_DIR" tests
ruff check "$SDK_OUTPUT_DIR" tests --fix

# Step 6: Type check
echo "Type checking..."
pyright "$SDK_OUTPUT_DIR" tests || true

# Step 7: Run tests
echo "Running tests..."
# Ensure the current directory is in PYTHONPATH so malloy_publisher_sdk is found
export PYTHONPATH=$(pwd):${PYTHONPATH:-}
pytest tests/

# Step 8: Clean up build artifacts (not the SDK itself)
find . -type d -name "__pycache__" -exec rm -rf {} + # General cleanup
find "$SDK_OUTPUT_DIR" -type d -name "__pycache__" -exec rm -rf {} + # SDK specific
find . -type f -name "*.pyc" -delete

echo "✅ Build complete!" 