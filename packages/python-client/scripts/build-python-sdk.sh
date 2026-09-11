#!/usr/bin/env bash
# Copyright (c) Credible Data Inc.
# SPDX-License-Identifier: MIT

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
#
# NOT `uvx`. uvx resolves the LATEST release into an ephemeral environment and
# ignores this project's declared dependencies entirely, so the pin in
# pyproject.toml's `dev` extra read as if it governed these tools and did not.
# The venv is already activated above and `uv pip install -e ".[dev,test]"` has
# already installed both of them into it, so a bare command name runs the
# version the project declares.
#
# This matters more since release.yml started publishing this package: the
# generated client IS the wheel's public surface, and a PyPI filename can never
# be reused. With uvx, what shipped was whatever the generator's latest emitted
# at publish time, and nothing recorded which generator produced a release.
echo "Validating OpenAPI spec..."
openapi-spec-validator ../../api-doc.yaml

# Step 4: Generate client
echo "Generating Python SDK into $SDK_OUTPUT_DIR/..."
# Clean previous generation if it exists in the target directory
rm -rf "$SDK_OUTPUT_DIR"
# No need to mkdir -p if overwrite is used and it creates the dir
openapi-python-client generate \
    --path ../../api-doc.yaml \
    --config ./openapi-client.json \
    --meta none \
    --output-path "$SDK_OUTPUT_DIR" \
    --overwrite

# Step 5: Add version to __init__.py
echo "Adding version to SDK __init__.py..."
VERSION=$(python -c "import tomllib; print(tomllib.load(open('pyproject.toml', 'rb'))['project']['version'])")
sed -i.bak "s/__version__ = \".*\"/__version__ = \"$VERSION\"/" "$SDK_OUTPUT_DIR/__init__.py" && rm "$SDK_OUTPUT_DIR/__init__.py.bak"

# Step 6: Format generated code
echo "Formatting generated code..."
black "$SDK_OUTPUT_DIR" tests
ruff check "$SDK_OUTPUT_DIR" tests --fix

# Step 7: Type check
echo "Type checking..."
pyright "$SDK_OUTPUT_DIR" tests || true

# Step 8: Run tests
echo "Running tests..."
# Ensure the current directory is in PYTHONPATH so malloy_publisher_sdk is found
export PYTHONPATH=$(pwd):${PYTHONPATH:-}
pytest tests/

# Step 9: Build package (optional, for validation)
if [ "${BUILD_PACKAGE:-}" = "true" ]; then
    echo "Building package..."
    uv pip install build
    rm -rf build/ dist/ *.egg-info/
    python -m build
    echo "Package built successfully in dist/"
fi

# Step 10: Clean up build artifacts (not the SDK itself)
find . -type d -name "__pycache__" -exec rm -rf {} + # General cleanup
find "$SDK_OUTPUT_DIR" -type d -name "__pycache__" -exec rm -rf {} + # SDK specific
find . -type f -name "*.pyc" -delete

echo "✅ Build complete!" 
