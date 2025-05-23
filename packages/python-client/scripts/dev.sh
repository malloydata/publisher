#!/usr/bin/env bash
set -euo pipefail

case "$1" in
    "test")
        pytest
        ;;
    "lint")
        black --check .
        ruff check .
        pyright .
        ;;
    "docs")
        mkdocs serve
        ;;
    "clean")
        rm -rf dist/ build/ .coverage htmlcov/
        find . -type d -name "__pycache__" -exec rm -rf {} +
        ;;
    *)
        echo "Unknown command: $1"
        echo "Available commands: test, lint, docs, clean"
        exit 1
        ;;
esac 