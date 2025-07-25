#!/bin/bash

# Malloy MCP Server Starter
# Helper script to start the Malloy Publisher MCP server from the slack-bot directory

set -e

echo "🚀 Starting Malloy Publisher MCP Server"
echo "======================================"
echo ""

# Check if we're in the slack-bot directory
if [ ! -f "bot.py" ]; then
    echo "❌ Error: This script should be run from the examples/slack-bot directory"
    echo "   Current directory: $(pwd)"
    exit 1
fi

# Navigate to publisher root
publisher_dir="../../"

if [ ! -f "$publisher_dir/package.json" ]; then
    echo "❌ Error: Could not find publisher package.json at $publisher_dir"
    echo "   Make sure you're running this from examples/slack-bot/"
    exit 1
fi

echo "📂 Found publisher directory at: $(realpath $publisher_dir)"

# Check if npm is installed
if ! command -v npm &> /dev/null; then
    echo "❌ Error: npm is not installed or not in PATH"
    echo "   Please install Node.js and npm first"
    exit 1
fi

echo "✅ npm is available"

# Navigate to publisher directory
cd "$publisher_dir"

echo "📦 Installing/updating dependencies..."
npm install

echo ""
echo "🚀 Starting MCP server..."
echo "   Server will be available at: http://localhost:4040/mcp"
echo "   Press Ctrl+C to stop the server"
echo ""

# Start the server
npm start 