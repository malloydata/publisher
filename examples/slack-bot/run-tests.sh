#!/bin/bash
# Simple test runner for essential slack-bot functionality

echo "🧪 Running Essential Slack Bot Tests"
echo "====================================="

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "❌ pytest not found. Installing..."
    pip install pytest pytest-asyncio
fi

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    echo "📦 Activating virtual environment..."
    source venv/bin/activate
fi

# Set environment variables for testing
export PYTHONPATH="$PWD:$PYTHONPATH"

echo "🔬 Running Unit Tests..."
echo "------------------------"

# Test MCP client
echo "📡 Testing MCP Client..."
python -m pytest tests/unit/test_enhanced_mcp.py -v

# Test prompt templates  
echo "💬 Testing Prompt Templates..."
python -m pytest tests/unit/test_prompt_templates.py -v

# Test chart generation
echo "📊 Testing Chart Generation..."
python -m pytest tests/charts/test_matplotlib_tool.py -v

echo ""
echo "✅ Essential tests completed!"
echo "🚀 Ready for local development" 