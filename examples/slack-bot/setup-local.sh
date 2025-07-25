#!/bin/bash

# Malloy Slack Bot - Local Setup Script
# This script sets up the local development environment for the Malloy Slack Bot

set -e

echo "🤖 Malloy Slack Bot - Local Setup"
echo "================================="
echo ""

# Check if we're in the right directory
if [ ! -f "bot.py" ]; then
    echo "❌ Error: This script must be run from the examples/slack-bot directory"
    echo "   Current directory: $(pwd)"
    echo "   Expected files: bot.py, requirements.txt, src/"
    exit 1
fi

echo "✅ Found bot.py - we're in the right directory"
echo ""

# Check Python version
python_version=$(python3 --version 2>&1 | cut -d' ' -f2)
echo "🐍 Python version: $python_version"

if ! python3 -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)" 2>/dev/null; then
    echo "❌ Error: Python 3.8+ is required. Found: $python_version"
    echo "   Please install Python 3.8 or later"
    exit 1
fi

echo "✅ Python version is compatible"
echo ""

# Create virtual environment if it doesn't exist
if [ ! -d "venv" ]; then
    echo "📦 Creating Python virtual environment..."
    python3 -m venv venv
    echo "✅ Virtual environment created"
else
    echo "✅ Virtual environment already exists"
fi

# Activate virtual environment
echo "🔄 Activating virtual environment..."
source venv/bin/activate

# Upgrade pip
echo "📦 Upgrading pip..."
pip install --upgrade pip

# Install requirements
echo "📦 Installing Python dependencies..."
pip install -r requirements.txt

echo "✅ Dependencies installed successfully"
echo ""

# Setup environment file
if [ ! -f ".env" ]; then
    echo "⚙️  Setting up environment configuration..."
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "✅ Created .env from .env.example"
        echo ""
        echo "📝 IMPORTANT: Please edit .env and fill in your actual values:"
        echo "   - SLACK_BOT_TOKEN (from your Slack app)"
        echo "   - SLACK_APP_TOKEN (from your Slack app)"
        echo "   - OPENAI_API_KEY (from OpenAI)"
        echo "   - Optionally: ANTHROPIC_API_KEY or Vertex AI credentials"
        echo ""
    else
        echo "❌ Warning: .env.example not found, creating basic .env"
        cat > .env << 'EOF'
# Fill in these values
SLACK_BOT_TOKEN=xoxb-your-bot-token-here
SLACK_APP_TOKEN=xapp-your-app-token-here
OPENAI_API_KEY=sk-your-openai-key-here
MCP_URL=http://localhost:4040/mcp
ENVIRONMENT=development
EOF
    fi
else
    echo "✅ .env file already exists"
fi

echo ""

# Test MCP server connectivity
echo "🔗 Testing MCP server connectivity..."
mcp_url=$(grep "^MCP_URL=" .env 2>/dev/null | cut -d'=' -f2 || echo "http://localhost:4040/mcp")

if curl -s --connect-timeout 5 "$mcp_url" > /dev/null 2>&1; then
    echo "✅ MCP server is reachable at $mcp_url"
else
    echo "⚠️  MCP server not reachable at $mcp_url"
    echo "   Make sure the Malloy Publisher MCP server is running:"
    echo "   → In another terminal: cd publisher && npm start"
    echo "   (This is normal if you haven't started the MCP server yet)"
fi

echo ""

# Final instructions
echo "🎉 Setup complete!"
echo ""
echo "Next steps:"
echo "1. 📝 Edit .env file with your actual tokens and API keys"
echo "2. 🚀 Start the Malloy MCP server in another terminal:"
echo "   cd ../../ && npm start"
echo "3. 🤖 Start the Slack bot:"
echo "   source venv/bin/activate && python bot.py"
echo ""
echo "📚 For detailed setup instructions, see:"
echo "   → README_LOCAL.md (when created)"
echo "   → LOCAL_SETUP_PLAN.md"
echo ""
echo "🛠️  Troubleshooting:"
echo "   - Ensure MCP server is running on localhost:4040"
echo "   - Check that your Slack app has proper permissions"
echo "   - Verify API keys are correct and have sufficient credits"
echo ""
echo "Happy data analyzing! 🚀" 