# 🤖 Malloy Slack Bot - Local Development Guide

Complete guide for setting up and running the Malloy Slack Bot locally with full LangChain agent capabilities.

## 📋 Prerequisites

Before you begin, ensure you have:

- **Python 3.8+** installed
- **Node.js 16+** (for the Malloy Publisher MCP server)
- **Slack App** with bot permissions (we'll create this)
- **LLM API Key** (OpenAI, Anthropic, or Vertex AI)

## 🚀 Quick Start

### 1. Automated Setup

```bash
cd examples/slack-bot
./setup-local.sh
```

This script will:
- ✅ Check Python version
- ✅ Create virtual environment
- ✅ Install dependencies
- ✅ Set up environment file
- ✅ Test MCP connectivity

### 2. Configure Environment

Edit the `.env` file created by the setup script:

```bash
# Required: Get from your Slack app
SLACK_BOT_TOKEN=xoxb-your-actual-bot-token
SLACK_APP_TOKEN=xapp-your-actual-app-token

# Required: At least one LLM API key
OPENAI_API_KEY=sk-your-actual-openai-key
# ANTHROPIC_API_KEY=your-anthropic-key
# VERTEX_PROJECT_ID=your-gcp-project

# Required: Local MCP server
MCP_URL=http://localhost:4040/mcp
```

### 3. Start the Servers

**Terminal 1 - Start MCP Server:**
```bash
cd publisher
bun run build
bun run start
```

**Terminal 2 - Start Slack Bot:**
```bash
cd examples/slack-bot
source venv/bin/activate
python bot.py
```

## 🔐 Slack App Setup

### Create Slack App

1. Go to [https://api.slack.com/apps](https://api.slack.com/apps)
2. Click **"Create New App"** → **"From scratch"**
3. Enter app name: `Malloy Data Bot`
4. Select your workspace

### Configure Bot Permissions

1. **OAuth & Permissions** → **Scopes** → **Bot Token Scopes**:
   ```
   app_mentions:read
   channels:history
   channels:read
   chat:write
   files:write
   groups:history
   ```

2. **Install App to Workspace** → Copy **Bot User OAuth Token** (starts with `xoxb-`) and paste into your .env file

### Enable Socket Mode

1. **Socket Mode** → **Enable Socket Mode** → ✅
2. **Generate App-Level Token**:
   - Token Name: `Malloy Bot Token`
   - Scope: `connections:write`
   - Copy **App-Level Token** (starts with `xapp-`) and paste into your .env file

### Subscribe to Events

1. **Event Subscriptions** → **Enable Events** → ✅
2. **Subscribe to bot events**:
   ```
   app_mention
   message.channels
   message.groups
   ```

## 🧠 Agent Architecture

The bot includes a full LangChain agent:

```
Slack Message
    ↓
bot.py (Socket Mode)
    ↓
LangChainCompatibilityAdapter
    ↓
MalloyLangChainAgent
    ↓
LLM API Call (OpenAI/Anthropic/Vertex)
    ↓
MCP Tools Discovery & Usage
    ↓
Malloy Publisher Server (localhost:4040)
    ↓
Intelligent Response + Optional Chart
```

### Agent Capabilities

- 🧠 **Intelligent Query Planning**: Analyzes questions and creates appropriate Malloy queries
- 📊 **Data Analysis**: Executes queries and provides insights
- 📈 **Chart Generation**: Creates matplotlib visualizations
- 🔄 **Multi-turn Conversations**: Maintains context across conversation
- 🛠️ **Dynamic Tool Usage**: Discovers and uses MCP tools automatically

## 🎯 Testing Your Setup

### 1. Test MCP Server
```bash
./start-mcp-server.sh
```

### 2. Test Slack Bot
In Slack, mention your bot:
```
@malloy-bot what datasets are available?
```

### 3. Test Chart Generation
```
@malloy-bot show me top 5 brands by sales for 2021 in a chart
```

## 📁 Project Structure

```
examples/slack-bot/
├── bot.py                     # Main bot entry point
├── .env.example              # Environment template
├── .env                      # Your actual config (gitignored)
├── setup-local.sh            # Automated setup script
├── requirements.txt          # Python dependencies
├── venv/                     # Virtual environment (created by setup)
├── src/
│   ├── agents/
│   │   ├── malloy_langchain_agent.py      # Main LangChain agent
│   │   └── langchain_compatibility_adapter.py  # Sync/async adapter
│   ├── clients/
│   │   ├── enhanced_mcp_client.py         # MCP client with retries
│   │   └── mcp_adapter.py                 # MCP protocol adapter
│   ├── tools/
│   │   ├── dynamic_malloy_tools.py        # Dynamic tool discovery
│   │   └── matplotlib_chart_tool.py       # Chart generation
│   └── prompts/
│       └── malloy_prompts.py              # Agent prompt templates
└── tests/                    # Test suite
```

## 🔧 Troubleshooting

### Common Issues

Check the logs for detailed error information:
```bash
tail -f examples/slack-bot/bot.log
```

### Debug Mode

For more verbose logging, set in .env:
```bash
ENVIRONMENT=development
```

Then restart the bot to see detailed debug information.

### Reset Environment

If you need to start fresh:
```bash
rm -rf venv/
rm .env
./setup-local.sh
```

## 🎨 Customization

### Change Model

You can change the LLM model using command line flags:

```bash
# Use Claude
python bot.py --model claude-4-sonnet

# Use GPT-4
python bot.py --model gpt-4o

# Use Gemini  
python bot.py --model gemini-1.5-pro
```

Or set the default in .env:
```bash
LLM_MODEL=claude-4-sonnet    # Anthropic
LLM_MODEL=gemini-1.5-pro     # Vertex AI
LLM_MODEL=gpt-4o             # OpenAI (default)
```

### Modify Agent Behavior

Edit `src/prompts/malloy_prompts.py` to change:
- System prompts
- Response style
- Query generation strategy
- Chart preferences

### Add Custom Tools

Create new tools in `src/tools/` and they'll be automatically discovered by the agent.

## 📚 Additional Resources

- **Malloy Documentation**: [malloydata.github.io/malloy](https://malloydata.github.io/malloy)
- **LangChain Documentation**: [python.langchain.com](https://python.langchain.com)
- **Slack API Documentation**: [api.slack.com](https://api.slack.com)
- **OpenAI API**: [platform.openai.com/docs](https://platform.openai.com/docs)

## 🆘 Getting Help

If you encounter issues:

1. **Check the logs**: `tail -f bot.log`
2. **Test components individually**: MCP server, API keys, Slack permissions
3. **Review error messages**: Often contain specific fix instructions
4. **Try with minimal config**: Use only required environment variables

The bot is designed to provide helpful error messages and suggestions for common issues. 