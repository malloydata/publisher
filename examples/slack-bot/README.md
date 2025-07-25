# 🤖 Malloy Slack Bot

An intelligent Slack bot powered by Malloy for data analysis and visualization.

## 🏠 Local Development Setup

This bot runs locally on your machine with the following architecture:
- **Terminal 1**: Malloy Publisher MCP Server (localhost:4040/mcp)
- **Terminal 2**: Slack Bot with LangChain Agent → LLM → MCP Tools

### Quick Start

For detailed local setup instructions, see:
**→ [README_LOCAL.md](./README_LOCAL.md)** - Complete setup guide with troubleshooting

Or use the automated setup:
```bash
cd examples/slack-bot
./setup-local.sh
```

### What the Bot Does

The bot includes a full LangChain agent that can:
- 🧠 **Intelligent Query Planning**: Analyzes user questions and plans appropriate Malloy queries
- 📊 **Data Analysis**: Executes Malloy queries against your datasets  
- 📈 **Chart Generation**: Creates matplotlib visualizations
- 🔄 **Multi-turn Conversations**: Maintains context across multiple questions
- 🛠️ **Tool Integration**: Dynamically discovers and uses MCP tools

### Agent Architecture

```
Slack Message → bot.py → LangChainCompatibilityAdapter → MalloyLangChainAgent → LLM (OpenAI/Anthropic/Vertex) → MCP Tools → Publisher Server → Intelligent Response
```

### Supported LLM Providers
- **OpenAI** (GPT-4, GPT-4o, GPT-3.5-turbo)
- **Anthropic** (Claude models)
- **Google Vertex AI** (Gemini models)

## 📁 Project Structure

```
examples/slack-bot/
├── bot.py                 # Main bot entry point
├── src/
│   ├── agents/           # LangChain agent implementation
│   ├── clients/          # Enhanced MCP client
│   ├── tools/            # Chart generation tools
│   └── prompts/          # Agent prompts and templates
├── tests/                # Test suite
└── README_LOCAL.md       # Local development guide
```

## 🧪 Development

See `README_LOCAL.md` for:
- Environment setup
- Slack app configuration
- Local testing workflow
- Troubleshooting common issues 