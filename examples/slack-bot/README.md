# 🤖 Malloy Slack Bot

An intelligent Slack bot powered by Malloy for data analysis and visualization using LangChain agents.

## Quick Setup

1. **Install dependencies:**
   ```bash
   cd examples/slack-bot
   python3 -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

2. **Configure environment** (create `.env` file):
   ```bash
   # Slack tokens (from your Slack app)
   SLACK_BOT_TOKEN=xoxb-your-bot-token
   SLACK_APP_TOKEN=xapp-your-app-token
   
   # LLM API key (choose one)
   OPENAI_API_KEY=sk-your-openai-key
   # ANTHROPIC_API_KEY=your-anthropic-key
   # VERTEX_PROJECT_ID=your-gcp-project
   
   # MCP server
   MCP_URL=http://localhost:4040/mcp
   ```

3. **Start the servers:**
   ```bash
   # Terminal 1: Start MCP server (from project root)
   bun run build && bun run start
   
   # Terminal 2: Start bot (from examples/slack-bot)
   source venv/bin/activate && python bot.py
   ```

## Slack App Setup

1. Create app at [api.slack.com/apps](https://api.slack.com/apps)
2. **Bot Token Scopes:** `app_mentions:read`, `channels:history`, `channels:read`, `chat:write`, `files:write`, `groups:history`
3. **Enable Socket Mode** with `connections:write` scope
4. **Subscribe to events:** `app_mention`, `message.channels`, `message.groups`

## What It Does

- **Smart Query Planning**: Understands questions and creates appropriate Malloy queries
- **Data Analysis**: Executes queries against your datasets
- **Chart Generation**: Creates visualizations with matplotlib  
- **Multi-turn Conversations**: Maintains context across interactions
- **Dynamic Tool Discovery**: Uses MCP tools automatically

## Usage

```
@malloy-bot what datasets are available?
@malloy-bot show top 5 brands by sales in a chart
```

## Troubleshooting

- **Check logs:** `tail -f bot.log`
- **Test MCP:** Visit `http://localhost:4040/mcp` 
- **Reset:** `rm -rf venv/ .env` then repeat setup steps
- **Debug mode:** Set `ENVIRONMENT=development` in `.env`
- **Tests:** `pytest tests/` (with venv activated) 