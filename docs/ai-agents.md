# AI Agents with the Model Context Protocol (MCP) Server

## 1. Overview

The Malloy Publisher includes a server implementing the **Model Context Protocol (MCP)**, a standardized interface designed to connect Large Language Models (LLMs) and AI agents directly to governed, semantic data models.

At its core, the MCP server acts as a **gateway**, allowing you to have a natural language conversation with your data. Instead of writing complex queries, you can ask questions in plain English. The server leverages a Malloy model—your single source of truth for business logic and data relationships—to interpret these questions and generate trustworthy, accurate answers.

The key benefit is that any MCP-compatible client can connect to your data. The ecosystem of clients is evolving quickly, and you could connect the server to various AI chat applications, custom scripts, or other tools.

For more comprehensive details on the Malloy Publisher, please visit the [Malloy Publisher GitHub repository](https://github.com/malloydata/publisher).

---

## 2. MCP Server Capabilities

When running, the Malloy Publisher exposes its capabilities via an **MCP endpoint** at `http://localhost:4040/mcp`. An MCP-compatible client can interact with this endpoint to access several features of your semantic model.

#### Tool Calls

The primary way clients interact with the server is through tool calls. These are functions the AI can use to explore and query your data models.

* **Retrieval Tools**: Used by the AI to ground itself in what a model actually defines instead of guessing names.
    * `malloy_getContext`: Given a plain-English question, returns the most relevant model entities (sources, views, named queries, and dimension/measure fields) for a package.
    * `malloy_searchDocs`: Keyword search over a bundled index of the Malloy documentation.
* **Query Execution Tool**: Used by the AI to get data.
    * `malloy_executeQuery`: Executes a Malloy query and returns the results in JSON format. Supports `givens` for supplying values to model-declared [runtime parameters](givens.md). Also supports the deprecated `filterParams` argument for the legacy [`#(filter)` annotation path](filters.md).

#### Prompts

The MCP server also exposes the bundled agent **skills** (under [`skills/`](../skills/)) as **prompts**, so hosts that ingest MCP but do not load skill files can pull the same guidance. For authoring or contributing skills, see [`docs/agent-skills/`](agent-skills/).

Developer tools like the **MCP Inspector** can allow you to explore these features. For the demonstration below, we will focus on **tool calls**, which are the primary interaction method used by the Claude app.

---

## 3. Demonstration: Connecting Claude Desktop to a Local MCP Server

This walkthrough will guide you through running the MCP server locally and configuring the Claude Desktop App to use its tool-calling capabilities.

### Prerequisites

Before you begin, ensure you have the following set up:

* **An Existing Malloy Model**: This demo will use the `hackernews.malloy` model from the [Malloy Samples Data Repository](https://github.com/malloydata/malloy-samples).
* **Node.js & Bun**: The Publisher server runs on Bun, a fast JavaScript runtime.
* **Claude Desktop App**: The specific AI client we are using for this demonstration. 

### Step 1: Start the Malloy Publisher MCP Server

The easiest way to get started is to run the server directly using `npx`, pointing it to a local copy of the Malloy samples.

1.  First, clone the `malloy-samples` repository:
    ```bash
    git clone https://github.com/malloydata/malloy-samples.git
    ```
2.  Navigate into the newly created directory:
    ```bash
    cd malloy-samples
    ```
3.  Run the publisher server, telling it to use the current directory as its root:
    ```bash
    npx @malloy-publisher/server --server_root .
    ```

After running the command, you should see output confirming the server is active:

```bash
MCP server listening at http://localhost:4040
```

This is the recommended approach for a quick start. For more details and alternative methods, such as building from the source, see the official **[Build and Run Instructions](https://github.com/malloydata/publisher?tab=readme-ov-file#build-and-run-instructions)**.

### Step 2: Configure the Claude Desktop App

The Publisher MCP server speaks the streamable HTTP transport at `http://localhost:4040/mcp`. Claude Desktop launches MCP servers over stdio, so we use [`mcp-remote`](https://www.npmjs.com/package/mcp-remote) — a small, widely-used shim that bridges a stdio MCP client to a remote HTTP MCP server. It runs on demand via `npx`, so there is nothing to install.

1.  In the Claude desktop app, navigate to **Settings > Developer > Edit Config**.
2.  This will open a JSON configuration file.
3.  Add or edit the `mcpServers` section to point `mcp-remote` at the local MCP endpoint:
    ```json
    {
      "mcpServers": {
        "malloy": {
          "command": "npx",
          "args": ["-y", "mcp-remote", "http://localhost:4040/mcp"]
        }
      }
    }
    ```
4.  Save the configuration file and restart Claude Desktop. Claude will now route any requests for the "malloy" toolset to the Publisher MCP server.

### Starting a Conversation

Once the server is running and Claude is configured, the setup is complete. You can now start a new conversation and ask questions about your data directly. Using the MCP retrieval tools, Claude will find the relevant entities in your models, understand their structure, and execute queries to answer your questions.

[Watch the demo video here.](https://www.loom.com/share/fcc5112ac1ca4bf78bee0985f1cd31be)

#### Example Prompts

Here are a few examples of questions you could ask, based on the models found in the `malloy-samples` repository:

* *"Use malloy to run an exploratory data analysis on the FAA dataset."*
* *"Use malloy to help me understand the ecommerce data. Create charts to visualize the data."*
* *"Use malloy to check how many movies Tom Hanks has been in."*

---

## 4. Troubleshooting and Debugging

### Common Issues

1.  **Connection Errors**:
    * Ensure the Malloy Publisher server is running and listening on port 4040.
    * Confirm the URL in Claude's JSON configuration is `http://localhost:4040/mcp`.
    * Verify that Node.js (which provides `npx`) is installed and available in your system's PATH.
2.  **Model or Query Errors**:
    * Confirm that your Malloy model files are located within the directory you pointed the server to.
    * Check the Malloy model syntax for errors.

### Debugging

To diagnose issues, you can inspect the logs from the Claude app and the Publisher server.

* **Claude App MCP Logs**: See requests from Claude's perspective. In the Claude desktop app, click the **Developer** menu and select **Open MCP Log file**.
* **Publisher Server Logs**: The server logs each MCP request it handles to stdout; run it in a terminal to watch requests and error details in real time.

---

## 5. Further Information

The Malloy Publisher and the Model Context Protocol are under active development. For the latest updates, advanced usage patterns, and information on future enhancements, please refer to the official **[Malloy Publisher repository](https://github.com/malloydata/publisher)**.