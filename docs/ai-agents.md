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

The primary way clients interact with the server is through tool calls. These are functions the AI can use to discover and query your data models.

* **Discovery Tools**: Used by the AI to understand what data is available.
    * `malloy_projectList`: Lists all available Malloy projects.
    * `malloy_packageList`: Lists all the packages contained within a specific project.
    * `malloy_packageGet`: Lists all the models contained within a specific package.
    * `malloy_modelGetText`: Gets the raw text content of a specific model file.
* **Query Execution Tool**: Used by the AI to get data.
    * `malloy_executeQuery`: Executes a Malloy query and returns the results in JSON format. Supports `sourceFilters` for applying [model-declared filters](source-filters.md) at query time.

#### Prompts & Resources

The MCP server can also provide clients with **prompts** (e.g., suggested questions to start a conversation) and **resources** (e.g., links to documentation or data dictionaries). However, these are nascent capabilities of the MCP standard, and many current MCP clients do not yet utilize them.

Developer tools like the **MCP Inspector** can allow you to explore these features. For the demonstration below, we will focus on **tool calls**, which are the primary interaction method used by the Claude app.

---

## 3. Demonstration: Connecting Claude Desktop to a Local MCP Server

This walkthrough will guide you through running the MCP server locally and configuring the Claude Desktop App to use its tool-calling capabilities.

### Prerequisites

Before you begin, ensure you have the following set up:

* **An Existing Malloy Model**: This demo will use the `hackernews.malloy` model from the [Malloy Samples Data Repository](https://github.com/malloydata/malloy-samples).
* **Node.js & Bun**: The Publisher server runs on Bun, a fast JavaScript runtime.
* **Python 3**: Required to run the intermediary bridge script.
* **Claude Desktop App**: The specific AI client we are using for this demonstration. 

### Step 1: Start the Malloy Publisher MCP Server

The easiest way to get started is to run the server directly using `npx`, pointing it to a local copy of the Malloy samples.

1.  First, clone the `malloy-samples` repository:
    ```bash
    git clone https://github.com/credibledata/malloy-samples.git
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

### Step 2: Download the Python Bridge Script

A small intermediary script is needed to connect the current version of the Claude desktop app (version 0.11.4) to the local Malloy MCP server.

* **Download the script here**: [malloy_bridge.py](https://raw.githubusercontent.com/malloydata/publisher/main/packages/server/dxt/malloy_bridge.py)

**Why is this bridge script needed?** This script acts as a "translator." The Claude client has specific expectations for how it communicates with external tools, and this script handles minor formatting differences on the fly to ensure seamless communication with the MCP server.

### Step 3: Configure the Claude Desktop App

Next, you need to tell Claude to use your Python bridge script as its tool server.

1.  In the Claude desktop app, navigate to **Settings > Developer > Edit Config**. 
2.  This will open a JSON configuration file.
3.  Add or edit the `mcpServers` section to point to the `malloy_bridge.py` script you downloaded. Replace `/path/to/your/` with the **actual file path** on your system.
    ```json
    {
      "mcpServers": {
        "malloy": {
          "command": "python3",
          "args": ["/path/to/your/malloy_bridge.py"],
          "env": {},
          "disabled": false
        }
      }
    }
    ```
4.  Save the configuration file. Claude will now route any requests for the "malloy" toolset through your local Python script to the MCP server.

### Starting a Conversation

Once the server is running and Claude is configured, the setup is complete. You can now start a new conversation and ask questions about your data directly. Thanks to the MCP discovery tools, Claude will automatically find your models, understand their structure, and execute queries to answer your questions.

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
    * Double-check that the file path in Claude's JSON configuration is correct.
    * Verify that Python 3 is installed and available in your system's PATH.
2.  **Model or Query Errors**:
    * Confirm that your Malloy model files are located within the directory you pointed the server to.
    * Check the Malloy model syntax for errors.

### Debugging

To diagnose issues, you can inspect the logs from both the Claude app and the Python bridge script.

* **Claude App MCP Logs**: See requests from Claude's perspective. In the Claude desktop app, click the **Developer** menu and select **Open MCP Log file**.
* **Python Bridge Script Logs**: The `malloy_bridge.py` script writes a detailed log of all activity. This is the best place to find specific error messages. You can find this log file at:
    ```
    /tmp/malloy_bridge.log
    ```

---

## 5. Further Information

The Malloy Publisher and the Model Context Protocol are under active development. For the latest updates, advanced usage patterns, and information on future enhancements, please refer to the official **[Malloy Publisher repository](https://github.com/malloydata/publisher)**.