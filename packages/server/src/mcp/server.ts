import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ProjectStore } from "../service/project_store";

import { formatDuration, logger } from "../logger";
import { registerPromptCapability } from "./prompts/prompt_service.js";
import { registerModelResource } from "./resources/model_resource";
import { registerNotebookResource } from "./resources/notebook_resource";
import { registerPackageResource } from "./resources/package_resource";
import { registerProjectResource } from "./resources/project_resource";
import { registerQueryResource } from "./resources/query_resource";
import { registerSourceResource } from "./resources/source_resource";
import { registerViewResource } from "./resources/view_resource";
import { registerTools } from "./tools/discovery_tools";
import { registerExecuteQueryTool } from "./tools/execute_query_tool";

export const testServerInfo = {
   name: "malloy-publisher-mcp-server",
   version: "0.0.1",
   displayName: "Malloy Publisher MCP Server",
   description: "Provides access to Malloy models and query execution via MCP.",
};

export function initializeMcpServer(projectStore: ProjectStore): McpServer {
   logger.info("[MCP Init] Starting initializeMcpServer...");
   const startTime = performance.now();

   const mcpServer = new McpServer(testServerInfo);

   logger.info("[MCP Init] Registering project resource...");
   registerProjectResource(mcpServer, projectStore);
   logger.info("[MCP Init] Registering package resource...");
   registerPackageResource(mcpServer, projectStore);

   // Register more specific templates first
   logger.info("[MCP Init] Registering notebook resource...");
   registerNotebookResource(mcpServer, projectStore);
   logger.info("[MCP Init] Registering source resource...");
   registerSourceResource(mcpServer, projectStore);
   logger.info("[MCP Init] Registering query resource...");
   registerQueryResource(mcpServer, projectStore);
   logger.info("[MCP Init] Registering view resource...");
   registerViewResource(mcpServer, projectStore);

   // Register the general model template last among resource types
   logger.info("[MCP Init] Registering model resource...");
   registerModelResource(mcpServer, projectStore);

   logger.info("[MCP Init] Registering executeQuery tool...");
   registerExecuteQueryTool(mcpServer, projectStore);

   registerTools(mcpServer, projectStore);

   logger.info("[MCP Init] Registering prompt capability...");
   registerPromptCapability(mcpServer, projectStore);

   const endTime = performance.now();
   logger.info(`[MCP Init] Finished initializeMcpServer`, {
      duration: formatDuration(endTime - startTime),
   });

   return mcpServer;
}
