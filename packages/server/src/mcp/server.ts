import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { ProjectStore } from "../service/project_store";

import { registerProjectResource } from "./resources/project_resource";
import { registerPackageResource } from "./resources/package_resource";
import { registerModelResource } from "./resources/model_resource";
import { registerSourceResource } from "./resources/source_resource";
import { registerQueryResource } from "./resources/query_resource";
import { registerViewResource } from "./resources/view_resource";
import { registerNotebookResource } from "./resources/notebook_resource";
import { registerExecuteQueryTool } from "./tools/execute_query_tool";
import { registerPromptCapability } from "./prompts/prompt_service.js";

export const testServerInfo = {
   name: "malloy-publisher-mcp-server",
   version: "0.0.1",
   displayName: "Malloy Publisher MCP Server",
   description: "Provides access to Malloy models and query execution via MCP.",
};

export function initializeMcpServer(projectStore: ProjectStore): McpServer {
   console.log("[MCP Init] Starting initializeMcpServer...");
   const startTime = Date.now();

   const mcpServer = new McpServer(testServerInfo);

   console.log("[MCP Init] Registering project resource...");
   registerProjectResource(mcpServer, projectStore);
   console.log("[MCP Init] Registering package resource...");
   registerPackageResource(mcpServer, projectStore);

   // Register more specific templates first
   console.log("[MCP Init] Registering notebook resource...");
   registerNotebookResource(mcpServer, projectStore);
   console.log("[MCP Init] Registering source resource...");
   registerSourceResource(mcpServer, projectStore);
   console.log("[MCP Init] Registering query resource...");
   registerQueryResource(mcpServer, projectStore);
   console.log("[MCP Init] Registering view resource...");
   registerViewResource(mcpServer, projectStore);

   // Register the general model template last among resource types
   console.log("[MCP Init] Registering model resource...");
   registerModelResource(mcpServer, projectStore);

   console.log("[MCP Init] Registering executeQuery tool...");
   registerExecuteQueryTool(mcpServer, projectStore);

   console.log("[MCP Init] Registering prompt capability...");
   registerPromptCapability(mcpServer, projectStore);

   const endTime = Date.now();
   console.log(
      `[MCP Init] Finished initializeMcpServer. Duration: ${endTime - startTime}ms`,
   );

   return mcpServer;
}
