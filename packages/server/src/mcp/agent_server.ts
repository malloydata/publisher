import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import cors from "cors";
import express from "express";
import * as http from "http";
import { registerHealthEndpoints } from "../health";
import { logger } from "../logger";
import { EnvironmentStore } from "../service/environment_store";
import { registerDocsSearchTool } from "./tools/docs_search_tool";
import { registerGetContextTool } from "./tools/get_context_tool";
import skillsBundle from "./skills/skills_bundle.json";

const AGENT_MCP_ENDPOINT = "/mcp";

// Build-time bundle of the agent skills (see skills/build_skills_bundle.ts), exposed
// as MCP prompts for the dual-channel delivery below.
const AGENT_SKILLS = (
   skillsBundle as {
      skills: { name: string; description: string; body: string }[];
   }
).skills;

export const agentServerInfo = {
   name: "malloy-publisher-agent-mcp-server",
   version: "0.0.1",
   displayName: "Malloy Publisher Agent MCP Server",
   description:
      "Agent retrieval tools (semantic model context and docs search) for Malloy, served on a separate MCP endpoint from the core Publisher MCP tools.",
};

/**
 * Builds an MCP server exposing ONLY the agent retrieval tools (malloy_getContext,
 * malloy_searchDocs). Kept deliberately separate from the core initializeMcpServer so
 * these tools and their tests stay isolated from the existing MCP tool surface.
 */
export function initializeAgentMcpServer(
   environmentStore: EnvironmentStore,
): McpServer {
   const mcpServer = new McpServer(agentServerInfo);
   registerGetContextTool(mcpServer, environmentStore);
   registerDocsSearchTool(mcpServer, environmentStore);

   // Dual-channel: also expose each skill as an MCP prompt, so hosts that ingest MCP
   // but do not load skill files (e.g. Codex, ChatGPT, Cursor) can pull the same
   // guidance. Skill-aware hosts (Claude Code/Desktop) use the native skill files.
   // Note: MCP prompts are on-demand, so always-on "prevention" skills become
   // on-demand here; see docs/agent-skills/design-principles.md.
   for (const skill of AGENT_SKILLS) {
      mcpServer.prompt(skill.name, skill.description, () => ({
         messages: [
            {
               role: "user" as const,
               content: { type: "text" as const, text: skill.body },
            },
         ],
      }));
   }

   return mcpServer;
}

/**
 * Starts the agent MCP server on its own HTTP listener (a third Publisher listener),
 * using the same stateless StreamableHTTP transport pattern as the core MCP server.
 * Returns the http.Server so the caller can manage shutdown.
 */
export function startAgentMcpServer(
   environmentStore: EnvironmentStore,
   host: string,
   port: number,
): http.Server {
   const agentMcpApp = express();
   registerHealthEndpoints(agentMcpApp);
   agentMcpApp.use(AGENT_MCP_ENDPOINT, express.json());
   agentMcpApp.use(AGENT_MCP_ENDPOINT, cors());

   agentMcpApp.all(AGENT_MCP_ENDPOINT, async (req, res) => {
      if (req.method !== "POST") {
         res.setHeader("Allow", "POST");
         res.status(405).json({
            jsonrpc: "2.0",
            error: {
               code: -32601,
               message: "Method Not Allowed in Stateless Mode",
            },
            id: null,
         });
         return;
      }

      try {
         const transport = new StreamableHTTPServerTransport({
            sessionIdGenerator: undefined,
         });
         transport.onerror = (err: Error) => {
            logger.error("[Agent MCP Transport Error]", {
               error: err instanceof Error ? err.message : String(err),
            });
         };
         const requestMcpServer = initializeAgentMcpServer(environmentStore);
         await requestMcpServer.connect(transport);
         res.on("close", () => {
            transport.close().catch((err) => {
               logger.error("[Agent MCP Transport Error] close failed", {
                  error: err instanceof Error ? err.message : String(err),
               });
            });
         });
         await transport.handleRequest(req, res, req.body);
      } catch (err) {
         logger.error("[Agent MCP] request handler error", {
            error: err instanceof Error ? err.message : String(err),
         });
         if (!res.headersSent) {
            res.status(500).json({
               jsonrpc: "2.0",
               error: { code: -32603, message: "Internal server error" },
               id: null,
            });
         }
      }
   });

   return agentMcpApp.listen(port, host, () => {
      logger.info(
         `Agent MCP server listening at http://${host}:${port}${AGENT_MCP_ENDPOINT}`,
      );
   });
}
