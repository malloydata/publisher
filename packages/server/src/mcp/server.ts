import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { EnvironmentStore } from "../service/environment_store";

import { formatDuration, logger } from "../logger";
import { registerDocsSearchTool } from "./tools/docs_search_tool";
import { registerExecuteQueryTool } from "./tools/execute_query_tool";
import { registerGetContextTool } from "./tools/get_context_tool";
import skillsBundle from "./skills/skills_bundle.json";

export const testServerInfo = {
   name: "malloy-publisher-mcp-server",
   version: "0.0.1",
   displayName: "Malloy Publisher MCP Server",
   description: "Provides access to Malloy models and query execution via MCP.",
};

// Build-time bundle of the agent skills (see skills/build_skills_bundle.ts),
// exposed as MCP prompts for the dual-channel delivery below.
const AGENT_SKILLS = (
   skillsBundle as {
      skills: { name: string; description: string; body: string }[];
   }
).skills;

export function initializeMcpServer(
   environmentStore: EnvironmentStore,
): McpServer {
   logger.info("[MCP Init] Starting initializeMcpServer...");
   const startTime = performance.now();

   const mcpServer = new McpServer(testServerInfo);

   registerExecuteQueryTool(mcpServer, environmentStore);
   registerGetContextTool(mcpServer, environmentStore);
   registerDocsSearchTool(mcpServer, environmentStore);

   // Dual-channel: also expose each skill as an MCP prompt, so hosts that ingest
   // MCP but do not load skill files (e.g. Codex, ChatGPT, Cursor) can pull the
   // same guidance. Skill-aware hosts (Claude Code/Desktop) use the native skill
   // files. Note: MCP prompts are on-demand, so always-on "prevention" skills
   // become on-demand here; see docs/agent-skills/design-principles.md.
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

   const endTime = performance.now();
   logger.info(`[MCP Init] Finished initializeMcpServer`, {
      duration: formatDuration(endTime - startTime),
   });

   return mcpServer;
}
