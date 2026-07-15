import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { EnvironmentStore } from "../service/environment_store";

import { formatDuration, logger } from "../logger";
import { registerCompileTool } from "./tools/compile_tool";
import { registerDocsSearchTool } from "./tools/docs_search_tool";
import { registerExecuteQueryTool } from "./tools/execute_query_tool";
import { registerGetContextTool } from "./tools/get_context_tool";
import {
   registerReloadPackageTool,
   RELOAD_FAILURE_IS_SAFE,
} from "./tools/reload_package_tool";
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

// Orientation delivered to any connecting MCP client via the initialize
// response (the on-the-wire analog of AGENTS.md), so an agent that reached the
// server over npx without cloning the repo still knows how to work. Kept
// workflow-focused rather than a tool catalog, which the client already gets
// from listTools, so it does not drift as the tool set changes.
// SECURITY: this is delivered pre-authorization to any connecting client, so it
// must stay STATIC. It may interpolate module-scope string LITERALS (see
// RELOAD_FAILURE_IS_SAFE, shared with the tool description so the two cannot
// drift). Not "constants": `const x = process.env.FOO` is a module-scope
// constant and would put deployment config into an unauthenticated string. It
// must never interpolate environment, package, connection, or request data.
const MCP_INSTRUCTIONS = `Malloy Publisher serves one or more Malloy semantic-model packages, so you can discover what data exists and answer questions against it, grounded in the names the model actually defines.

Start with malloy_getContext. Call it with no arguments to list the environments (each with its packages), with an environment to list its packages, with a package to list its sources, and with a package plus a plain-English question to get the sources, views, and fields most relevant to it. Use the names it returns verbatim and do not guess. Then run a query with malloy_executeQuery. To change a model: validate the edit with malloy_compile, save it, then call malloy_reloadPackage so the new sources and views become queryable by name without restarting the server. ${RELOAD_FAILURE_IS_SAFE}

Task-specific guidance is served as prompts you can fetch by name: getting-started to begin, malloy-modeling to build or change a model, malloy-analysis to explore and answer questions, and malloy-review to check correctness.

Results and any charts render in the Publisher web UI on the REST port (4000 by default).`;

export function initializeMcpServer(
   environmentStore: EnvironmentStore,
): McpServer {
   logger.info("[MCP Init] Starting initializeMcpServer...");
   const startTime = performance.now();

   const mcpServer = new McpServer(testServerInfo, {
      instructions: MCP_INSTRUCTIONS,
   });

   registerExecuteQueryTool(mcpServer, environmentStore);
   registerGetContextTool(mcpServer, environmentStore);
   registerDocsSearchTool(mcpServer, environmentStore);
   registerCompileTool(mcpServer, environmentStore);
   registerReloadPackageTool(mcpServer, environmentStore);

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
