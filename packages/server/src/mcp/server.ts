// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import type { McpServices } from "../app_services";

import { registerCompileTool } from "./tools/compile_tool";
import { registerDocsSearchTool } from "./tools/docs_search_tool";
import { registerExecuteQueryTool } from "./tools/execute_query_tool";
import { registerGetContextTool } from "./tools/get_context_tool";
import { registerListPackagesTool } from "./tools/list_packages_tool";
import {
   registerReloadPackageTool,
   RELOAD_FAILURE_IS_SAFE,
   RELOAD_NEEDS_WATCH_MODE,
} from "./tools/reload_package_tool";
import { registerGetAgentTool } from "./tools/get_agent_tool";
import { registerGetSkillTool } from "./tools/get_skill_tool";
import { registerSearchDatabaseSchemaTool } from "./tools/search_database_schema_tool";
import { registerGetStatusTool } from "./tools/get_status_tool";
import { BUNDLED_SKILLS } from "../service/skills/package_skills";
import { SERVER_VERSION } from "../version";

const testServerInfo = {
   name: "malloy-publisher-mcp-server",
   version: SERVER_VERSION,
   displayName: "Malloy Publisher MCP Server",
   description: "Provides access to Malloy models and query execution via MCP.",
};

// Orientation sent in the initialize response (the on-the-wire AGENTS.md).
// Workflow-focused, not a tool catalog (listTools covers that), so it does not
// drift as tools change.
// SECURITY: delivered pre-authorization, so it must stay STATIC. It may
// interpolate module-scope string LITERALS (e.g. RELOAD_FAILURE_IS_SAFE), never
// a constant read from process.env, nor environment, package, connection, or
// request data.
const MCP_INSTRUCTIONS = `Malloy Publisher serves one or more Malloy semantic-model packages, so you can discover what data exists and answer questions against it, grounded in the names the model actually defines.

Start with get_context. Called with no arguments it lists the environments; with environment, that environment's packages; with scopes naming a package and no search_targets, that package's sources; with scopes naming a source, its fields. A level with one choice is entered, so a server with one package answers a no-argument call with its sources. To answer a question, send search_targets -- one per concept your question needs, each {target_type, search_text} -- with scopes naming exactly one environment and package: retrieval is indexed per package, so a search names one. One call usually answers: the response is a sources array with the entities that matched nested inside each source, so there is no drill-down step. When a field you need is missing, a target is in unresolved_targets, or a warning says results were cut, call again with scopes[].source naming that source and one target per missing concept. Omit a target's search_text to enumerate that type instead of ranking it. list_packages also names every environment and package, and it is the only place a package that failed to load is visible rather than simply absent. Use the names it returns verbatim and do not guess. Then run a query with execute_query. To change a model: validate the edit with compile_model, save it, then call reload_package so the new sources and views become queryable by name without restarting the server. ${RELOAD_NEEDS_WATCH_MODE} ${RELOAD_FAILURE_IS_SAFE} After every model edit, call reload_package before querying: the server serves the model compiled at the last load, not your files, and a watch-mode save that fails to compile is otherwise silent. An empty get_context result does not mean the package is empty; check get_status for load errors and stale packages before concluding there is no data.

To build a model from a database rather than from an existing package, start with search_database_schema: it lists the connections, their schemas, and their tables, and ranks those tables against a plain-English description of the data you want. Each table it returns carries the source line to start from. It returns names and types only: no row value is returned.

Task-specific guidance is served two ways. As prompts you can fetch by name: malloy-getting-started to begin, malloy-modeling to build or change a model, malloy-analysis to explore and answer questions, and malloy-review to check correctness. And through get_skill, which serves the same built-in guides plus any a PACKAGE ships about itself: call it with a package scope before answering questions against a package you have not worked in, because a package can document its own conventions and can replace a built-in guide with its own version for that package, which the prompts do not reflect.

Results and any charts render in the Publisher web UI on the REST port (4000 by default).`;

/**
 * Builds one MCP server over the services the composition root built. The
 * tools call those shared instances; none constructs its own controller.
 */
export function initializeMcpServer(services: McpServices): McpServer {
   const {
      environmentStore,
      retrieval,
      queryUseCase,
      compileController,
      packageController,
      connectionController,
      querySlots,
   } = services;
   const mcpServer = new McpServer(testServerInfo, {
      instructions: MCP_INSTRUCTIONS,
   });

   registerExecuteQueryTool(mcpServer, environmentStore, queryUseCase);
   registerGetContextTool(mcpServer, environmentStore, retrieval);
   registerListPackagesTool(mcpServer, environmentStore);
   registerDocsSearchTool(mcpServer, environmentStore);
   registerCompileTool(mcpServer, compileController, querySlots);
   registerReloadPackageTool(mcpServer, packageController);
   registerSearchDatabaseSchemaTool(
      mcpServer,
      environmentStore,
      connectionController,
      retrieval,
   );
   registerGetStatusTool(mcpServer, environmentStore);
   registerGetSkillTool(mcpServer, environmentStore, BUNDLED_SKILLS);
   registerGetAgentTool(mcpServer, environmentStore);

   // Dual-channel: also expose each skill as an MCP prompt, so hosts that ingest
   // MCP but do not load skill files (e.g. Codex, ChatGPT, Cursor) can pull the
   // same guidance. Skill-aware hosts (Claude Code/Desktop) use the native skill
   // files. Note: MCP prompts are on-demand, so always-on "prevention" skills
   // become on-demand here; see docs/agent-skills/design-principles.md.
   for (const skill of BUNDLED_SKILLS) {
      mcpServer.registerPrompt(
         skill.name,
         { description: skill.description },
         () => ({
            messages: [
               {
                  role: "user" as const,
                  content: { type: "text" as const, text: skill.body },
               },
            ],
         }),
      );
   }

   return mcpServer;
}
