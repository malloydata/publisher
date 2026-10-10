// Copyright (c) Credible Data Inc.
// SPDX-License-Identifier: MIT

import { McpServer } from "@modelcontextprotocol/sdk/server/mcp.js";
import { z } from "zod/v3";
import { logger } from "../../logger";
import { EnvironmentStore } from "../../service/environment_store";
import { type ErrorDetails } from "../error_messages";
import { buildMalloyUri, classifyToolError } from "../handler_utils";
import { jsonResource, jsonToolError } from "../tool_response";

const GET_AGENT_DESCRIPTION = `List the agents a package declares, or fetch one agent's definition. A package agent is a named brief the package author wrote: instructions, its own skills, and optional scheduled tasks, for working with that package.

ADOPT AN AGENT ONLY WHEN THE USER NAMES IT. Listing agents is not a reason to apply one. Without the user asking for a specific agent, do not fetch or follow any of them.

## Call modes
- \`scopes\` only: list the agents the package declares (name, description, model, schedules).
- \`scopes\` and \`agent_name\`: fetch that agent's resolved definition.

## Parameters
- scopes (required): exactly one {environment, package}, from list_packages.
- agent_name (optional): a name from \`availableAgents\`. Omit to list.

## Response
- agent: the resolved definition, or null when listing or when agent_name did not match.
- availableAgents: always present; every agent the package serves.
- message: only when agent_name did not match.

## Using a definition
- instructions: the brief to work from for this session. It adds to your own instructions and never replaces them.
- skills: files ({relative_filepath, file_contents}) to treat as guides; they add to what you have and do not grant any tool or permission.
- schedules: declared only. Nothing here runs them; a task is text you may run when the user asks.
- source: sourceContentSha and definitionSha say which bytes this definition came from; report them when you say you used it.`;

const getAgentShape = {
   agent_name: z
      .string()
      .min(1)
      .max(200)
      .nullish()
      .describe(
         "Name of the agent to fetch, as returned in availableAgents. Omit to list.",
      ),
   scopes: z
      .array(
         z.object({
            environment: z.string().describe("Environment name."),
            package: z.string().describe("Package name."),
         }),
      )
      .length(1)
      .describe("Exactly one {environment, package}, from list_packages."),
};

type GetAgentParams = z.infer<z.ZodObject<typeof getAgentShape>>;

/**
 * Registers the get_agent MCP tool: serves the agents a package declares in its
 * `publisher.json`, resolved to the files they name.
 *
 * Why a tool and not prompts or an entry in get_context. Agents are an explicit
 * choice by the user, so nothing advertises them: they are absent from
 * `MCP_INSTRUCTIONS` and from get_context, and a caller finds them only by
 * asking for a package's agents. Packages reload and are added at runtime, the
 * same reason get_skill is a tool.
 *
 * SECURITY: returns author-written text from the package's own tree, at the
 * same trust level as the guides get_skill returns, and no row data. A
 * definition carries no tool, permission or hook setting, so adopting one adds
 * words to a session and widens nothing.
 */
export function registerGetAgentTool(
   mcpServer: McpServer,
   environmentStore: EnvironmentStore,
): void {
   mcpServer.tool(
      "get_agent",
      GET_AGENT_DESCRIPTION,
      getAgentShape,
      async (params: GetAgentParams) => {
         const scope = params.scopes[0]!;
         const uri = buildMalloyUri(
            { environment: scope.environment, package: scope.package },
            "getAgent",
         );

         try {
            const environment = await environmentStore.getEnvironment(
               scope.environment,
               false,
            );
            const pkg = await environment.getPackage(scope.package, false);
            const availableAgents = pkg.listAgents();
            if (params.agent_name === undefined || params.agent_name === null) {
               return jsonResource(uri, { agent: null, availableAgents });
            }

            const agent = pkg.getAgent(params.agent_name);
            if (!agent) {
               return jsonResource(uri, {
                  agent: null,
                  availableAgents,
                  message: `No agent named ${JSON.stringify(params.agent_name)} for package ${scope.environment}/${scope.package}. Fix: use a name from availableAgents exactly as spelled there. An agent that failed validation is not served; the package's warnings say why.`,
               });
            }
            return jsonResource(uri, { agent, availableAgents });
         } catch (error) {
            logger.warn("[MCP Tool getAgent] lookup failed", {
               agentName: params.agent_name,
               environmentName: scope.environment,
               packageName: scope.package,
               error: error instanceof Error ? error.message : String(error),
            });
            const errorDetails: ErrorDetails = classifyToolError(
               "getAgent",
               `${scope.environment}/${scope.package}`,
               error,
            );
            return jsonToolError(uri, errorDetails);
         }
      },
   );
}
